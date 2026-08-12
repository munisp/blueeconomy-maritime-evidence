package evidence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("evidence package not found")

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Create(ctx context.Context, request CreateRequest) (Package, bool, error) {
	transaction, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Package{}, false, fmt.Errorf("begin evidence creation transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var existing Package
	err = scanPackage(transaction.QueryRow(ctx, `
		SELECT evidence_package_id, idempotency_key, external_reference, evidence_type,
		       content_sha256, content_location, received_at, classification, correlation_id,
		       created_at, validation_status
		FROM evidence_packages
		WHERE idempotency_key = $1
	`, request.IdempotencyKey), &existing)
	if err == nil {
		if err := transaction.Commit(ctx); err != nil {
			return Package{}, false, fmt.Errorf("commit idempotent lookup: %w", err)
		}
		return existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Package{}, false, fmt.Errorf("look up idempotency key: %w", err)
	}

	var created Package
	err = scanPackage(transaction.QueryRow(ctx, `
		INSERT INTO evidence_packages (
			evidence_package_id, idempotency_key, external_reference, evidence_type,
			content_sha256, content_location, received_at, validation_status, classification, correlation_id
		) VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING evidence_package_id, idempotency_key, external_reference, evidence_type,
		          content_sha256, content_location, received_at, classification, correlation_id,
		          created_at, validation_status
	`, request.IdempotencyKey, request.ExternalReference, request.EvidenceType, request.ContentSHA256,
		request.ContentLocation, request.ReceivedAt.UTC(), StatusReceived, request.Classification, request.CorrelationID), &created)
	if err != nil {
		return Package{}, false, fmt.Errorf("insert evidence package: %w", err)
	}

	_, err = transaction.Exec(ctx, `
		INSERT INTO evidence_validation_history (
			validation_history_id, evidence_package_id, prior_validation_status, validation_status,
			reason_code, actor_subject_reference, occurred_at, correlation_id
		) VALUES (gen_random_uuid(), $1, NULL, $2, $3, $4, $5, $6)
	`, created.EvidencePackageID, StatusReceived, "evidence_package_received", "system:evidence-service", created.ReceivedAt, created.CorrelationID)
	if err != nil {
		return Package{}, false, fmt.Errorf("record initial validation history: %w", err)
	}
	if err := transaction.Commit(ctx); err != nil {
		return Package{}, false, fmt.Errorf("commit evidence creation: %w", err)
	}
	return created, true, nil
}

func (s *Store) Get(ctx context.Context, packageID string) (Package, error) {
	var record Package
	err := scanPackage(s.pool.QueryRow(ctx, `
		SELECT evidence_package_id, idempotency_key, external_reference, evidence_type,
		       content_sha256, content_location, received_at, classification, correlation_id,
		       created_at, validation_status
		FROM evidence_packages
		WHERE evidence_package_id = $1
	`, packageID), &record)
	if errors.Is(err, pgx.ErrNoRows) {
		return Package{}, ErrNotFound
	}
	if err != nil {
		return Package{}, fmt.Errorf("get evidence package: %w", err)
	}
	return record, nil
}

func (s *Store) RecordValidation(ctx context.Context, packageID string, request ValidationRequest) error {
	transaction, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin validation transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var existingPackageID string
	err = transaction.QueryRow(ctx, `
		SELECT evidence_package_id
		FROM evidence_packages
		WHERE evidence_package_id = $1
		FOR KEY SHARE
	`, packageID).Scan(&existingPackageID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("load evidence package for validation: %w", err)
	}

	var terminalDecisionExists bool
	err = transaction.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM evidence_validation_history
			WHERE evidence_package_id = $1
			  AND validation_status IN ('validated', 'rejected')
		)
	`, packageID).Scan(&terminalDecisionExists)
	if err != nil {
		return fmt.Errorf("check terminal validation history: %w", err)
	}
	if terminalDecisionExists {
		return errors.New("evidence package already has a terminal validation decision")
	}

	_, err = transaction.Exec(ctx, `
		INSERT INTO evidence_validation_history (
			validation_history_id, evidence_package_id, prior_validation_status, validation_status,
			reason_code, actor_subject_reference, occurred_at, correlation_id
		) VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7)
	`, packageID, StatusReceived, request.ValidationStatus, request.ReasonCode,
		request.ActorSubjectReference, request.OccurredAt.UTC(), request.CorrelationID)
	if err != nil {
		return fmt.Errorf("record evidence validation: %w", err)
	}

	// The package row is immutable by design. Its original received status is retained;
	// current state is derived from append-only history for auditability.
	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("commit validation history: %w", err)
	}
	return nil
}

func scanPackage(row pgx.Row, destination *Package) error {
	if err := row.Scan(
		&destination.EvidencePackageID,
		&destination.IdempotencyKey,
		&destination.ExternalReference,
		&destination.EvidenceType,
		&destination.ContentSHA256,
		&destination.ContentLocation,
		&destination.ReceivedAt,
		&destination.Classification,
		&destination.CorrelationID,
		&destination.CreatedAt,
		&destination.ValidationStatus,
	); err != nil {
		return err
	}
	destination.ReceivedAt = destination.ReceivedAt.UTC()
	destination.CreatedAt = destination.CreatedAt.UTC()
	return nil
}
