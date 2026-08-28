package evidence

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound            = errors.New("evidence package not found")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with retained evidence metadata")
)

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

	var created Package
	err = scanPackage(transaction.QueryRow(ctx, `
		INSERT INTO evidence_packages (
			evidence_package_id, idempotency_key, external_reference, evidence_type,
			content_sha256, content_location, received_at, validation_status, classification, correlation_id
		) VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING evidence_package_id, idempotency_key, external_reference, evidence_type,
		          content_sha256, content_location, received_at, classification, correlation_id,
		          created_at, validation_status
	`, request.IdempotencyKey, request.ExternalReference, request.EvidenceType, request.ContentSHA256,
		request.ContentLocation, request.ReceivedAt.UTC(), StatusReceived, request.Classification, request.CorrelationID), &created)
	if err == nil {
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
	if !errors.Is(err, pgx.ErrNoRows) {
		return Package{}, false, fmt.Errorf("insert evidence package: %w", err)
	}

	var existing Package
	err = scanPackage(transaction.QueryRow(ctx, `
		SELECT evidence_package_id, idempotency_key, external_reference, evidence_type,
		       content_sha256, content_location, received_at, classification, correlation_id,
		       created_at, validation_status
		FROM evidence_packages
		WHERE idempotency_key = $1
		FOR KEY SHARE
	`, request.IdempotencyKey), &existing)
	if err != nil {
		return Package{}, false, fmt.Errorf("load retained idempotency key: %w", err)
	}
	if !createRequestMatchesPackage(request, existing) {
		return Package{}, false, ErrIdempotencyConflict
	}
	if err := transaction.Commit(ctx); err != nil {
		return Package{}, false, fmt.Errorf("commit idempotent lookup: %w", err)
	}
	return existing, false, nil
}

func createRequestMatchesPackage(request CreateRequest, existing Package) bool {
	return request.IdempotencyKey == existing.IdempotencyKey &&
		request.ExternalReference == existing.ExternalReference &&
		request.EvidenceType == existing.EvidenceType &&
		request.ContentSHA256 == existing.ContentSHA256 &&
		request.ContentLocation == existing.ContentLocation &&
		request.ReceivedAt.UTC().Equal(existing.ReceivedAt.UTC()) &&
		request.Classification == existing.Classification &&
		request.CorrelationID == existing.CorrelationID
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

// rowScanner is satisfied by both pgx.Row and pgx.Rows, so single-row and
// multi-row queries share one package scan. Extra destinations are appended
// after the package columns for queries that join supplementary state.
type rowScanner interface {
	Scan(destinations ...any) error
}

func scanPackage(row rowScanner, destination *Package, extra ...any) error {
	destinations := append([]any{
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
	}, extra...)
	if err := row.Scan(destinations...); err != nil {
		return err
	}
	destination.ReceivedAt = destination.ReceivedAt.UTC()
	destination.CreatedAt = destination.CreatedAt.UTC()
	return nil
}
