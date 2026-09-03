package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/events"
)

var (
	ErrNotFound            = errors.New("evidence package not found")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with retained evidence metadata")
)

type Store struct {
	pool *pgxpool.Pool

	// Transactional outbox wiring. When signer is nil the store persists
	// without emitting events (migration tooling/tests); the production
	// services always wire it and fail closed without a signing key.
	signer        *events.Signer
	principalID   string
	principalRole string
}

// NewStore builds the store over the pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// WithEvents wires the transactional outbox: every committed creation or
// validation also appends a signed envelopeVersion 1.0 event to
// evidence_outbox in the same transaction. It fails closed when the signer
// or producer principal is missing.
func (s *Store) WithEvents(signer *events.Signer, principalID, principalRole string) (*Store, error) {
	if signer == nil {
		return nil, errors.New("an envelope signer is required for the evidence outbox")
	}
	if principalID == "" || principalRole == "" {
		return nil, errors.New("the evidence producer principal id and role are required")
	}
	clone := *s
	clone.signer = signer
	clone.principalID = principalID
	clone.principalRole = principalRole
	return &clone, nil
}

// ConfigurePool makes every pooled connection assert the approved evidence
// service context so the evidence_outbox row-level security policy admits
// the session. Without it the RLS default-deny policy fails closed.
func ConfigurePool(config *pgxpool.Config) {
	config.AfterConnect = func(ctx context.Context, connection *pgx.Conn) error {
		if _, err := connection.Exec(ctx, "SET app.evidence_service = 'on'"); err != nil {
			return fmt.Errorf("assert evidence service RLS context: %w", err)
		}
		return nil
	}
}

// enqueueOutbox appends a signed envelope to evidence_outbox inside tx.
func (s *Store) enqueueOutbox(ctx context.Context, tx pgx.Tx, eventType, topic, correlationID, subjectID string, payload any, occurredAt time.Time) error {
	if s.signer == nil {
		return nil
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode outbox payload: %w", err)
	}
	envelope, err := events.Message(eventType, topic, correlationID, subjectID, payloadJSON, nil, events.Provenance{
		PrincipalID:   s.principalID,
		PrincipalRole: s.principalRole,
	}, occurredAt, s.signer)
	if err != nil {
		return fmt.Errorf("build outbox envelope: %w", err)
	}
	envelopeJSON, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode outbox envelope: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO evidence_outbox (event_id, evidence_package_id, topic, event_type, payload, created_at)
		VALUES (gen_random_uuid(), $1, $2, $3, $4, now())
	`, subjectID, topic, eventType, envelopeJSON); err != nil {
		return fmt.Errorf("enqueue evidence outbox event: %w", err)
	}
	return nil
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
		if err := s.enqueueOutbox(ctx, transaction, "evidence.package.received", events.TopicPackage,
			created.CorrelationID, created.EvidencePackageID, map[string]any{
				"evidence_package_id": created.EvidencePackageID,
				"external_reference":  created.ExternalReference,
				"evidence_type":       created.EvidenceType,
				"content_sha256":      created.ContentSHA256,
				"content_location":    created.ContentLocation,
				"classification":      created.Classification,
				"received_at":         created.ReceivedAt,
			}, created.ReceivedAt); err != nil {
			return Package{}, false, err
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
		return ErrTerminalValidation
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

	if err := s.enqueueOutbox(ctx, transaction, "evidence.validation.recorded", events.TopicValidation,
		request.CorrelationID, packageID, map[string]any{
			"evidence_package_id":     packageID,
			"prior_validation_status": StatusReceived,
			"validation_status":       request.ValidationStatus,
			"reason_code":             request.ReasonCode,
			"actor_subject_reference": request.ActorSubjectReference,
			"occurred_at":             request.OccurredAt.UTC(),
		}, request.OccurredAt.UTC()); err != nil {
		return err
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

// ErrTerminalValidation marks an illegal validation transition: the package
// already carries a terminal validated/rejected decision.
var ErrTerminalValidation = errors.New("evidence package already has a terminal validation decision")

// List returns packages ordered by received_at descending with a caller
// bound of limit/offset. Callers cap limit before invocation.
func (s *Store) List(ctx context.Context, limit, offset int) ([]Package, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT evidence_package_id, idempotency_key, external_reference, evidence_type,
		       content_sha256, content_location, received_at, classification, correlation_id,
		       created_at, validation_status
		FROM evidence_packages
		ORDER BY received_at DESC, evidence_package_id
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list evidence packages: %w", err)
	}
	defer rows.Close()
	packages := make([]Package, 0, limit)
	for rows.Next() {
		var record Package
		if err := rows.Scan(
			&record.EvidencePackageID,
			&record.IdempotencyKey,
			&record.ExternalReference,
			&record.EvidenceType,
			&record.ContentSHA256,
			&record.ContentLocation,
			&record.ReceivedAt,
			&record.Classification,
			&record.CorrelationID,
			&record.CreatedAt,
			&record.ValidationStatus,
		); err != nil {
			return nil, fmt.Errorf("scan evidence package: %w", err)
		}
		record.ReceivedAt = record.ReceivedAt.UTC()
		record.CreatedAt = record.CreatedAt.UTC()
		packages = append(packages, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list evidence packages: %w", err)
	}
	return packages, nil
}
