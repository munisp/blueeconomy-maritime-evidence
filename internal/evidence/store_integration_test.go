package evidence

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStorePostgreSQLIntegration(t *testing.T) {
	dsn := os.Getenv("EVIDENCE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("EVIDENCE_TEST_POSTGRES_DSN is required for the real PostgreSQL integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}

	store := NewStore(pool)
	request := CreateRequest{
		IdempotencyKey:    "11111111-1111-4111-8111-111111111111",
		ExternalReference: "approved-local-integration-reference",
		EvidenceType:      "integration.conformance",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "https://objects.blueeconomy.local/evidence/integration-object",
		ReceivedAt:        time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		Classification:    "internal",
		CorrelationID:     "22222222-2222-4222-8222-222222222222",
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate integration request: %v", err)
	}

	created, wasCreated, err := store.Create(ctx, request)
	if err != nil {
		t.Fatalf("create evidence package: %v", err)
	}
	if !wasCreated {
		t.Fatal("first create did not report a new package")
	}
	repeated, wasCreated, err := store.Create(ctx, request)
	if err != nil {
		t.Fatalf("repeat idempotent create: %v", err)
	}
	if wasCreated || repeated.EvidencePackageID != created.EvidencePackageID {
		t.Fatal("idempotent create did not return the original package")
	}

	conflicting := request
	conflicting.ContentSHA256 = "0000000000000000000000000000000000000000000000000000000000000000"
	if _, _, err := store.Create(ctx, conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting idempotency key returned %v instead of ErrIdempotencyConflict", err)
	}

	if _, err := pool.Exec(ctx, "UPDATE evidence_packages SET external_reference = 'mutated' WHERE evidence_package_id = $1", created.EvidencePackageID); err == nil {
		t.Fatal("immutable evidence package accepted an update")
	}

	validationRequests := []ValidationRequest{
		{
			ValidationStatus:      StatusValidated,
			ReasonCode:            "local_integration_validated",
			ActorSubjectReference: "service:local-integration-validator-a",
			OccurredAt:            time.Date(2026, 8, 12, 12, 1, 0, 0, time.UTC),
			CorrelationID:         "33333333-3333-4333-8333-333333333333",
		},
		{
			ValidationStatus:      StatusRejected,
			ReasonCode:            "local_integration_rejected",
			ActorSubjectReference: "service:local-integration-validator-b",
			OccurredAt:            time.Date(2026, 8, 12, 12, 1, 1, 0, time.UTC),
			CorrelationID:         "44444444-4444-4444-8444-444444444444",
		},
	}
	for _, validation := range validationRequests {
		if err := validation.Validate(); err != nil {
			t.Fatalf("validate terminal request: %v", err)
		}
	}

	results := make(chan error, len(validationRequests))
	var waitGroup sync.WaitGroup
	for _, validation := range validationRequests {
		validation := validation
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			results <- store.RecordValidation(context.Background(), created.EvidencePackageID, validation)
		}()
	}
	waitGroup.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one terminal validation success, got %d", successes)
	}

	var initialHistory, terminalHistory int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE validation_status = 'received'),
		       count(*) FILTER (WHERE validation_status IN ('validated', 'rejected'))
		FROM evidence_validation_history WHERE evidence_package_id = $1
	`, created.EvidencePackageID).Scan(&initialHistory, &terminalHistory); err != nil {
		t.Fatalf("inspect validation history: %v", err)
	}
	if initialHistory != 1 || terminalHistory != 1 {
		t.Fatalf("unexpected validation history counts: received=%d terminal=%d", initialHistory, terminalHistory)
	}

	loaded, err := store.Get(ctx, created.EvidencePackageID)
	if err != nil {
		t.Fatalf("load evidence package: %v", err)
	}
	if loaded.ValidationStatus != StatusReceived {
		t.Fatalf("immutable package status changed to %q", loaded.ValidationStatus)
	}
}
