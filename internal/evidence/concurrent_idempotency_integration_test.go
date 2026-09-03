package evidence

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreConcurrentIdempotentCreatePostgreSQLIntegration(t *testing.T) {
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
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE OR REPLACE FUNCTION evidence_test_delay_insert() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_sleep(0.25);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER evidence_test_delay_insert
		BEFORE INSERT ON evidence_packages
		FOR EACH ROW EXECUTE FUNCTION evidence_test_delay_insert()`); err != nil {
		t.Fatalf("install deterministic concurrent-insert delay: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), "DROP TRIGGER IF EXISTS evidence_test_delay_insert ON evidence_packages; DROP FUNCTION IF EXISTS evidence_test_delay_insert()")
	}()

	store := NewStore(pool)
	request := CreateRequest{
		IdempotencyKey:    "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		ExternalReference: "concurrent-idempotency-reference",
		EvidenceType:      "integration.concurrent-idempotency",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "https://objects.blueeconomy.local/evidence/concurrent-idempotency-object",
		ReceivedAt:        time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		Classification:    "internal",
		CorrelationID:     "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
	}

	const callerCount = 16
	type result struct {
		packageID string
		created   bool
		err       error
	}
	results := make(chan result, callerCount)
	start := make(chan struct{})
	var waitGroup sync.WaitGroup
	for index := 0; index < callerCount; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			created, wasCreated, createErr := store.Create(context.Background(), request)
			results <- result{packageID: created.EvidencePackageID, created: wasCreated, err: createErr}
		}()
	}
	close(start)
	waitGroup.Wait()
	close(results)

	createdCount := 0
	retainedID := ""
	for outcome := range results {
		if outcome.err != nil {
			t.Errorf("concurrent idempotent create failed: %v", outcome.err)
			continue
		}
		if outcome.created {
			createdCount++
		}
		if retainedID == "" {
			retainedID = outcome.packageID
		} else if outcome.packageID != retainedID {
			t.Errorf("concurrent create returned package %q; expected %q", outcome.packageID, retainedID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("expected exactly one creator, got %d", createdCount)
	}

	var packageCount, historyCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM evidence_packages").Scan(&packageCount); err != nil {
		t.Fatalf("count evidence packages: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM evidence_validation_history").Scan(&historyCount); err != nil {
		t.Fatalf("count validation history: %v", err)
	}
	if packageCount != 1 || historyCount != 1 {
		t.Fatalf("expected one package and one initial history row, got packages=%d history=%d", packageCount, historyCount)
	}
}
