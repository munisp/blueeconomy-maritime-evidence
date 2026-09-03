package evidence

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/events"
)

func integrationPool(t *testing.T, ctx context.Context, configure bool) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("EVIDENCE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("EVIDENCE_TEST_POSTGRES_DSN is required for the real PostgreSQL integration test")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	if configure {
		ConfigurePool(config)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func integrationSigner(t *testing.T) *events.Signer {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	signer, err := events.NewSigner(privateKey, "13")
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	return signer
}

// TestStoreOutboxIntegration asserts that package creation and validation
// atomically append signed, verifiable envelopes to evidence_outbox.
func TestStoreOutboxIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationPool(t, ctx, true)
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}

	signer := integrationSigner(t)
	store, err := NewStore(pool).WithEvents(signer, "service:evidence", "evidence-producer")
	if err != nil {
		t.Fatalf("wire outbox events: %v", err)
	}

	request := CreateRequest{
		IdempotencyKey:    "99999999-9999-4999-8999-999999999999",
		ExternalReference: "outbox-integration-reference",
		EvidenceType:      "integration.outbox",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "s3://evidence-bucket/evidence/outbox-integration",
		ReceivedAt:        time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		Classification:    "internal",
		CorrelationID:     "88888888-8888-4888-8888-888888888888",
	}
	created, wasCreated, err := store.Create(ctx, request)
	if err != nil {
		t.Fatalf("create with outbox: %v", err)
	}
	if !wasCreated {
		t.Fatal("expected a new package")
	}

	var topic, eventType string
	var payload []byte
	err = pool.QueryRow(ctx, `
		SELECT topic, event_type, payload FROM evidence_outbox
		WHERE evidence_package_id = $1 AND published_at IS NULL
	`, created.EvidencePackageID).Scan(&topic, &eventType, &payload)
	if err != nil {
		t.Fatalf("load outbox event: %v", err)
	}
	if topic != events.TopicPackage || eventType != "evidence.package.received" {
		t.Fatalf("unexpected outbox event %s %s", topic, eventType)
	}
	var envelope events.Envelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode outbox envelope: %v", err)
	}
	if err := events.Verify(envelope, signer.PublicKey()); err != nil {
		t.Fatalf("outbox envelope must verify against the producer key: %v", err)
	}
	if envelope.CorrelationID != request.CorrelationID || envelope.Producer != "maritime-evidence" {
		t.Fatalf("unexpected envelope provenance: %+v", envelope)
	}

	// A validation decision enqueues evidence.validation.v1 in the same tx.
	validation := ValidationRequest{
		ValidationStatus:      StatusValidated,
		ReasonCode:            "integrity_confirmed",
		ActorSubjectReference: "service:validator",
		OccurredAt:            time.Date(2026, 8, 12, 13, 0, 0, 0, time.UTC),
		CorrelationID:         "77777777-7777-4777-8777-777777777777",
	}
	if err := store.RecordValidation(ctx, created.EvidencePackageID, validation); err != nil {
		t.Fatalf("record validation with outbox: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM evidence_outbox
		WHERE evidence_package_id = $1 AND topic = 'evidence.validation.v1' AND published_at IS NULL
	`, created.EvidencePackageID).Scan(&count); err != nil {
		t.Fatalf("count validation events: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one validation outbox event, got %d", count)
	}

	// Idempotent replay must not enqueue a second package event.
	if _, wasCreated, err := store.Create(ctx, request); err != nil || wasCreated {
		t.Fatalf("idempotent replay: created=%v err=%v", wasCreated, err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM evidence_outbox
		WHERE evidence_package_id = $1 AND topic = 'evidence.package.v1'
	`, created.EvidencePackageID).Scan(&count); err != nil {
		t.Fatalf("count package events: %v", err)
	}
	if count != 1 {
		t.Fatalf("idempotent replay must not duplicate outbox events, got %d", count)
	}
}

// TestOutboxRLSDefaultDeny asserts the fail-closed RLS posture: a session
// that never asserts the approved service context cannot read or append
// outbox rows even when connecting as a non-superuser role.
func TestOutboxRLSDefaultDeny(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationPool(t, ctx, false)
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'evidence_rls_probe') THEN
				CREATE ROLE evidence_rls_probe LOGIN;
			END IF;
		END $$;
		GRANT USAGE ON SCHEMA public TO evidence_rls_probe;
		GRANT SELECT, INSERT, UPDATE ON evidence_outbox TO evidence_rls_probe;
		GRANT SELECT, INSERT ON evidence_packages TO evidence_rls_probe;
		GRANT SELECT, INSERT ON evidence_validation_history TO evidence_rls_probe;
	`); err != nil {
		t.Fatalf("provision probe role: %v", err)
	}

	probeDSN := strings.Replace(os.Getenv("EVIDENCE_TEST_POSTGRES_DSN"), "postgres://postgres@", "postgres://evidence_rls_probe@", 1)
	if probeDSN == os.Getenv("EVIDENCE_TEST_POSTGRES_DSN") {
		t.Fatal("EVIDENCE_TEST_POSTGRES_DSN must be a postgres:// URL for the RLS probe rewrite")
	}
	probeConfig, err := pgxpool.ParseConfig(probeDSN)
	if err != nil {
		t.Fatalf("parse probe DSN: %v", err)
	}
	probe, err := pgxpool.NewWithConfig(ctx, probeConfig)
	if err != nil {
		t.Fatalf("open probe pool: %v", err)
	}
	defer probe.Close()

	// Seed one package so the unauthorized insert attempt has a source row.
	if _, _, err := NewStore(pool).Create(ctx, CreateRequest{
		IdempotencyKey:    "12121212-1212-4212-8212-121212121212",
		ExternalReference: "rls-probe-reference",
		EvidenceType:      "integration.rls",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "s3://evidence-bucket/evidence/rls-probe",
		ReceivedAt:        time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		Classification:    "public",
		CorrelationID:     "55555555-5555-4555-8555-555555555555",
	}); err != nil {
		t.Fatalf("seed package for RLS probe: %v", err)
	}

	var count int
	if err := probe.QueryRow(ctx, "SELECT count(*) FROM evidence_outbox").Scan(&count); err != nil {
		t.Fatalf("probe select: %v", err)
	}
	if count != 0 {
		t.Fatalf("RLS default-deny leaked %d outbox rows to an unapproved session", count)
	}
	if _, err := probe.Exec(ctx, `
		INSERT INTO evidence_outbox (event_id, evidence_package_id, topic, event_type, payload)
		SELECT gen_random_uuid(), evidence_package_id, 'evidence.package.v1', 'evidence.package.received', '{}'::jsonb
		FROM evidence_packages LIMIT 1
	`); err == nil {
		t.Fatal("RLS default-deny allowed an unapproved session to append an outbox row")
	}
}

// TestStoreWithoutEventsLeavesOutboxEmpty documents that the unwired store
// (migration tooling) persists without emitting events.
func TestStoreWithoutEventsLeavesOutboxEmpty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationPool(t, ctx, true)
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}
	store := NewStore(pool)
	request := CreateRequest{
		IdempotencyKey:    "66666666-6666-4666-8666-666666666666",
		ExternalReference: "no-events-reference",
		EvidenceType:      "integration.outbox",
		ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
		ContentLocation:   "s3://evidence-bucket/evidence/no-events",
		ReceivedAt:        time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC),
		Classification:    "public",
		CorrelationID:     "55555555-5555-4555-8555-555555555555",
	}
	if _, _, err := store.Create(ctx, request); err != nil {
		t.Fatalf("create without events: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM evidence_outbox").Scan(&count); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if count != 0 {
		t.Fatalf("unwired store must not emit events, got %d", count)
	}
}

// TestListIntegration exercises the capped listing path against real PG.
func TestListIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := integrationPool(t, ctx, true)
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_outbox, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}
	store := NewStore(pool)
	for index, key := range []string{
		"44444444-4444-4444-8444-444444444441",
		"44444444-4444-4444-8444-444444444442",
		"44444444-4444-4444-8444-444444444443",
	} {
		if _, _, err := store.Create(ctx, CreateRequest{
			IdempotencyKey:    key,
			ExternalReference: "list-reference",
			EvidenceType:      "integration.list",
			ContentSHA256:     "277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9",
			ContentLocation:   "s3://evidence-bucket/evidence/list",
			ReceivedAt:        time.Date(2026, 8, 12, 12, index, 0, 0, time.UTC),
			Classification:    "public",
			CorrelationID:     "55555555-5555-4555-8555-555555555555",
		}); err != nil {
			t.Fatalf("seed list package: %v", err)
		}
	}
	page, err := store.List(ctx, 2, 0)
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if len(page) != 2 {
		t.Fatalf("expected 2 rows on page 1, got %d", len(page))
	}
	page, err = store.List(ctx, 2, 2)
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if len(page) != 1 {
		t.Fatalf("expected 1 row on page 2, got %d", len(page))
	}
}
