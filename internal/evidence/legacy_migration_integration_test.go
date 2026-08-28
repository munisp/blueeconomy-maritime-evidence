package evidence

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestLegacyS3ReRegistrationIntegration exercises the approved
// re-registration path against a real PostgreSQL with migrations 0001-0004
// applied (see integration/run-local.sh). It verifies:
//   - apply: replacement package, history row and supersession link commit
//     together while the legacy row is left untouched (immutable);
//   - resume: a second apply returns ErrAlreadySuperseded and the listing
//     marks the package superseded.
func TestLegacyS3ReRegistrationIntegration(t *testing.T) {
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
	if _, err := pool.Exec(ctx, "TRUNCATE evidence_package_supersession, evidence_validation_history, evidence_packages"); err != nil {
		t.Fatalf("reset evidence tables: %v", err)
	}

	// A legacy s3 package predating the Azure Government posture, inserted
	// directly as an approved legacy row.
	var legacyID string
	err = pool.QueryRow(ctx, `
		INSERT INTO evidence_packages (
			evidence_package_id, idempotency_key, external_reference, evidence_type,
			content_sha256, content_location, received_at, validation_status, classification, correlation_id
		) VALUES (gen_random_uuid(), gen_random_uuid(), 'legacy-case-2024-1187', 'boarding.report',
			'277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9',
			's3://agency-evidence/case-2024-1187/object.bin',
			'2024-11-02T10:15:00Z', 'received', 'confidential', gen_random_uuid())
		RETURNING evidence_package_id
	`).Scan(&legacyID)
	if err != nil {
		t.Fatalf("seed legacy s3 package: %v", err)
	}

	migration := NewLegacyMigration(pool)
	store := NewStore(pool)

	legacy, err := store.Get(ctx, legacyID)
	if err != nil {
		t.Fatalf("load legacy package: %v", err)
	}

	target, err := PlanLegacyS3Relocation(legacy.ContentLocation, testTargetPrefix)
	if err != nil {
		t.Fatalf("plan relocation: %v", err)
	}
	correlationID, err := NewMigrationCorrelationID()
	if err != nil {
		t.Fatalf("correlation id: %v", err)
	}

	replacement, err := migration.RegisterReplacement(
		ctx, legacy, target, "operator:migration-officer", correlationID, time.Now())
	if err != nil {
		t.Fatalf("register replacement: %v", err)
	}
	if replacement.EvidencePackageID == legacyID {
		t.Fatal("replacement must be a new package id")
	}
	if replacement.ContentLocation != target {
		t.Fatalf("replacement location %q, expected %q", replacement.ContentLocation, target)
	}
	if replacement.ContentSHA256 != legacy.ContentSHA256 ||
		replacement.ExternalReference != legacy.ExternalReference ||
		!replacement.ReceivedAt.Equal(legacy.ReceivedAt) {
		t.Fatal("replacement must retain the evidentiary metadata of the legacy package")
	}

	// The legacy row is untouched and still readable.
	after, err := store.Get(ctx, legacyID)
	if err != nil {
		t.Fatalf("legacy package must remain readable: %v", err)
	}
	if after != legacy {
		t.Fatal("legacy package row must be unchanged by re-registration")
	}

	// Idempotent resume: a second apply refuses via the supersession guard.
	if _, err := migration.RegisterReplacement(
		ctx, legacy, target, "operator:migration-officer", correlationID, time.Now()); !errors.Is(err, ErrAlreadySuperseded) {
		t.Fatalf("second apply must return ErrAlreadySuperseded, got %v", err)
	}

	listed, err := migration.ListLegacyS3Packages(ctx, testTargetPrefix)
	if err != nil {
		t.Fatalf("list legacy packages: %v", err)
	}
	if len(listed) != 1 || !listed[0].Superseded || listed[0].SupersededBy != replacement.EvidencePackageID {
		t.Fatalf("listing must mark the package superseded by %s: %+v", replacement.EvidencePackageID, listed)
	}
	if listed[0].TargetLocation != target {
		t.Fatalf("dry-run listing must plan %q, got %q", target, listed[0].TargetLocation)
	}

	// The supersession record links old to new and is append-only.
	var linked string
	err = pool.QueryRow(ctx, `
		SELECT superseded_by FROM evidence_package_supersession WHERE evidence_package_id = $1
	`, legacyID).Scan(&linked)
	if err != nil || linked != replacement.EvidencePackageID {
		t.Fatalf("supersession link: %v (%q)", err, linked)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE evidence_package_supersession SET reason_code = 'tamper' WHERE evidence_package_id = $1
	`, legacyID); err == nil {
		t.Fatal("supersession rows must be immutable")
	}

	// The replacement carries an initial received history row.
	var historyRows int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM evidence_validation_history
		WHERE evidence_package_id = $1 AND validation_status = 'received'
		  AND reason_code = 'legacy_s3_reregistration'
	`, replacement.EvidencePackageID).Scan(&historyRows)
	if err != nil || historyRows != 1 {
		t.Fatalf("replacement history rows: %v (%d)", err, historyRows)
	}
}
