package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/evidence"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/provenance"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/telemetry"
)

func main() {
	// Telemetry is environment-configured: without OTEL_EXPORTER_OTLP_ENDPOINT
	// the run is byte-identical to pre-instrumentation behavior (no-op
	// tracer); when set, spans export batched/async and flush on exit.
	telemetryConfig, err := telemetry.LoadConfig("blueeconomy-maritime-evidence")
	if err != nil {
		fail(err)
	}
	pipeline, err := telemetry.Setup(context.Background(), telemetryConfig)
	if err != nil {
		fail(fmt.Errorf("telemetry setup: %w", err))
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = pipeline.Shutdown(shutdownCtx)
	}()
	if len(os.Args) > 1 && os.Args[1] == "legacy-s3" {
		runLegacyS3(os.Args[2:])
		return
	}
	runApplySQL(os.Args[1:])
}

// runApplySQL keeps the original behaviour: apply one approved SQL migration
// file to the authorised PostgreSQL target.
func runApplySQL(arguments []string) {
	flags := flag.NewFlagSet("evidence-migrate", flag.ExitOnError)
	var migrationPath string
	flags.StringVar(&migrationPath, "migration", "", "path to an approved SQL migration file")
	_ = flags.Parse(arguments)

	if migrationPath == "" {
		fail(errors.New("usage: evidence-migrate --migration <file> | evidence-migrate legacy-s3 (--dry-run | --apply)"))
	}
	if os.Getenv("EVIDENCE_MIGRATION_APPROVED") != "true" {
		fail(errors.New("set EVIDENCE_MIGRATION_APPROVED=true after an approved change record; migrations never run by default"))
	}
	pool := mustPool()
	defer pool.Close()

	absolutePath, err := filepath.Abs(migrationPath)
	if err != nil {
		fail(fmt.Errorf("resolve migration path: %w", err))
	}
	migration, err := os.ReadFile(absolutePath)
	if err != nil {
		fail(fmt.Errorf("read migration: %w", err))
	}
	if len(migration) == 0 {
		fail(errors.New("migration file is empty"))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		fail(fmt.Errorf("apply migration: %w", err))
	}
	fmt.Printf("Applied migration %s to the authorised PostgreSQL target.\n", absolutePath)
}

// runLegacyS3 executes the approved legacy s3 re-registration runbook
// (docs/legacy-s3-migration.md): plan in --dry-run, or copy + verify +
// re-register + supersede in --apply.
func runLegacyS3(arguments []string) {
	flags := flag.NewFlagSet("evidence-migrate legacy-s3", flag.ExitOnError)
	var dryRun, apply bool
	flags.BoolVar(&dryRun, "dry-run", false, "list legacy s3 packages and their planned re-registration without writing")
	flags.BoolVar(&apply, "apply", false, "copy, verify, re-register and supersede legacy s3 packages")
	_ = flags.Parse(arguments)

	if dryRun == apply {
		fail(errors.New("exactly one of --dry-run or --apply is required"))
	}
	targetPrefix := os.Getenv("EVIDENCE_LEGACY_S3_TARGET_PREFIX")
	if targetPrefix == "" {
		fail(errors.New("EVIDENCE_LEGACY_S3_TARGET_PREFIX must name the approved abfs base location (abfs://<filesystem>@<account>.dfs.core.usgovcloudapi.net/<base-path>)"))
	}

	pool := mustPool()
	defer pool.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	migration := evidence.NewLegacyMigration(pool)

	if dryRun {
		legacyPackages, err := migration.ListLegacyS3Packages(ctx, targetPrefix)
		if err != nil {
			fail(err)
		}
		if len(legacyPackages) == 0 {
			fmt.Println("No legacy s3 evidence packages remain.")
			return
		}
		pending := 0
		for _, legacy := range legacyPackages {
			switch {
			case legacy.Superseded:
				fmt.Printf("superseded  %s -> %s (replacement %s)\n",
					legacy.Package.EvidencePackageID, legacy.Package.ContentLocation, legacy.SupersededBy)
			case legacy.RelocationError != "":
				fmt.Printf("blocked     %s %s: %s\n",
					legacy.Package.EvidencePackageID, legacy.Package.ContentLocation, legacy.RelocationError)
			default:
				pending++
				fmt.Printf("planned     %s %s -> %s\n",
					legacy.Package.EvidencePackageID, legacy.Package.ContentLocation, legacy.TargetLocation)
			}
		}
		fmt.Printf("%d legacy s3 package(s): %d pending re-registration.\n", len(legacyPackages), pending)
		return
	}

	// --apply: approval gate and operator identity, then the real copier.
	if os.Getenv("EVIDENCE_MIGRATION_APPROVED") != "true" {
		fail(errors.New("set EVIDENCE_MIGRATION_APPROVED=true after an approved change record; re-registration never runs by default"))
	}
	actor := os.Getenv("EVIDENCE_MIGRATE_ACTOR")
	if actor == "" {
		fail(errors.New("EVIDENCE_MIGRATE_ACTOR must name the accountable operator subject reference"))
	}
	copier, err := evidence.NewStagedCommandCopierFromEnv()
	if err != nil {
		fail(err)
	}
	correlationID, err := evidence.NewMigrationCorrelationID()
	if err != nil {
		fail(err)
	}
	// Fail-closed: an --apply run emits a signed provenance attestation for
	// every completed re-registration; without the producer key no run may
	// start, so no unattested re-registration can ever complete.
	signer, err := provenance.LoadSignerFromEnv(evidence.SigningKeyID)
	if err != nil {
		fail(fmt.Errorf("load provenance signer: %w", err))
	}
	fmt.Printf("Re-registration run correlation id: %s\n", correlationID)

	legacyPackages, err := migration.ListLegacyS3Packages(ctx, targetPrefix)
	if err != nil {
		fail(err)
	}
	completed, skipped := 0, 0
	for _, legacy := range legacyPackages {
		if legacy.Superseded {
			skipped++
			continue
		}
		if legacy.RelocationError != "" {
			fail(fmt.Errorf("package %s cannot be planned: %s", legacy.Package.EvidencePackageID, legacy.RelocationError))
		}
		plan := evidence.LegacyS3Plan{
			LegacyPackageID: legacy.Package.EvidencePackageID,
			TargetLocation:  legacy.TargetLocation,
		}
		if err := copier.CopyAndVerify(ctx, legacy.Package, plan); err != nil {
			fail(fmt.Errorf("package %s: %w", legacy.Package.EvidencePackageID, err))
		}
		replacement, err := migration.RegisterReplacement(
			ctx, legacy.Package, plan.TargetLocation, actor, correlationID, time.Now())
		if errors.Is(err, evidence.ErrAlreadySuperseded) {
			skipped++
			continue
		}
		if err != nil {
			fail(fmt.Errorf("package %s: %w", legacy.Package.EvidencePackageID, err))
		}
		completed++
		attestation, err := evidence.SignReregistrationAttestation(signer,
			legacy.Package.EvidencePackageID, replacement.EvidencePackageID,
			plan.TargetLocation, actor, correlationID, time.Now())
		if err != nil {
			fail(fmt.Errorf("package %s: %w", legacy.Package.EvidencePackageID, err))
		}
		encoded, err := json.Marshal(attestation)
		if err != nil {
			fail(fmt.Errorf("package %s: encode attestation: %w", legacy.Package.EvidencePackageID, err))
		}
		fmt.Printf("re-registered %s -> %s (replacement %s)\n",
			legacy.Package.EvidencePackageID, plan.TargetLocation, replacement.EvidencePackageID)
		fmt.Printf("attestation %s\n", encoded)
	}
	fmt.Printf("Re-registration complete: %d re-registered, %d already superseded.\n", completed, skipped)
}

func mustPool() *pgxpool.Pool {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fail(errors.New("DATABASE_URL must be injected by the approved secret-management path"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// otelpgx traces every migration/custody query as a span; with the no-op
	// global provider (telemetry disabled) it is a pass-through.
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		fail(fmt.Errorf("parse PostgreSQL configuration: %w", err))
	}
	poolConfig.ConnConfig.Tracer = otelpgx.NewTracer()
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		fail(fmt.Errorf("connect to PostgreSQL: %w", err))
	}
	if err := pool.Ping(ctx); err != nil {
		fail(fmt.Errorf("ping PostgreSQL: %w", err))
	}
	return pool
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "evidence-migrate:", err)
	os.Exit(1)
}
