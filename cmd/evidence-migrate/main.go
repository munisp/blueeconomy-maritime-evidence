package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	var migrationPath string
	flag.StringVar(&migrationPath, "migration", "", "path to an approved SQL migration file")
	flag.Parse()

	if migrationPath == "" {
		fail(errors.New("--migration is required"))
	}
	if os.Getenv("EVIDENCE_MIGRATION_APPROVED") != "true" {
		fail(errors.New("set EVIDENCE_MIGRATION_APPROVED=true after an approved change record; migrations never run by default"))
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fail(errors.New("DATABASE_URL must be injected by the approved secret-management path"))
	}

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
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fail(fmt.Errorf("connect to PostgreSQL: %w", err))
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		fail(fmt.Errorf("ping PostgreSQL: %w", err))
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		fail(fmt.Errorf("apply migration: %w", err))
	}
	fmt.Printf("Applied migration %s to the authorised PostgreSQL target.\n", absolutePath)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "evidence-migrate:", err)
	os.Exit(1)
}
