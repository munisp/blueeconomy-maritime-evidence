// evidence-outbox-publisher drains the transactional evidence outbox to
// Kafka (evidence.package.v1, evidence.validation.v1) at-least-once: events
// are marked published only after an all-ISR acknowledgement, and the event
// id is the idempotent record key, so re-delivery after a crash is safe.
// Without the envelope signing key the publisher fails closed at startup —
// an unsigned event pipeline must never run.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/events"
	"github.com/munisp/blueeconomy-maritime-evidence/internal/evidence"
)

func main() {
	if err := run(); err != nil {
		log.Printf("evidence-outbox-publisher: %v", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := requiredEnv("DATABASE_URL")
	brokers := strings.Split(requiredEnv("KAFKA_BROKERS"), ",")
	for _, broker := range brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("KAFKA_BROKERS contains an empty entry")
		}
	}
	batchSize, err := strconv.Atoi(defaultEnv("OUTBOX_BATCH_SIZE", "100"))
	if err != nil || batchSize < 1 || batchSize > 1000 {
		return errors.New("OUTBOX_BATCH_SIZE must be between 1 and 1000")
	}
	pollInterval, err := time.ParseDuration(defaultEnv("OUTBOX_POLL_INTERVAL", "2s"))
	if err != nil || pollInterval <= 0 {
		return errors.New("OUTBOX_POLL_INTERVAL must be a positive duration")
	}
	// Fail closed without the signing key: the publisher verifies every
	// drained envelope against the producer key before publishing.
	signer, err := events.SignerFromEnv()
	if err != nil {
		return fmt.Errorf("envelope signing key: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	evidence.ConfigurePool(poolConfig)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	defer pool.Close()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return fmt.Errorf("build kafka producer: %w", err)
	}
	defer producer.Close()
	if err := producer.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}

	log.Printf("evidence-outbox-publisher draining evidence_outbox to %v", brokers)
	for {
		published, err := publishBatch(ctx, pool, producer, signer.PublicKey(), batchSize)
		if err != nil {
			return err
		}
		if published == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(pollInterval):
			}
		}
	}
}

func publishBatch(ctx context.Context, pool *pgxpool.Pool, producer *kgo.Client, publicKey ed25519.PublicKey, batchSize int) (int, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin outbox batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT event_id, topic, payload FROM evidence_outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("load unpublished outbox events: %w", err)
	}
	type pendingEvent struct {
		eventID string
		topic   string
		payload []byte
	}
	var pending []pendingEvent
	for rows.Next() {
		var event pendingEvent
		if err := rows.Scan(&event.eventID, &event.topic, &event.payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox event: %w", err)
		}
		pending = append(pending, event)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	published := 0
	for _, event := range pending {
		// Fail closed: an outbox row whose envelope does not verify against
		// the producer key is never published.
		var envelope events.Envelope
		if err := json.Unmarshal(event.payload, &envelope); err != nil {
			return published, fmt.Errorf("decode outbox envelope %s: %w", event.eventID, err)
		}
		if err := events.Verify(envelope, publicKey); err != nil {
			return published, fmt.Errorf("outbox envelope %s does not verify: %w", event.eventID, err)
		}
		record := &kgo.Record{
			Topic: event.topic,
			Key:   []byte(event.eventID), // deterministic idempotence key
			Value: event.payload,
		}
		if err := producer.ProduceSync(ctx, record).FirstErr(); err != nil {
			return published, fmt.Errorf("publish %s to %s: %w", event.eventID, event.topic, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE evidence_outbox SET published_at = now() WHERE event_id = $1 AND published_at IS NULL`, event.eventID); err != nil {
			return published, fmt.Errorf("mark event %s published: %w", event.eventID, err)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox batch: %w", err)
	}
	return published, nil
}

func requiredEnv(name string) string {
	value := os.Getenv(name)
	if value == "" {
		log.Fatalf("%s must be set", name)
	}
	return value
}

func defaultEnv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
