# Blue Economy Maritime Evidence

This repository implements the full **immutable maritime evidence** service: the PostgreSQL persistence kernel, the `/v1/evidence` REST API, the S3-compatible object-store client, and the transactional-outbox Kafka event publisher. Raw evidence is retained in an approved object store outside the database; the service stores a SHA-256 digest and a credential-free object location, never document content or object-store credentials.

## Components

| Component | Path | Purpose |
|---|---|---|
| Migration runner | `cmd/evidence-migrate` | Applies an approved migration to the authorised PostgreSQL target (gated by `EVIDENCE_MIGRATION_APPROVED=true`). |
| Evidence API | `cmd/evidence-api` | `/v1/evidence` REST boundary with PBAC (Keycloak RS256 JWKS) and clearance-floor enforcement. |
| Outbox publisher | `cmd/evidence-outbox-publisher` | Drains `evidence_outbox` to Kafka at-least-once. |
| Domain + store | `internal/evidence` | Strict validation, idempotent create, append-only validation history, outbox enqueue in the same transaction. |
| PBAC auth | `internal/auth` | Keycloak RS256 JWT validation (JWKS, issuer/audience, ≥2048-bit keys, short key TTL) and the evidence clearance ladder; fails closed on every ambiguity. |
| Object store | `internal/objstore` | S3-compatible presigned PUT/GET with a server-side SHA-256 verification contract. |
| Event envelope | `internal/events` | Signed envelope v1.0: FHIR R4 message Bundle, JWS-EdDSA over RFC 8785 JCS, kid `maritime-evidence-<epoch>`. |

## Persistence controls

`db/migrations/0001_evidence.sql` creates `evidence_packages` (immutable by trigger) and append-only `evidence_validation_history`; creation is idempotent on the caller-provided UUID key. `0002_terminal_validation_invariant.sql` enforces exactly one terminal validation decision per package. `0003_evidence_outbox.sql` adds the transactional outbox with fail-closed row-level security: only sessions asserting the approved service context (`app.evidence_service = on`, applied to every pooled connection by `evidence.ConfigurePool`) may read or append outbox rows.

The Go domain layer rejects absent provenance, non-UUID idempotency/correlation references, unsupported classifications, malformed lower-case SHA-256 digests, insecure/credential-bearing content locations and invalid validation transitions before database interaction.

## HTTP API

`cmd/evidence-api` serves:

| Route | Role | Behaviour |
|---|---|---|
| `POST /v1/evidence/packages` | evidence-writer | Idempotent create; returns the package plus a presigned PUT upload descriptor. `content_location` is service-derived (the approved bucket); callers never supply it. |
| `GET /v1/evidence/packages/{id}` | evidence-reader | Package metadata plus a presigned GET download descriptor, only once the record exists and the caller's clearance covers its classification. |
| `POST /v1/evidence/packages/{id}/validations` | evidence-validator | Appends a terminal `validated`/`rejected` decision; a second terminal decision is 409. The actor subject is taken from the authenticated principal. |
| `POST /v1/evidence/packages/{id}/upload-confirmation` | evidence-writer | Digest-verification hook: HEADs the retained object and confirms it matches the recorded SHA-256; a mismatch or absent object is 409. |
| `GET /v1/evidence/packages?limit=&offset=` | evidence-reader | Listing ordered by `received_at` desc; `limit` defaults to `EVIDENCE_LIST_DEFAULT_LIMIT` (50) and is capped at `EVIDENCE_LIST_MAX_LIMIT` (200). Rows above the caller's clearance floor are omitted. |

Configuration is fail-closed from the environment: `DATABASE_URL`, `EVIDENCE_API_ADDR`, `EVIDENCE_OIDC_ISSUER` / `EVIDENCE_OIDC_AUDIENCE` / `EVIDENCE_OIDC_JWKS_URL` (optional `EVIDENCE_OIDC_CA_FILE`), `EVIDENCE_PRODUCER_PRINCIPAL_ID` / `EVIDENCE_PRODUCER_PRINCIPAL_ROLE`, the `EVIDENCE_S3_*` object-store settings, and the envelope signing key. Missing or malformed values stop the service at startup.

## Object store

`internal/objstore` targets any S3-compatible store via AWS SDK for Go v2 (Apache-2.0). Raw bytes never transit the database or this service:

* **Upload** — package creation returns a presigned PUT URL whose signed headers bind `x-amz-checksum-sha256`, so the object store rejects a digest-mismatched payload server-side.
* **Download** — a presigned GET URL is issued only after the metadata record exists and authorization (role + clearance floor) has passed.
* **Verification** — `upload-confirmation` performs a checksum-mode HEAD and compares the retained object's SHA-256 against the recorded digest; any mismatch fails closed.

Object-store credentials come from the environment (`EVIDENCE_S3_ACCESS_KEY` / `EVIDENCE_S3_SECRET_KEY`) only and are never persisted.

## Event publisher

`cmd/evidence-outbox-publisher` drains `evidence_outbox` to Kafka at-least-once: events are marked published only after an all-ISR acknowledgement, and the event id is the idempotent record key, so re-delivery after a crash is safe. Every envelope is verified against the producer key before publish; the publisher fails closed at startup without `EVIDENCE_ENVELOPE_SIGNING_PRIVATE_KEY` and `EVIDENCE_ENVELOPE_SIGNING_KEY_EPOCH`.

Envelopes are the platform contract: `envelopeVersion` 1.0, producer `maritime-evidence`, a FHIR R4 message Bundle entry carrying the domain payload, and a provenance signature that is a JWS compact serialization (EdDSA over the RFC 8785 JCS-canonicalized envelope excluding the signature field) with kid `maritime-evidence-<epoch>`.

| Topic | Event type | Emitted when |
|---|---|---|
| `evidence.package.v1` | `evidence.package.received` | A package is created (in the same transaction). |
| `evidence.validation.v1` | `evidence.validation.recorded` | A terminal validation decision is appended (in the same transaction). |

Required environment: `DATABASE_URL`, `KAFKA_BROKERS`, the signing key pair above; optional `OUTBOX_BATCH_SIZE` (1–1000, default 100) and `OUTBOX_POLL_INTERVAL` (default 2s).

## Migrations

The database owner applies the migrations in order (`0001`, `0002`, `0003`) through the approved migration process. They require permission to enable PostgreSQL's `pgcrypto` extension because UUID values are generated server-side. Each migration must be executed first in the authorised non-production PostgreSQL target and the resulting schema/backup/restore evidence retained before any production promotion.

## Tests

```bash
# unit tests (model transitions, presign logic against an in-process fake S3, envelope signing)
go test ./...

# real-PostgreSQL integration tests (idempotency, immutability, outbox atomicity, RLS default-deny)
EVIDENCE_TEST_POSTGRES_DSN=postgres://postgres@127.0.0.1:5433/postgres go test -race ./...
```

Object-store tests run against an in-process fake S3 server that honours the signed checksum contract; the production path stays fail-closed.
