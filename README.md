# Blue Economy Maritime Evidence

This repository implements the core persistence model for **immutable maritime evidence packages**. It is written in Go and uses PostgreSQL as the authoritative metadata store. Raw evidence is retained in an approved object store outside the database; the service stores a SHA-256 digest and a credential-free object location, not document content or object-store credentials.

## Implemented persistence controls

The included PostgreSQL migration creates `evidence_packages` and append-only `evidence_validation_history` tables. The package row is protected against update and delete by a database trigger. Creation is idempotent on the caller-provided UUID key. Validation outcomes are recorded as immutable history records; they do not mutate the original package.

The Go domain layer rejects absent provenance, non-UUID idempotency/correlation references, unsupported classifications, malformed lower-case SHA-256 digests, insecure/credential-bearing content locations and invalid validation transitions before database interaction.

Content-location schemes accepted under the Azure Government posture:

| Scheme | Status | Constraint |
| --- | --- | --- |
| `https://` | Accepted | No userinfo, query or fragment |
| `abfs://` | Accepted | ADLS Gen2 Azure Government endpoints only (`<account>.dfs.core.usgovcloudapi.net`); filesystem in the userinfo position is addressing, not a credential; passwords and ports rejected |
| `s3://` | Deprecated | Rejected unless `EVIDENCE_ALLOW_LEGACY_S3=true` is set for an approved legacy migration; any other flag value fails closed |

Migration [`db/migrations/0003_azure_gov_content_location.sql`](db/migrations/0003_azure_gov_content_location.sql) adds the matching `NOT VALID` database constraint so legacy `s3:` rows already stored do not block adoption.

## Deployment and integration boundary

This repository does **not** claim that an evidence API, Keycloak realm, APISIX route, Kafka event, Temporal workflow, object store or Ministry/partner document interface is live. A real deployment requires the approved PostgreSQL target, object-store contract, Keycloak/OIDC identity model, API-edge route policy, event contract and authorised non-production evidence source described in the programme integration gates.

## Migration

The database owner applies [`db/migrations/0001_evidence.sql`](db/migrations/0001_evidence.sql) through the approved migration process. It requires permission to enable PostgreSQL's `pgcrypto` extension because UUID values are generated server-side. The migration must be executed first in the authorised non-production PostgreSQL target and the resulting schema/backup/restore evidence retained before any production promotion.
