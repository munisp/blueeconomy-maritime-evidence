# Legacy s3 Evidence Re-registration Runbook

This runbook is the supported procedure for retiring legacy `s3://`
`content_location` values under the Azure Government posture. It replaces
indefinite reliance on the deprecated-scheme flag `EVIDENCE_ALLOW_LEGACY_S3`.

## Why re-registration instead of UPDATE

`evidence_packages` rows are immutable: the `evidence_packages_immutable`
trigger (db/migrations/0001_evidence.sql) raises on any UPDATE or DELETE, and
that invariant is intentionally preserved. A legacy package row is therefore
never rewritten. Instead the evidentiary content is re-registered:

1. The object is copied from its legacy `s3://bucket/key` location to the
   approved ADLS Gen2 (abfs) location.
2. A **new** `evidence_packages` row is inserted pointing at the abfs
   `content_location` — a new package id and idempotency key, the same
   `external_reference`, `evidence_type`, `content_sha256`, `received_at` and
   `classification`, because the evidentiary content is unchanged.
3. The old→new link is recorded in `evidence_package_supersession`
   (db/migrations/0004_legacy_s3_supersession.sql), an append-only side table
   protected by its own immutability trigger. The legacy row itself is never
   updated, so the immutability trigger stays fully intact and legacy readers
   keep working. The replacement package also carries an initial
   `evidence_validation_history` row with reason code
   `legacy_s3_reregistration`, giving the old→new chain an audit trail entry.
4. The planned abfs location is
   `<EVIDENCE_LEGACY_S3_TARGET_PREFIX>/<bucket>/<key>` and must satisfy the
   default location policy — the deprecated s3 scheme is never acceptable for
   the new row, even while `EVIDENCE_ALLOW_LEGACY_S3=true` remains set for
   reading legacy data.

Supersession is a one-way door: each package can be superseded exactly once
(the table's primary key), which makes an interrupted run safely resumable —
already-superseded rows are skipped.

## Prerequisites

- An approved change record; export `EVIDENCE_MIGRATION_APPROVED=true` only
  after approval.
- `DATABASE_URL` injected by the approved secret-management path.
- Migration 0004 applied through the approved migration process:
  `evidence-migrate --migration db/migrations/0004_legacy_s3_supersession.sql`.
- `EVIDENCE_LEGACY_S3_TARGET_PREFIX` naming the approved abfs base location,
  e.g. `abfs://evidence@<account>.dfs.core.usgovcloudapi.net/legacy-s3`.
- For `--apply` additionally:
  - `EVIDENCE_MIGRATE_ACTOR` — the accountable operator subject reference,
    recorded in validation history and supersession rows.
  - `EVIDENCE_MIGRATE_WORK_DIR` — an operator-provisioned staging directory.
  - The AWS CLI on `PATH` (override with `EVIDENCE_MIGRATE_AWS_CLI`) with
    read credentials for the legacy bucket.
  - azcopy on `PATH` (override with `EVIDENCE_MIGRATE_AZCOPY`) authenticated
    to the Azure Government ADLS Gen2 account (AAD login / managed identity;
    credentials never pass through this tool).

## Procedure

1. Dry-run — lists every legacy s3 package, its planned abfs relocation, and
   any package already superseded or blocked (e.g. an unplannable location):

   ```bash
   evidence-migrate legacy-s3 --dry-run
   ```

   Review the plan. Any `blocked` line must be resolved before apply; the
   tool fails closed on the first blocked package.

2. Apply — for each pending package, in order, inside one transaction per
   package:

   - fetch the legacy object (`aws s3 cp`) into the staging directory;
   - verify the staged object's SHA-256 equals the registered
     `content_sha256` — mismatch refuses re-registration;
   - upload to the planned abfs location (`azcopy copy`);
   - re-download the abfs object and verify its SHA-256 again — mismatch
     refuses supersession;
   - insert the replacement package, its initial validation history row and
     the supersession link in a single serializable transaction.

   ```bash
   evidence-migrate legacy-s3 --apply
   ```

   Any copy or verification failure aborts the run with a non-zero exit
   before the failing package's transaction commits; already-completed
   packages stay committed and are skipped on re-run.

3. Re-run `--dry-run` until no package reports `planned` (all either
   `superseded` or explicitly resolved), then retain the run output and the
   printed run correlation id as migration evidence.

4. Only after every legacy row is superseded and downstream readers have
   migrated to the replacement package ids may the operator flag
   `EVIDENCE_ALLOW_LEGACY_S3` be retired through the normal change process.

## Reader guidance

Until readers follow supersession links, both rows remain readable:
`evidence_packages` is unchanged, and current state for a migrated package is
"superseded by <replacement id>" as recorded in
`evidence_package_supersession`. Consumers that resolve the active location
of an evidence item should prefer the replacement row when a supersession
record exists.
