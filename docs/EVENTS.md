# Published event topics

All events are signed platform envelopes (version 1.0, producer
`maritime-evidence`, schema in `internal/events/envelope.go`), staged in
`evidence_outbox` and drained by `cmd/evidence-outbox-publisher`. The
published topics are logged at startup
(`evidence-outbox-publisher: published topics: ...`).

| Topic | Producer | Consumer | Schema ref |
|---|---|---|---|
| `evidence.package.v1` | cmd/evidence-outbox-publisher (evidence package receipts) | reserved — no consumer yet (platform-wide, Phase 20 audit) | internal/events/envelope.go |
| `evidence.validation.v1` | cmd/evidence-outbox-publisher (validation decisions) | reserved — no consumer yet (platform-wide, Phase 20 audit) | internal/events/envelope.go |

"Reserved" means the signed-envelope production path is intentionally live
while downstream consumers land in a later phase. Do not remove producers;
update this table when a consumer is wired.
