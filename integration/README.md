# Maritime Evidence PostgreSQL Integration Suite

This suite runs the repository against a real local PostgreSQL 16 container. It does not emulate an agency, object store, Keycloak, API gateway or production cluster.

## Execution

```bash
./integration/run-local.sh
```

The runner generates an ephemeral PostgreSQL password, applies all committed migrations and runs the race-enabled Go integration test. It verifies that:

| Invariant | Assertion |
|---|---|
| Idempotent receipt | Repeating one `idempotency_key` returns the original package and creates one row. |
| Immutable package | A direct update is rejected by the database trigger. |
| Append-only history | One initial `received` history row and one terminal row remain. |
| Terminal exclusivity | Concurrent `validated` and `rejected` attempts produce exactly one successful terminal outcome. |
| Stored state semantics | The package row retains its original `received` status; current validation state remains history-derived. |

Generated credentials, logs, coverage profiles and evidence are ignored by Git. The non-secret result is written to `integration/results/local-integration-result.json`; statement coverage is written to `integration/results/integration.coverage.txt`.

This suite establishes local PostgreSQL compatibility and invariant behavior only. A live claim still requires an approved PostgreSQL environment, object storage, identity/API edge, authorised evidence source, backup/restore and accountable operational acceptance.
