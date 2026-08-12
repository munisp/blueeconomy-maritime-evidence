#!/usr/bin/env bash
set -euo pipefail

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
integration="$root/integration"
results="$integration/results"
compose=(sudo docker compose --env-file "$integration/.env" -f "$integration/compose.yaml")

cleanup() {
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

for command in docker go jq openssl sudo; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "required command missing: $command" >&2
    exit 1
  }
done

umask 077
mkdir -p "$results"
rm -f "$integration/.env" "$results/local-integration-result.json" "$results/integration.cover.out" "$results/integration.coverage.txt" "$results/integration.log"
password="$(openssl rand -hex 24)"
printf 'EVIDENCE_POSTGRES_PASSWORD=%s\n' "$password" > "$integration/.env"

"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d
for _ in $(seq 1 60); do
  if "${compose[@]}" exec -T postgres pg_isready -U evidence -d evidence -p 55432 >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"${compose[@]}" exec -T postgres pg_isready -U evidence -d evidence -p 55432 >/dev/null

"${compose[@]}" exec -T postgres psql -p 55432 -v ON_ERROR_STOP=1 -U evidence -d evidence < "$root/db/migrations/0001_evidence.sql"
"${compose[@]}" exec -T postgres psql -p 55432 -v ON_ERROR_STOP=1 -U evidence -d evidence < "$root/db/migrations/0002_terminal_validation_invariant.sql"

export EVIDENCE_TEST_POSTGRES_DSN="postgres://evidence:$password@127.0.0.1:55432/evidence?sslmode=disable"
cd "$root"
GOTOOLCHAIN=local go test -count=1 -race -covermode=atomic -coverprofile="$results/integration.cover.out" ./internal/evidence 2>&1 | tee "$results/integration.log"
go tool cover -func="$results/integration.cover.out" | tee "$results/integration.coverage.txt"

history="$("${compose[@]}" exec -T postgres psql -p 55432 -At -U evidence -d evidence -c "SELECT count(*) || ':' || count(*) FILTER (WHERE validation_status IN ('validated','rejected')) FROM evidence_validation_history" | tr -d '\r')"
packages="$("${compose[@]}" exec -T postgres psql -p 55432 -At -U evidence -d evidence -c 'SELECT count(*) FROM evidence_packages' | tr -d '\r')"
immutable_trigger="$("${compose[@]}" exec -T postgres psql -p 55432 -At -U evidence -d evidence -c "SELECT count(*) FROM pg_trigger WHERE tgname = 'evidence_packages_immutable' AND NOT tgisinternal" | tr -d '\r')"
coverage="$(awk '/^total:/ {gsub(/%/,"",$NF); print $NF}' "$results/integration.coverage.txt")"

jq -n \
  --argjson evidence_packages "$packages" \
  --arg history_counts "$history" \
  --argjson immutable_trigger_count "$immutable_trigger" \
  --argjson statement_coverage_percent "$coverage" \
  '{evidence_packages:$evidence_packages,history_counts:$history_counts,immutable_trigger_count:$immutable_trigger_count,statement_coverage_percent:$statement_coverage_percent}' \
  > "$results/local-integration-result.json"
cat "$results/local-integration-result.json"
