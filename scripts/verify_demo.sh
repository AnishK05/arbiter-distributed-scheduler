#!/usr/bin/env bash
# Smoke-check the Phase 10 demo stack after `make demo-up`.
# Run from the repo root inside WSL2 / Linux / macOS (not native Windows shells).
set -euo pipefail

API="${ARBITER_HTTP_ADDR:-http://localhost:8080}"
ARBITERCTL="${ARBITERCTL:-./bin/arbiterctl}"
MIN_READY="${MIN_READY:-10}"

echo "== healthz =="
curl -sf "${API}/healthz"
echo

echo "== nodes (expect ${MIN_READY} ready) =="
ready="$(
  curl -sf "${API}/api/v1/nodes" | python3 -c "
import sys, json
ns = json.load(sys.stdin).get('nodes') or []
ready = sum(1 for n in ns if n.get('status') == 'ready')
print(ready)
print(f'ready={ready} total={len(ns)}', file=sys.stderr)
"
)"
if [[ "${ready}" -lt "${MIN_READY}" ]]; then
  echo "FAIL: only ${ready} ready nodes (want >= ${MIN_READY})" >&2
  exit 1
fi

if [[ ! -x "${ARBITERCTL}" ]]; then
  echo "WARN: ${ARBITERCTL} missing — run \`make build\` to exercise CLI submit" >&2
  echo "ok: demo API healthy with ${ready} ready workers"
  exit 0
fi

echo "== submit hello (3 replicas) =="
"${ARBITERCTL}" --scheduler-addr "${ARBITER_SCHEDULER_ADDR:-localhost:7000}" \
  submit "verify-demo-$$" --replicas 3 --wait --wait-timeout 90s

echo "ok: demo verified (${ready} ready workers + job succeeded)"
