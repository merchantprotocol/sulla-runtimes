#!/usr/bin/env bash
# End-to-end test for shell-runtime against the shell-command sample routine.
set -euo pipefail

SOCK="${SULLA_SOCK:-/tmp/sulla-shell-sock/shell-runtime.sock}"

if [ ! -S "$SOCK" ]; then
  echo "✗ Socket not found at $SOCK" >&2
  echo "  docker run --rm -v ~/sulla/routines:/var/routines:ro -v /tmp/sulla-shell-sock:/run/sulla sulla/shell-runtime:dev" >&2
  exit 1
fi

req() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS --unix-socket "$SOCK" -X "$method" -H 'Content-Type: application/json' -d "$body" "http://localhost$path"
  else
    curl -sS --unix-socket "$SOCK" -X "$method" "http://localhost$path"
  fi
}

step() { echo; echo "─── $1 ───"; }

step "Health"
req GET /health | python3 -m json.tool

step "Load shell-command@0.1.0"
req POST /load '{"name":"shell-command","version":"0.1.0"}' | python3 -m json.tool

step "Invoke: echo hello"
req POST /invoke '{"name":"shell-command","version":"0.1.0","inputs":{"command":"echo hello","working_dir":"."}}' | python3 -m json.tool

step "Invoke: exit-1 command"
req POST /invoke '{"name":"shell-command","version":"0.1.0","inputs":{"command":"echo hi && false","working_dir":"."}}' | python3 -m json.tool

step "Unload"
req POST /unload '{"name":"shell-command","version":"0.1.0"}' | python3 -m json.tool

echo
echo "✓ All checks passed"
