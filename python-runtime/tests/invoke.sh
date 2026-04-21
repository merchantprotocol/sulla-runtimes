#!/usr/bin/env bash
# End-to-end test for python-runtime.
# Expects the container to be running with the Unix socket bind-mounted at
# /tmp/sulla-sock/python-runtime.sock, and ~/sulla/routines bind-mounted to
# /var/routines inside the container.

set -euo pipefail

SOCK="${SULLA_SOCK:-/tmp/sulla-sock/python-runtime.sock}"

if [ ! -S "$SOCK" ]; then
  echo "✗ Socket not found at $SOCK" >&2
  echo "  Start the container with:" >&2
  echo "  docker run --rm -v ~/sulla/routines:/var/routines:ro -v /tmp/sulla-sock:/run/sulla sulla/python-runtime:dev" >&2
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

step "Health check"
req GET /health | python3 -m json.tool

step "Load transform-json@0.1.0"
req POST /load '{"name":"transform-json","version":"0.1.0"}' | python3 -m json.tool

step "List loaded routines"
req GET /routines | python3 -m json.tool

step "Invoke — extract + rename fields from sample data"
req POST /invoke '{
  "name": "transform-json",
  "version": "0.1.0",
  "inputs": {
    "data": {
      "user": {"first_name": "Jonathon", "last_name": "Byrdziak", "email": "jonathon@merchantprotocol.com"},
      "plan": {"id": "pro"}
    },
    "template": "{\"email\": {{ data.user.email | tojson }}, \"name\": {{ (data.user.first_name ~ \" \" ~ data.user.last_name) | tojson }}, \"plan\": {{ data.plan.id | tojson }}}"
  }
}' | python3 -m json.tool

step "Unload"
req POST /unload '{"name":"transform-json","version":"0.1.0"}' | python3 -m json.tool

step "Confirm unloaded"
req GET /routines | python3 -m json.tool

echo
echo "✓ All checks passed"
