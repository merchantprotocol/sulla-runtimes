#!/usr/bin/env bash
# End-to-end test for node-runtime.
# Requires a sample node routine on disk — once we add one, this script will
# load + invoke it and verify the result.
set -euo pipefail

SOCK="${SULLA_SOCK:-/tmp/sulla-node-sock/node-runtime.sock}"

if [ ! -S "$SOCK" ]; then
  echo "✗ Socket not found at $SOCK" >&2
  echo "  docker run --rm -v ~/sulla/routines:/var/routines:ro -v /tmp/sulla-node-sock:/run/sulla sulla/node-runtime:dev" >&2
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

echo
echo "Note: end-to-end invoke tests will be added once a node routine exists in ~/sulla/routines."
