#!/usr/bin/env bash
# Proves the agentsessions integrity chain is verifiable by an INDEPENDENT, non-Go implementation.
# It builds the proto descriptor, records a real journal via agentctl, then runs hack/verify_chain.py
# (Python + protobuf + RFC 8785 JCS) to: (1) reproduce the golden hash with no deps, (2) verify the
# full real chain from raw proto bytes, and (3) reject a tampered record. This is the "any auditor can
# verify the chain" property the vision doc claims. Requires: buf, go, python3, sqlite3.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$ROOT"

echo "==> build the language-neutral proto descriptor"
buf build api -o "$WORK/schema.binpb"

echo "==> record a real journal via agentctl"
CGO_ENABLED=0 go build -o "$WORK/agentctl" ./cmd/agentctl
"$WORK/agentctl" exec --journal "$WORK/journal.db" --input "audit me" >/dev/null

echo "==> create a python venv with protobuf + jcs"
python3 -m venv "$WORK/venv"
"$WORK/venv/bin/pip" install --quiet --upgrade pip >/dev/null
"$WORK/venv/bin/pip" install --quiet -r hack/requirements.txt
PY="$WORK/venv/bin/python"

echo "==> (1) golden self-check (stdlib, no deps)"
"$PY" hack/verify_chain.py --golden

echo "==> (2) full audit of the real chain — must PASS"
"$PY" hack/verify_chain.py --journal "$WORK/journal.db" --descriptor "$WORK/schema.binpb"

echo "==> (3) tamper a record and re-audit — must FAIL closed"
cp "$WORK/journal.db" "$WORK/tampered.db"
sqlite3 "$WORK/tampered.db" "UPDATE events SET hash='deadbeef' WHERE seq=2"
if "$PY" hack/verify_chain.py --journal "$WORK/tampered.db" --descriptor "$WORK/schema.binpb"; then
	echo "FAIL: verifier did not detect tampering" >&2
	exit 1
fi

echo "==> OK: independent verifier accepted the real chain and rejected the tampered one"
