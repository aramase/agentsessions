#!/usr/bin/env python3
"""Independent (non-Go) verifier for the agentsessions integrity chain.

It reconstructs the published golden event in proto3-JSON form — the SAME language-neutral mapping
the Go host uses (proto field names / snake_case, enums as names, RFC 3339 timestamp, unpopulated
fields omitted; determinism contract §7) — applies RFC 8785 JCS, and SHA-256s it. If a second
implementation reproduces the Go content_hash, the tamper-evident chain is verifiable by any
auditor, not just this repo's Go code. That is the provenance wedge ax lacks.

This dict is built from the event definition, NOT copied from Go output.
"""
import hashlib
import json
import sys

# content_hash = SHA-256( JCS({ "event": proto3-JSON(Event), "prev_hash": <hex>, "seq": <int64-as-string> }) )
EVENT = {
    "execution_id": "exec-1",
    "schema_version": 1,
    "ts": "2023-11-14T22:13:20Z",
    "kind": "EVENT_OUTPUT",
    "message": {
        "role": "assistant",
        "parts": [{"text": {"text": "hello"}}],
    },
    "actor": {"principal": "agent://a", "issuer": "entra", "subject": "sub-1"},
}
RECORD = {"event": EVENT, "prev_hash": "", "seq": "1"}

# Published Go golden vector (canon.goldenEvent at prev_hash="" seq=1).
EXPECTED = "551bd146050c8d630b0c3b999a4445f3792a470db9bca443d8d4a67706283fcc"


def jcs(value: object) -> bytes:
    # For this value set (strings + a small integer, ASCII keys) RFC 8785 JCS is exactly Python's
    # json with recursively sorted keys, compact separators, and no ASCII escaping. (Floats and
    # non-ASCII would need JCS's number/string rules; none appear here.)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def main() -> int:
    canonical = jcs(RECORD)
    digest = hashlib.sha256(canonical).hexdigest()
    print("canonical:", canonical.decode("utf-8"))
    print("computed :", digest)
    print("expected :", EXPECTED)
    if digest != EXPECTED:
        print("MISMATCH: a non-Go verifier did NOT reproduce the chain hash", file=sys.stderr)
        return 1
    print("OK: non-Go (Python) verifier reproduced the Go content_hash — the chain is language-neutral")
    return 0


if __name__ == "__main__":
    sys.exit(main())
