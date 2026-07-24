#!/usr/bin/env python3
"""Independent (non-Go) verifier for the agentsessions tamper-evident integrity chain.

Two modes:

  (default / --golden)  Zero-dependency self-check: recompute the published golden content_hash from
                        the proto3-JSON + RFC 8785 (JCS) spec using only the stdlib. Proves the hash
                        FORMULA is language-neutral with no third-party packages.

  --journal PATH        Full audit of a REAL sqlite journal: parse each stored proto Event dynamically
                        (from a buf FileDescriptorSet), recompute content_hash exactly as the Go host
                        does (canon.HashRecord), and verify BOTH the content_hash and the prev_hash
                        chain-link of every record. Exits non-zero on the first tampered/broken record.
                        Requires: protobuf, jcs  (pip install -r hack/requirements.txt).

content_hash = lc_hex( SHA-256( JCS({ "event": proto3JSON(Event), "prev_hash": <hex>, "seq": <int-as-string> }) ) )
proto3-JSON uses proto field names (snake_case), enum names, and omits unpopulated fields — matching
the Go host's canon.marshalOpts (determinism contract §7). This is the "any auditor can verify the
chain" property: a second, independent implementation reproduces the hash from the raw bytes.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile

# The published golden event (canon.goldenEvent at prev_hash="" seq=1), built from the event
# definition — NOT copied from Go output.
GOLDEN_EVENT = {
    "execution_id": "exec-1",
    "schema_version": 1,
    "ts": "2023-11-14T22:13:20Z",
    "kind": "EVENT_OUTPUT",
    "message": {"role": "assistant", "parts": [{"text": {"text": "hello"}}]},
    "actor": {"principal": "agent://a", "issuer": "entra", "subject": "sub-1"},
}
GOLDEN_EXPECTED = "551bd146050c8d630b0c3b999a4445f3792a470db9bca443d8d4a67706283fcc"

EVENT_TYPE = "agentsessions.v1.Event"


def _stdlib_jcs(value):
    # RFC 8785 JCS reduces to this for our golden value set (ASCII strings, a small int, ASCII keys):
    # recursively sorted keys, compact separators, no ASCII escaping.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def golden_check():
    record = {"event": GOLDEN_EVENT, "prev_hash": "", "seq": "1"}
    digest = hashlib.sha256(_stdlib_jcs(record)).hexdigest()
    print("computed:", digest)
    print("expected:", GOLDEN_EXPECTED)
    if digest != GOLDEN_EXPECTED:
        print("MISMATCH: non-Go verifier did NOT reproduce the golden content_hash", file=sys.stderr)
        return 1
    print("OK: non-Go (Python) verifier reproduced the Go golden content_hash (stdlib)")
    return 0


def _event_class(descriptor_path):
    from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

    fds = descriptor_pb2.FileDescriptorSet()
    with open(descriptor_path, "rb") as fh:
        fds.ParseFromString(fh.read())
    pool = descriptor_pool.DescriptorPool()
    for f in fds.file:
        pool.Add(f)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName(EVENT_TYPE))


def _content_hash(event_cls, prev_hash, seq, blob):
    from google.protobuf import json_format
    import jcs

    ev = event_cls()
    ev.ParseFromString(blob)
    event_json = json_format.MessageToDict(ev, preserving_proto_field_name=True)
    record = {"event": event_json, "prev_hash": prev_hash, "seq": str(seq)}
    return hashlib.sha256(jcs.canonicalize(record)).hexdigest()


def _resolve_descriptor(path):
    if path:
        return path, None
    # No checked-in binary: build the descriptor fresh from the .proto via buf.
    tmp = tempfile.NamedTemporaryFile(suffix=".binpb", delete=False).name
    subprocess.run(["buf", "build", "api", "-o", tmp], check=True)
    return tmp, tmp


def audit(journal, descriptor_path, only_session):
    import sqlite3

    event_cls = _event_class(descriptor_path)
    con = sqlite3.connect(journal)
    if only_session:
        sessions = [only_session]
    else:
        sessions = [r[0] for r in con.execute("SELECT DISTINCT session FROM events ORDER BY session")]

    all_ok = True
    for s in sessions:
        rows = con.execute(
            "SELECT seq, prev_hash, hash, event FROM events WHERE session=? ORDER BY seq", (s,)
        ).fetchall()
        session_ok = True
        prev = ""
        for seq, stored_prev, stored_hash, blob in rows:
            link_ok = stored_prev == prev
            got = _content_hash(event_cls, stored_prev, seq, blob)
            hash_ok = got == stored_hash
            if not (link_ok and hash_ok):
                session_ok = False
                reason = "content_hash mismatch (TAMPERED)" if not hash_ok else "prev_hash link broken"
                print(
                    f"  FAIL session={s} seq={seq}: {reason} "
                    f"(stored={stored_hash[:12]} computed={got[:12]} link_ok={link_ok})",
                    file=sys.stderr,
                )
            prev = stored_hash
        all_ok = all_ok and session_ok
        print(f"session {s}: {len(rows)} records — {'VERIFIED' if session_ok else 'FAILED'}")

    if all_ok:
        print("OK: independent Python verifier reproduced + verified the full chain from raw proto "
              "(protobuf + RFC 8785 JCS) — any auditor can recompute this")
    else:
        print("TAMPER DETECTED: the stored chain does not match an independent recomputation", file=sys.stderr)
    return 0 if all_ok else 1


def main():
    ap = argparse.ArgumentParser(description="Independent verifier for the agentsessions hash-chain.")
    ap.add_argument("--journal", help="sqlite journal to audit (full cross-language mode)")
    ap.add_argument("--descriptor", help="buf FileDescriptorSet; if omitted, built via `buf build api`")
    ap.add_argument("--session", help="restrict the audit to one session UID")
    ap.add_argument("--golden", action="store_true", help="stdlib golden-vector self-check only")
    args = ap.parse_args()

    if args.journal and not args.golden:
        desc, cleanup = _resolve_descriptor(args.descriptor)
        try:
            return audit(args.journal, desc, args.session)
        finally:
            if cleanup and os.path.exists(cleanup):
                os.remove(cleanup)
    return golden_check()


if __name__ == "__main__":
    sys.exit(main())
