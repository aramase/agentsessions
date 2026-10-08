# Security posture

What `agentsessions` protects, what it does not, and how to deploy it accordingly.

This is the operator's view. For reporting a vulnerability, see [`SECURITY.md`](../SECURITY.md).

## The short version

> `agentsessions` protects the **integrity of the record**. It does not protect **access to the
> system**.

Everything below follows from that split. The project makes strong, testable claims about what a
session log says and whether it can be trusted. It makes no claims at all about who is allowed to
reach it, because as of this release nothing checks.

## What is protected

These are enforced in code and exercised by the conformance suite.

**The log is tamper-evident.** Every record is hash-chained over a canonical form (RFC 8785 JCS over
proto3-JSON), so altering, removing, or reordering a record breaks verification. The canonicalization
is language-neutral by design: `hack/verify_chain.py` is an independent Python implementation that
reproduces the Go hashes, so an auditor does not have to trust the Go binary that wrote the journal.

**A superseded writer cannot corrupt a session.** Each incarnation holds a fencing token, and the log
rejects an append carrying a stale one. A process that was presumed dead and then wakes up cannot
write. Combined with a compare-and-swap on append, that is what makes "single writer" enforceable
rather than aspirational.

**Replay does not re-execute side effects.** Reconstructing a session serves recorded model results
from the journal and never calls a provider, so replaying someone's session cannot spend their
budget or trigger a new external action.

**Payloads stay out of the logs.** Operational logging records structure — session ids, counts,
outcomes — and never message contents, event payloads, or fence values.

**Model credentials stay out of argv.** `agentsessionsd` reads the model key from the environment
and never accepts it as a flag, so it does not appear in the process list or shell history.

## What is not protected

**There is no authentication.** Every gRPC path in the reference implementation uses plaintext
transport with no credentials. Any process that can reach the port can call every RPC.

**There is no authorization.** `IdentityRef` exists in the contract and is carried on the wire, but
nothing verifies or enforces it. It is metadata today, not a control.

**`project` is not a tenancy boundary.** It is an exact-match filter on listing. A caller that names
another project gets that project's sessions. Do not treat it as isolation.

**There is no transport security.** No TLS anywhere, including between the host and a harness. Traffic
is readable and modifiable in flight by anything on the path.

**The harness is trusted code.** The host mediates model calls and host-executed tools, which is what
makes replay exact, but that is a determinism mechanism, not a containment one. Whatever isolation a
harness has comes from the `Runtime` backend underneath it, and the backends differ sharply:
`runtime/local` serves the harness **inside the host process** over a unix socket, so a harness there
shares the host's memory, filesystem, and credentials. A sandboxed backend is what puts a boundary
there; the local one has none.

**A harness registered by address is whatever answers there.** `runtime/remote` (`agentsessionsd
--harness name=address`) dials the operator-configured address over cleartext h2c with no
authentication of either side. Clients cannot supply an address; only the operator can, at startup.
Anything that can listen on that address, or sit on the network path to it, is the harness: it
writes every event the session journals, chooses which model calls the host makes and pays for with
its credential, and sees the full history the host sends each turn. The chain makes later tampering
with the journal detectable; it cannot tell a forged harness from the real one. Bind the harness to
loopback or a unix socket on the host, or keep the hop on a network you trust entirely. For a unix
socket, "can listen on that address" means "can write to the socket's directory", so put it in a
directory only the harness's user can write, not a shared one such as `/tmp`. `cmd/harnessnode`
refuses a lock file next to its socket that is a symlink, a hard link, or owned by another user, so
a hostile directory cannot redirect its writes, but it cannot stop another user who can write there
from binding the socket first. The host
does refuse a harness that declares `REQUIRES_MEMORY_SNAPSHOT`, but that is a correctness check on
what the harness declares, not authentication.

That check also has a known limit. The host checks a harness's capabilities with a `Describe` call
before it opens the turn's `Connect` stream, and the answer is not bound to that stream. If a
different harness takes over the same address between the two calls, for instance because the
harness restarted, gRPC reconnected to another replica, or a proxy routed the two calls to different
backends, the turn runs on that harness and the host does not detect it. Registering a harness by
address assumes the operator controls what serves that address, and that every harness that can
answer there declares the same resumability.

## Deploying it

Given the above, there is one safe shape for this release:

- Run it on **loopback**, or on a network segment you already trust entirely.
- Treat every host as **single-tenant**. One project per host if you need separation.
- Do not put it behind a public ingress, even an authenticated one, unless that proxy is doing
  **all** of the authentication and authorization and nothing else can reach the port.
- Keep the journal file's permissions tight. It holds full conversation contents and opaque execution
  config in the clear, and anyone who can read it can read every session. Do not put credentials in
  execution config; operational logging omits it, but the durable journal and Replay API expose it.
- Anyone who can **write** the journal can rewrite history. The chain makes that detectable, not
  impossible: verification tells you the log was altered, it does not prevent the alteration.

## What changes later

Authentication and authorization are tracked work, not a decision against them. The contract already
carries the pieces they need: `IdentityRef` is OIDC-shaped (`issuer`, `subject`, `principal`), so
enforcement is a matter of adding an interceptor that validates a token and checks it, rather than
reshaping the API.

Until that lands, this page describes the whole of the security model, and the honest summary is that
you should not expose a host you do not control the network around.
