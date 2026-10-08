# Changelog

Release notes are generated from commits and published on the
[releases page](https://github.com/aramase/agentsessions/releases). This file records what a reader
needs to know beyond a commit list: what a release means for compatibility, and what it does not
provide.

## Unreleased

### Fixed

- Session-level suspend no longer destroys the Substrate actor needed by resume. Both replay-based
  and memory-snapshot sessions retain their restore handle after `Placer.Suspend`; explicit `Stop`
  remains destructive teardown.

### Added

- The chat harness accepts a per-execution `system_prompt` in its JSON config, prepended before
  conversation history and current inputs.
- `agentsessionsd --harness name=address` registers a harness that is already running, over TCP
  (`host:port`, `dns:///host:port` or `dns://resolver/host:port`) or a unix socket (`unix:` or
  `unix://` followed by an absolute or relative path), instead of compiling it into the server. Any
  other address form is refused at startup. It is served by the new `runtime/remote` backend, which
  owns no sandbox: such a harness must be `STATELESS_REPLAY`, and one that declares
  `REQUIRES_MEMORY_SNAPSHOT` is refused, including when it is only the replica that answers on a
  turn's own connection behind a load balancer. The check is not bound to the turn's `Connect`
  stream, so a harness that takes over the address between the two calls is not detected; the
  operator must control what serves the address. `--harness` requires `--model`, so a remote
  harness's model calls never fall back to the built-in echo stub.
  The connection is cleartext and unauthenticated; see `docs/security.md`. On unix platforms,
  `harnessnode` serves on a unix socket when `HARNESS_ADDR` is `unix:///path`. It holds a lock on
  `path.lock` while it serves, so a second instance on the same address fails, and on restart it
  reclaims a socket file left behind by a crashed instance. It never removes a socket another
  process created, and it refuses a lock file that is a symlink, a hard link, or owned by another
  user.

### Compatibility

- Overlapping `Exec`, `Suspend`, and `Resume` calls for one session across all Placers in one Registry
  now return gRPC `ABORTED` without changing compute or the journal. Retry after the in-flight operation
  finishes; other sessions remain independent. Direct Placer calls return `placement.ErrSessionBusy`.
  Standalone Placers have private guards; separate Registries or hosts are not fenced by this guard.
  Construct the Registry before using its Placers; registering already-used Placers or sharing them
  between Registries is unsupported. Idle session guard entries are reclaimed.

- `Runtime.Snapshot(..., SnapshotExternal)` owns the cold transition: capture state, release dedicated
  compute where applicable, and retain any handle needed by `Restore`. Out-of-tree runtimes that only
  capture state must implement that transition themselves; the Placer no longer calls `Stop` afterward.
  Suspended actors and fork-child snapshot pins are retained indefinitely: `DeleteSession` is
  unimplemented and no session teardown calls `Stop`. Operators must reclaim them directly in Substrate.
  No signatures or wire formats change, and restoring a snapshot after destructive `Stop` is not
  guaranteed.

- `controller.ToolFunc` now takes `controller.ToolCallContext` between `ctx` and `call`. Custom tool
  executors must update their Go signatures and scope authorization and durable deduplication to
  `ToolCallContext.SessionUID` plus the harness-chosen idempotency key. Placement binds the UID for
  Exec and Resume; `controller.New` rejects a non-nil tool executor without a nonempty
  `controller.WithSessionUID` with `controller.ErrMissingSessionUID`, before advancing the fence.
  This is a Go source compatibility change, not a wire or journal schema change.
- Resume now rejects a forked unfinished execution with unresolved inherited tool intents with
  `controller.ErrInheritedToolIntent`, rather than re-driving a parent's effect under the child's
  UID. This applies to existing journals, including legacy markerless turns; Fork still copies the
  prefix, and completed inherited intent/result pairs remain recoverable without invoking the
  executor, including legacy results recorded after the fork marker. `Sessions.Resume` reports
  `FAILED_PRECONDITION` for unresolved inherited intents with guidance to fork at or after the
  `TOOL_RESULT`, or Exec a new turn. No wire or journal schema change is required.
- Replay and interrupted-turn resume now reject tool-call identity and result-correlation mismatches
  that v0.1.0–v0.1.2 previously accepted. Resume also rejects harnesses that leave recorded effects
  unconsumed instead of marking the turn complete. Direct controller sinks can recover handled tool
  executor failures without re-executing nonterminal intents that have no result; Harness.Connect
  still ends the turn on a sink tool-call error. An immediately following in-harness TOOL_RESULT
  remains ambiguous without a failure receipt and is rejected if its ID differs from the preceding
  call. Handled pre-intent mediation rejections remain recoverable when no recorded tool call is next,
  without relaxing mediation identity checks on actual recorded calls.
- Chat executions written on `main` since execution-config journaling was added in #76 with a
  non-empty `system_prompt` no longer pass deterministic controller replay or interrupted resume
  when their recorded model-input hashes exclude the prompt. Non-JSON config now fails whenever
  the harness re-runs that execution. The chat harness previously ignored both. Reading recorded
  events through `Sessions.Replay` is unchanged, as is Resume of an already-completed turn.
  No tagged release is affected.
- Every new execution records `EXECUTION_START`, including default-config turns, so interrupted
  recovery can reject partially committed inputs. Binaries older than this release fail chain
  verification with a `content_hash` mismatch for sessions containing this event. Rollback is not
  supported for any session that executed a turn on this release, including existing markerless
  sessions that execute another turn after upgrading. No SQLite schema migration or log rewrite is
  required. Replay streams and harness `History` now carry one extra `EXECUTION_START` event per
  turn (in `History`, only prior turns).
- `ForkRequest.identity` is now rejected with `INVALID_ARGUMENT`, including an empty identity
  message, because per-child principals are not enforced. v0.1.0–v0.1.2 accepted this field,
  stored it on the child, and returned it from `GetSession` and `ListSessions`; callers must now
  omit it to fork. `Session.identity` on `CreateSession` remains recorded provenance, not
  authorization, and is unchanged.
- `Resume` and `Fork` now run the same `CanPlace` check as `Exec` before touching compute or the
  journal, and a refusal is `FAILED_PRECONDITION` (`Resume` previously returned `INTERNAL` for any
  placement error). A harness that cannot be reached to describe itself when the call is admitted
  is `UNAVAILABLE` rather than `INTERNAL` on `Exec`, `Resume`, and `Fork`, and nothing is
  journaled; one that has not answered within 10 seconds counts as unreachable, so a call with no
  deadline cannot be held by a harness that never answers. If the call's own deadline runs out, or the caller cancels, while the harness is being
  described, the call is `DEADLINE_EXCEEDED` or `CANCELLED` instead, also with nothing journaled. A
  harness lost mid-turn is still `INTERNAL` and leaves an interrupted turn to `Resume`. Forking a
  `REQUIRES_MEMORY_SNAPSHOT` harness on a backend without memory snapshots is refused before the
  parent is checkpointed, so it no longer leaves a `SUSPEND` event on the parent.
- The substrate backend now targets current agent-substrate (`fc0e3586`) instead of the `b1bd558a`
  pin, and the two are not wire-compatible: the old client decodes a current `Actor` without an
  error but with the wrong state and garbage for the worker address. Upgrade the backend and the
  substrate cluster together. `integrations/substrate` now requires Go 1.27, following substrate.
- The substrate backend reaches the harness through `atenet-router` (h2c, `ate-target-actor`
  metadata) instead of dialing the worker pod's IP on port 80, which current substrate no longer
  serves. The host must be able to reach `atenet-router.ate-system.svc:80`
  (`substrate.WithRouter` overrides it). The router does not authenticate callers; see
  [docs/security.md](docs/security.md) before running it outside a fully trusted cluster.
- `api.Incarnation` gains `CallMetadata`, which the host attaches to every harness call.
  `placement.Dialer` now takes the `api.Incarnation` instead of its address, so custom dialers
  passed to `placement.WithDialer` must change signature; `placement.DefaultDial` is the stock one.
- A stateful `Placer.Fork` now fences the parent's log and closes its open harness streams before
  checkpointing it, and refuses new turns on the parent until the checkpoint is recorded, so
  nothing holds the checkpoint up; on kind, a checkpoint under an idle stream took 5m30s. This holds
  across every Placer of a `placement.Registry`, so a turn that `ExecRequest.harness` routed to
  another harness is covered: `placement.NewRegistry` gives its Placers one shared per-session
  state and session guard, and now fails if a Placer already belongs to another registry or has
  an operation in progress. Three behavior changes follow:
  - Before it snapshots, the fork waits until every turn whose stream it closed has minted its
    fence or returned, bounded by the caller's context. If the context ends first, the fork fails
    before snapshotting and records nothing.
  - A fork that reaches its checkpoint supersedes the parent's in-flight turn even if the checkpoint
    then fails. The turn stops as an incomplete execution (no `ERROR` event), the parent stays live,
    and the caller runs the next turn or retries the fork at the new head.
  - A turn superseded by a fork's checkpoint, or started while one runs, returns an error wrapping
    the new `placement.ErrCheckpointing` (the superseded turn also wraps `eventlog.ErrFenced`).
    `Exec` and `Resume` report it as `ABORTED`, which callers retry.
- Through `atenet-router`, a harness stream that stays idle for about 5m30s (the router's default
  5m route timeout plus 30s) is reset with `RST_STREAM` (gRPC `Internal`; measured on kind at
  substrate `fc0e3586` with the stream held idle for 6m30s), so a turn parked on a slower model
  call fails. Raise the router's `--route-timeout` if turns can run that long.
- `runtime/substrate.ControlClient.ResumeActor` drops its `boot` argument (substrate removed
  `ResumeActorRequest.boot`); a new actor starts from its template's golden snapshot, or cold-boots
  if none is built. `SnapshotCloner` follows substrate's Tag API (`TagActor`,
  `CreateActorFromTag`, `DeleteTag`); `TagActor` must report a name already taken as the new
  `ErrTagExists`, so a fork whose tag create fails deletes only a tag it reserved.
  `ObjectRef.Namespace` is now `ObjectRef.Atespace`, and
  `ActorInfo` reports `Worker` and `Snapshot` instead of `PodIP` and `MeshDNS`.
  `integrations/substrate`'s `New` and `FromClient` drop their `dnsSuffix` argument, since nothing
  dials an actor's mesh DNS name any more. A stateful fork now
  checks the parent still holds the checkpoint it took around the tag, and fails with
  `ErrSnapshotSuperseded` if the parent was resumed and suspended in between.

## v0.1.2

A patch release. No API change; it exists because the v0.1.1 images were unusable.

### Fixed

- Container images are published under the documented names. v0.1.1 pushed them as
  `agentsessionsd-<md5>` and `harnessnode-<md5>`, because ko derives a repository from the Go import
  path and appends a hash unless told not to, so every name in the README and release notes returned
  404.
- Image tags carry the `v` prefix, matching the git tag and the release archives, so one string
  identifies a release everywhere.

## v0.1.1

A patch release. No API change; the reason to take it is the toolchain and the images.

### Security

- Built with Go 1.26.6. The v0.1.0 binaries were built with 1.26.0, which carries 23 reachable
  standard-library vulnerabilities including TLS, x509, and asn1 issues. Both modules now pin a
  toolchain floor so a release cannot ship an unpatched runtime again.
- gRPC 1.83.2, fixing two reachable denial-of-service vulnerabilities against a gRPC server
  (`GO-2026-6443`, `GO-2026-6348`).

### Added

- Container images, so a deployment no longer has to build its own:
  `ghcr.io/aramase/agentsessions/agentsessionsd` and `.../harnessnode`. Multi-arch, built with ko
  from the tagged source, each with an SBOM. The server runs as uid 65532.
- `golangci-lint` and `govulncheck` gate every change, on both modules.

### Fixed

- A failed `Serve` in the embedded CLI server and the local runtime backend discarded its error,
  leaving a dead server that surfaced later as a confusing client dial failure.
- The substrate manifests named a `ko://` placeholder that only the conformance workflow could
  resolve; they now name the published image and apply as written.
- Release notes link each commit. A bare 40-character SHA is not a link in a release body.

## v0.1.0

The first public release. Everything is new, so the useful summary is what the project does and what
it does not promise.

### What it does

- **Sessions API** over gRPC: create, list, get, exec, replay, fork, suspend, resume.
- **Deterministic replay.** Reconstructing a session serves recorded model results from the journal
  and invokes the model zero times, so replay is exact and free.
- **Tamper-evident provenance.** Every record is hash-chained over RFC 8785 JCS, which is
  language-neutral: an independent Python verifier reproduces the Go hashes.
- **Fork.** Branch a session into children that each continue independently from one checkpoint.
- **Bring your own harness.** Implement two methods, in process or over the `Harness.Connect`
  stream.
- **Pluggable compute** through the `Runtime` SPI, with capability-gated placement across
  replay-based and memory-snapshot-based resumption.
- **Live streaming.** Output relays as the model produces it. Deltas are transport only: never
  logged, never hash-chained, never produced on replay.
- **Model access** for any endpoint speaking the chat-completions body, with no vendor SDK.

### Compatibility

`agentsessions.v1` is a proto namespace, not a stability promise. The Go module is pre-1.0 and makes
no backward-compatibility guarantee; the wire contract and the Go SPI may both change. From this
release onward, the schema is gated against the previous release and breaking changes are called out
here.

### Not provided

- No authentication, authorization, or transport security. `IdentityRef` is recorded as provenance
  and never enforced, and `project` is a filter rather than a tenancy boundary. A host is not safe
  to expose to an untrusted network or to treat as multi-tenant. See
  [`docs/security.md`](docs/security.md).
- `DeleteSession` and `Cancel` are declared in the contract and not implemented.
- Harnesses register at build time, so adding one means building a server.
- The full session history is passed to the harness on every turn, which does not scale to long
  sessions.
