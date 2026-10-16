# Changelog

Release notes are generated from commits and published on the
[releases page](https://github.com/agent-primitives/sessions/releases). This file records what a
reader needs to know beyond a commit list: what a release means for compatibility, and what it does
not provide.

## Unreleased

### Security

- `model/openai` no longer sends configured headers (`WithHeader`, `WithAPIKey`) to another host on
  a redirect. net/http copied custom headers such as `api-key` to any redirect target, so a provider
  or proxy answering with a cross-host redirect received the credential. A redirect that changes
  scheme, host or port is now refused; same-origin redirects are followed as before.
- `agentsessionsd` refuses to start when a model API key is set (`MODEL_API_KEY` or
  `OPENAI_API_KEY`) and `-model-base-url` is plain `http` to a host other than a loopback address,
  `localhost`, or a name ending in `.svc` or `.svc.cluster.local` (case-insensitive, port ignored):
  the key and every prompt would cross the network unencrypted. The service-name allowance is a
  trusted-network exception: it checks the name only, not that it resolves to a cluster service or
  that the traffic stays in the cluster, and that traffic is still cleartext on the pod network. The
  error names the host and asks for `https`; it never prints the key. `https`, and `http` with no
  key, are unchanged. The check is in `internal/modelconfig.New`.

### Fixed

- A harness can now journal the usage of a host-mediated model call. `harnesswire` dropped
  `ModelResponse.Usage` in both directions, so a remote harness always saw zero usage, and Replay
  and Resume served a recorded completion with no usage, so a harness reporting the usage it was
  given diverged from its record. The host now sends `ModelResult.usage` (already in the proto) and
  the harness side reads it; Replay and Resume serve the `USAGE` recorded directly after the
  completion. A harness built against an older `harnesswire` ignores the field, and an older host
  sends none, which reads as zero usage. If a turn is cut between its `OUTPUT` and its `USAGE`,
  Resume serves zero usage and does not re-invoke the model, so that call's usage is journaled as
  zero (a harness that always reports the usage it was given), or not at all (one that skips zero).
  v0.1.2 journals replay as before, with no usage served.

- `model/openai` joins the completions path to the base URL's path instead of appending it to the
  string, so a base URL with a query (`-model-base-url 'https://h/v1?api-version=2'`) reaches
  `/v1/chat/completions` with the query kept, ahead of any query `-model-path` carries. A fragment
  is dropped. Before, the path landed inside the query or fragment and every turn went to `/v1`.
  `agentsessionsd`'s startup log names that joined endpoint (`openai.Client.Endpoint`), with a
  userinfo password masked, instead of the base URL and path concatenated.

- The substrate adapter (`integrations/substrate`) builds against agent-substrate `96dffb28`, which
  replaced `ActorStatus.external_snapshot` with a `snapshots` list (substrate `f03327bd`) and
  `SnapshotConfig.on_commit` with `preferred_fidelity`. The held snapshot is now the
  highest-generation entry whose durable copy is `COMPLETED`, the one substrate restores and tags,
  so a stateful fork keeps cloning the snapshot its suspend produced. Against an ate-api older than
  `f03327bd` the adapter reads no snapshot, so a stateful fork refuses as unplaceable (no external
  snapshot handle); see Compatibility. The CI cluster moves to the same commit, and the
  `deploy/substrate` WorkerPools use its `sandboxClasses` list in place of `sandboxClass`.
- Session-level suspend no longer destroys the Substrate actor needed by resume. Both replay-based
  and memory-snapshot sessions retain their restore handle after `Placer.Suspend`; explicit `Stop`
  remains destructive teardown.
- `Resume` recovers a turn cut between `MODEL_CALL` and its recorded completion, whether by a
  provider error or a process death. Previously every attempt failed with "recorded completion
  missing" and appended another `ERROR`, so the session could never resume. When the harness
  re-issues a request with the recorded input hash, the host invokes the provider once and records
  the completion after the existing `MODEL_CALL`; a different hash still fails as divergence. Each
  cut during a model call costs at most one extra provider call within one host, where the
  per-session guard serializes `Exec` and `Resume`; the bound does not hold across hosts sharing a
  journal. A fork at a `MODEL_CALL` costs one provider call when the child resumes. A recovering
  call that fails is itself a cut, and the next `Resume` re-drives it again. Journals with
  execution IDs that are in this state recover on the next `Resume`; their earlier `ERROR` events
  remain as audit records. A v0.1.2 journal without execution IDs is not re-driven and still fails
  with "recorded completion missing". No journal or wire schema change.
- The `Start` comment in `harness.proto`, `api.Start.History`, the API reference, and the harness
  authoring guide said `Start.history` is empty after a memory restore. The host sends every event
  before the execution's start on every turn, including after a restore; `Exec` has done so since
  v0.1.0. A harness must not treat an empty history as a sign that it was restored. Documentation
  only.
- `wire` decodes a file part's `bytes` source or a reasoning part's `opaque_bytes` that is set to
  empty bytes as no bytes (nil), the same as the encoders, which never send an empty source.
  Previously it decoded to a non-nil empty slice, while the journal stores the encoded form without
  it, so an `Exec` input carrying such a part (an empty file) gave the live turn a different model
  input hash (I0) from the one recomputed on the journal's copy. For a harness running in the host
  that sends its inputs to the model (`chatagent`), the turn ran, but `Replay` of it once completed,
  or a `Resume` of it cut after its model call was recorded, failed with "model input hash
  mismatch". New turns replay; a turn journaled before this change still fails those checks, because
  its recorded hash covers the empty value. No journal or wire schema change. The hash algorithms
  and the hashes of existing records are unchanged; a new turn with such a part records a different
  model input hash than it would have before, and so a different `content_hash` on that record and
  every later one in the chain.

### Added

- Optional `ResumeRequest.expected_last_seq` rejects a moved journal head before re-driving a turn.
- Go SDK `Client.DeleteSession(ctx, uid)` and `agentctl delete --session <uid>` request permanent
  soft deletion. Get/delete CLI report deletion state and requested time; PENDING output explicitly
  says durable intent only, with compute and fork pins retained.
- Controller `InspectApproval` and decision-only `Approve` validate durable request/decision/receipt
  evidence, including exact retries and session-owned versus inherited effects. The internal
  approval engine supports captured-head repair and correlated denial/executor-error receipts.
  Live `REQUIRES_APPROVAL` calls now park stateless executions at a durable host-owned request.
  Placement closes the invocation, snapshots externally and commits SUSPEND before reporting
  a successful pause; transport/close/snapshot/append failures remain errors with the request
  recoverable.
- `Harness.Connect` bridge supports the stateless approval exchange: full session/execution/call
  correlation, park acknowledgement plus actual stream closure, and decision-before-receipt order.
  A transport park alone is not a successful public Session pause.
- `Sessions.Approve` and `client.Client.Approve` commit only a decision; call Resume separately.
  CLI `approve` and `deny` commit then Resume, explicitly preserving the committed decision on
  recovery failure. Identity is recorded provenance, not authorization. Cancel remains
  `UNIMPLEMENTED`; unresolved requests and retained snapshots have no expiry.
- Every Session reply and initial/final approval Exec frame uses captured metadata/cursor and
  the storage projection. Pending owned requests report AWAITING with `pending_approval`;
  clearing a reference after a decision does not prove a receipt or completion. SDK Exec retains
  the latest Session and maximum committed cursor, including partial failure.
- Approval wire types provide correlated request references, explicit receipt statuses/sequences
  and transport-only park acknowledgements without changing old field numbers or default hashes.
- SQLite `SessionInfo` captures the journal-derived `last_seq` and a reporting-only
  `PendingApproval` with metadata, using an incremental approval projection in the append
  transaction. Calls whose `TOOL_CALL` precedes the child's latest `FORK` are excluded, as are
  invalid approval-bearing histories and projections lagging the journal head; the reference
  is not command authority or a runtime/integrity probe.
- `agentctl replay -v` also prints each record's content hash and previous hash (16 hex), the
  execution id, a model call's model and input hash, and a usage record's token counts. Without
  `-v` the output is unchanged.
- Go SDK callers can pass opaque per-execution config through `client.ExecOptions.Config`.
  Omitting it keeps the existing empty-config behavior. Config is durable journal content exposed
  by Replay; do not include credentials. No wire or journal schema change.
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
- `HarnessRegistry` API (`api/harness_registry.proto`): register, get, list and retire harnesses on a
  running host. A registration is immutable and identified by its name and `spec_digest`. Retiring a
  harness refuses new sessions while existing sessions keep running, resuming and forking;
  registering the same spec again reactivates it. When a spec sets `descriptor_id`, `Exec`, `Resume`
  and `Fork` are refused with `FAILED_PRECONDITION`, before anything is journaled, if the harness
  reports another id (`placement.WithDescriptorID`). A harness added with `placement.Registry.Add`
  shares the Registry's session guard, so overlapping calls for one session through it and a static
  harness are `ABORTED` too. It is an admin API available in-process through
  `session.NewHarnessRegistry` and, optionally, on `agentsessionsd`'s separate loopback operator
  listener (see [security.md](docs/security.md)). `agentctl harness register|get|list|retire`
  uses that listener with an explicit `--server` address.

### Compatibility

- Breaking for turns journaled from a remote harness that sent an empty bytes source. A remote
  harness model request whose file part sets `bytes`, or whose reasoning part sets `opaque_bytes`,
  to empty bytes now hashes the same as one with no source. A turn such a harness journaled before
  this change recorded its model input hash (I0) over the empty value. `Replay` of that turn once
  completed now fails with "model input hash mismatch", as does a `Resume` of such a turn cut after
  its model call was recorded; `Resume` of a completed turn is a no-op and does not check. The
  reverse also holds: such a turn journaled after this change fails the same checks on a host
  without it. No known harness sends an empty source: `harnesswire` omits it, so only a harness that
  builds the request proto itself could. There is no migration for such a turn.
- Optional `ResumeRequest.expected_last_seq` checks the journal head under the session guard
  before inspection, after an early fence before restore/approval repair, and after a restored
  turn's regular writer fence before Run. A mismatch is `ABORTED` with expected/head values;
  explicit zero requires an empty journal. A late mismatch may already have restored compute,
  but does not run the harness or append records. This catches other hosts' commits before each
  acquired fence. Acquiring a fence supersedes older writers even if Resume then refuses: their
  remaining writes fail with `ErrFenced`, leaving any unanswered tool intent eligible for retry.
  Fencing cannot cancel effects already in flight or exclude a later writer; this is not an
  execution lease or exactly-once tool execution. Omission preserves existing behavior.
- Older servers silently ignore `ResumeRequest.expected_last_seq`; clients must not assume the
  check ran without operator-confirmed deployment of a supporting server version/build. Sessions
  exposes no server-version/capability query, so clients need out-of-band deployment provenance;
  automatic support discovery is follow-up work.
- The Go module path is now `github.com/agent-primitives/sessions`; v0.1.x remain available at
  `github.com/aramase/agentsessions`. Container images move to `ghcr.io/agent-primitives/sessions`.
  One binary cannot link both module paths: both register the same proto files, and protobuf-go
  panics at init (`file "common.proto" is already registered`). Move every dependency together.
- <a name="sqlite-v4-rollout"></a>
  `DeleteSession` permanently retains the UID, metadata and hash-chained journal for audit, hides
  the session from List and fences further mutations, including historical approval retries and
  forks. Get and raw Replay remain available; children are independent. PENDING acknowledges only
  durable intent: no Runtime.Stop, cleanup or resource reclamation is attempted. Actors, snapshots
  and fork pins remain retained until ownership/cleanup work (#88).
  SQLite schema v4 adds the tombstone state, timestamps and audit anchor after approval v3 in one
  transaction without rewriting old events or hashes; existing sessions stay ACTIVE. The
  `deletion_completed_at` column is reserved for cleanup (#88): this build never reads or writes
  it, and no completion state or time is exposed in Go or on the wire. Any stored deletion state
  other than ACTIVE or PENDING is treated as deleted.
  Older builds refuse v4 at open time only. Already-open older writers can still append, mint
  fences and update metadata after DELETE because they do not enforce tombstones. Before the
  first v4 open, stop every older host and embedded `agentctl` process sharing the journal;
  upgrade all writers before restarting them. Back up before upgrading. The new LIFECYCLE_DELETE
  hash requires a deletion-aware reader. Cancel remains unimplemented.
- An `ExecRequest.harness` override cannot involve a `REQUIRES_MEMORY_SNAPSHOT` harness: when
  either the session's served stored harness or the override requires memory snapshots, `Exec` is
  `FAILED_PRECONDITION` (`placement.ErrStatefulOverride`) before any compute is provisioned, fence
  minted or journal written. Parking only an override of a memory-tier session cannot checkpoint
  the stored harness's live memory, and Suspend and Fork follow the session's own harness, so they
  would neither checkpoint nor clone the RAM a memory-tier override leaves behind. `Resume` and
  no-input Exec recovery also refuse an older interrupted override before compute changes or
  journal repair; an ordinary interrupted turn can be superseded by a new Exec on the stored
  harness. An override onto a name that aliases the session's own registry entry is not an
  override and still runs. Stateless-to-stateless overrides are unchanged: a stored runtime without
  memory-snapshot support is not contacted for this check, even when its remote harness is down.
- `Approve` and a stateful `Fork` on the same session now exclude each other through the shared
  checkpoint mark. A refused approval returns `ABORTED` without minting a fence or writing a
  decision; retry after the fork finishes. This coordination covers one Registry/host only.
- Ordinary `Resume` and `Suspend` now return `ABORTED`, rather than `INTERNAL`, for journal CAS
  conflicts (`eventlog.ErrConflict`) and superseded fences (`eventlog.ErrFenced`).
- Owned unreceipted approval calls block new input, including after a decision. No-input Exec
  recovers the existing current approval invocation with its recorded config, inputs, route and
  cursor, checking CAS/deadline under the guard before runtime IO or request repair. Unrelated
  inputless Exec retains ordinary new-turn behavior. Pending Resume queries do not touch compute;
  recorded-route Suspend can retry failed automatic handoff for owned approvals, with no
  stored-default fallback. Inherited approvals suspend through the child's stored-default harness
  so its own compute is checkpointed; Resume still refuses unresolved inherited tool intents.
  Gated executor failures become handled EXECUTOR_ERROR receipts for model continuation; a
  missing executor is a recoverable precondition, not a receipt. Ordinary tool errors are unchanged.
- Controller Start now carries the session UID on live, replay and resumed invocations, and the
  bridge includes it on host control frames. Hosts serving tool results must echo the Start
  session as well as execution/call correlation; omitted or different tool-frame namespaces
  are rejected, including for ordinary tools. Model reply namespace validation is unchanged.
  Harness-emitted host-owned request/decision/execution-start records and unsolicited PARKED
  acknowledgements are rejected by the bridge rather than forwarded to the host sink.
- Controller and bridge sink methods are serialized per invocation and refuse use after Run
  closes. A recorded-prefix identity/correlation mismatch remains fatal even if the harness
  handles the error; it cannot enable later effects or successful completion. Callbacks must
  not re-enter the same sink. Ordinary handled pre-intent rejections and operational executor
  errors retain their continuation behavior.
- Controller and bridge sink calls now honor their own context cancellation, both while queued
  behind another effect and when an uncontended call arrives with an already-canceled context.
  Canceled calls start no effects; Replay and Resume also refuse such calls against older journals
  without consuming recorded evidence. An uncontended call still returns an existing sticky
  park/failure before cancellation. Request/reply serialization and Run shutdown drainage are
  unchanged.
- SQLite schema version 3 adds the approval projection and captured cursor. Opening a version 2
  database backfills them atomically without changing event bytes or hashes. An undecodable v2
  session is isolated with no actionable reference; healthy sessions remain readable. SQL faults
  roll back the migration, and the older version 1 namespace migration still fails closed.
  Builds supporting at most version 2 refuse the upgraded database; back it up before upgrading
  if rollback may be needed. An already-open older writer can still append without updating
  the projection: `last_seq` stays current, but references remain suppressed for that session
  rather than skipping missing evidence. Direct out-of-band edits do not refresh the projection.
- Approval additions preserve old field numbers, default receipt/decision bytes and model-input
  hashes. New durable receipt status/correlation fields are hash-bound; transient Go approval
  carriers are excluded from wire receipts and model-input hashing. Low-level Append refuses
  transport-only `PARKED` events but otherwise remains permissive; projection suppression does
  not replace validation at command boundaries. Remote harness reports with host-owned approval
  receipt fields or codes other than `CODE_UNSPECIFIED`/`EXECUTOR_ERROR` are rejected by
  approval-aware Report validation; `EXECUTOR_ERROR` remains accepted on harness reports.
- `agentsessionsd -registry-addr` is off by default. Enabling it creates a plaintext,
  unauthenticated **local admin surface** on a separate literal-loopback listener; Sessions still
  refuses registry RPCs, and neither listener gains TLS or authentication. Any local or same-pod
  process that can reach it can administer harnesses. The daemon now loads persisted registrations
  even when the listener is off, so existing sessions can still use retired harnesses after restart
  with `-model` set. Both new and stored remote registrations require `-model`; without it, stored
  remote registrations load as unservable and their existing sessions fail `FAILED_PRECONDITION`,
  whether active or retired. Inspecting, listing and retiring do not require `-model`; substrate
  placements are unsupported by this reference daemon. No Sessions transport/access guarantees
  change.
- `model/openai` refuses a redirect to another origin instead of following it. Set the base URL
  to the endpoint that serves the request. A client passed to `WithHTTPClient` is copied, not
  modified; its own `CheckRedirect` still runs for same-origin redirects.
- An unsupported stored harness registration (unknown spec fields or enum values, or a local
  placement/factory failure) no longer prevents other harnesses from starting. Its name stays
  reserved, and Get/List/Retire retain its metadata, original digest and ACTIVE/RETIRED state.
  `HarnessRegistration.unservable_reason` explains this host's inability to run it; an undecodable
  spec is unset, not partially returned. Existing sessions on a registration isolated during this
  host's startup load and new attempts to use it fail `FAILED_PRECONDITION` until this host restarts
  on a compatible version. A registration added to a shared journal after this host started remains
  visible but is unknown to this host's Sessions routing: Create and auto-create Exec selecting it
  remain `INVALID_ARGUMENT` (a pending recorded Resume retains its `FAILED_PRECONDITION` recovery rule)
  until this host restarts or loads it through `RegisterHarness` with the identical compatible spec.
  For a RETIRED row, restart this host to serve its existing sessions without reactivating it:
  `RegisterHarness` reactivates the row for every host sharing the journal.
- `openai.New` refuses a base URL that is not absolute (no scheme or host), so `agentsessionsd`
  exits at startup with `-model-base-url` set to one. Such a URL used to fail every request.
  It also refuses a query parameter that both the base URL and the path (`WithPath`,
  `-model-path`) set, such as an `api-version` in each, which was sent twice; set it in one of
  them. The error names the parameter, never its value.
- `agentsessionsd` exits at startup with a model API key and a plain-`http` `-model-base-url` to a
  host that is not loopback, `localhost` or a `.svc` name (see Security). Switch such an
  endpoint to `https`, or unset the key, before upgrading.
- `runtime/substrate` no longer cold-creates an actor for a `REQUIRES_MEMORY_SNAPSHOT` session that
  has a started turn on that harness (completed, failed or interrupted) but no actor (deleted out of
  band, or a host restarted with another atespace). `Exec` fails with
  `FAILED_PRECONDITION` (`api.ErrMemoryStateLost`) and nothing is created, where it used to continue
  the journal on a fresh harness that had lost the session's RAM state. `Runtime.Create` gets a
  journal counter in the new `api.SessionSpec.StartedTurns`, which `runtime/substrate` calls only
  after it finds the actor missing, so a turn another host journaled before then is counted; other
  backends ignore it. The count is per harness: a turn an `ExecRequest.harness` override ran
  elsewhere does not count, while a turn with no recorded harness name, or one this host does not
  serve, does. Two substrate backends on one atespace share each session's actor, so the count
  misses the turns the other one ran there: hosts sharing a journal and an atespace must serve the
  same substrate harness, and a program that builds its own registry must use one. `Sessions.Resume` of any
  memory-tier session whose actor is gone, whatever its journal records, is `FAILED_PRECONDITION`
  (`api.ErrMemoryStateLost`) too, where it used to be `INTERNAL`; a stateless session's `Resume`
  keeps its error.
- A memory-tier turn on substrate records its actor's UID in the new
  `EXECUTION_START.compute_instance` (`api.Incarnation.ComputeInstance`, set by `runtime/substrate`
  from the new `substrate.ActorInfo.UID`). `Exec` or `Resume` placed on an actor whose UID differs
  from the latest one recorded for that harness since the session's `FORK` marker is
  `FAILED_PRECONDITION` (`api.ErrMemoryStateLost`) and journals nothing. This catches an actor
  another host created under the session's name after it counted no turns, while the actor the first
  turn ran on was lost; the count alone let later turns attach to it. `Suspend` and a stateful
  `Fork` of such a session, or of one whose actor is gone, are `FAILED_PRECONDITION` too (they were
  `INTERNAL`, and `Suspend` of a replaced actor succeeded), and suspend, clone and journal nothing:
  the Placer passes the recorded UID to `Runtime.Snapshot` in `api.Incarnation.ComputeInstance`, and
  `runtime/substrate` refuses another actor or a missing one, checking the UID before it suspends
  and the one the suspend reports. A memory-tier fork child whose cloned actor is gone before its
  first turn, and a stateful `Fork` whose parent actor is deleted around or during the tag, are
  `FAILED_PRECONDITION` the same way; a tag that substrate refuses because
  another operation holds the parent is `ABORTED`, and one whose parent was suspended again while it
  ran is `ABORTED` (`api.ErrSnapshotSuperseded`). `runtime/substrate` refuses a memory-tier actor that
  reports no UID (`INTERNAL` from `Snapshot`, a fork child before its first turn included), so a
  custom `ControlClient` must report one from `GetActor`, `ResumeActor` and `SuspendActor`, and must
  report a missing actor as `substrate.ErrActorNotFound` from `SuspendActor` as well as `GetActor`
  (the `integrations/substrate` adapter now maps substrate's `NOT_FOUND` from both, and from
  `ResumeActor`), or a Suspend or Fork whose actor is deleted between the check and the suspend is
  `INTERNAL`. It should report it from `ResumeActor` too, and the backend checks it with `GetActor`
  before reading it as the actor being gone, since substrate also answers `NOT_FOUND` for an actor
  whose template lacks a volume: such an actor keeps the resume error. Otherwise a `Resume` whose
  actor is replaced between the failed resume and `Restore`'s `GetActor` is `INTERNAL`; with it, a
  replacement that does not hold the session's snapshot is `FAILED_PRECONDITION`. For a session with
  no `SUSPEND` recorded there is no snapshot to compare, so the backend compares the actor's UID with
  the one the journal last recorded, read through the new optional `api.InstanceRestorer`, which the
  Placer uses in place of `Restore`: only another UID is `FAILED_PRECONDITION`, and the recorded
  actor, or one with no UID recorded to compare, keeps the resume error. An `Exec` whose
  `SUSPENDED` actor is deleted before its resume is treated as a missing actor
  (`FAILED_PRECONDITION` for a memory-tier session with started turns, a new actor otherwise), and
  one replaced there, or deleted again after `CreateActor` found the name taken, is `ABORTED`; both
  were `INTERNAL`. A `Placer` outside a `placement.Registry` now counts every started turn, and
  compares every recorded UID, whatever harness name the turn recorded, since it cannot tell another
  name's backend from its own.
  `ControlClient.SuspendActor` now returns the suspended actor's `substrate.ActorInfo` (snapshot
  handle and UID) instead of the snapshot handle alone. When `CreateActor` finds the name taken
  (`substrate.ErrActorExists`, which a `ControlClient` must return for it), `Create` attaches to
  that actor where it used to fail the turn with `INTERNAL`. Stateless turns record no UID and their
  canonical bytes are unchanged; turns recorded before this change are not compared. The router
  reaches an actor by name, so a turn already placed when its actor is replaced still runs; the next
  one is refused. `compute_instance` is part of the record's canonical bytes. A binary built before
  this change drops the field when it reads the record, so it fails chain verification with a
  `content_hash` mismatch on any session with a memory-tier substrate turn, as older binaries do for
  `EXECUTION_START` itself. Rollback is not supported for such sessions, and every host sharing a
  journal must run this release before any of them runs a memory-tier substrate turn. This change
  adds no SQLite schema migration (`user_version` stays 4, from soft deletion above), so a version 4
  binary from before this change still opens the journal and reports only those sessions as
  tampered; a build that supports at most version 3 refuses the journal at open, as it does any
  version 4 database.
- A memory-tier fork child's `FORK` marker records the UID of the actor it was cloned onto, in the
  new `Lifecycle.compute_instance` (`api.Lifecycle.ComputeInstance`, from the `ComputeInstance` of
  the incarnation `Runtime.Fork` returns; Go callers use the new
  `controller.ForkWithComputeInstance`). `SnapshotCloner.CreateActorFromTag` now returns the created
  actor's `substrate.ActorInfo` instead of only an error, so a custom `ControlClient` must report
  the clone's UID from it: `runtime/substrate` records that UID and refuses a fork whose
  `ResumeActor` reports another one (the clone deleted and replaced under its name while
  `SUSPENDED`) with `ABORTED` (`substrate.ErrActorBusy`) and records no child; the replacement may
  have been woken by that resume and is left running, not suspended or deleted. A clone reported
  without a UID is torn down and the fork fails. Until the child's first turn records its own UID,
  `Exec`, `Resume`, `Suspend` and a stateful `Fork` of the child compare its actor with that UID, so
  an actor created under the child's name before its first turn is `FAILED_PRECONDITION`
  (`api.ErrMemoryStateLost`) and nothing is journaled, snapshotted or cloned, where the replacement
  used to be treated as holding the cloned state. A stateless fork records no UID. A `FORK` marker
  written before this change records none either, so such a child keeps the old behavior: its first
  turn is not compared. An empty field is omitted from the canonical bytes, so those markers hash as
  before; a set one is part of them (new golden vectors in `canon` and `hack/verify_chain.py`).
  Downgrade: v0.1.0 through v0.1.2, and any build that supports at most schema version 3, refuse the
  journal at open (`unsupported database schema`, since it is schema version 4), so they never read
  the field. A build from before this change that opens version 4 drops the field on read: it does
  not compare the child, its `Verify` fails with a `content_hash` mismatch on such a child, and a
  `Fork` it takes of such a child copies the prefix without the field, so the new child no longer
  shares its parent's prefix hashes. As for `EXECUTION_START.compute_instance`, upgrade every host
  sharing a journal before any of them forks a memory-tier session; rollback is not supported for
  such children.
- An actor another operation holds is now retryable: `Exec`, `Resume`, `Suspend` and `Fork` are
  `ABORTED` (new `api.ErrComputeBusy`) where they used to be `INTERNAL` when `runtime/substrate`
  finds the session's actor `RESUMING`, `SUSPENDING` or `PAUSING` (new
  `substrate.StatusTransitioning`), or when substrate refuses a resume or suspend with `ABORTED`
  because another operation holds the actor's lease. The adapter reports the latter as the new
  `substrate.ErrActorBusy`; a `ControlClient` that returns a raw transport status still gets
  `INTERNAL`. This is the usual outcome for the host that loses a race for a session's first turn
  while the winner's actor is still resuming.
- New `EXECUTION_START` records include the resolved registry harness name and optional advertised
  harness version (`HarnessDescriptor.version`). Interrupted `Sessions.Resume` uses the recorded
  registry entry, not the session default; an unserved recorded entry or known-version mismatch is
  `FAILED_PRECONDITION`. Session metadata and later default Exec selection are unchanged, as are
  completed Resume and Suspend/Fork routing. Older markers without a name fall back to session
  metadata; missing/empty recorded versions keep existing unversioned behavior. New Exec still
  accepts unversioned harnesses. Nonempty recorded versions must exactly match the served version,
  including rejection of served empty. Controller Replay preflights all completed invocations
  before running its single supplied harness; raw `Sessions.Replay` is unchanged. Remote checks
  remain Describe-based, not bound to the Connect stream. The reference harnesses advertise `"1"`,
  independent of host build/release versions. No journal rewrite or SQLite migration is required;
  binaries missing these new fields cannot verify hashes of records where they are populated.
- Controller Replay and Resume accept the ID-less journals written by v0.1.2 without rewriting
  existing records or hashes. Pure legacy Replay retains v0.1.2's whole-log harness invocation,
  including its multi-turn replay limitation. Resume retains the last-INPUT rule and continues
  interrupted legacy turns with ID-less events; END or ERROR finishes a legacy turn. Original
  config, resume cursor and input completeness remain unavailable. Harness invocations use a
  deterministic `legacy-<sha256>` transport-only ID derived from host-bound session UID, original
  record position and canonical content; it is not journaled. Direct controller callers must supply
  a nonempty `WithSessionUID` for selected legacy invocations, or receive `ErrMissingSessionUID`
  before the harness runs. Exec permits a new modern turn after an unfinished or errored legacy
  tail. Its EXECUTION_START establishes the boundary: mixed Replay preserves the complete legacy
  prefix's whole-log semantics and skips the abandoned tail, while modern History retains the
  original bytes. Only the ID-less event kinds the released writer persisted qualify: ID-less
  start markers and non-legacy kinds are invalid. Once a modern start marker or ID-bearing
  execution event appears, later ID-less execution events still fail closed. Remaining invalid-log
  refusals are FAILED_PRECONDITION. Inherited-tool safety checks and rollback restrictions remain.
- Overlapping `Exec`, `Suspend`, and `Resume` calls for one session across all Placers in one Registry
  now return gRPC `ABORTED` without changing compute or the journal. Retry after the in-flight operation
  finishes; other sessions remain independent. Direct Placer calls return `placement.ErrSessionBusy`.
  Standalone Placers have private guards; separate Registries or hosts are not fenced by this guard.
  Construct the Registry before using its Placers; registering already-used Placers or sharing them
  between Registries is unsupported. Idle session guard entries are reclaimed.

- `Runtime.Snapshot(..., SnapshotExternal)` owns the cold transition: capture state, release dedicated
  compute where applicable, and retain any handle needed by `Restore`. Out-of-tree runtimes that only
  capture state must implement that transition themselves; the Placer no longer calls `Stop` afterward.
  Suspended actors and fork-child snapshot pins are retained indefinitely: `DeleteSession` records
  durable intent only and no session teardown calls `Stop`. Operators must reclaim them directly
  in Substrate.
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
- `Resume` can now spend a provider call where it previously failed: a turn whose last effect is a
  `MODEL_CALL` with no recorded completion is re-driven with one provider call instead of failing
  with "recorded completion missing". This includes the child of a fork taken at a `MODEL_CALL`. No
  wire or journal schema change.
- Replay and interrupted-turn resume now reject tool-call identity and result-correlation mismatches
  that v0.1.0–v0.1.2 previously accepted. Resume also rejects harnesses that leave recorded effects
  unconsumed instead of marking the turn complete. Direct controller sinks can recover handled tool
  executor failures without re-executing nonterminal intents that have no result; Harness.Connect
  still ends the turn on a sink tool-call error. An immediately following in-harness TOOL_RESULT
  remains ambiguous without a failure receipt and is rejected if its ID differs from the preceding
  call. On direct controller sinks, handled pre-intent mediation rejections remain recoverable when
  no recorded tool call is next, or the next call has a different ID: reject the unsupported call
  without consuming the fallback's recorded intent. Same-ID mismatches and malformed evidence
  remain fatal, without relaxing identity checks on actual recorded calls. No wire/schema change.
- Chat executions written on `main` since execution-config journaling was added in #76 with a
  non-empty `system_prompt` no longer pass deterministic controller replay or interrupted resume
  when their recorded model-input hashes exclude the prompt. Non-JSON config now fails whenever
  the harness re-runs that execution. The chat harness previously ignored both. Reading recorded
  events through `Sessions.Replay` is unchanged, as is Resume of an already-completed turn.
  No tagged release is affected.
- The SQLite journal schema moves from version 1 to version 2: `harnesses` and
  `reserved_harness_names` tables and an index on `sessions(harness)`. A v0.1.x database migrates
  in place, in one transaction, the first time this release opens it. The migration reserves every
  harness name an existing session uses, since all of them were built-in, so no host can register
  one later. Downgrade is refused: v0.1.0 through v0.1.2 fail to open a version 2 database
  with "unsupported database schema". Back up the journal before upgrading if you may need to roll
  back. A database stamped with a negative schema version is refused.
- `ExecRequest.harness` cannot move a session onto or off a registered harness for one turn; that is
  `FAILED_PRECONDITION`. Overrides between static harnesses (built-in or given with `--harness`)
  are unchanged.
- The `echo` and `chat` names are reserved for the built-in harnesses, including `chat` on a host
  started without `--model`, and so is every name given to `agentsessionsd --harness`.
  `agentsessionsd` and embedded `agentctl` record the names they run in the journal at startup, so
  no host sharing it can register them later, and refuse to start on a journal that already holds a
  registration, active or retired, under one of them. Opening a journal with either binary
  therefore writes to it. A reservation is permanent.
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
- `ResumeRequest.boot` is deprecated. The service never read it, and substrate no longer has a
  per-resume boot flag; the backend decides how a resumed session comes back. The field keeps its
  number, so existing clients still encode. `client.Client.Resume` still sends its `boot` argument
  in the field; the reference service ignores it.
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
  checkpointing it, and refuses new turns on the parent until every child is cloned from the
  checkpoint, so nothing holds the checkpoint up or replaces it before the last clone; on kind, a
  checkpoint under an idle stream took 5m30s. A fan-out that fails partway releases the parent
  before it tears the cloned children down. This holds across every Placer of a
  `placement.Registry`, so a turn that `ExecRequest.harness` routed to another harness is covered:
  `placement.NewRegistry` gives its Placers one shared per-session state and session guard, and
  `placement.Registry.Add` gives a registered harness's Placer the same, so a fork through a static
  harness covers a turn on a registered one and the reverse. Both now fail if a Placer already
  belongs to another registry or has an operation in progress. Five behavior changes follow:
  - Before it snapshots, the fork waits until every turn whose stream it closed has minted its
    fence or returned, bounded by the caller's context. If the context ends first, the fork fails
    before snapshotting and records nothing.
  - A fork that reaches its checkpoint supersedes the parent's in-flight turn even if the checkpoint
    then fails. The turn stops as an incomplete execution (no `ERROR` event), the parent stays live,
    and the caller runs the next turn or retries the fork at the new head.
  - A turn superseded by a fork's checkpoint, or started while one runs, returns an error wrapping
    the new `placement.ErrCheckpointing` (the superseded turn also wraps `eventlog.ErrFenced`).
    `Exec` and `Resume` report it as `ABORTED`, which callers retry.
  - Stateful forks and `Suspend` of one session do not overlap; they hold the same checkpoint
    mark. A fork or `Suspend` that arrives while a fork checkpoints the parent or clones its
    children, or a fork that arrives while a `Suspend` runs, is refused before it fences or
    snapshots anything, with an error wrapping `placement.ErrCheckpointing`, which `Fork` and
    `Suspend` report as `ABORTED`.
  - The hold starts when the fork installs its checkpoint mark and covers the turns this
    `placement.Registry` drives. A turn whose `Runtime.Create` or `Runtime.Restore` was already in
    flight when the mark was installed, or a request from outside this Registry, can still wake the
    parent before a child is cloned. A runtime that finds the fork source superseded returns an
    error wrapping the new `api.ErrSnapshotSuperseded`, which `Fork` reports as `ABORTED`; callers
    retry at the parent's new head.
- The substrate conformance suite (`integrations/substrate/e2e`) runs both tiers on the micro-VM
  sandbox class by default; the stateless tier used gVisor before. A host without KVM opts each
  tier back into gVisor with `ECHO_SANDBOX_CLASS=gvisor` and `COUNTER_SANDBOX_CLASS=gvisor`, after
  applying `deploy/substrate/namespace.yaml`, `echo-gvisor-workerpool.yaml` (formerly
  `echo-workerpool.yaml`, which also created the Namespace) and `counter-gvisor-workerpool.yaml`
  with the worker image resolved, as `docs/substrate-conformance.md` shows.
  `TestStatelessReplayOnGVisor` is now `TestStatelessReplay`, and `TestSessionSuspendResumeOnGVisor`
  is now `TestSessionSuspendResume`.
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
  dials an actor's mesh DNS name any more. A stateful fork now checks on both sides of the tag that
  the parent still holds the checkpoint the fork took, and otherwise fails with
  `ErrSnapshotSuperseded`, which wraps `api.ErrSnapshotSuperseded`.
- `integrations/substrate` now reads the held snapshot from `ActorStatus.snapshots` (substrate
  `f03327bd`) and no longer reads `ActorStatus.external_snapshot`, so upgrade substrate and the
  adapter together. Against an older ate-api the adapter reports no held snapshot: a stateful
  `Placer.Fork` checkpoints the parent, records the `SUSPEND`, and then refuses as unplaceable (no
  external snapshot handle), which `Fork` reports as `FAILED_PRECONDITION`; a direct
  `runtime/substrate.Backend.Fork` returns `ErrNoSnapshotToClone`.

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
