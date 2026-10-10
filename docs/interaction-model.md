# How you interact with agentsessions

You implement **one interface with two methods**: your agent, behind `api.Harness`. Everything else
you call. The Sessions API is implemented by this project and its client is generated from the
protos, so there are no RPCs for you to write.

This page is the whole picture: what you write, what you call, and what is deliberately absent.
[`concepts.md`](concepts.md) has the nouns; [`architecture.md`](architecture.md) has the internals.

## What you implement

Your agent. Two methods.

```go
type Harness interface {
    // Describe returns the static contract, used to match the harness to a Runtime.
    Describe(ctx context.Context) (Descriptor, error)

    // Run drives exactly one execution (turn). It reads the Start, emits events via
    // sink, and returns nil on COMPLETED or an error on FAILED.
    Run(ctx context.Context, s *Start, sink EventSink) error
}
```

Here is a complete one. This is `harness/echoagent`, not a sketch:

```go
type Harness struct{}

func (Harness) Describe(ctx context.Context) (api.Descriptor, error) {
    return api.Descriptor{
        ID:     "echo",
        Models: []string{"echo"},
        Capabilities: api.Capabilities{
            Resumability: api.ResumabilityStatelessReplay,
            ForkSafe:     true,
        },
    }, nil
}

func (Harness) Run(ctx context.Context, s *api.Start, sink api.EventSink) error {
    text := ""
    if n := len(s.Inputs); n > 0 {
        text = s.Inputs[n-1].Text()
    }
    _, err := sink.Model(ctx, api.ModelRequest{
        Model:    "echo",
        Messages: []api.Message{*api.TextMessage("user", text)},
    })
    return err
}
```

That is the entire contract. In return you get durability, byte-identical replay, fork, suspend and
resume, live output streaming, and a tamper-evident provenance chain, none of which your harness
implements. The rules that keep replay exact are in
[`harness-authoring.md`](harness-authoring.md); the load-bearing one is that every model call goes
through `sink.Model` rather than a provider SDK.

That single rule is why your agent streams without doing anything. Because the **host** makes the
model call, it relays partial output to the client while your `Run` sits blocked on one
`sink.Model`, never learning that anything streamed.

The only other interface you might implement is `api.Runtime`, and only if you are bringing your own
sandbox. Most people never touch it.

## What you call

The `Sessions` service. You implement none of it.

| RPC | Does | Status |
|---|---|---|
| `CreateSession` | register a session, with labels, annotations, origin, identity | available |
| `GetSession` | read metadata, state, and the log cursor | available |
| `ListSessions` | enumerate a project, newest first, paged | available |
| `Exec` | run one turn, streaming the session, deltas, and committed records | available |
| `Replay` | re-deliver the committed log, read only | available |
| `Fork` | branch a session into one or more children | available |
| `Suspend` / `Resume` | free and restore compute, or recover an approval-bearing turn | available |
| `Approve` | commit an explicit approval or denial; no implicit Resume | available, stateless replay only |
| `DeleteSession`, `Cancel` | — | declared, **not implemented** |

Treat that last row as unavailable regardless of what a generated client offers.

### Run a turn

One call. It creates the session if you do not name one, and appends at the current head:

```go
c, err := client.Dial("127.0.0.1:8080", client.WithProject("acme"))
if err != nil {
    return err
}
defer c.Close()

turn, err := c.Exec(ctx, client.ExecOptions{
    Inputs:  []string{"name three primes"},
    OnDelta: func(d *v1.Delta) { fmt.Print(d.GetChunk()) },
})
if err != nil {
    return err
}
fmt.Println(turn.Session.GetMetadata().GetUid(), turn.Output)
```

Or with no code at all:

```bash
agentctl exec --server 127.0.0.1:8080 --input "name three primes"
```

### Single-writer, when you want it

The log has one writer, and `ExpectedLastSeq` is how that is enforced: the host commits the turn's
first event only if the head still matches, and a mismatch is `Aborted` rather than a silent
interleave.

It is optional, because the guarantee should be opt-in rather than the price of every call. Leave it
nil and the turn appends at the head. Set it and you get the strict check, with `turn.LastSeq`
carrying the cursor forward so it costs no extra round trip:

```go
next, err := c.Exec(ctx, client.ExecOptions{
    Session:         turn.Session.GetMetadata().GetUid(),
    Inputs:          []string{"and three more"},
    ExpectedLastSeq: &turn.LastSeq,
})
```

Setting it to `0` is a real assertion, not an absent value: it says the session has no events yet.

### Reading the stream

Every `Exec` stream opens with the session, then carries ephemeral deltas and committed records. The
session frame is sent before the turn runs, so **an error arrives after it**: read the stream to
completion rather than treating the first receive as the result. A successful approval pause or
approval-bearing no-input recovery also sends a **final current Session** before OK EOF. Its cursor,
execution/compute axes and pending correlation describe the durable outcome, not just the starting
point. The SDK's `TurnResult.Session` is the latest frame, `OnSession` sees both frames, and `LastSeq`
accounts for Session and record cursors even when recovery produced no records. An initial UID and
cursor remain available alongside an execution error.

Deltas are transport only. They are never logged, never hash-chained, and never produced on replay,
so the journal is identical whether or not anyone watched the turn.

### Approval decisions and recovery

A `STATELESS_REPLAY` harness can request `REQUIRES_APPROVAL` through `sink.ToolCall`. The host journals
`TOOL_CALL` then `APPROVAL_REQUEST`; after a correlated park acknowledgement and actual stream closure,
it snapshots externally and commits `SUSPEND`. The Exec stream ends successfully with a final
`EXEC_AWAITING` Session and `pending_approval`. It does not invent `END`, `ERROR`, or a tool result.
A handoff/snapshot/append failure remains an RPC error with the durable request available for recovery;
use `Suspend` to retry the recorded route's cold transition when needed.

`GetSession` and `ListSessions` expose valid owned pending decision tuples after a restart. Each tuple
is derived only through that response's `last_seq`, not a mutable flag or a local SDK cache.
Decodable legacy/handwritten or malformed approval evidence remains readable in Get/List and Fork
response metadata, but confers no actionable `pending_approval` reference. Malformed historical
approval evidence can withhold a reference even for a valid latest request: strict inspection
validates all approval-bearing executions through the captured cursor. Database/read failures and
undecodable journal or metadata values still return `INTERNAL`. This rendering tolerance does not
relax command validation or grant admission authority.

Host `APPROVAL_REQUEST` and `APPROVAL_RESULT` writes alone do not imply live compute. They preserve
NONE/COLD; only actual execution or lifecycle evidence moves that independent durable projection.

`Approve` requires the
session UID, execution ID, tool call ID, positive request sequence, and explicit approved presence:
`false` is denial, not an omitted choice. It commits only `APPROVAL_RESULT` and returns that record plus
the current Session, without resolving a harness, describing compute, or running a tool. Identical
retries of tuple, decision, reason and identity return the original record even after a receipt or
later turn; changed choices/provenance and stale or inherited requests fail `FAILED_PRECONDITION`.
Missing fields fail `INVALID_ARGUMENT`, unknown UIDs `NOT_FOUND`, and local overlap/CAS/fence conflict
`ABORTED`.

The Go SDK takes an `api.ApprovalDecision` value, so approved presence is always supplied:

```go
sess, err := c.GetSession(ctx, uid)
if err != nil { return err }
ref := sess.GetPendingApproval()
if ref == nil { return fmt.Errorf("no pending approval; use Resume if already decided") }
response, err := c.Approve(ctx, uid, api.ApprovalDecision{
    ExecutionID: ref.GetExecutionId(), ToolCallID: ref.GetToolCallId(),
    RequestSeq: ref.GetRequestSeq(), Approved: true, Reason: "reviewed",
    Identity: api.IdentityRef{Principal: "reviewer", Issuer: "issuer", Subject: "subject"},
})
if err != nil { return err }
// response.Decision is durable; a recovery failure must not be treated as an uncommitted decision.
fmt.Println("decision committed", response.GetDecision().GetSeq())
_, err = c.Resume(ctx, uid, false)
```

Both CLI commands commit a decision **then Resume**; examples assume a pending session on a custom
stateless harness with a host executor, not the bundled echo/chat harnesses:

```bash
agentctl approve "$SID" --server 127.0.0.1:8080 --reason "reviewed" \
  --actor reviewer --identity-issuer issuer --identity-subject subject
agentctl deny --server 127.0.0.1:8080 --session "$SID" --reason "not allowed"
# Complete explicit correlation permits an exact retry after pending_approval has cleared:
agentctl approve --server 127.0.0.1:8080 --session "$SID" \
  --execution "$EXECUTION" --tool-call "$CALL" --request-seq "$REQUEST_SEQ" \
  --reason "reviewed" --actor reviewer --identity-issuer issuer --identity-subject subject --json
```

Omit all three tuple flags to discover through GetSession, or supply all three; partial tuples are
rejected instead of mixed with newly discovered fields. With no pending reference, discovery advises
Resume or an explicit exact retry. If Resume fails, either command exits unsuccessfully with
**"decision committed"** and the recovery cause; it never retracts or changes the decision. `--json`
prints the original committed decision and resumed current Session as protobuf JSON.

While undecided, `Resume` returns a paused Session without runtime IO. A crash cut after `TOOL_CALL`
but before its request repairs the request first; this alone does not prove compute cold. For an
existing current approval-bearing execution, empty-input Exec takes the same recorded recovery path,
with observer records and initial/final Session frames. It checks explicit `expected_last_seq` under
the Registry guard before repair/provisioning and honors `deadline_unix`. Request harness/config/cursor
overrides cannot replace the original invocation. Use Resume for other interrupted executions; other
inputless Exec calls retain ordinary new-turn behavior.

A decision clears `pending_approval`, but **only a committed receipt unlocks new input** for an owned
gate. Until then a new-input Exec is `FAILED_PRECONDITION`, even if its requested harness is unserved.
Decided recovery restores the recorded snapshot/name/version/config/inputs. It can park at another
gate without `RESUME`, or finish and record `RESUME`. An approved call without a host executor is
`FAILED_PRECONDITION` and leaves its decision recoverable. Denial records `APPROVAL_DENIED` without
executing the tool; an approved executor failure records `EXECUTOR_ERROR` for harness/model
continuation rather than turning that handled receipt into an RPC transport failure.

This gate is stateless only. Identity is provenance, not authorization; there is no policy hook,
automatic decision, approval timeout or expiry. Snapshots and unresolved requests are retained
indefinitely. Cancel/DeleteSession remain `UNIMPLEMENTED`; a decision is not cancellation or teardown.
A fork's inherited unresolved request cannot authorize effects in the child; fork at/after a receipt,
or explicitly Exec a new turn in the child. See [security.md](security.md) and the
[executor requirements](harness-authoring.md#host-composition-for-controller-mediated-tools).

## Choosing a client

Same protocol underneath, so moving between these is never a rewrite.

- **Go client SDK** (`client/`) — connection setup, the session frame, the CAS bookkeeping above,
  pagination, and stream draining.
- **`agentctl` and `agentsessionsd`** — drive a session with no code.
- **Generated from the protos** — for any other language. `api/session.proto` and
  `api/harness.proto` are the normative wire form. Nothing about the protocol is Go-specific, and
  neither is the provenance chain: `content_hash` is RFC 8785 JCS over proto3-JSON, so a non-Go
  implementation reproduces the same hashes and can verify a journal it did not write.

That last option is the point of the project even if you never use it. The contract is what you
depend on; this repository is the reference implementation that proves the contract is real. When
the two disagree, the proto wins and the code is wrong.

## Connecting a model

The core ships no provider and never calls a model itself: the host mediates the call, which is what
makes replay free and exact. `model/openai` fills that seam for anything speaking the
chat-completions body, with no vendor SDK:

```bash
export MODEL_API_KEY=...
agentsessionsd --model gpt-4o-mini
```

With `--model` configured, the server also registers the text-only conversational `chat` harness.
Echo remains the default; select chat explicitly for a new session:

```bash
agentctl exec --server 127.0.0.1:8080 --harness chat --input "What is a prime number?"
```

Continue with `--session <uid>` using the UID printed by that command. Chat sends the full recorded
conversation on each turn through `EventSink.Model`; the host handles provider communication.
See the [chat harness example](harness-authoring.md#reference-harness-2-conversational-chat-harnesschatagent)
for an end-to-end Ollama example and the current context-window and
`Session.model` limitations.

Endpoints that differ only in envelope (a model scoped into the path, a pinned API version, a
credential header other than `Authorization`) are reached with `--model-path` and
`--model-auth-header` rather than by rebuilding. An endpoint with its own request body or signing
scheme is a different protocol: implement `controller.ModelFunc` for those, and every guarantee
still holds, because the host still owns the call.

## What this project does not provide

Being explicit here is part of the contract.

- **No hosted service.** There is no SaaS and no account. You run the host.
- **No authentication, authorization, or transport security.** Every gRPC path is plaintext,
  `IdentityRef` is recorded but never enforced, and `project` is a filter rather than a tenancy
  boundary. See [`security.md`](security.md) before exposing a host to anything.
- **No bundled model provider.** `model/openai` speaks a wire protocol; it ships no vendor SDK and
  no credentials.
- **No vendor-specific anything.** Compute, identity, storage, and model access are all seams, and
  concrete backends live in separate modules or repositories. A CI gate enforces that the core links
  no backend vendor.
- **No agent framework.** `agentsessions` does not tell you how to write an agent. It tells you what
  an agent must do to be durable, replayable, and auditable.

## Known limits

Real today, and worth knowing before you build on it:

- **A harness registered by address is stateless-replay only.** `runtime/remote` attaches to a
  harness it did not start, so it cannot capture that process's memory and `CanPlace` refuses a
  `REQUIRES_MEMORY_SNAPSHOT` harness on it. Run those on a backend that owns the sandbox.
- **Registering a harness by address takes a restart.** `agentsessionsd` reads `--harness` at
  startup; there is no API to add one to a running host.
- **History is pushed whole on every turn.** The controller hands the harness the full log each
  time, which is fine for demos and does not scale to long sessions.
- **Execution status is not admission authority.** An owned unresolved host approval is narrowly
  projected as `EXEC_AWAITING`; ordinary/legacy interrupted turns still use metadata's `COMPLETED`
  fallback. Legacy/malformed approval evidence also retains that fallback without an actionable
  reference. A decided call with no receipt has no pending reference and is not reported awaiting,
  but that fallback does not prove completion or unlock new input. Recovery/admission use the journal,
  not the status enum. Compute status is an independent durable projection, not a live probe.
- **`Capabilities.streaming` is declared but unused.** Streaming comes from the model provider, not
  from a harness declaring it.

## Stability

`agentsessions.v1` is a proto namespace, not a stability promise. The Go module is pre-1.0 and makes
no backward-compatibility guarantee. See the README's API stability section for what CI enforces,
including the compatibility gate that runs against the most recent release tag.

## Where next

- [`quickstart.md`](quickstart.md): drive a session end to end.
- [`harness-authoring.md`](harness-authoring.md): the rules that keep replay exact.
- [`security.md`](security.md): what is protected and what is not.
- [`api-reference.md`](api-reference.md): every message, field, and RPC.
