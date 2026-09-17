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
| `Suspend` / `Resume` | free and restore compute | available |
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
completion rather than treating the first receive as the result. The SDK does this for you.

Deltas are transport only. They are never logged, never hash-chained, and never produced on replay,
so the journal is identical whether or not anyone watched the turn.

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
for an end-to-end Ollama example and the current context-window, controller replay, and
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

- **Harnesses register at build time.** Adding one means building a server with a larger registry.
- **History is pushed whole on every turn.** The controller hands the harness the full log each
  time, which is fine for demos and does not scale to long sessions.
- **`exec_state` does not distinguish an interrupted turn** from a completed one. Recovery keys off
  the log, not off that field.
- **`Capabilities.streaming` is declared but unused.** Streaming comes from the model provider, not
  from a harness declaring it.

## Stability

`agentsessions.v1` is a proto namespace, not a stability promise. The Go module is pre-1.0 and makes
no backward-compatibility guarantee. See the README's API stability section for what CI enforces and
when the compatibility gate arms itself.

## Where next

- [`quickstart.md`](quickstart.md): drive a session end to end.
- [`harness-authoring.md`](harness-authoring.md): the rules that keep replay exact.
- [`security.md`](security.md): what is protected and what is not.
- [`api-reference.md`](api-reference.md): every message, field, and RPC.
