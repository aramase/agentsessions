# Quickstart

Run a durable agent session end to end in a few minutes: exec a turn, reconstruct it by replay with
zero model calls, fork it, suspend and resume it, and verify its provenance chain with a non-Go
verifier. Everything here uses the built-in `echoagent` harness and a local sqlite journal, so no
Kubernetes, no cloud, and no model key are required. Section 8 points it at a real model.

The command output below is real, captured from a live run.

## Prerequisites

- Go 1.26 or newer.
- Optional, for the independent chain verifier: `buf`, `python3`, and `sqlite3`.

## Build

```bash
git clone https://github.com/aramase/agentsessions
cd agentsessions
go build ./...
go build -o /tmp/agentctl ./cmd/agentctl
```

`agentctl` is the client CLI. By default it starts an embedded Sessions gRPC server over a local unix
socket, backed by a sqlite journal on disk, and drives it as a real gRPC client. Pass `--server <addr>`
to drive a remote controller instead. The journal path defaults to `agentsessions.db`; the examples use
`--journal /tmp/qs.db` to keep it out of the way.

## 1. Run a turn

```bash
/tmp/agentctl exec --journal /tmp/qs.db --input "hello world"
```

```
session sess-6ff29e8d93d3a4c65f6f38bc
seq=1   EVENT_INPUT  user: hello world
seq=2   EVENT_MODEL_CALL
seq=3   EVENT_OUTPUT  assistant: echo:hello world
seq=4   EVENT_END
```

The first line is the new session UID. The four events are the durable, typed log of that turn: the
input, the mediated model call, the output, and the terminal `END`. `echoagent` is a trivial harness
whose "model" returns the input prefixed with `echo:`, so the loop is real without a provider key.

Keep the UID; the next steps use it.

```bash
SID=sess-6ff29e8d93d3a4c65f6f38bc   # replace with your UID
```

## 2. Continue the session

Pass `--session` to add a turn to the same session. The sequence numbers keep climbing in the one log.

```bash
/tmp/agentctl exec --journal /tmp/qs.db --session "$SID" --input "how are you"
```

```
seq=5   EVENT_INPUT  user: how are you
seq=6   EVENT_MODEL_CALL
seq=7   EVENT_OUTPUT  assistant: echo:how are you
seq=8   EVENT_END
```

## 3. Replay: reconstruct the session, model called zero times

This is the property that makes the log the source of truth. `replay` re-delivers the committed log.
On replay the controller serves each recorded model result from the journal and never calls the model
(invariant I1), so the reconstruction is byte-identical and free.

```bash
/tmp/agentctl replay --journal /tmp/qs.db --session "$SID"
```

```
seq=1   EVENT_INPUT  user: hello world
seq=2   EVENT_MODEL_CALL
seq=3   EVENT_OUTPUT  assistant: echo:hello world
seq=4   EVENT_END
seq=5   EVENT_INPUT  user: how are you
seq=6   EVENT_MODEL_CALL
seq=7   EVENT_OUTPUT  assistant: echo:how are you
seq=8   EVENT_END
```

The same durable log is what a *different* process reconstructs after the original one dies. For the
Kubernetes version of this (exec on one pod, delete the pod, a fresh pod replays the journal), see
`hack/demo.sh`.

## 4. Fork: branch a session at a point in its history

`fork --at N` creates a child session that inherits the parent's log up to seq `N`, then evolves
independently. `echoagent` is a stateless-replay harness, so the child is rebuilt by replay and the
parent is untouched. (A `REQUIRES_MEMORY_SNAPSHOT` harness checkpoints the parent instead — see the
[FAQ](faq.md#what-are-fork-semantics).)

```bash
/tmp/agentctl fork --journal /tmp/qs.db --session "$SID" --at 4
```

```
child sess-c63948fb24b628057b7b0920 parent=sess-6ff29e8d93d3a4c65f6f38bc fork_seq=4 name=
```

A child is unnamed unless you name it. It inherits the parent's project, harness and model, but not
its label, so a listing does not fill up with rows that all read the same thing. Use `--names` to
label a fan-out, one comma-separated name per child:

```bash
/tmp/agentctl fork --journal /tmp/qs.db --session "$SID" --at 4 --count 2 --names "optimistic,pessimistic"
```

Replay the child: it carries the parent's first four events, plus a `LIFECYCLE` record marking the fork.

```bash
CHILD=sess-c63948fb24b628057b7b0920   # replace with your child UID
/tmp/agentctl replay --journal /tmp/qs.db --session "$CHILD"
```

```
seq=1   EVENT_INPUT  user: hello world
seq=2   EVENT_MODEL_CALL
seq=3   EVENT_OUTPUT  assistant: echo:hello world
seq=4   EVENT_END
seq=5   EVENT_LIFECYCLE
```

Now exec on the child. It diverges from the parent while the parent still ends at seq 8.

```bash
/tmp/agentctl exec --journal /tmp/qs.db --session "$CHILD" --input "child-only turn"
```

```
seq=6   EVENT_INPUT  user: child-only turn
seq=7   EVENT_MODEL_CALL
seq=8   EVENT_OUTPUT  assistant: echo:child-only turn
seq=9   EVENT_END
```

## 5. Suspend and resume

Suspend frees compute and records the transition in the log; resume brings the session back. On the
local filesystem-only backend, resume reconstructs the session by replay (no memory snapshot). On a
memory-capable backend the same commands restore RAM instead. The CLI is identical either way.

Use a fresh session so the sequence numbers are easy to follow:

```bash
SID2=$(/tmp/agentctl exec --journal /tmp/sr.db --input "before suspend" | head -1 | cut -d' ' -f2)
/tmp/agentctl suspend --journal /tmp/sr.db --session "$SID2"
/tmp/agentctl resume  --journal /tmp/sr.db --session "$SID2"
```

```
session sess-... suspend compute=COMPUTE_COLD last_seq=5
session sess-... resume  compute=COMPUTE_LIVE last_seq=6
```

The first turn is seq 1 to 4, so `SUSPEND` lands at seq 5 and `RESUME` at seq 6. Both are `LIFECYCLE`
events in the log, so the compute history is auditable alongside the conversation.

## 6. List sessions

`create` registers a session without running anything, and `list` enumerates a project. Every
`agentctl` invocation opens the journal fresh, so the listing below is already proving the
restart case: nothing is held in memory between commands.

```bash
/tmp/agentctl create --journal /tmp/qs.db --project acme --name triage --model echo-1
/tmp/agentctl list   --journal /tmp/qs.db --project acme
```

```
sess-0d879482...	last_seq=0	compute=NONE	harness=echo	name=triage
```

`last_seq=0` is the point: the session has no events and no compute yet, and it still appears. That
is the case a listing built from the event log alone would miss.

Sessions are returned newest first and filtered on an exact `--project`. `list` walks every page for
you; `--page-size` only changes how many are fetched per request.

```bash
/tmp/agentctl list --journal /tmp/sr.db --project default
```

```
sess-...	last_seq=6	compute=LIVE	harness=echo	name=
```

`compute` reflects what the log implies about compute, which is why the resumed session above reads
`LIVE`. It is not a live probe of the backend; see
[`concepts.md`](concepts.md#metadata-and-listing) for what that does and does not guarantee.

## 7. Verify the provenance chain with a non-Go verifier

The hash chain is defined on the wire proto (RFC 8785 JCS over proto3-JSON), not on Go, so any
implementation can verify it. `hack/verify_chain.sh` records a real journal, then runs a small Python
verifier that reproduces the Go hash, audits the full chain, and confirms a tampered record fails
closed.

```bash
./hack/verify_chain.sh
```

It reproduces a golden hash with no dependencies, audits a real chain from raw proto bytes (must pass),
and tampers one record and re-audits (must fail). This is the "any auditor can verify the chain"
property, demonstrated.

## 8. Point it at a real model

Everything above uses the built-in echo model, so it runs with no key. To drive a real one, serve
the Sessions API with `agentsessionsd` and give it an OpenAI-compatible endpoint.

```bash
go build -o /tmp/agentsessionsd ./cmd/agentsessionsd

export OPENAI_API_KEY=sk-...
/tmp/agentsessionsd \
  --addr 127.0.0.1:8080 \
  --journal /tmp/real.db \
  --model gpt-4o-mini
```

The key is read from the environment, never a flag, so it stays out of your shell history and the
process list. `MODEL_API_KEY` works too, and an endpoint that needs no credential works with both
unset.

`--model-base-url` points at any endpoint that accepts the chat-completions request body: a
gateway, a self-hosted server, or a proxy fronting another provider. Endpoints that differ only in
the envelope are reached with two more flags, rather than by rebuilding the server:

```bash
/tmp/agentsessionsd \
  --model gpt-4o \
  --model-base-url https://example.com/openai/deployments/gpt-4o \
  --model-path '/chat/completions?api-version=2024-10-21' \
  --model-auth-header api-key
```

`--model-path` covers an endpoint that scopes the model into its path or pins an API version;
`--model-auth-header` covers one that authenticates with something other than `Authorization`. An
endpoint with its own request body or signing scheme is a different protocol: implement
`controller.ModelFunc` for those, and every guarantee below still holds, because the host still
owns the call.

Then drive it with the same CLI, using `--server` instead of a local journal:

```bash
/tmp/agentctl exec --server 127.0.0.1:8080 --input "name three prime numbers"
```

The interesting part is what happens next. Replay the session and watch your provider dashboard:

```bash
/tmp/agentctl replay --server 127.0.0.1:8080 --session "$SID"
```

The turn comes back byte for byte, and the provider is not called. Same for `fork`: branching a
session costs nothing at the model, because the children inherit the recorded completions. You pay
the model once, for the live turn, and every later reconstruction is free.

That is the whole point of host-mediated model calls. Because the completion flows through the host
and lands in the journal, the log is a replayable record of the run rather than a description of
one.

## Where next

- [`concepts.md`](concepts.md): the nouns and why each exists.
- [`harness-authoring.md`](harness-authoring.md): write your own harness against the `api.Harness` SPI.
- [`architecture.md`](architecture.md): how the core is built.
- [`substrate-conformance.md`](substrate-conformance.md): running on real agent-substrate across both
  capability tiers.
- [`faq.md`](faq.md): common questions.
