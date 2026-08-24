# FAQ

Common questions about what `agentsessions` is, what it is not, and how it behaves. See
[`concepts.md`](concepts.md) for the model and [`architecture.md`](architecture.md) for the
implementation.

## What is `agentsessions` in one sentence?

A neutral, open contract and working reference implementation for durable agent sessions: create, run,
suspend, resume, fork, and replay an agent session, with a bring-your-own-harness SPI on top and a
pluggable compute backend underneath.

## How is this different from LangGraph, Temporal, or Durable Task?

It is a different layer, not a competitor. Those are workflow or durable-execution frameworks you build
an application inside. `agentsessions` is the neutral session contract that sits between a producer (what
starts and drives sessions) and a compute backend (what runs them). It defines the wire form and the
determinism guarantees, and it composes with durable engines: a Durable Task or Dapr backend can sit
underneath as an event-log or state store. You do not rewrite your agent to adopt it; you implement a
thin harness adapter.

## Do I need Kubernetes?

No. The default backend (`runtime/local`) is filesystem-only and runs entirely on your machine, which is
what the [quickstart](quickstart.md) uses. Kubernetes and agent-substrate are one compute backend among
several (pod, Kata, Cloud Hypervisor, agent-substrate). Compute is pluggable through the `Runtime` SPI,
and the core knows about none of these specifically.

## Why replay instead of always snapshotting memory?

Because replay runs anywhere, including a plain pod with no snapshot support, and costs nothing special.
A session's truth is its durable log, so a killed session is reconstructed byte-for-byte by replaying
that log, invoking the model zero times. Memory snapshot is the opt-in tier for harnesses whose state
genuinely cannot be replayed (a live REPL, a browser, a warm process). Those declare
`REQUIRES_MEMORY_SNAPSHOT` and are scheduled only on memory-capable compute. You pay for the heavier
capability only when you actually need it.

## What is a fence, and why does it exist?

A fence is a monotonic token bound to a live incarnation (a running sandbox). The event log is the fence
authority: a new incarnation mints a higher fence, and the log rejects any append carrying a lower fence.
This is how a session with automatic failover still has exactly one writer. If a supposedly-dead
incarnation wakes up and tries to write (a zombie), its stale fence is refused. Fencing plus compare-and-
swap on append is what makes "single writer" enforceable rather than a hope.

## Is the provenance chain Go-specific?

No. The `content_hash` is RFC 8785 JCS (JSON Canonicalization Scheme) computed over the proto3-JSON form
of each event, not over any language's serializer. A roughly 30-line Python verifier
(`hack/verify_chain.py`) reproduces the exact Go hash and audits a full journal, and `hack/verify_chain.sh`
demonstrates it end to end, including rejecting a tampered record. Any implementation or auditor can
verify the chain.

## What model providers are supported?

It is model-agnostic. The harness selects a model by id and the host mediates the call; the core has no
provider-specific code. Opaque provider reasoning parts are carried in one neutral `ReasoningPart` type
and replayed verbatim, which was validated across Anthropic, OpenAI, and Gemini in the reasoning-
continuity work. Because the harness routes model calls through the sink and not a provider SDK, swapping
providers does not change the replay path.

## Can I bring my existing agent framework?

Yes. Implement the `api.Harness` SPI (two methods, `Describe` and `Run`) as a thin adapter over your
framework. A Microsoft Agent Framework agent, a GitHub Copilot agent, a LangChain loop, or a custom agent
all plug in the same way. See [`harness-authoring.md`](harness-authoring.md).

## What happens if the harness crashes in the middle of a turn?

The controller re-drives the interrupted execution at most once (invariant I3). An `Exec` with no inputs
means "continue the last execution" rather than "start a new turn". Host-executed tools carry an
idempotency key so a re-driven side-effecting call dedups on the tool side and the effect happens once.

## What is fork actually for?

Fork makes a session a branch point. Four patterns the implementation supports today:

- **Parallel exploration from identical state.** `Fork(count: N)` takes one checkpoint and clones N
  children from it, so a fleet of agents branches from exactly the same base instead of N separate
  re-derivations that can drift. Run several strategies against one task, keep the best, discard the
  rest.
- **Reuse a warmed base session.** A session that has already installed dependencies, cloned a repo, or
  built an index can act as the base for many later jobs, paying that setup once rather than per job.
  Fork skips re-deriving the state; it does not make copying it free, and where that beats a cold boot
  depends on setup cost versus snapshot size, which is not yet measured.
- **Retry from before a bad turn.** For a `STATELESS_REPLAY` harness `--at N` may name any historical
  seq, so a session that went wrong at turn 12 can branch at turn 11 and take a different path without
  discarding the work before it.
- **Auditable A/B.** Each branch is its own hash-chained log carrying a `FORK` marker that names its
  parent and seq. Two branches differing only in a prompt or a model stay independently verifiable, so
  a result traces back to the exact branch that produced it.

The cost is real: no runtime forks copy-on-write today, so an N-way fan-out is N restores rather than N
references to one image.

## What are fork semantics?

`fork --at N` creates a child session that inherits the parent's log up to seq `N`, records a `FORK`
lifecycle marker, and then evolves independently. Because each session is a hash-chained log, a fork is
a branch in a verifiable hash tree.

What happens to compute depends on the harness's declared resumability:

- **`STATELESS_REPLAY`** — the child gets a fresh incarnation and the host replays the inherited prefix
  into it. The parent is untouched, and `--at N` may name any historical seq.
- **`REQUIRES_MEMORY_SNAPSHOT`** — the child's RAM has to come from somewhere, so the parent is first
  checkpointed: it is suspended, its worker is freed, and a `SUSPEND` event is appended to its chain.
  Each child is then cloned from that snapshot. The parent is recoverable with `resume`, but it is *not*
  untouched. A memory snapshot captures RAM as of now, so forking at a historical seq is refused.

No runtime forks copy-on-write today. On substrate each child is a full snapshot restore, so an N-way
fan-out costs N restores rather than one shared image — see
[substrate conformance](substrate-conformance.md#fork-cloning-an-actor-from-a-durable-snapshot).

## How do tool calls work, and can I gate sensitive ones?

Each tool declares a mediation tier. `IN_HARNESS_REPORTED` (the default) runs the tool in the sandbox and
reports the result for audit. `CONTROLLER_MEDIATED` has the host execute the tool so it can apply authz,
policy, and audit centrally. `REQUIRES_APPROVAL` pauses for a human or policy decision. The approval tier
is declared in the contract; the approval gate itself is a tracked follow-up. Tool calls align with the
MCP shape (name plus structured arguments), so MCP tools map onto them directly.

## How do I list sessions, and can I trust the compute state I get back?

`ListSessions` reads a metadata row written at create time, not the event log, which is why a session
with no turns and no compute still appears and why a listing is complete after a process restart. It
filters on an exact `project` and pages with a cursor over `(create time, uid)`, so a session created
while you are walking pages neither skips nor duplicates a row.

`last_seq` is read straight from the log and is exact. `compute_state` is not a live probe: it records
what the log implies about compute. Any ordinary event moves a session to `LIVE`, since something had
to be running to produce it; `SUSPEND` and `FORK` land it `COLD`. So `LIVE` goes stale if the process
behind it later died, and a session that was created but never run stays `NONE`. Treat it as recorded
intent. Making it exact needs the backend to enumerate its own incarnations, which the `Runtime` SPI
does not yet expose.

## Is it production-ready?

It is a working reference implementation (Go 1.26), not yet a productized service. The Sessions API, the
single-writer event-sourced controller, the durable hash-chained log, bring-your-own-harness over
`Harness.Connect`, the `Runtime` SPI with local and substrate backends, the replay-conformance suite, and
real-substrate conformance across both capability tiers all run today. Productization (managed control
plane, enterprise identity and provenance, confidential and GPU snapshots) and session-level suspend and
resume orchestrated through the `Placer` are in progress. See the Status section of the root README.

## Where is the wire format?

In `api/*.proto` (Sessions, Harness, Runtime, and the shared Event). The hand-written Go SPI in
`api/*.go` is what hosts and backends code against. One neutral wire form, one Go SPI.

## How do I run it on real agent-substrate?

See [`substrate-conformance.md`](substrate-conformance.md). It runs in CI across both capability tiers
(stateless-replay on gVisor and memory-snapshot continuity on a micro-VM), against a real `ate-system` in
kind, with the core importing zero substrate code.
