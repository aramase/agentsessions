# agentsessions

**A neutral, open contract for durable agent sessions.** Create, run, suspend, resume,
**fork**, and replay an agent session — with a Bring-Your-Own-Harness SPI on top and a
pluggable compute backend underneath.

It is the **narrow waist** for agent runtimes: **producers plug in on top** (an issue tracker,
a chat platform, a custom app), **compute plugs in underneath** (pod, Kata, Cloud Hypervisor,
agent-substrate), and the core knows about neither. Build an agent once; run it managed,
self-hosted, or in CI — with the same identity, provenance, and suspend / resume / fork, and no
rewrite when it moves.

> **proto + interfaces only** (v0, for discussion). The contract comes first; implementations
> follow.

## Why it exists

Agents are becoming durable, long-running, mostly-idle workers. They need a runtime that can
hold session state, suspend to free compute, resume in place, **fork** to explore in parallel,
and attribute every action — independent of which framework wrote the agent or which cloud runs
it. Kubernetes has no durable-session primitive, and most agent runtimes today bind you to
one framework, one compute model, or one vendor. `agentsessions` makes this layer an **open,
vendor-neutral contract** anyone can implement and target.

## Neutral core, adapters on both edges

- **Producers plug in on top** as adapters that map their world onto sessions and attach context
  via `Origin` + `annotations`. No producer-specific fields in the core.
- **Compute plugs in underneath** via the `Runtime` SPI; backend specifics ride in
  `ComputeRef.attributes`. No kernel-isolation technology is assumed.
- **Identity is OIDC-neutral** (`issuer` / `subject` / `principal`).

The same `Session` runs one agent on top with one compute backend underneath, and a different
agent on a different backend — with no change to the proto.

## The three contracts

- **Sessions** — client-facing lifecycle: create / exec / pause / suspend / resume / fork / replay.
- **Harness** — Bring-Your-Own-Harness: the host drives one execution; the harness streams typed
  events. Implement it directly, or adapt an existing agent framework with a thin shim.
- **Runtime** — compute / sandbox: create / snapshot / restore / fork on pod, Kata, Cloud
  Hypervisor, agent-substrate, or any backend that implements the SPI.

One neutral wire form (`api/*.proto`) with a Go SPI (`api/*.go`). The `Event` type is shared by
the session log and the harness stream.

## How it behaves

- **Resumability** defaults to `STATELESS_REPLAY` — history is replayed into the harness on
  resume / fork, so a session runs on any runtime (even a plain pod). Stateful harnesses opt into
  `REQUIRES_MEMORY_SNAPSHOT`, which the host schedules only on a memory-snapshot-capable runtime.
- **Tool calls** default to `IN_HARNESS_REPORTED` (fast path, reported for audit); sensitive
  tools opt into host-mediated execution or human / policy approval.
- **Capability matching:** a harness declares `Capabilities`, a runtime declares
  `RuntimeCapabilities`, and the host places a harness only on a runtime that satisfies it —
  degrading honestly instead of resuming wrong.
- **Recovery:** an `Exec` with no inputs re-drives the last interrupted execution from history.

## Ecosystem

`agentsessions` defines the session contract and composes with existing OSS at its seams:

- **Tools:** MCP — `ToolCall` mirrors an MCP tool call
- **Agent-to-agent:** A2A
- **Event envelope:** CloudEvents — `Origin` = `source` / `subject`
- **Durable engine:** Durable Task / Dapr — event-log / state backends
- **Telemetry:** OpenTelemetry GenAI
- **Compute backends:** agent-substrate, agent-sandbox, Kata, Cloud Hypervisor, pods
- **Harnesses:** any agent framework via the `Harness` SPI, or a custom agent

## Status

- v0 — proto + interfaces only. No host, runtime, or CLI yet.
- Build: `go build ./...`. Layout: `api/*.proto` (wire) + `api/*.go` (Go SPI).
- `TODO(spike):` map the `Runtime` SPI onto agent-substrate to prove "substrate underneath."
