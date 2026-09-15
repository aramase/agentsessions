# agentsessions

**A neutral, open contract — and working reference implementation — for durable agent sessions.**
Create, run, suspend, resume, **fork**, and replay an agent session, with a Bring-Your-Own-Harness
SPI on top and a pluggable compute backend underneath.

It is the **narrow waist** for agent runtimes: **producers plug in on top** (an issue tracker, a chat
platform, a custom app), **compute plugs in underneath** (pod, Kata, Cloud Hypervisor, agent-substrate),
and the core knows about neither. Build an agent once; run it managed, self-hosted, or in CI — with the
same identity, provenance, and suspend / resume / fork, and no rewrite when it moves.

## Why it exists

Agents are becoming durable, long-running, mostly-idle workers. They need a runtime that can hold
session state, suspend to free compute, resume in place, **fork** to explore in parallel, and attribute
every action — independent of which framework wrote the agent or which cloud runs it. Kubernetes has no
durable-session primitive, and most agent runtimes bind you to one framework, one compute model, or one
vendor. `agentsessions` makes this layer an **open, vendor-neutral contract** anyone can implement and
target — plus a reference implementation that proves the contract holds.

## Neutral core, adapters on both edges

- **Producers plug in on top** as adapters that map their world onto sessions and attach context via
  `Origin` + `annotations`. No producer-specific fields in the core.
- **Compute plugs in underneath** via the `Runtime` SPI. No kernel-isolation technology is assumed; the
  same session runs on a plain pod or on agent-substrate.
- **Identity is OIDC-neutral** (`issuer` / `subject` / `principal`).

## The three contracts

- **Sessions** (`api/session.proto`) — client-facing lifecycle: create / list / exec / suspend / resume
  / fork / replay.
- **Harness** (`api/harness.proto`) — Bring-Your-Own-Harness: the host drives one execution; the harness
  streams typed events over the `Harness.Connect` gRPC stream. Implement it directly, or adapt an agent
  framework with a thin shim.
- **Runtime** (`api/runtime.go`) — compute / sandbox: create / snapshot / restore / stop on pod, Kata,
  Cloud Hypervisor, agent-substrate, or any backend that implements the SPI.

One neutral wire form (`api/*.proto`) with a Go SPI (`api/*.go`). The `Event` type is shared by the
session log and the harness stream. See [`docs/architecture.md`](docs/architecture.md).

## What's proven

This is not a paper contract. The load-bearing claims are **demonstrated end-to-end**, and the
determinism guarantees are exercised by a replay-conformance suite (`conformance/`):

- **Runs on a plain pod, survives pod death, resumes by replay — no memory snapshot.** Exec a turn on
  one incarnation, kill it, and a *different* one reconstructs the session **byte-identically** from the
  durable journal (`sqlitelog/`), invoking the model **zero** times.
- **Tamper-evident, language-neutral provenance.** Every event is hash-chained with a canonical form
  (RFC 8785 JCS over proto3-JSON, `canon/`), so the chain is not Go-specific — a non-Go verifier
  reproduces the Go hash (`hack/verify_chain.py`).
- **Bring-your-own-harness, out of process.** The harness runs behind the `Harness.Connect` gRPC stream
  (`harnesswire/`); the host mediates the model over the wire, so the determinism guarantees hold across
  the process boundary and a harness never touches a provider SDK on the replay path.
- **Runs on real agent-substrate — both capability tiers, green in CI.** The conformance suite runs
  against a real `ate-system` in kind (not a mock). `STATELESS_REPLAY` on gVisor: byte-identical
  replay. `REQUIRES_MEMORY_SNAPSHOT` on a micro-VM: drive → suspend (memory snapshot) → restore → the
  in-RAM state **continues**, with the chain verifying across the snapshot boundary — what a plain pod
  structurally cannot do. Fork fans a stateful session out to k children from one checkpoint and each
  continues independently. `CanPlace` gates the tiers, and the neutral core imports **zero** substrate
  code (a CI gate asserts it). See
  [`docs/substrate-conformance.md`](docs/substrate-conformance.md).

## How it behaves

- **Resumability** defaults to `STATELESS_REPLAY` — history is replayed into the harness on resume /
  fork, so a session runs on any runtime (even a plain pod). Stateful harnesses opt into
  `REQUIRES_MEMORY_SNAPSHOT`, which the host schedules only on a memory-snapshot-capable runtime.
- **Capability matching:** a harness declares `Capabilities`, a runtime declares `RuntimeCapabilities`,
  and `CanPlace` places a harness only on a runtime that satisfies it — degrading honestly instead of
  resuming wrong.
- **Tool calls** default to `IN_HARNESS_REPORTED` (fast path, reported for audit); sensitive tools opt
  into host-mediated execution (`CONTROLLER_MEDIATED`), with crash-mid-tool at-most-once re-drive. A
  `REQUIRES_APPROVAL` tier is declared; the approval gate is a tracked follow-up.
- **Recovery:** an `Exec` with no inputs re-drives the last interrupted execution from history.

## Documentation

Full docs live in [`docs/`](docs/README.md):

- [**Concepts**](docs/concepts.md): the mental model: sessions, typed events, the log, fences, resumability.
- [**Quickstart**](docs/quickstart.md): exec / replay / fork / suspend / resume and verify the chain, locally.
- [**Writing a harness**](docs/harness-authoring.md): plug your agent into the `api.Harness` SPI.
- [**Architecture**](docs/architecture.md): how the neutral core is built.
- [**Running on agent-substrate**](docs/substrate-conformance.md): both capability tiers, green in CI.
- [**Observability**](docs/observability.md): structured request-flow logs and correlation.
- [**FAQ**](docs/faq.md): what this is, what it is not, and how it behaves.
- [**API reference**](docs/api-reference.md): every message, field, enum, and RPC in
  `agentsessions.v1`, generated from the `.proto` comments.

## Repo layout

| Path | What |
|---|---|
| `api/` | The wire proto (`*.proto`) + Go SPI (`*.go`): Sessions, Harness, Runtime, Event. |
| `controller/` | Single-writer, event-sourced core: drives one session, mediates the model, enforces the determinism invariants. |
| `eventlog/`, `sqlitelog/` | The durable event log: CAS + fencing token + hash chain (in-memory reference + sqlite backend). |
| `canon/` | Language-neutral canonical serialization (RFC 8785 JCS) for the hash chain. |
| `observability/` | Structured operation logging and gRPC request-correlation helpers. |
| `harnesswire/` | Bridges the in-process `api.Harness` SPI and the out-of-process `Harness.Connect` gRPC stream. |
| `placement/` | The `Placer`: wires Sessions to the `Runtime` SPI, mints/binds fences, gates via `CanPlace`. The `Registry` routes a session to the harness it names. |
| `runtime/local`, `runtime/substrate` | `Runtime` backends: a filesystem-only local backend, and agent-substrate. |
| `harness/echoagent`, `harness/counteragent` | Reference harnesses: `STATELESS_REPLAY` echo, `REQUIRES_MEMORY_SNAPSHOT` counter. |
| `model/openai` | Model provider for OpenAI-compatible endpoints; no vendor SDK, so any compatible service works. |
| `session/` | The `Sessions` gRPC service: the client-facing seam over the log and the Placer. |
| `cmd/agentsessionsd` | The Sessions server: a TCP entry point over the journal and the harness registry. |
| `cmd/agentctl` | Client CLI (create / exec / replay / fork / suspend / resume). |
| `conformance/` | The replay-conformance suite (the neutral determinism checks). |
| `integrations/substrate/` | The substrate `ControlClient` adapter — a **separate module** so the core stays substrate-free. |
| `deploy/substrate/`, `.github/workflows/substrate-e2e.yml` | Manifests + CI for the real-substrate conformance. |

## Try it

```bash
go build ./...
go test ./...                       # unit + replay-conformance suite
go test ./conformance/ -v           # just the determinism checks
```

For a narrated end-to-end walkthrough (exec, replay with zero model calls, fork, suspend / resume, and
independent chain verification), follow [`docs/quickstart.md`](docs/quickstart.md).

The real-substrate conformance (both tiers) runs in the `substrate-conformance` workflow
(`.github/workflows/substrate-e2e.yml`), on every pull request and nightly. See
[`docs/substrate-conformance.md`](docs/substrate-conformance.md) to reproduce it.

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

Working reference implementation (Go 1.26). The Sessions API, the single-writer event-sourced controller,
the durable hash-chained log, BYOH over `Harness.Connect`, the `Runtime` SPI with local + substrate
backends, the replay-conformance suite, and real-substrate conformance across both capability tiers all
run today. Productization (managed control plane, enterprise identity/provenance, confidential/GPU
snapshots) and session-level suspend/resume *orchestrated through the `Placer`* (the conformance driver
exercises the raw SPI today) are in progress.
