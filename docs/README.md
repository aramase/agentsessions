# agentsessions documentation

Start here. Each document is small and problem-scoped; this page is the index.

## Reading order

1. **[concepts.md](concepts.md)**: the mental model. The nouns (session, event, log, incarnation,
   fence, resumability, capabilities) and why each exists. Read this first.
2. **[quickstart.md](quickstart.md)**: hands on in a few minutes. Exec a turn, replay it with zero
   model calls, fork it, suspend and resume it, verify the provenance chain. No Kubernetes or model key
   required.
3. **[harness-authoring.md](harness-authoring.md)**: write your own agent against the `api.Harness`
   SPI. The contract, the rules that keep replay exact, and two reference harnesses walked through.
4. **[architecture.md](architecture.md)**: how the core is built. The three contracts, the durable
   event log, the controller and its determinism invariants, transport, placement, and runtime backends.
5. **[substrate-conformance.md](substrate-conformance.md)**: running the neutral core on real
   agent-substrate across both capability tiers, proven end to end in CI.
6. **[observability.md](observability.md)**: structured operation logs, request correlation, and the
   data-safety contract.
7. **[faq.md](faq.md)**: common questions about what this is, what it is not, and how it behaves.

## By goal

| I want to... | Read |
|---|---|
| Understand what this is and why | [concepts.md](concepts.md), [faq.md](faq.md) |
| Try it on my machine | [quickstart.md](quickstart.md) |
| Plug in my agent | [harness-authoring.md](harness-authoring.md) |
| Understand the internals | [architecture.md](architecture.md) |
| Run it on agent-substrate | [substrate-conformance.md](substrate-conformance.md) |
| Operate and debug it | [observability.md](observability.md) |

For the project overview and status, see the [root README](../README.md).
