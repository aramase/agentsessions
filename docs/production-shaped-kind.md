# Production-shaped deployment on kind

This example runs the complete client-to-harness path in a kind cluster:

```text
agentctl --server
        |
        | Sessions gRPC
        v
agentsessions Deployment
  - session.Service
  - placement.Registry and Placer
  - SQLite journal on a PersistentVolume
  - host-side OpenAI-compatible model client
        |
        +---- model provider HTTP API
        |
        | agent-substrate Control API
        v
gVisor substrate actor
  - harnessnode
  - chatagent
        |
        | Harness.Connect
        v
agentsessions controller
```

It is **production-shaped, not production-ready**. It demonstrates the intended process boundaries,
durable record, host-side model mediation, isolated harness compute, and restart behavior. It does
not provide controller high availability, authentication for the Sessions API, tenant isolation, or
a production storage backend.

## What runs where

The `agentsessions` Deployment is the persistent control plane. It hosts the Sessions gRPC API,
controller, substrate-backed Placer, model client, and SQLite journal. The journal is mounted from a
single-node hostPath PersistentVolume so replacing the pod does not replace the session record.

`harnessnode` runs separately in a gVisor actor selected by the `chat-harness` ActorTemplate. It runs
the provider-neutral `chatagent` harness. The actor receives only the configured model name; model
endpoint configuration and credentials remain in the control-plane pod. Every model request travels
back through `EventSink.Model` and is executed by the host-side model client.

The control plane uses ate-api to create, resume, inspect, suspend, and delete actors. It does not
use the Kubernetes API to manage harness compute.

## Prerequisites

- Docker
- Go
- `kubectl`
- `ko`
- Either the optional in-cluster Ollama deployment, or another OpenAI-compatible endpoint reachable
  from inside the kind cluster

The setup script checks out the substrate revision pinned by
[`integrations/substrate/go.mod`](../integrations/substrate/go.mod) into the ignored `_substrate/`
directory. That checkout manages its pinned `kind` binary and recreates the selected cluster.

### Credential-free real model with Ollama

The recommended local path deploys Ollama inside kind and pulls the small `gemma3:270m` chat model:

```bash
USE_OLLAMA=1 hack/kind-production.sh setup
```

This uses the official `ollama/ollama:0.34.1` image, exposes Ollama only as a ClusterIP service, and
persists its model cache on a kind hostPath volume. The control plane calls Ollama's
OpenAI-compatible `/v1/chat/completions` endpoint; `chatagent` remains provider-neutral and receives
only the model name.

The Ollama image is several gigabytes and the model is roughly 300 MB, so first setup is much slower
than the deterministic CI fixture. Inference is CPU-only unless the kind cluster is separately
configured to expose a GPU. Set `MODEL_NAME` alongside `USE_OLLAMA=1` to pull another Ollama chat
model instead.

For a lightweight infrastructure check rather than a real model, set `USE_MODELSTUB=1`. This deploys
the deterministic OpenAI-compatible fixture used by CI while still exercising `chatagent` and
host-side mediation:

```bash
USE_MODELSTUB=1 hack/kind-production.sh setup
EXPECT_MODELSTUB=1 hack/kind-production.sh smoke
```

## Deploy

Alternatively, configure an external compatible endpoint:

```bash
export MODEL_NAME=gpt-4o-mini
export MODEL_BASE_URL=https://api.openai.com/v1
export MODEL_API_KEY=...

hack/kind-production.sh setup
```

For an endpoint using a different path or credential header:

```bash
export MODEL_PATH=/openai/deployments/example/chat/completions?api-version=...
export MODEL_AUTH_HEADER=api-key
```

The endpoint must be reachable from the control-plane pod, not merely from the host.

You can also run Ollama directly on the development machine instead of inside Kubernetes:

```bash
OLLAMA_HOST=0.0.0.0:11434 ollama serve
ollama pull gemma3:270m
```

Then set `MODEL_NAME=gemma3:270m` and `MODEL_BASE_URL` to a host address reachable from Docker
containers, such as `http://host.docker.internal:11434/v1` on Docker Desktop. Linux installations
typically need the Docker bridge gateway or another explicit host address. Do not use
`127.0.0.1`: from the control-plane pod that address refers to the pod itself.

The deployment contains:

- One `agentsessions` replica with `Recreate` rollout strategy.
- A statically bound `ReadWriteOnce` hostPath volume for `/data/agentsessions.db`.
- A projected service-account token used to authenticate to ate-api.
- A two-worker gVisor pool and `chat-harness` ActorTemplate.
- A NetworkPolicy exception allowing only the control-plane pod to dial worker pods directly.
- Optionally, an in-cluster Ollama Deployment and persistent model cache.

## Drive it with `agentctl`

Start a local port-forward:

```bash
hack/kind-production.sh port-forward
```

In another terminal:

```bash
go build -o ./agentctl ./cmd/agentctl

SID=$(./agentctl create \
  --server 127.0.0.1:8080 \
  --harness chat \
  --name production-shaped)

./agentctl exec \
  --server 127.0.0.1:8080 \
  --session "${SID}" \
  --input "What is a durable agent session?"

./agentctl exec \
  --server 127.0.0.1:8080 \
  --session "${SID}" \
  --input "Summarize that in one sentence."

./agentctl verify --server 127.0.0.1:8080 --session "${SID}"
```

`agentctl` communicates only with the Sessions endpoint. The server selects the substrate-backed
`chat` Placer, names the actor after the session UID, reaches `harnessnode` through
`Harness.Connect`, and mediates the model request on the control-plane side.

## Restart experiment

The automated smoke performs the same sequence:

```bash
hack/kind-production.sh smoke
```

It:

1. Executes a chat turn through `agentctl --server`.
2. Reads the session and verifies its hash chain through the Sessions API.
3. Deletes the `agentsessions` pod and waits for its replacement.
4. Reads and replays the same committed session after replacement.
5. Executes another turn in the same session.
6. Verifies the enlarged hash chain and prints correlation evidence.

The second turn proves continuation after control-plane restart. The stateless chat harness receives
the committed first turn as history and the current input separately. The existing substrate actor
may still be running, so this experiment does **not** claim that harness compute was reconstructed.

The `substrate-e2e` workflow runs this smoke with a deterministic, CI-only OpenAI-compatible model
fixture. That fixture reports the number of messages it received, allowing CI to assert that the
post-restart turn contained the first input, first output, and new input. The documented deployment
uses Ollama or another configured real provider unless `USE_MODELSTUB=1` is explicitly selected.

## Persistence and replay semantics

These operations are intentionally distinct:

| Operation | What it proves |
|---|---|
| `agentctl replay` | The Sessions API can read and stream committed journal records. It does not run the harness. |
| `agentctl verify` | A client can recompute every content hash and link from records returned by Sessions. |
| Control-plane pod replacement | Session metadata and committed history survive because SQLite is outside the pod. |
| Continuing the session | A new service/controller instance obtains a fresh fence and safely appends to the durable log. |
| Harness reconstruction | A fresh actor re-executes the harness against recorded effects. This is a separate operation. |
| Memory restoration | A substrate memory snapshot restores in-actor RAM. The chat harness does not require this capability. |

Controller-level reconstruction of the history-aware chat harness depends on the turn-boundary fix
tracked in [#41](https://github.com/aramase/agentsessions/issues/41). Until that lands, this example
does not delete the chat actor and present journal inspection as proof of harness reconstruction.
The real-substrate conformance suite separately covers stateless echo replay and memory-snapshot
restoration.

## Trust boundaries and limitations

- The Sessions endpoint is plaintext and unauthenticated. The documented path exposes it only
  through a local `kubectl port-forward`.
- The kind ate-api setup authenticates the projected service-account token but uses
  `--ateapi-insecure-skip-verify` because its internal certificate is not rooted in the control-plane
  image. A production deployment must provide and verify the ate-api CA.
- The pinned substrate revision is required. Newer revisions remove the direct pod-IP ingress used
  here, while the supported router path cannot yet carry the bidirectional gRPC harness stream.
- `Harness.Connect` therefore uses the actor worker's reported pod IP over plaintext h2c. The
  additional NetworkPolicy is a temporary, narrowly selected exception for the control-plane pod.
- SQLite plus one `ReadWriteOnce` volume supports a single control-plane replica. It demonstrates
  durable restart, not high availability or concurrent replicas.
- The static hostPath volume survives pod replacement but belongs to the kind node and disappears
  with the cluster. It is not a portable or production storage class.
- The optional Ollama model cache uses the same kind-only hostPath approach and has no authentication;
  its Service is cluster-internal.
- Project is a filter, not an authorization boundary. There is no production authentication,
  authorization, TLS termination, secret manager, or tenant isolation in this example.
- `chatagent` sends the full recorded conversation on every turn. It is a reference context policy,
  not a scalable context-window strategy.

## Tear down

```bash
hack/kind-production.sh teardown
```
