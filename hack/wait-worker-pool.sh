#!/usr/bin/env bash
# Waits until a WorkerPool's worker pods are rolled out and stable.
#
# An ActorTemplate reports Ready before its WorkerPool's Deployment has finished replacing pods, so a
# suite that starts on the Ready condition alone can place an actor on a worker that is about to go
# away. The actor then reports a pod IP that stops existing mid-test and the harness dial fails with
# an i/o timeout — indistinguishable, from the client, from a dropped SYN.
#
# Usage: hack/wait-worker-pool.sh <worker-pool-name> [namespace] [timeout-seconds]
set -euo pipefail

POOL="${1:?usage: wait-worker-pool.sh <worker-pool-name> [namespace] [timeout-seconds]}"
NAMESPACE="${2:-ate-agentsessions}"
TIMEOUT="${3:-600}"

KUBECTL=(kubectl --context kind-kind)

echo "=== waiting for worker pool ${POOL} to settle ==="

# The WorkerPool controller creates the Deployment asynchronously, so `rollout status` would race
# against its existence and fail outright rather than wait.
deadline=$((SECONDS + TIMEOUT))
until "${KUBECTL[@]}" get "deployment/${POOL}" -n "${NAMESPACE}" >/dev/null 2>&1; do
	if ((SECONDS >= deadline)); then
		echo "timed out after ${TIMEOUT}s waiting for deployment/${POOL} to be created" >&2
		exit 1
	fi
	sleep 2
done

"${KUBECTL[@]}" rollout status "deployment/${POOL}" -n "${NAMESPACE}" --timeout="$((deadline - SECONDS))s"

# rollout status returns for the generation it observed, so a second update landing just behind it
# (the WorkerPool controller reconciles the Deployment more than once) can still swap pods out from
# under a running test. Require the pod set — names and IPs, which is exactly what the suite dials —
# to be identical across two reads before declaring the pool settled.
settle() {
	"${KUBECTL[@]}" get pods -n "${NAMESPACE}" -l "ate.dev/worker-pool=${POOL}" \
		--field-selector=status.phase=Running \
		-o jsonpath='{range .items[*]}{.metadata.name}={.status.podIP} {end}'
}

previous=""
while :; do
	current="$(settle)"
	if [[ -n "${current}" && "${current}" == "${previous}" ]]; then
		break
	fi
	if ((SECONDS >= deadline)); then
		echo "timed out after ${TIMEOUT}s waiting for ${POOL} worker pods to stop changing" >&2
		exit 1
	fi
	previous="${current}"
	sleep 5
done

"${KUBECTL[@]}" get pods -n "${NAMESPACE}" -l "ate.dev/worker-pool=${POOL}" \
	-o custom-columns=NAME:.metadata.name,IP:.status.podIP,PHASE:.status.phase
