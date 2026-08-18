#!/usr/bin/env bash
# Runs the real-substrate conformance suite as an in-cluster Job and fails unless it succeeds.
#
# One script for every capability: $1 selects the tests (a -test.run regexp), so a new capability is
# a new Go test rather than another driver binary, another Job manifest, and another copy of this
# polling loop.
#
# Usage: hack/run-e2e-job.sh <test-regexp> <image> [timeout-seconds]
set -euo pipefail

TEST_RUN="${1:?usage: run-e2e-job.sh <test-regexp> <image> [timeout-seconds]}"
IMAGE="${2:?missing image}"
BUDGET="${3:-1200}"

NAMESPACE="ate-agentsessions"
JOB="substrate-e2e"
KUBECTL=(kubectl --context kind-kind)
MANIFEST="$(dirname "$0")/../deploy/substrate/e2e-job.yaml"

echo "=== running conformance tests matching /${TEST_RUN}/ ==="

# A previous selection left a completed Job behind; Jobs are immutable, so replace it outright.
"${KUBECTL[@]}" delete job "${JOB}" -n "${NAMESPACE}" --ignore-not-found --wait=true

sed -e "s#E2E_IMAGE#${IMAGE}#" -e "s#E2E_RUN_PLACEHOLDER#${TEST_RUN}#" "${MANIFEST}" |
	"${KUBECTL[@]}" apply -f -

result=""
deadline=$((SECONDS + BUDGET))
while ((SECONDS < deadline)); do
	succeeded="$("${KUBECTL[@]}" get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
	failed="$("${KUBECTL[@]}" get job "${JOB}" -n "${NAMESPACE}" -o jsonpath='{.status.failed}' 2>/dev/null || true)"
	if [[ "${succeeded}" == "1" ]]; then
		result=succeeded
		break
	fi
	if [[ -n "${failed}" && "${failed}" != "0" ]]; then
		result=failed
		break
	fi
	sleep 3
done

# Always surface the full `go test -v` output: on failure it names the failing test and assertion,
# which an exit code alone cannot.
echo "=== conformance output ==="
"${KUBECTL[@]}" logs -n "${NAMESPACE}" "job/${JOB}" --tail=-1 || true

if [[ "${result}" != "succeeded" ]]; then
	echo "=== Job did not succeed (${result:-timed out after ${BUDGET}s}); describing pods ==="
	"${KUBECTL[@]}" describe pods -n "${NAMESPACE}" -l "app=${JOB}" || true

	# A dial failure is almost always about reachability rather than the test logic, and the causes
	# are indistinguishable from an i/o timeout alone: the actor may have been placed on a worker in
	# a different namespace, the reported pod IP may be stale, or a NetworkPolicy may be dropping the
	# SYN. Print all three so the next reader does not have to reproduce the cluster to tell them apart.
	#
	# Worker pods are listed across ALL namespaces on purpose. Worker eligibility is label-matched
	# cluster-wide with no namespace filter, so an actor can legally land on a pool this repo did not
	# deploy (substrate's demos install their own); a namespaced listing hides exactly that case.
	echo "=== worker pods, all namespaces (the dial targets) ==="
	"${KUBECTL[@]}" get pods -A -l ate.dev/worker-pool \
		-o custom-columns=NS:.metadata.namespace,NAME:.metadata.name,IP:.status.podIP,PHASE:.status.phase,POOL:'.metadata.labels.ate\.dev/worker-pool' || true
	echo "=== worker the api-server actually picked (namespace tells you if it left ${NAMESPACE}) ==="
	"${KUBECTL[@]}" logs -n ate-system -l app=ate-api-server --tail=-1 --prefix 2>/dev/null |
		grep -F 'Picked worker' | tail -20 || true
	echo "=== NetworkPolicies selecting them ==="
	"${KUBECTL[@]}" get networkpolicy -A -o wide || true
	exit 1
fi
echo "=== conformance tests matching /${TEST_RUN}/ PASSED ==="
