#!/usr/bin/env bash
# Runs the real-substrate conformance suite as an in-cluster Job and fails unless it succeeds.
#
# One script for every capability: $1 selects the tests (a -test.run regexp), so a new capability is
# a new Go test rather than another driver binary, another Job manifest, and another copy of this
# polling loop.
#
# Usage: hack/run-e2e-job.sh <test-regexp> <image> [timeout-seconds]
#
# Environment:
#   HARNESS_IMAGE          digest-pinned harnessnode image the suite's ActorTemplates run (required)
#   ECHO_SANDBOX_CLASS     microvm (default) or gvisor, the echo template's sandbox class
#   COUNTER_SANDBOX_CLASS  microvm (default) or gvisor, the counter template's sandbox class
#   E2E_IDLE_GAP           idle gap for TestHarnessStreamIdlePastRouteTimeout; empty skips it
#   KIND_CLUSTER_NAME      kind cluster to run in (default kind)
set -euo pipefail

TEST_RUN="${1:?usage: run-e2e-job.sh <test-regexp> <image> [timeout-seconds]}"
IMAGE="${2:?missing image}"
BUDGET="${3:-1200}"
HARNESS_IMAGE="${HARNESS_IMAGE:?set HARNESS_IMAGE to the digest-pinned harnessnode image}"
ECHO_SANDBOX_CLASS="${ECHO_SANDBOX_CLASS:-microvm}"
COUNTER_SANDBOX_CLASS="${COUNTER_SANDBOX_CLASS:-microvm}"

# Both tiers run on the micro-VM class unless a host without KVM opts into gvisor. Refuse anything
# else here, before a Job is created, rather than minutes later inside the suite.
for class in "${ECHO_SANDBOX_CLASS}" "${COUNTER_SANDBOX_CLASS}"; do
	case "${class}" in
	microvm | gvisor) ;;
	*)
		echo "unknown sandbox class '${class}': ECHO_SANDBOX_CLASS and COUNTER_SANDBOX_CLASS take microvm or gvisor" >&2
		exit 2
		;;
	esac
done

NAMESPACE="ate-agentsessions"
JOB="substrate-e2e"
KUBECTL=(kubectl --context "kind-${KIND_CLUSTER_NAME:-kind}")
MANIFEST="$(dirname "$0")/../deploy/substrate/e2e-job.yaml"

echo "=== running conformance tests matching /${TEST_RUN}/ (echo: ${ECHO_SANDBOX_CLASS}, counter: ${COUNTER_SANDBOX_CLASS}) ==="

# A previous selection left a completed Job behind; Jobs are immutable, so replace it outright.
"${KUBECTL[@]}" delete job "${JOB}" -n "${NAMESPACE}" --ignore-not-found --wait=true

sed -e "s#E2E_IMAGE#${IMAGE}#" -e "s#E2E_RUN_PLACEHOLDER#${TEST_RUN}#" \
	-e "s#HARNESS_IMAGE_PLACEHOLDER#${HARNESS_IMAGE}#" \
	-e "s#ECHO_SANDBOX_CLASS_PLACEHOLDER#${ECHO_SANDBOX_CLASS}#" \
	-e "s#COUNTER_SANDBOX_CLASS_PLACEHOLDER#${COUNTER_SANDBOX_CLASS}#" \
	-e "s#E2E_IDLE_GAP_PLACEHOLDER#${E2E_IDLE_GAP:-}#" "${MANIFEST}" |
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

	# A harness call failure is usually about routing rather than the test logic: the actor may have
	# been placed on a worker in a different namespace, or the router may have failed to resume or
	# reach it. Print the workers, the api-server's placement decisions and the router's view so the
	# next reader does not have to reproduce the cluster to tell them apart.
	#
	# Worker pods are listed across ALL namespaces on purpose. Worker eligibility is label-matched
	# cluster-wide with no namespace filter, so an actor can legally land on a pool this repo did not
	# deploy (substrate's demos install their own); a namespaced listing hides exactly that case.
	echo "=== worker pods, all namespaces ==="
	"${KUBECTL[@]}" get pods -A -l ate.dev/worker-pool \
		-o custom-columns=NS:.metadata.namespace,NAME:.metadata.name,IP:.status.podIP,PHASE:.status.phase,POOL:'.metadata.labels.ate\.dev/worker-pool' || true
	echo "=== worker the api-server actually picked (namespace tells you if it left ${NAMESPACE}) ==="
	"${KUBECTL[@]}" logs -n ate-system -l app=ate-api-server --tail=-1 --prefix 2>/dev/null |
		grep -F 'Picked worker' | tail -20 || true
	echo "=== atenet-router (the harness ingress) ==="
	"${KUBECTL[@]}" logs -n ate-system -l app=atenet-router --all-containers --tail=100 --prefix 2>/dev/null || true
	exit 1
fi
echo "=== conformance tests matching /${TEST_RUN}/ PASSED ==="
