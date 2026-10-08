#!/usr/bin/env bash
# Creates or removes the production-shaped agentsessions kind deployment.
#
# Usage:
#   USE_OLLAMA=1 hack/kind-production.sh setup
#   MODEL_NAME=... MODEL_BASE_URL=... hack/kind-production.sh setup
#   hack/kind-production.sh port-forward
#   hack/kind-production.sh smoke
#   hack/kind-production.sh teardown
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ACTION="${1:-}"
CLUSTER="${CLUSTER:-agentsessions-production}"
CONTEXT="kind-${CLUSTER}"
NAMESPACE="ate-agentsessions"
SUBSTRATE_REF="${SUBSTRATE_REF:-b1bd558aba3c}"
SUBSTRATE_DIR="${SUBSTRATE_DIR:-${ROOT}/_substrate}"
KO_DOCKER_REPO="${KO_DOCKER_REPO:-localhost:5001}"
KO_DEFAULTBASEIMAGE="${KO_DEFAULTBASEIMAGE:-gcr.io/distroless/static-debian13}"

kubectl_kind() {
	kubectl --context "${CONTEXT}" "$@"
}

require_command() {
	command -v "$1" >/dev/null 2>&1 || {
		echo "missing required command: $1" >&2
		exit 1
	}
}

render() {
	local file="$1"
	shift
	local args=()
	while (($#)); do
		local name="$1"
		local value="$2"
		shift 2
		value="${value//\\/\\\\}"
		value="${value//|/\\|}"
		value="${value//&/\\&}"
		args+=("-e" "s|${name}|${value}|g")
	done
	sed "${args[@]}" "${file}"
}

setup() {
	# The pinned Substrate checkout runs its own kind version through hack/kind.sh.
	for command in git docker kubectl ko; do
		require_command "${command}"
	done
	USE_OLLAMA="${USE_OLLAMA:-0}"
	USE_MODELSTUB="${USE_MODELSTUB:-0}"
	if [[ "${USE_OLLAMA}" == "1" && "${USE_MODELSTUB}" == "1" ]]; then
		echo "USE_OLLAMA and USE_MODELSTUB are mutually exclusive" >&2
		exit 2
	fi
	if [[ "${USE_OLLAMA}" == "1" ]]; then
		MODEL_NAME="${MODEL_NAME:-gemma3:270m}"
		MODEL_BASE_URL="http://ollama.${NAMESPACE}.svc:11434/v1"
		MODEL_PATH="/chat/completions"
	elif [[ "${USE_MODELSTUB}" == "1" ]]; then
		MODEL_NAME="${MODEL_NAME:-smoke-model}"
		MODEL_BASE_URL="http://modelstub.${NAMESPACE}.svc:8080/v1"
		MODEL_PATH="/chat/completions"
	else
		: "${MODEL_NAME:?MODEL_NAME is required}"
		: "${MODEL_BASE_URL:?MODEL_BASE_URL must be reachable from inside the kind cluster}"
		MODEL_PATH="${MODEL_PATH:-/chat/completions}"
	fi
	MODEL_AUTH_HEADER="${MODEL_AUTH_HEADER:-Authorization}"
	PROJECT="${PROJECT:-default}"

	if [[ ! -d "${SUBSTRATE_DIR}/.git" ]]; then
		git clone https://github.com/agent-substrate/substrate "${SUBSTRATE_DIR}"
	fi
	git -C "${SUBSTRATE_DIR}" fetch origin --quiet
	git -C "${SUBSTRATE_DIR}" checkout --detach "${SUBSTRATE_REF}" --quiet

	echo "==> create kind cluster ${CLUSTER} and install pinned ate-system"
	(cd "${SUBSTRATE_DIR}" && KIND_CLUSTER_NAME="${CLUSTER}" hack/create-kind-cluster.sh)
	(cd "${SUBSTRATE_DIR}" && hack/install-ate-kind.sh --deploy-ate-system --ateapi-client-auth=token)

	echo "==> build and publish control-plane, harness, and worker images"
	export KO_DOCKER_REPO KO_DEFAULTBASEIMAGE
	HARNESS_IMG="$(cd "${ROOT}" && ko build ./cmd/harnessnode)"
	CONTROL_IMG="$(cd "${ROOT}/integrations/substrate" && ko build ./cmd/controlplane)"
	ATEOM_IMG="$(cd "${SUBSTRATE_DIR}" && ko build ./cmd/ateom-gvisor)"
	if [[ "${USE_MODELSTUB}" == "1" ]]; then
		MODELSTUB_IMG="$(cd "${ROOT}/integrations/substrate" && ko build ./cmd/modelstub)"
	fi

	echo "==> configure host-side model mediation"
	kubectl_kind create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl_kind apply -f -
	if [[ "${USE_OLLAMA}" == "1" ]]; then
		kubectl_kind apply -f "${ROOT}/deploy/substrate/kind/ollama.yaml"
		kubectl_kind rollout status deployment/ollama -n "${NAMESPACE}" --timeout=600s
		echo "==> pull Ollama model ${MODEL_NAME}"
		kubectl_kind exec -n "${NAMESPACE}" deployment/ollama -- ollama pull "${MODEL_NAME}"
	elif [[ "${USE_MODELSTUB}" == "1" ]]; then
		render "${ROOT}/deploy/substrate/kind/modelstub.yaml" \
			"MODELSTUB_IMAGE" "${MODELSTUB_IMG}" |
			kubectl_kind apply -f -
		kubectl_kind rollout status deployment/modelstub -n "${NAMESPACE}" --timeout=300s
	fi
	kubectl_kind create configmap agentsessions-model -n "${NAMESPACE}" \
		--from-literal=project="${PROJECT}" \
		--from-literal=model="${MODEL_NAME}" \
		--from-literal=base-url="${MODEL_BASE_URL}" \
		--from-literal=path="${MODEL_PATH}" \
		--from-literal=auth-header="${MODEL_AUTH_HEADER}" \
		--dry-run=client -o yaml | kubectl_kind apply -f -
	if [[ -n "${MODEL_API_KEY:-}" ]]; then
		kubectl_kind create secret generic agentsessions-model -n "${NAMESPACE}" \
			--from-literal=api-key="${MODEL_API_KEY}" \
			--dry-run=client -o yaml | kubectl_kind apply -f -
	fi

	echo "==> deploy chat ActorTemplate and worker pool"
	render "${ROOT}/deploy/substrate/kind/chat-actortemplate.yaml" \
		"ghcr.io/aramase/agentsessions/harnessnode:latest" "${HARNESS_IMG}" \
		"ko://github.com/agent-substrate/substrate/cmd/ateom-gvisor" "${ATEOM_IMG}" \
		"MODEL_NAME" "${MODEL_NAME}" |
		kubectl_kind apply -f -
	kubectl_kind wait --for=condition=Ready actortemplate/chat-harness -n "${NAMESPACE}" --timeout=600s
	CLUSTER="${CLUSTER}" "${ROOT}/hack/wait-worker-pool.sh" chat-harness "${NAMESPACE}"

	echo "==> deploy persistent Sessions control plane"
	render "${ROOT}/deploy/substrate/kind/control-plane.yaml" \
		"ghcr.io/aramase/agentsessions/controlplane:latest" "${CONTROL_IMG}" |
		kubectl_kind apply -f -
	kubectl_kind rollout status deployment/agentsessions -n "${NAMESPACE}" --timeout=600s

	echo
	echo "Deployment ready. In another terminal:"
	echo "  CLUSTER=${CLUSTER} ${ROOT}/hack/kind-production.sh port-forward"
	echo "Then use: agentctl exec --server 127.0.0.1:8080 --harness chat --input 'hello'"
}

case "${ACTION}" in
setup)
	setup
	;;
port-forward)
	kubectl_kind port-forward -n "${NAMESPACE}" service/agentsessions 8080:8080
	;;
smoke)
	CLUSTER="${CLUSTER}" "${ROOT}/hack/smoke-production-kind.sh"
	;;
teardown)
	if [[ -x "${SUBSTRATE_DIR}/hack/delete-kind-cluster.sh" ]]; then
		(cd "${SUBSTRATE_DIR}" && KIND_CLUSTER_NAME="${CLUSTER}" hack/delete-kind-cluster.sh)
	else
		require_command kind
		require_command docker
		kind delete cluster --name "${CLUSTER}"
		if [[ "$(docker inspect --format '{{index .Config.Labels "created-by"}}' kind-registry 2>/dev/null || true)" == "agent-substrate" ]]; then
			docker rm -f kind-registry
		fi
	fi
	;;
*)
	echo "usage: $0 <setup|port-forward|smoke|teardown>" >&2
	exit 2
	;;
esac
