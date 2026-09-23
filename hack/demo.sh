#!/usr/bin/env bash
# Live demo: agentsessions resumes a session on a FRESH Kubernetes pod via deterministic replay of
# a durable journal — no substrate, no memory snapshot.
#
#   exec on pod A  ->  kubectl delete pod  ->  fresh pod B replays the journal byte-identically
#
# Requires: kind, kubectl, docker, go. Tear down with: kind delete cluster --name agentsessions-demo
set -euo pipefail

CLUSTER="${CLUSTER:-agentsessions-demo}"
IMAGE="${IMAGE:-agentnode:demo}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pod() { kubectl get pod -l app=agentnode -o jsonpath='{.items[0].metadata.name}'; }

echo "==> kind cluster: ${CLUSTER}"
if ! kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
	kind create cluster --name "${CLUSTER}"
fi

echo "==> build agentnode (host, pure Go) + image ${IMAGE}"
CGO_ENABLED=0 GOOS=linux GOARCH="$(go env GOARCH)" \
	go build -trimpath -o "${ROOT}/agentnode" "${ROOT}/cmd/agentnode"
docker build -t "${IMAGE}" -f "${ROOT}/Dockerfile" "${ROOT}"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

echo "==> apply manifests"
kubectl apply -f "${ROOT}/deploy/agentnode.yaml"
kubectl rollout status deploy/agentnode --timeout=180s

P1="$(pod)"
kubectl wait --for=condition=Ready "pod/${P1}" --timeout=90s
sleep 2
echo ""
echo "================= EXEC on pod ${P1} ================="
kubectl logs "${P1}" | grep -E "MODE=exec|idle" || kubectl logs "${P1}"

echo ""
echo "==> kubectl delete pod ${P1}   (simulate pod death)"
kubectl delete pod "${P1}" --wait=true
kubectl rollout status deploy/agentnode --timeout=180s
P2="$(pod)"
kubectl wait --for=condition=Ready "pod/${P2}" --timeout=90s
sleep 2
echo ""
echo "============== RESUME on fresh pod ${P2} =============="
kubectl logs "${P2}" | grep -E "MODE=resume|RESUME OK|BYTE_IDENTICAL" || kubectl logs "${P2}"

echo ""
echo "==> done. tear down: kind delete cluster --name ${CLUSTER}"
