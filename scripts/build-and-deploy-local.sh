#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="local-dev"
IMAGE_NAME="riskstream"
IMAGE_TAG="${IMAGE_TAG:-local}"

echo "[1/4] Building Docker image..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" "${ROOT_DIR}/app"

echo "[2/4] Importing image to k3s..."
docker save "${IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -

echo "[3/4] Creating namespace: ${NAMESPACE}..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "[4/4] Deploying to ${NAMESPACE} using local-dev overlay..."
kubectl apply -k "${ROOT_DIR}/k8s/overlays/local-dev" -n "${NAMESPACE}"

echo ""
echo "✓ Build and deployment complete!"
echo "  Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "Check status:"
echo "  kubectl get pods -n ${NAMESPACE}"
echo "  kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=riskstream --tail=50 -f"
echo ""
echo "Port forward to access locally:"
echo "  kubectl port-forward -n ${NAMESPACE} svc/local-riskstream 8081:80"
echo ""
echo "Access at: http://localhost:8081"
