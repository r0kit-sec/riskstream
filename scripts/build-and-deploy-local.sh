#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="local-dev"
IMAGE_NAME="riskstream"
CISA_KEV_IMAGE_NAME="cisa-kev-ingestion"
THREATFOX_IMAGE_NAME="threatfox-ingestion"
URLHAUS_IMAGE_NAME="urlhaus-ingestion"
IMAGE_TAG="${IMAGE_TAG:-local}"

echo "[1/11] Building app Docker image..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" "${ROOT_DIR}/app"

echo "[2/11] Building CISA KEV Docker image..."
docker build -f "${ROOT_DIR}/riskstream/services/ingestion/cisa-kev/Dockerfile" \
  -t "${CISA_KEV_IMAGE_NAME}:${IMAGE_TAG}" \
  "${ROOT_DIR}"

echo "[3/11] Building ThreatFox Docker image..."
docker build -f "${ROOT_DIR}/riskstream/services/ingestion/threatfox/Dockerfile" \
  -t "${THREATFOX_IMAGE_NAME}:${IMAGE_TAG}" \
  "${ROOT_DIR}"

echo "[4/11] Building URLhaus Docker image..."
docker build -f "${ROOT_DIR}/riskstream/services/ingestion/urlhaus/Dockerfile" \
  -t "${URLHAUS_IMAGE_NAME}:${IMAGE_TAG}" \
  "${ROOT_DIR}"

echo "[5/11] Importing images to k3s..."
docker save "${IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -
docker save "${CISA_KEV_IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -
docker save "${THREATFOX_IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -
docker save "${URLHAUS_IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -

echo "[6/11] Creating namespace: ${NAMESPACE}..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -

echo "[7/11] Deploying to ${NAMESPACE} using local-dev overlay..."
kubectl apply -k "${ROOT_DIR}/k8s/overlays/local-dev" -n "${NAMESPACE}"

echo "[8/11] Waiting for MinIO to be ready..."
kubectl wait --for=condition=ready pod -l app=minio -n "${NAMESPACE}" --timeout=120s

# Give MinIO a few extra seconds to fully start accepting connections
echo "Waiting for MinIO service to be fully available..."
sleep 10

echo "[9/11] Waiting for MinIO bucket initialization to complete..."
# Wait for the init job to complete (it should start automatically after MinIO is ready)
if kubectl wait --for=condition=complete job/minio-init -n "${NAMESPACE}" --timeout=120s; then
  echo "✓ MinIO initialization completed successfully"
  echo ""
  echo "View initialization logs:"
  kubectl logs -n "${NAMESPACE}" job/minio-init --tail=100
else
  echo "⚠ Warning: MinIO init job did not complete in time"
  echo ""
  echo "=== MinIO Init Job Status ==="
  kubectl get job/minio-init -n "${NAMESPACE}"
  echo ""
  echo "=== MinIO Init Job Logs ==="
  kubectl logs -n "${NAMESPACE}" job/minio-init --tail=100 2>&1 || echo "Could not retrieve logs"
  echo ""
  echo "=== MinIO Pod Status ==="
  kubectl get pods -n "${NAMESPACE}" -l app=minio
  echo ""
  echo "Manual check: kubectl logs -n ${NAMESPACE} job/minio-init"
fi

echo "[10/11] Local images ready for app and ingestion services."
echo ""
echo "✓ Build and deployment complete!"
echo "  Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "  CISA KEV Image: ${CISA_KEV_IMAGE_NAME}:${IMAGE_TAG}"
echo "  ThreatFox Image: ${THREATFOX_IMAGE_NAME}:${IMAGE_TAG}"
echo "  URLhaus Image: ${URLHAUS_IMAGE_NAME}:${IMAGE_TAG}"
echo "  MinIO: Buckets initialized"
echo "[11/11] Local deployment summary complete."
echo ""
echo "Check status:"
echo "  kubectl get pods -n ${NAMESPACE}"
echo "  kubectl logs -n ${NAMESPACE} -l app.kubernetes.io/name=riskstream --tail=50 -f"
echo ""
echo "Port forward services:"
echo "  kubectl port-forward -n ${NAMESPACE} svc/riskstream 8081:80"
echo "  ${ROOT_DIR}/scripts/port-forward-minio.sh local-dev"
echo ""
echo "Access at:"
echo "  App:           http://localhost:8081"
echo "  MinIO Console: http://localhost:9001 (after port-forward)"
