#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="${NAMESPACE:-local-dev}"
IMAGE_NAME="${IMAGE_NAME:-urlhaus-ingestion}"
IMAGE_TAG="${IMAGE_TAG:-local}"
JOB_NAME="urlhaus-archive-lifecycle-integration-test"
CONFIGMAP_NAME="urlhaus-archive-lifecycle-test-code"
TEST_FILE="${ROOT_DIR}/riskstream/tests/integration/test_urlhaus_archive_lifecycle.py"
JOB_MANIFEST="${ROOT_DIR}/k8s/local-dev/urlhaus-archive-lifecycle-integration-test-job.yaml"
DOCKERFILE_PATH="${ROOT_DIR}/riskstream/services/ingestion/urlhaus/Dockerfile"

echo "Building URLhaus image ${IMAGE_NAME}:${IMAGE_TAG}..."
docker build -f "${DOCKERFILE_PATH}" -t "${IMAGE_NAME}:${IMAGE_TAG}" "${ROOT_DIR}"

echo "Importing URLhaus image into k3s..."
docker save "${IMAGE_NAME}:${IMAGE_TAG}" | sudo k3s ctr images import -

echo "Deploying local-dev overlay to ${NAMESPACE}..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k "${ROOT_DIR}/k8s/overlays/local-dev" -n "${NAMESPACE}"

echo "Waiting for MinIO to be ready in ${NAMESPACE}..."
kubectl wait --for=condition=ready pod -l app=minio -n "${NAMESPACE}" --timeout=180s

echo "Refreshing lifecycle test ConfigMap..."
kubectl create configmap "${CONFIGMAP_NAME}" \
  --from-file=test_urlhaus_archive_lifecycle.py="${TEST_FILE}" \
  -n "${NAMESPACE}" \
  --dry-run=client \
  -o yaml | kubectl apply -f -

echo "Recreating lifecycle integration test job..."
kubectl delete job "${JOB_NAME}" -n "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${JOB_MANIFEST}"

echo "Waiting for lifecycle test job to complete..."
if kubectl wait --for=condition=complete "job/${JOB_NAME}" -n "${NAMESPACE}" --timeout=300s; then
  kubectl logs -n "${NAMESPACE}" "job/${JOB_NAME}"
else
  kubectl logs -n "${NAMESPACE}" "job/${JOB_NAME}" || true
  exit 1
fi
