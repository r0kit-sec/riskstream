#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[1/5] Applying namespaces..."
kubectl apply -k "${ROOT_DIR}/k8s/namespaces"

echo "[2/5] Installing Argo CD..."
kubectl apply --server-side --force-conflicts -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml

echo "[3/5] Waiting for Argo CD server rollout..."
kubectl -n argocd rollout status deployment/argocd-server --timeout=180s

echo "[4/5] Installing Argo CD Image Updater..."
kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj-labs/argocd-image-updater/release-0.12/manifests/install.yaml

echo "[5/5] Applying Argo CD project + applications..."
kubectl apply -k "${ROOT_DIR}/k8s/argocd"

echo "Bootstrap complete."