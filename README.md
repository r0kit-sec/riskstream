# riskstream

Kubernetes-native real-time threat intelligence risk scoring platform built with Argo CD GitOps and GitHub Actions CI. Demonstrates digest-pinned deployments, strict environment isolation (dev/staging/prod), and production-grade delivery patterns.

## Scaffolding

GitOps scaffold for a k3s-based Kubernetes platform using Argo CD and GitHub Actions with GHCR images.

## What this scaffold provides

- Three namespaces:
  - `argocd`
  - `staging`
  - `production`
- Argo CD AppProject + Applications
  - `riskstream-staging` auto-syncs from `main`
  - `riskstream-production` is manual sync by default
- Kubernetes manifests structured with Kustomize
- GitHub Actions CI pipeline to build and push container images to GHCR
- Local bootstrap scripts for k3s + Argo CD

## Repository layout

```text
.
├── .github/workflows/ci.yml
├── app/
│   ├── Dockerfile
│   └── main.py
├── k8s/
│   ├── argocd/
│   │   ├── kustomization.yaml
│   │   ├── production-application.yaml
│   │   ├── project.yaml
│   │   └── staging-application.yaml
│   ├── base/
│   │   ├── deployment.yaml
│   │   ├── kustomization.yaml
│   │   └── service.yaml
│   ├── namespaces/
│   │   ├── argocd.yaml
│   │   ├── kustomization.yaml
│   │   ├── production.yaml
│   │   └── staging.yaml
│   └── overlays/
│       ├── local-dev/
│       │   ├── kustomization.yaml
│       │   └── patch.yaml
│       ├── production/
│       │   ├── kustomization.yaml
│       │   └── patch.yaml
│       └── staging/
│           ├── kustomization.yaml
│           └── patch.yaml
└── scripts/
    ├── bootstrap-k3s.sh
    ├── build-and-deploy-local.sh
    └── port-forward-argocd.sh
```

## Prerequisites

- k3s cluster running locally (`kubectl` context pointing to it)
- `kubectl` and `kustomize`
- GitHub repo with Actions enabled
- GHCR access (`GITHUB_TOKEN` with `packages:write` is used in CI)

## Bootstrap on k3s

```bash
chmod +x scripts/bootstrap-k3s.sh scripts/port-forward-argocd.sh
./scripts/bootstrap-k3s.sh
```

This will:

1. Create `argocd`, `staging`, and `production` namespaces
2. Install Argo CD in the `argocd` namespace
3. Apply the Argo CD project and applications from this repo

Then access Argo CD:

```bash
./scripts/port-forward-argocd.sh
```

Get initial admin password:

```bash
kubectl -n argocd get secret argocd-initial-admin-secret \
  -o jsonpath="{.data.password}" | base64 -d; echo
```

## Local development

Build the Docker image locally and deploy to a `local-dev` namespace:

```bash
./scripts/build-and-deploy-local.sh
```

This script will:

1. Build the Docker image from `app/`
2. Import the image into k3s
3. Create a `local-dev` namespace
4. Deploy and override the image to use your local build

You can specify a custom tag:

```bash
IMAGE_TAG=my-feature ./scripts/build-and-deploy-local.sh
```

Access the app:

```bash
# Check deployment status
kubectl get pods -n local-dev

# Check logs
kubectl logs -n local-dev -l app.kubernetes.io/name=riskstream --tail=50 -f

# Port forward to access locally
kubectl port-forward -n local-dev svc/local-riskstream 8081:80

# Access at http://localhost:8081
```

Clean up:

```bash
kubectl delete namespace local-dev
```

## GitHub Actions + GHCR

Workflow: `.github/workflows/ci.yml`

- On PR to `main`: build image (no push)
- On push to `main`: build and push image tags to GHCR:
  - `ghcr.io/r0kit-sec/riskstream:main`
  - `ghcr.io/r0kit-sec/riskstream:latest`
  - `ghcr.io/r0kit-sec/riskstream:<sha>`

If your GHCR package is private, create pull secrets in `staging` and `production` namespaces and attach them to service accounts used by workloads.

## Argo CD behavior

- Argo CD tracks this repository on branch `main`
- The `riskstream-staging` Application has automated sync enabled (`prune` + `selfHeal`)
- Any commit merged to `main` that changes manifests under `k8s/overlays/staging` is automatically applied to `staging`

## Notes

- This scaffold includes a minimal demo app in `app/` for CI image publishing.
- Production is intentionally manual sync by default; you can enable automated sync in `k8s/argocd/production-application.yaml` if desired.
