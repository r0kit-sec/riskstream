#!/usr/bin/env bash

set -euo pipefail

kubectl -n argocd port-forward svc/argocd-server 8080:443 --address 0.0.0.0