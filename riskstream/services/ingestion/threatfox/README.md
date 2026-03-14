# ThreatFox Ingestion Service

This microservice ingests threat intelligence data from the [abuse.ch ThreatFox](https://threatfox.abuse.ch/) community API.

## Overview

ThreatFox is a free platform for sharing indicators of compromise (IOCs) associated with malware. This service provides:

- Real-time threat data ingestion
- IOC search capabilities
- Malware family tagging
- RESTful API for threat data access

## API Endpoints

### Health Check
```
GET /healthz
```
Returns service health status.

### Recent Threats
```
GET /recent
```
Fetches IOCs from the last 24 hours.

### Service Info
```
GET /
```
Returns service metadata.

## Running Locally

### Using Python
```bash
cd src
export THREATFOX_AUTH_KEY=your-threatfox-auth-key
python main.py
```

### Using Docker
```bash
docker build -t threatfox-ingestion .
docker run -e THREATFOX_AUTH_KEY=your-threatfox-auth-key -p 8081:8081 threatfox-ingestion
```

## Environment Variables

- `PORT`: Service port (default: 8081)
- `ENVIRONMENT`: Deployment environment (default: unknown)
- `THREATFOX_AUTH_KEY`: ThreatFox API auth key required for upstream requests
- `DEBUG_MODE`: Enable remote debugging with debugpy (default: false)
- `DEBUG_PORT`: Debug port for debugpy (default: 5678)

## Kubernetes Secret Setup

The ThreatFox auth key should be created out-of-band as a Kubernetes Secret and not committed to git.

Create the secret in each namespace where `threatfox-ingestion` runs:

```bash
kubectl create secret generic threatfox-secret \
  --from-literal=auth-key='your-threatfox-auth-key' \
  -n local-dev
```

For staging or production, run the same command in the target namespace:

```bash
kubectl create secret generic threatfox-secret \
  --from-literal=auth-key='your-threatfox-auth-key' \
  -n staging
```

If the secret already exists, replace it with:

```bash
kubectl create secret generic threatfox-secret \
  --from-literal=auth-key='your-threatfox-auth-key' \
  -n local-dev \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Debugging in Kubernetes

The ThreatFox service supports remote debugging via `debugpy` when running in the `local-dev` Kubernetes cluster.

### Setup

1. **Build the container image:**
   ```bash
   docker build -t threatfox-ingestion:local riskstream/services/ingestion/threatfox
   ```

2. **Import to k3s:**
   ```bash
   docker save threatfox-ingestion:local | sudo k3s ctr images import -
   ```

3. **Deploy to local-dev namespace:**
   ```bash
   kubectl create secret generic threatfox-secret \
     --from-literal=auth-key='your-threatfox-auth-key' \
     -n local-dev \
     --dry-run=client -o yaml | kubectl apply -f -
   kubectl apply -k k8s/overlays/local-dev
   ```

4. **Wait for pod to be ready:**
   ```bash
   kubectl wait --for=condition=ready pod -l app=threatfox-ingestion -n local-dev
   ```

5. **Port-forward the debug port:**
   ```bash
   kubectl port-forward -n local-dev svc/threatfox-ingestion 5678:5678
   ```

6. **Attach VS Code debugger:**
   - Press `F5` in VS Code
   - Select **"Attach to ThreatFox (K8s)"** from the dropdown
   - Set breakpoints in your Python files

The service will wait for the debugger to attach before starting when `DEBUG_MODE=true` (enabled by default in local-dev).

### Testing Endpoints

Once the debugger is attached, you can test the service:

```bash
# Port-forward HTTP port (in another terminal)
kubectl port-forward -n local-dev svc/threatfox-ingestion 8081:80

# Test health endpoint
curl http://localhost:8081/healthz

# Test recent threats
curl http://localhost:8081/recent
```

### Troubleshooting

- **Pod not starting:** Check logs with `kubectl logs -n local-dev -l app=threatfox-ingestion`
- **Can't attach debugger:** Ensure port-forward is running and port 5678 is not in use
- **Breakpoints not hitting:** Verify path mappings in `.vscode/launch.json`

## ThreatFox API

The service uses the [ThreatFox API v1](https://threatfox.abuse.ch/api/) which provides:
- Recent IOCs (last N days)
- IOC search by value
- IOC lookup by ID
- Tag-based queries (malware families)

## Architecture

```
src/
├── main.py     # HTTP server and request handling
├── client.py   # ThreatFox API client
└── models.py   # Data models for threat indicators
```

## Future Enhancements

- Scheduled polling of ThreatFox API
- Data persistence (database integration)
- Message queue for downstream processing
- Advanced filtering and enrichment
- Metrics and monitoring
