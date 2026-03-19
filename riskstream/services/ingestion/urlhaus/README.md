# URLhaus Ingestion Service

This microservice ingests the recent malware URL export from [abuse.ch URLhaus](https://urlhaus.abuse.ch/about/).

## API Endpoints

### Health Check
```text
GET /healthz
```

### Recent URLs
```text
GET /recent
```
Fetches the current URLhaus recent CSV export, parses it into JSON records, and returns it without persisting it.

### Ingest Recent URLs
```text
POST /ingest/recent
```
Fetches the current URLhaus recent CSV export, computes a deterministic content hash, and persists a new timestamped raw snapshot to the `raw-feeds` MinIO bucket under `urlhaus/recent/...` only when the feed content has changed.

### Service Info
```text
GET /
```

## Environment Variables

- `PORT`: Service port (default: `8083`)
- `ENVIRONMENT`: Deployment environment (default: `unknown`)
- `URLHAUS_RECENT_URL`: URLhaus recent CSV export URL
- `URLHAUS_AUTH_KEY`: Optional URLhaus community auth key for upstream requests
- `S3_ENDPOINT`: MinIO/S3 endpoint for raw feed storage
- `S3_ACCESS_KEY`: MinIO/S3 access key for raw feed storage
- `S3_SECRET_KEY`: MinIO/S3 secret key for raw feed storage
- `S3_USE_SSL`: Whether to use TLS for MinIO/S3 connections

## Ingestion Behavior

- The Kubernetes CronJob triggers `POST /ingest/recent` every 5 minutes
- The service fetches the current URLhaus recent CSV export on each run
- A new snapshot is written only when the upstream payload differs from the latest stored snapshot
- Successful ingest responses include `changed`, `snapshot_written`, `checked_at`, and `content_hash`
- No-change responses include `last_object_key` instead of writing a duplicate snapshot

## Running Locally

```bash
docker build -f riskstream/services/ingestion/urlhaus/Dockerfile -t urlhaus-ingestion .
docker run -p 8083:8083 -e URLHAUS_AUTH_KEY=your-urlhaus-auth-key urlhaus-ingestion
```

## Kubernetes Secret Setup

If you are using an auth key, create it out-of-band as a Kubernetes Secret and do not commit it to git.

Create the secret in each namespace where `urlhaus-ingestion` runs:

```bash
kubectl create secret generic urlhaus-secret \
  --from-literal=auth-key='your-urlhaus-auth-key' \
  -n local-dev
```

For staging or production, run the same command in the target namespace:

```bash
kubectl create secret generic urlhaus-secret \
  --from-literal=auth-key='your-urlhaus-auth-key' \
  -n staging
```

If the secret already exists, replace it with:

```bash
kubectl create secret generic urlhaus-secret \
  --from-literal=auth-key='your-urlhaus-auth-key' \
  -n local-dev \
  --dry-run=client -o yaml | kubectl apply -f -
```

## Troubleshooting

```bash
kubectl logs -n local-dev -l app=urlhaus-ingestion -f
pytest riskstream/tests/unit/test_urlhaus_ingestion.py -q
./scripts/run-urlhaus-integration-test.sh
```

To run all ingestion integration tests in sequence from one entrypoint:

```bash
./scripts/run-ingestion-integration-tests.sh
```
