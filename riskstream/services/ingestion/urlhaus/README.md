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
POST /ingestion/recent
```
Fetches the current URLhaus recent CSV export, writes a daily checkpoint when needed, writes an immutable delta when the feed content changes, and updates a mutable latest-state object used for diffing.

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
- `URLHAUS_HOT_RETENTION_DAYS`: Hot retention window before archive move (default: `30`)
- `URLHAUS_ARCHIVE_RETENTION_DAYS`: Archive retention window before delete (default: `180`)

## Ingestion Behavior

- The Kubernetes CronJob triggers `POST /ingestion/recent` every 5 minutes
- The service fetches the current URLhaus recent CSV export on each run
- The first successful poll each UTC day writes `raw-feeds/urlhaus/checkpoints/YYYY/MM/DD/000000Z.json.gz`
- Changed polls write `raw-feeds/urlhaus/deltas/YYYY/MM/DD/<content_hash>.json.gz`
- The latest parsed feed state is maintained at `raw-feeds/urlhaus/state/latest.json.gz`
- Successful ingest responses include `changed`, `checkpoint_written`, `delta_written`, `delta_counts`, `checked_at`, and `content_hash`

## Archive Lifecycle

- A dedicated `urlhaus-archive-lifecycle` CronJob moves URLhaus checkpoints and deltas older than the hot retention window from `raw-feeds` into `archives`
- The lifecycle job never archives or deletes `raw-feeds/urlhaus/state/latest.json.gz`
- Archived URLhaus artifacts older than the archive retention window are deleted

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
./scripts/run-urlhaus-archive-lifecycle-integration-test.sh
```

To run all ingestion integration tests in sequence from one entrypoint:

```bash
./scripts/run-ingestion-integration-tests.sh
```
