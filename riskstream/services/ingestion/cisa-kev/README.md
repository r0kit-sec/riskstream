# CISA KEV Ingestion Service

This microservice ingests the official [CISA Known Exploited Vulnerabilities Catalog](https://www.cisa.gov/known-exploited-vulnerabilities-catalog).

## API Endpoints

### Health Check
```text
GET /healthz
```

### Live Catalog
```text
GET /catalog
```
Fetches the current KEV catalog from CISA and returns it without persisting it.

### Ingest Catalog
```text
POST /ingest/catalog
```
Fetches the current KEV catalog and persists a timestamped raw snapshot to the `raw-feeds` MinIO bucket under `cisa-kev/catalog/...`.

### Service Info
```text
GET /
```

## Environment Variables

- `PORT`: Service port (default: `8082`)
- `ENVIRONMENT`: Deployment environment (default: `unknown`)
- `CISA_KEV_URL`: Official CISA KEV JSON URL
- `S3_ENDPOINT`: MinIO/S3 endpoint for raw feed storage
- `S3_ACCESS_KEY`: MinIO/S3 access key for raw feed storage
- `S3_SECRET_KEY`: MinIO/S3 secret key for raw feed storage
- `S3_USE_SSL`: Whether to use TLS for MinIO/S3 connections

## Running Locally

```bash
docker build -f riskstream/services/ingestion/cisa-kev/Dockerfile -t cisa-kev-ingestion .
docker run -p 8082:8082 cisa-kev-ingestion
```

## Troubleshooting

```bash
kubectl logs -n local-dev -l app=cisa-kev-ingestion -f
pytest riskstream/tests/unit/test_cisa_kev_ingestion.py -q
./scripts/run-cisa-kev-integration-test.sh
```
