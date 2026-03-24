# Test Report

## Runtime Environment

- Unit validation: container-matched Docker images
- Integration target: `local-dev` k3s cluster

## Tests Run

1. `docker build -f riskstream/services/normalization/threat-signal/Dockerfile -t threat-signal-normalizer:test-preflight .`
2. `docker run --rm -v "$PWD:/app" -w /app threat-signal-normalizer:test-preflight sh -lc "python -m pip install --quiet pytest jsonschema && pytest -q riskstream/tests/unit/test_threat_signal_normalizer.py"`
3. `docker build -f riskstream/services/ingestion/urlhaus/Dockerfile -t urlhaus-ingestion:test-preflight .`
4. `docker run --rm -v "$PWD:/app" -w /app urlhaus-ingestion:test-preflight sh -lc "python -m pip install --quiet pytest && pytest -q riskstream/tests/unit/test_urlhaus_ingestion.py"`
5. `./scripts/run-threat-signal-normalization-integration-test.sh`

## Results

- Threat-signal normalizer unit tests: `passed` (`9 passed`)
- URLhaus ingestion unit tests: `passed` (`22 passed`)
- Threat-signal normalization integration rerun: `passed` (`2 passed`)

## Integration Validation Notes

- The original rerun was temporarily blocked at `sudo k3s ctr images import -`.
- After the non-interactive wrapper was installed, the image import succeeded and the in-cluster rerun could proceed.
- The next failed rerun exposed the real environment issue: the `local-dev` `minio` Service was routing to both `local-minio` and `minio`.
- After narrowing the Service selector to the managed `minio` deployment in `local-dev`, the integration Job passed.

## Remaining Uncertainty

- The feature is validated in `local-dev`, but the stale `local-minio` deployment should still be reviewed separately to prevent future test confusion.
