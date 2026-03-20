# Threat Signal Normalization Service

This batch component normalizes raw feed artifacts from `raw-feeds` into `threat_signal.v1` JSONL batches under `processed-data`.

It is intended to run as Kubernetes Jobs or CronJobs rather than as a long-running HTTP service.

## Entrypoints

Normalize one raw artifact:

```bash
python riskstream/services/normalization/threat-signal/src/main.py \
  --raw-object-key threatfox/recent/2026/03/19/140500Z.json
```

Normalize all pending artifacts for one source:

```bash
python riskstream/services/normalization/threat-signal/src/main.py --source threatfox
python riskstream/services/normalization/threat-signal/src/main.py --source urlhaus
```
