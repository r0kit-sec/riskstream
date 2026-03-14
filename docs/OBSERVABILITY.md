# Observability

RiskStream uses a staging-first log aggregation stack built from:

- Fluent Bit
- Loki
- Grafana

The stack is deployed into the `observability` namespace and collects logs from the `staging` namespace.

## Accessing Grafana

```bash
kubectl port-forward -n observability svc/grafana 3000:80
```

Open `http://localhost:3000`.

## Suggested LogQL Queries

All staging logs:

```logql
{namespace="staging"}
```

ThreatFox ingestion logs:

```logql
{namespace="staging", container="threatfox-ingestion"}
```

ThreatFox structured JSON logs:

```logql
{namespace="staging", container="threatfox-ingestion"} | json
```

ThreatFox failed requests:

```logql
{namespace="staging", container="threatfox-ingestion"} | json | event="request_failed"
```

All RiskStream app logs by pod:

```logql
{namespace="staging"} |= "riskstream"
```
