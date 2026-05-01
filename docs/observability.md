# Observability

dbshuffle emits OpenTelemetry traces, metrics, and logs. Everything is disabled by default and enabled entirely through env vars — no code changes or flags required.

## Env vars

| Env var | Values | Default | Description |
|---|---|---|---|
| `OTEL_TRACES_EXPORTER` | `otlp` \| `stdout` \| `none` | `none` (unless endpoint set) | Trace exporter to use |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `host:port` or URL | — | Setting this auto-enables `otlp` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` \| `http/protobuf` | `grpc` | Transport for OTLP |
| `OTEL_METRICS_EXPORTER` | `otlp` \| `stdout` \| `none` | same as traces | Metric exporter (independent of traces) |
| `OTEL_LOGS_EXPORTER` | `otlp` \| `stdout` \| `none` | same as traces | Log exporter (independent of traces) |
| `OTEL_SERVICE_NAME` | string | `dbshuffle` | Service name attached to all signals |
| `OTEL_METRIC_EXPORT_INTERVAL` | duration | `60s` | How often metrics are collected and exported |

## Quick examples

```bash
# Send to a local Jaeger / Tempo / Collector on the default gRPC port
OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317 dbshuffle server

# Print spans to stdout (useful for debugging)
OTEL_TRACES_EXPORTER=stdout dbshuffle assign blog myfeature_test

# Explicit OTLP over HTTP
OTEL_TRACES_EXPORTER=otlp \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
dbshuffle server
```

## Logs

All `slog` output is bridged to OTel via the `otelslog` handler and sent alongside the existing stderr text output. The level filter (`--verbose` / default info) applies to both outputs — no extra configuration needed.

## Traces

The HTTP server wraps every request in a span named `METHOD /path` (e.g. `POST /assign`). Each service operation (`Assign`, `Reset`, `Clean`, `Refill`, `Extend`, `Status`) and each database operation (`CopyDB`, `RenameDB`, `DropDB`, `CreateDBFromPath`) is a child span, giving a full call hierarchy per request. Errors are recorded on the span that produced them.

## Metrics

Metrics are available in server mode only.

| Metric | Type | Attribute | Description |
|---|---|---|---|
| `dbshuffle.buffer.size` | Gauge | `template` | Current number of ready buffer copies |
| `dbshuffle.assigned.size` | Gauge | `template` | Current number of assigned databases |

Both gauges are observed together in a single query per collection cycle (default 60 s, configurable via `OTEL_METRIC_EXPORT_INTERVAL`). Templates with no rows yet are reported as zero.
