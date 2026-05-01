# CLI reference

All flags can be set via environment variables as well.

## Global flags

| Flag | Env | Default | Description |
|---|---|---|---|
| `--db-host` | `DB_HOST` | `localhost` | MySQL host |
| `--db-port` | — | `3306` | MySQL port |
| `--db-user` | `DB_USER` | `root` | MySQL user |
| `--db-password` | `DB_PASSWORD` | _(empty)_ | MySQL password |
| `--config` | — | `config.yaml` | Path to config file |

## OpenTelemetry env vars

OTEL is configured entirely through env vars (no flags). See [Observability](observability.md) for full details.

| Env var | Default | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OTLP endpoint; setting this auto-enables `otlp` for all signals |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Transport: `grpc` or `http/protobuf` |
| `OTEL_TRACES_EXPORTER` | `none` | Trace exporter: `otlp` \| `stdout` \| `none` |
| `OTEL_METRICS_EXPORTER` | same as traces | Metric exporter: `otlp` \| `stdout` \| `none` |
| `OTEL_LOGS_EXPORTER` | same as traces | Log exporter: `otlp` \| `stdout` \| `none` |
| `OTEL_SERVICE_NAME` | `dbshuffle` | Service name attached to all signals |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60s` | How often metrics are collected and exported |

## Commands

### `status`
Show the current state of all managed databases, grouped by template.

```bash
dbshuffle status
dbshuffle --db-password secret status
```

Example output:
```
=== template: blog ===
  buffer   : 3
  assigned : 1
  expired  : 0
    [assigned] myfeature_test  expires: 2026-04-23 10:00:00 +0000 UTC
=== template: shop ===
  buffer   : 2
  assigned : 0
  expired  : 0
```

### `assign <template> <dbname>`
Rename a buffered copy to `<dbname>`. Blocks until the rename is complete.

```bash
dbshuffle assign blog myfeature_test
dbshuffle assign shop checkout_test
```

Errors:
- `unknown template` — template name not in config
- `no buffer databases available` — buffer is empty; run `refill` first
- `database name already assigned` — `<dbname>` is already in use

### `reset <template> <dbname>`
Drop the existing `<dbname>` assignment (if one exists) and assign a fresh buffer copy under the same name. If `<dbname>` is not currently assigned, behaves identically to `assign`.

```bash
dbshuffle reset blog myfeature_test
```

Errors:
- `unknown template` — template name not in config
- `no buffer databases available` — buffer is empty; run `refill` first

### `refill`
Create buffer copies for all templates until each reaches its configured `buffer` size. Skips templates that are already at capacity.

```bash
dbshuffle refill
```

### `clean`
Drop all assigned databases whose `last_extended_at + expire` is in the past, and remove their records.

```bash
dbshuffle clean
```

### `server`
Start the HTTP server. Runs buffer refill in the background on a configurable interval.

```bash
dbshuffle server
dbshuffle server --addr :9090 --refill-period 30s
```

| Flag | Default | Description |
|---|---|---|
| `--addr` / `ADDR` | `:8080` | Listen address |
| `--refill-period` | `1m` | How often the background refill runs |
