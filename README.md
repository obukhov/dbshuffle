# dbshuffle

dbshuffle pre-creates copies of MySQL database templates so they can be assigned to a name instantly — without waiting for a full copy at request time. Useful for test environments where each test run needs a fresh, isolated database.

## How it works

1. You define one or more **templates** — existing MySQL databases that serve as the source of truth.
2. dbshuffle maintains a **buffer** of ready copies for each template (named `<template>_<uuid>`).
3. When you **assign** a template to a name, one buffered copy is instantly renamed to that name.
4. Assigned databases **expire** after a configurable number of hours. Running **clean** drops them.
5. Running **refill** (or letting the server do it in the background) tops the buffer back up.

All state is tracked in the `_dbshuffle.databases` management table.

## Project structure

```
cmd/main.go                  single binary — all commands live here
internal/
  config/config.go           YAML config loading
  db/db.go                   MySQL connection + schema bootstrap
  db/operations.go           CopyDB / RenameDB / DropDB (raw SQL, no ORM)
  service/shuffle.go         business logic: Status, Assign, Clean, Refill
  handler/shuffle.go         HTTP handlers delegating to service
docker/mysql/init/           SQL scripts run on first container start
config.yaml                  template configuration
docker-compose.yml           MySQL 8 service for local development
```

## Configuration

```yaml
# config.yaml
dbtemplates:
  blog:
    from_db: '_template_blog'    # copy from an existing MySQL database
    buffer: 3                    # number of copies to keep ready
    expire: 24                   # hours before an assigned database is considered expired
  shop:
    from_path: 'dumps/shop'      # copy from a directory of SQL files (mutually exclusive with from_db)
    buffer: 2
    expire: 48
```

Each template requires exactly one source — `from_db` or `from_path`, not both.

**`from_path`** points to a directory. dbshuffle reads all `.sql` and `.sql.gz` files in that directory (in sorted order) and executes them in a single transaction to seed the new database. Plain `.sql` and gzip-compressed `.sql.gz` files can be mixed freely.

## Observability

dbshuffle emits OpenTelemetry traces. Tracing is disabled by default and enabled entirely through env vars — no code changes or flags required.

| Env var | Values | Default | Description |
|---|---|---|---|
| `OTEL_TRACES_EXPORTER` | `otlp` \| `stdout` \| `none` | `none` (unless endpoint set) | Trace exporter to use |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `host:port` or URL | — | Setting this auto-enables `otlp` |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` \| `http/protobuf` | `grpc` | Transport for OTLP |
| `OTEL_METRICS_EXPORTER` | `otlp` \| `stdout` \| `none` | same as traces | Metric exporter (independent of traces) |
| `OTEL_LOGS_EXPORTER` | `otlp` \| `stdout` \| `none` | same as traces | Log exporter (independent of traces) |
| `OTEL_SERVICE_NAME` | string | `dbshuffle` | Service name attached to all signals |

**Quick examples:**

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

**Logs:** All `slog` output is bridged to OTel via the `otelslog` handler and sent alongside the existing stderr text output. The level filter (`--verbose` / default info) applies to both outputs — no extra configuration needed.

**What is traced:**

The HTTP server wraps every request in a span named `METHOD /path` (e.g. `POST /assign`). Each service operation (`Assign`, `Reset`, `Clean`, `Refill`, `Extend`, `Status`) and each database operation (`CopyDB`, `RenameDB`, `DropDB`, `CreateDBFromPath`) is a child span, giving a full call hierarchy per request. Errors are recorded on the span that produced them.

**Metrics (server mode only):**

| Metric | Type | Attribute | Description |
|---|---|---|---|
| `dbshuffle.buffer.size` | Gauge | `template` | Current number of ready buffer copies |
| `dbshuffle.assigned.size` | Gauge | `template` | Current number of assigned databases |

Both gauges are observed together in a single query per collection cycle (default 60 s, configurable via `OTEL_METRIC_EXPORT_INTERVAL`). Templates with no rows yet are reported as zero.

## Prerequisites

- Go 1.21+
- Docker + Docker Compose (for local MySQL)

## Quick start

```bash
# 1. Start MySQL
make db-up

# 2. Wait for healthy, then fill the buffer
make refill

# 3. Check status
make status

# 4. Assign a database
make assign TEMPLATE=blog DB=myfeature_test

# 5. (Optional) Start the HTTP server
make run-server
```

## CLI reference

All flags can be set via environment variables as well.

### Global flags

| Flag | Env | Default | Description |
|---|---|---|---|
| `--db-host` | `DB_HOST` | `localhost` | MySQL host |
| `--db-port` | — | `3306` | MySQL port |
| `--db-user` | `DB_USER` | `root` | MySQL user |
| `--db-password` | `DB_PASSWORD` | _(empty)_ | MySQL password |
| `--config` | — | `config.yaml` | Path to config file |

### Commands

#### `status`
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

#### `assign <template> <dbname>`
Rename a buffered copy to `<dbname>`. Blocks until the rename is complete.

```bash
dbshuffle assign blog myfeature_test
dbshuffle assign shop checkout_test
```

Errors:
- `unknown template` — template name not in config
- `no buffer databases available` — buffer is empty; run `refill` first
- `database name already assigned` — `<dbname>` is already in use

#### `reset <template> <dbname>`
Drop the existing `<dbname>` assignment (if one exists) and assign a fresh buffer copy under the same name. If `<dbname>` is not currently assigned, behaves identically to `assign`.

```bash
dbshuffle reset blog myfeature_test
```

Errors:
- `unknown template` — template name not in config
- `no buffer databases available` — buffer is empty; run `refill` first

#### `refill`
Create buffer copies for all templates until each reaches its configured `buffer` size. Skips templates that are already at capacity.

```bash
dbshuffle refill
```

#### `clean`
Drop all assigned databases whose `last_extended_at + expire` is in the past, and remove their records.

```bash
dbshuffle clean
```

#### `server`
Start the HTTP server. Runs buffer refill in the background on a configurable interval.

```bash
dbshuffle server
dbshuffle server --addr :9090 --refill-period 30s
```

| Flag | Default | Description |
|---|---|---|
| `--addr` / `ADDR` | `:8080` | Listen address |
| `--refill-period` | `1m` | How often the background refill runs |

## HTTP API

All endpoints return `Content-Type: application/json`.

### `GET /status`

```bash
curl http://localhost:8080/status
```

```json
[
  {
    "template": "blog",
    "buffer": [
      {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "template_name": "blog",
        "created_at": "2026-04-22T09:00:00Z"
      }
    ],
    "assigned": [
      {
        "id": "661f9511-f3ac-52e5-b827-557766551111",
        "template_name": "blog",
        "db_name": "myfeature_test",
        "created_at": "2026-04-22T08:00:00Z",
        "assigned_at": "2026-04-22T10:00:00Z",
        "last_extended_at": "2026-04-22T10:00:00Z"
      }
    ],
    "expired": []
  }
]
```

### `POST /assign`

```bash
curl -X POST http://localhost:8080/assign \
  -H 'Content-Type: application/json' \
  -d '{"template": "blog", "db_name": "myfeature_test"}'
```

```json
{
  "id": "661f9511-f3ac-52e5-b827-557766551111",
  "template_name": "blog",
  "db_name": "myfeature_test",
  "created_at": "2026-04-22T08:00:00Z",
  "assigned_at": "2026-04-22T10:00:00Z",
  "last_extended_at": "2026-04-22T10:00:00Z"
}
```

HTTP status codes:
| Code | Meaning |
|---|---|
| `200` | Assigned successfully |
| `404` | Unknown template |
| `409` | Database name already assigned |
| `503` | No buffer databases available |

### `POST /reset`

Drop the existing assignment for `db_name` (if any) and assign a fresh buffer copy under the same name.

```bash
curl -X POST http://localhost:8080/reset \
  -H 'Content-Type: application/json' \
  -d '{"template": "blog", "db_name": "myfeature_test"}'
```

```json
{
  "id": "772a0622-a4bd-63f6-c938-668877662222",
  "template_name": "blog",
  "db_name": "myfeature_test",
  "created_at": "2026-04-22T11:00:00Z",
  "assigned_at": "2026-04-22T12:00:00Z",
  "last_extended_at": "2026-04-22T12:00:00Z"
}
```

HTTP status codes:
| Code | Meaning |
|---|---|
| `200` | Reset and assigned successfully |
| `404` | Unknown template |
| `503` | No buffer databases available |

### `POST /clean`

```bash
curl -X POST http://localhost:8080/clean
```

```json
{"cleaned": 2}
```

### `POST /refill`

```bash
curl -X POST http://localhost:8080/refill
```

```json
{"created": 3}
```

## Docker / local development

```bash
make db-up       # start MySQL 8 in Docker (detached)
make db-down     # stop the MySQL container
make db-shell    # open a mysql shell as root
```

The container mounts `docker/mysql/init/` — any `.sql` files there run automatically on first start. The included `01_templates.sql` creates `_template_blog` and `_template_shop` with seed data.

To reset the database volume entirely:

```bash
docker compose down -v
make db-up
```

## Management database

dbshuffle stores state in `_dbshuffle.databases`:

| Column | Type | Description |
|---|---|---|
| `id` | `CHAR(36)` | UUID v4 primary key |
| `template_name` | `VARCHAR(255)` | Which template this copy belongs to |
| `db_name` | `VARCHAR(255) NULL` | `NULL` = in buffer; value = assigned name |
| `created_at` | `DATETIME` | When the buffer copy was created |
| `assigned_at` | `DATETIME NULL` | When it was assigned |
| `last_extended_at` | `DATETIME NULL` | Used to compute expiry: `last_extended_at + expire hours` |

The physical MySQL database name of a buffer copy is derived as `<template_name>_<uuid_no_hyphens>` and never stored explicitly.

## Makefile targets

| Target | Description |
|---|---|
| `make build` | Compile binary to `bin/dbshuffle` |
| `make run-server` | Run the HTTP server via `go run` |
| `make status` | Print current status |
| `make assign TEMPLATE=x DB=y` | Assign template `x` to database name `y` |
| `make reset TEMPLATE=x DB=y` | Drop existing assignment and assign a fresh copy |
| `make clean` | Drop expired databases |
| `make refill` | Fill buffers up to configured size |
| `make db-up` | Start MySQL in Docker |
| `make db-down` | Stop MySQL container |
| `make db-shell` | Open mysql CLI shell |
| `make tidy` | Run `go mod tidy` |
