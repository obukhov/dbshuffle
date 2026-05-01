# dbshuffle

dbshuffle pre-creates copies of MySQL database templates so they can be assigned to a name instantly — without waiting for a full copy at request time. Useful for test environments where each test run needs a fresh, isolated database.

## Table of contents

- [How it works](#how-it-works)
- [Configuration](#configuration)
- [Observability](docs/observability.md)
- [Quick start](#quick-start)
- [CLI reference](docs/cli.md)
- [HTTP API](docs/api.md)
- [Helm chart](docs/helm.md)
- [Management database](#management-database)
- [Development & contribution](docs/development.md)

## How it works

1. You define one or more **templates** — existing MySQL databases (or a folder with dump) that serve as the source of truth.
2. dbshuffle maintains a **buffer** of ready copies for each template (named `<template>_<uuid>`).
3. When you **assign** a template to a name, one buffered copy is instantly renamed to that name.
4. Assigned databases **expire** after a configurable number of hours. Running **clean** drops them.
5. Running **refill** (or letting the server do it in the background) tops the buffer back up.

All state is tracked in the `_dbshuffle.databases` management table.

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

Traces, metrics, and logs via OpenTelemetry. Disabled by default; enabled entirely through env vars — no code changes required.

See **[docs/observability.md](docs/observability.md)** for env var reference, quick-start examples, and details on what is traced and measured.

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

Commands: `status`, `assign`, `reset`, `refill`, `clean`, `server`. Global flags cover MySQL connection and config path; all flags are also available as env vars.

See **[docs/cli.md](docs/cli.md)** for the full flag reference and command descriptions.

## HTTP API

Endpoints: `GET /status`, `POST /assign`, `POST /reset`, `POST /clean`, `POST /refill`. All return `Content-Type: application/json`.

See **[docs/api.md](docs/api.md)** for request/response shapes and status codes.

## Helm chart

Install from OCI registry; configure MySQL connection, template definitions, and OTel via `values.yaml`.

See **[docs/helm.md](docs/helm.md)** for the full values reference, connection setup, and OTel configuration example.

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

## Development & contribution

See **[docs/development.md](docs/development.md)** for prerequisites, project structure, local MySQL setup, test template descriptions, and the full Makefile reference.
