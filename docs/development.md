# Development & contribution

## Prerequisites

- Go 1.21+
- Docker + Docker Compose (for local MySQL)

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
templates/                   SQL dump directories used as from_path templates
config.yaml                  template configuration
docker-compose.yml           MySQL 8 service for local development
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

## Local MySQL

Start and stop the development database:

```bash
make db-up       # start MySQL 8 in Docker (detached)
make db-down     # stop the MySQL container
make db-shell    # open a mysql shell as root
```

The container mounts `docker/mysql/init/` — any `.sql` files there run automatically on first start.

To reset the database volume entirely:

```bash
docker compose down -v
make db-up
```

## Test templates

Two templates are pre-configured for local development and testing:

**`_template_blog`** (loaded from `docker/mysql/init/01_templates.sql`, used via `from_db`)
Exercises the full `CopyDB` surface: named foreign keys, virtual and stored generated columns, two views, a stored function, a stored procedure, and a trigger.

**`shop`** (loaded from `templates/shop/`, used via `from_path`)
Covers the `CreateDBFromPath` path with the same object variety: named FKs with `ON DELETE`/`ON UPDATE` rules, `VIRTUAL` and `STORED` generated columns, a view, a function, a procedure, and a trigger.

Both templates include seed data so buffer copies are non-empty from the start.

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
