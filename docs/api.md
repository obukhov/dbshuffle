# HTTP API

All endpoints return `Content-Type: application/json`.

## `GET /status`

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

## `POST /assign`

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

| Code | Meaning |
|---|---|
| `200` | Assigned successfully |
| `404` | Unknown template |
| `409` | Database name already assigned |
| `503` | No buffer databases available |

## `POST /reset`

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

| Code | Meaning |
|---|---|
| `200` | Reset and assigned successfully |
| `404` | Unknown template |
| `503` | No buffer databases available |

## `POST /clean`

```bash
curl -X POST http://localhost:8080/clean
```

```json
{"cleaned": 2}
```

## `POST /refill`

```bash
curl -X POST http://localhost:8080/refill
```

```json
{"created": 3}
```
