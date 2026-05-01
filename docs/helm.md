# Helm chart

## Install

```bash
helm install dbshuffle oci://ghcr.io/obukhov/charts/dbshuffle --version <version>
```

Or with a custom values file:

```bash
helm install dbshuffle oci://ghcr.io/obukhov/charts/dbshuffle \
  --version <version> \
  -f my-values.yaml
```

## Key values

| Value | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/obukhov/dbshuffle` | Container image |
| `image.tag` | `v0.1.0` | Image tag (set to the chart's appVersion on release) |
| `replicaCount` | `1` | Number of server replicas |
| `config` | `dbtemplates: {}` | Contents of `config.yaml` mounted into the pod |
| `env` | `[]` | Extra env vars (see below) |
| `service.port` | `8080` | ClusterIP port |
| `ingress.enabled` | `false` | Enable Ingress |
| `httpRoute.enabled` | `false` | Enable Gateway API HTTPRoute |
| `cronjobs.refill.schedule` | `* * * * *` | Cron schedule for buffer refill |
| `cronjobs.clean.schedule` | `0 * * * *` | Cron schedule for expired DB cleanup |

> **Warning:** dbshuffle has no built-in authentication. Only expose it via Ingress or HTTPRoute inside a trusted network, behind an auth proxy, or with network policies.

## MySQL connection

Pass MySQL connection details via `env`:

```yaml
env:
  - name: DB_HOST
    value: "mysql.default.svc.cluster.local"
  - name: DB_PORT
    value: "3306"
  - name: DB_USER
    value: "dbshuffle"
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: mysql-secret
        key: password
```

## Template configuration

Set the `config` value to the full contents of your `config.yaml`:

```yaml
config: |
  dbtemplates:
    blog:
      from_db: '_template_blog'
      buffer: 3
      expire: 24
    shop:
      from_path: 'dumps/shop'
      buffer: 2
      expire: 48
```

## OpenTelemetry

OTel is configured entirely through env vars — no code changes or chart flags required:

```yaml
env:
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "otel-collector.monitoring.svc.cluster.local:4317"
  - name: OTEL_EXPORTER_OTLP_PROTOCOL
    value: "grpc"                      # or http/protobuf
  - name: OTEL_SERVICE_NAME
    value: "dbshuffle"
  # Optional: independent control per signal
  - name: OTEL_TRACES_EXPORTER
    value: "otlp"
  - name: OTEL_METRICS_EXPORTER
    value: "otlp"
  - name: OTEL_LOGS_EXPORTER
    value: "otlp"
```

Setting `OTEL_EXPORTER_OTLP_ENDPOINT` is sufficient to enable all three signals (traces, metrics, logs) over gRPC. See [Observability](observability.md) for the full env var reference.
