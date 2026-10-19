# Grafana corre en k3s (k8s/grafana.yaml, puerto 3000).
#  - Prometheus: la fuente de datos y el dashboard del pipeline se aprovisionan solos
#    (deploy.sh guarda este dashboard en el ConfigMap grafana-dashboards).
#  - Neo4j: el plugin queda instalado; la conexión se configura en la interfaz
#    (ver README, sección Grafana).
# Cambiar un panel aquí no recrea nada: el siguiente apply actualiza el dashboard.

resource "random_password" "grafana" {
  length  = 16
  special = false
}

locals {
  prom = { type = "prometheus", uid = "prometheus" }

  kong_sel = "service=\"${local.kong_service}\""
  group    = "consumergroup=\"${local.consumer_group}\""

  grafana_panels = [
    {
      title = "Kong · peticiones por código (por segundo)", unit = "reqps"
      targets = [
        { expr = "sum by (code) (rate(kong_http_requests_total{${local.kong_sel}}[1m]))", legend = "{{code}}" },
      ]
    },
    {
      title = "Kong · latencia", unit = "ms"
      targets = [
        { expr = "histogram_quantile(0.5, sum by (le) (rate(kong_request_latency_ms_bucket{${local.kong_sel}}[1m])))", legend = "p50" },
        { expr = "histogram_quantile(0.95, sum by (le) (rate(kong_request_latency_ms_bucket{${local.kong_sel}}[1m])))", legend = "p95" },
        { expr = "histogram_quantile(0.95, sum by (le) (rate(kong_upstream_latency_ms_bucket{${local.kong_sel}}[1m])))", legend = "ingesta p95 (upstream)" },
      ]
    },
    {
      title = "Kong · rechazos 401 y 429, errores 5xx (por segundo)", unit = "reqps"
      targets = [
        { expr = "sum(rate(kong_http_requests_total{${local.kong_sel},code=\"401\"}[1m])) or vector(0)", legend = "401 (sin API key)" },
        { expr = "sum(rate(kong_http_requests_total{${local.kong_sel},code=\"429\"}[1m])) or vector(0)", legend = "429 (límite)" },
        { expr = "sum(rate(kong_http_requests_total{${local.kong_sel},code=~\"5..\"}[1m])) or vector(0)", legend = "5xx" },
      ]
    },
    {
      title = "Ingesta · envíos publicados en Kafka (por segundo)", unit = "short"
      targets = [
        { expr = "sum(rate(ingest_records_total{result=\"ok\"}[1m])) or vector(0)", legend = "publicados" },
        { expr = "sum(rate(ingest_records_total{result=\"failed\"}[1m])) or vector(0)", legend = "fallidos (503)" },
      ]
    },
    {
      title = "Kafka · registros entrantes vs consumidos (por segundo)", unit = "short"
      targets = [
        { expr = "sum(rate(kafka_topic_partition_current_offset{topic=\"${local.topic}\"}[1m]))", legend = "entrantes" },
        { expr = "sum(rate(kafka_consumergroup_current_offset{${local.group},topic=\"${local.topic}\"}[1m]))", legend = "consumidos" },
      ]
    },
    {
      title = "Ingesta · latencia de publicación en Kafka", unit = "ms"
      targets = [
        { expr = "1000 * histogram_quantile(0.5, sum by (le) (rate(ingest_publish_seconds_bucket[1m])))", legend = "p50" },
        { expr = "1000 * histogram_quantile(0.95, sum by (le) (rate(ingest_publish_seconds_bucket[1m])))", legend = "p95" },
      ]
    },
    {
      title = "Procesadora · envíos escritos, inválidos y fallidos (por segundo)", unit = "short"
      targets = [
        { expr = "sum by (result) (rate(processor_records_total[1m]))", legend = "{{result}}" },
      ]
    },
    {
      title = "Procesadora · escritura de cada lote en Neo4j", unit = "ms"
      targets = [
        { expr = "1000 * sum(rate(processor_neo4j_seconds_sum[1m])) / sum(rate(processor_neo4j_seconds_count[1m]))", legend = "promedio" },
        { expr = "1000 * histogram_quantile(0.95, sum by (le) (rate(processor_neo4j_seconds_bucket[1m])))", legend = "p95" },
      ]
    },
    {
      title = "Atraso del consumidor (lag, en registros)", unit = "short"
      targets = [
        { expr = "sum(kafka_consumergroup_lag{${local.group},topic=\"${local.topic}\"})", legend = "total" },
        { expr = "sum by (partition) (kafka_consumergroup_lag{${local.group},topic=\"${local.topic}\"})", legend = "partición {{partition}}" },
      ]
    },
    {
      title = "Pods de Knative (ingesta y procesadora)", unit = "short"
      targets = [
        { expr = "count(up{job=\"ingest\"} == 1) or vector(0)", legend = "ingesta" },
        { expr = "count(up{job=\"processor\"} == 1) or vector(0)", legend = "procesadora" },
      ]
    },
    {
      title = "Procesadora · tamaño promedio del lote", unit = "short"
      targets = [
        { expr = "sum(rate(processor_batch_size_sum[1m])) / sum(rate(processor_batch_size_count[1m]))", legend = "envíos por lote" },
      ]
    },
    {
      title = "DLQ · eventos en ${local.dlq_topic}", unit = "short"
      targets = [
        { expr = "sum(kafka_topic_partition_current_offset{topic=\"${local.dlq_topic}\"})", legend = "eventos" },
      ]
    },
  ]

  grafana_dashboard = {
    uid           = "judge-pipeline-k3s"
    title         = "Lab 5 · Pipeline Kong → Kafka → Knative → Neo4j"
    tags          = ["lab5", "k3s", "kafka", "knative"]
    refresh       = "10s"
    time          = { from = "now-30m", to = "now" }
    schemaVersion = 39
    editable      = true
    panels = [for i, p in local.grafana_panels : {
      id         = i + 1
      type       = "timeseries"
      title      = p.title
      datasource = local.prom
      gridPos    = { h = 8, w = 12, x = (i % 2) * 12, y = floor(i / 2) * 8 }
      fieldConfig = {
        defaults  = { unit = p.unit, custom = { drawStyle = "line", lineWidth = 2, fillOpacity = 10, spanNulls = true } }
        overrides = []
      }
      options = {
        legend  = { displayMode = "list", placement = "bottom", showLegend = true }
        tooltip = { mode = "multi", sort = "none" }
      }
      targets = [for j, t in p.targets : {
        refId        = substr("ABCDEFGH", j, 1)
        datasource   = local.prom
        expr         = t.expr
        legendFormat = t.legend
        range        = true
      }]
    }]
  }
}
