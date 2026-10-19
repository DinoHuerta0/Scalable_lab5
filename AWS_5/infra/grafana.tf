# Grafana corre en la misma EC2 que Neo4j (contenedor Docker, puerto 3000).
#  - CloudWatch: fuente de datos y dashboard del pipeline se aprovisionan solos.
#  - Neo4j: el plugin queda instalado; la conexión se configura en la interfaz
#    (ver README, sección Grafana).

resource "random_password" "grafana" {
  length  = 16
  special = false
}

locals {
  cw = { type = "cloudwatch", uid = "cloudwatch" }

  # Nombres armados a mano (no referencias a los recursos) para no crear un ciclo:
  # la Lambda depende de la IP de la EC2 y la EC2 recibe este dashboard.
  stream_dim = { StreamName = local.stream_name }
  fn_dim     = { FunctionName = "${var.project_name}-processor" }
  dlq_dim    = { QueueName = "${var.project_name}-dlq" }
  api_dim    = { ApiName = "${var.project_name}-api", Stage = "v1" }
  ingest_dim = { FunctionName = "${var.project_name}-ingest" }

  grafana_panels = [
    {
      title = "API Gateway · peticiones (por minuto)", unit = "short"
      targets = [
        { ns = "AWS/ApiGateway", metric = "Count", dims = local.api_dim, stat = "Sum", label = "peticiones" },
      ]
    },
    {
      title = "API Gateway · latencia", unit = "ms"
      targets = [
        { ns = "AWS/ApiGateway", metric = "Latency", dims = local.api_dim, stat = "Average", label = "promedio" },
        { ns = "AWS/ApiGateway", metric = "Latency", dims = local.api_dim, stat = "p95", label = "p95" },
        { ns = "AWS/ApiGateway", metric = "IntegrationLatency", dims = local.api_dim, stat = "Average", label = "Lambda de ingesta (prom.)" },
      ]
    },
    {
      title = "API Gateway · errores 4XX (incluye 403 y 429) y 5XX", unit = "short"
      targets = [
        { ns = "AWS/ApiGateway", metric = "4XXError", dims = local.api_dim, stat = "Sum", label = "4XX" },
        { ns = "AWS/ApiGateway", metric = "5XXError", dims = local.api_dim, stat = "Sum", label = "5XX" },
      ]
    },
    {
      title = "Lambdas · throttles (límite de concurrencia)", unit = "short"
      targets = [
        { ns = "AWS/Lambda", metric = "Throttles", dims = local.ingest_dim, stat = "Sum", label = "ingesta" },
        { ns = "AWS/Lambda", metric = "Throttles", dims = local.fn_dim, stat = "Sum", label = "procesadora" },
      ]
    },
    {
      title = "Kinesis · registros entrantes vs leídos (por minuto)", unit = "short"
      targets = [
        { ns = "AWS/Kinesis", metric = "IncomingRecords", dims = local.stream_dim, stat = "Sum", label = "entrantes" },
        { ns = "AWS/Kinesis", metric = "GetRecords.Records", dims = local.stream_dim, stat = "Sum", label = "leídos" },
      ]
    },
    {
      title = "Kinesis · escrituras rechazadas por throttling", unit = "short"
      targets = [
        { ns = "AWS/Kinesis", metric = "WriteProvisionedThroughputExceeded", dims = local.stream_dim, stat = "Sum", label = "throttled" },
      ]
    },
    {
      title = "Lambda procesadora · invocaciones y errores", unit = "short"
      targets = [
        { ns = "AWS/Lambda", metric = "Invocations", dims = local.fn_dim, stat = "Sum", label = "invocaciones" },
        { ns = "AWS/Lambda", metric = "Errors", dims = local.fn_dim, stat = "Sum", label = "errores" },
      ]
    },
    {
      title = "Lambda procesadora · duración por lote", unit = "ms"
      targets = [
        { ns = "AWS/Lambda", metric = "Duration", dims = local.fn_dim, stat = "Average", label = "promedio" },
        { ns = "AWS/Lambda", metric = "Duration", dims = local.fn_dim, stat = "Maximum", label = "máximo" },
      ]
    },
    {
      title = "Atraso del consumidor (IteratorAge)", unit = "ms"
      targets = [
        { ns = "AWS/Lambda", metric = "IteratorAge", dims = local.fn_dim, stat = "Maximum", label = "Lambda IteratorAge" },
        { ns = "AWS/Kinesis", metric = "GetRecords.IteratorAgeMilliseconds", dims = local.stream_dim, stat = "Maximum", label = "Kinesis IteratorAge" },
      ]
    },
    {
      title = "Lambda procesadora · ejecuciones concurrentes", unit = "short"
      targets = [
        { ns = "AWS/Lambda", metric = "ConcurrentExecutions", dims = local.fn_dim, stat = "Maximum", label = "concurrencia" },
      ]
    },
    {
      title = "Kinesis · bytes entrantes", unit = "bytes"
      targets = [
        { ns = "AWS/Kinesis", metric = "IncomingBytes", dims = local.stream_dim, stat = "Sum", label = "bytes" },
      ]
    },
    {
      title = "DLQ · lotes fallidos en la cola", unit = "short"
      targets = [
        { ns = "AWS/SQS", metric = "ApproximateNumberOfMessagesVisible", dims = local.dlq_dim, stat = "Maximum", label = "mensajes" },
      ]
    },
  ]

  grafana_dashboard = {
    uid           = "judge-pipeline"
    title         = "Lab 5 · Pipeline API Gateway → Kinesis → Lambda → Neo4j"
    tags          = ["lab5", "apigateway", "kinesis", "lambda"]
    refresh       = "10s"
    time          = { from = "now-30m", to = "now" }
    schemaVersion = 39
    editable      = true
    panels = [for i, p in local.grafana_panels : {
      id         = i + 1
      type       = "timeseries"
      title      = p.title
      datasource = local.cw
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
        refId            = substr("ABCDEFGH", j, 1)
        datasource       = local.cw
        queryMode        = "Metrics"
        region           = "default"
        namespace        = t.ns
        metricName       = t.metric
        dimensions       = t.dims
        statistic        = t.stat
        period           = "60"
        matchExact       = true
        metricQueryType  = 0
        metricEditorMode = 0
        id               = ""
        expression       = ""
        label            = t.label
      }]
    }]
  }
}
