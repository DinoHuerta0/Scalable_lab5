# --- Kong: la puerta de entrada del juez (equivale a API Gateway + usage plan) ---
#
#   POST /submissions        un envío
#   POST /submissions/batch  hasta 500 envíos
#
# Kong corre sin base de datos (modo DB-less): toda su configuración es este archivo
# declarativo, que deploy.sh guarda en el ConfigMap kong-config.
#  - key-auth: exige el encabezado x-api-key (sin key o con key inválida -> 401).
#  - rate-limiting: peticiones por segundo y por día de la key (si se excede -> 429).
#    A diferencia del usage plan, Kong open source no tiene límite de ráfaga.
#  - prometheus: peticiones por código y latencias, para Grafana y metrics.py.
# Kong reenvía a la ingesta por su nombre interno de Knative; Kourier lo enruta al pod.

resource "random_password" "api_key" {
  length  = 40
  special = false
}

locals {
  kong_service = "ingest" # etiqueta "service" de las métricas de Kong

  kong_config = {
    _format_version = "3.0"

    services = [{
      name            = local.kong_service
      url             = "http://ingest.${local.namespace}.svc.cluster.local"
      retries         = 0
      connect_timeout = 5000
      read_timeout    = 30000
      write_timeout   = 30000

      routes = [{
        name       = "submissions"
        paths      = ["/submissions"] # también cubre /submissions/batch
        methods    = ["POST"]
        strip_path = false
      }]

      plugins = [
        {
          name   = "key-auth"
          config = { key_names = ["x-api-key"], hide_credentials = true }
        },
        {
          name = "rate-limiting"
          config = {
            second         = var.api_rate_limit
            day            = var.api_quota_per_day
            policy         = "local"
            limit_by       = "consumer"
            fault_tolerant = true
          }
        },
      ]
    }]

    consumers = [{
      username            = "judge"
      keyauth_credentials = [{ key = random_password.api_key.result }]
    }]

    plugins = [{
      name = "prometheus"
      config = {
        status_code_metrics     = true
        latency_metrics         = true
        bandwidth_metrics       = true
        upstream_health_metrics = false
      }
    }]
  }

  kong_yaml = yamlencode(local.kong_config)
}
