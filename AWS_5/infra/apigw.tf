# --- API Gateway (REST) con API key: la puerta de entrada del juez ---
#
#   POST /submissions        un envío
#   POST /submissions/batch  hasta 500 envíos
#
# Ambas rutas exigen el encabezado x-api-key (sin key -> 403) y van por proxy a la
# Lambda de ingesta, que publica en Kinesis. El usage plan limita peticiones por
# segundo, ráfaga y cuota diaria de la key: si se excede, API Gateway responde 429.

resource "aws_api_gateway_rest_api" "api" {
  name        = "${var.project_name}-api"
  description = "Entrada del juez en linea al pipeline"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "submissions" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id   = aws_api_gateway_rest_api.api.root_resource_id
  path_part   = "submissions"
}

resource "aws_api_gateway_resource" "batch" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id   = aws_api_gateway_resource.submissions.id
  path_part   = "batch"
}

locals {
  api_routes = {
    single = aws_api_gateway_resource.submissions.id
    batch  = aws_api_gateway_resource.batch.id
  }
}

resource "aws_api_gateway_method" "post" {
  for_each         = local.api_routes
  rest_api_id      = aws_api_gateway_rest_api.api.id
  resource_id      = each.value
  http_method      = "POST"
  authorization    = "NONE"
  api_key_required = true
}

resource "aws_api_gateway_integration" "post" {
  for_each                = local.api_routes
  rest_api_id             = aws_api_gateway_rest_api.api.id
  resource_id             = each.value
  http_method             = aws_api_gateway_method.post[each.key].http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.ingest.invoke_arn
}

# Permiso para que API Gateway invoque la Lambda de ingesta
resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.api.execution_arn}/*/*"
}

# Se vuelve a desplegar si cambian rutas, métodos o integraciones
resource "aws_api_gateway_deployment" "api" {
  rest_api_id = aws_api_gateway_rest_api.api.id

  triggers = {
    redeploy = sha1(jsonencode([
      aws_api_gateway_resource.submissions.id,
      aws_api_gateway_resource.batch.id,
      [for m in aws_api_gateway_method.post : m.id],
      [for i in aws_api_gateway_integration.post : i.id],
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_api_gateway_integration.post]
}

resource "aws_api_gateway_stage" "v1" {
  rest_api_id   = aws_api_gateway_rest_api.api.id
  deployment_id = aws_api_gateway_deployment.api.id
  stage_name    = "v1"
}

# --- API key del juez y su usage plan ---
resource "random_password" "api_key" {
  length  = 40
  special = false
}

resource "aws_api_gateway_api_key" "judge" {
  name  = "${var.project_name}-judge-key"
  value = random_password.api_key.result
}

resource "aws_api_gateway_usage_plan" "judge" {
  name = "${var.project_name}-judge-plan"

  api_stages {
    api_id = aws_api_gateway_rest_api.api.id
    stage  = aws_api_gateway_stage.v1.stage_name
  }

  throttle_settings {
    rate_limit  = var.api_rate_limit
    burst_limit = var.api_burst_limit
  }

  quota_settings {
    limit  = var.api_quota_per_day
    period = "DAY"
  }
}

resource "aws_api_gateway_usage_plan_key" "judge" {
  key_id        = aws_api_gateway_api_key.judge.id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.judge.id
}
