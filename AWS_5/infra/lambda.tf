# AWS Academy no permite crear roles: se usa LabRole
data "aws_iam_role" "lab" { name = "LabRole" }

# --- Lambda consumidora: Kinesis -> valida, limpia y carga en Neo4j ---
# Sin dependencias externas (Neo4j se usa por su API HTTP con urllib), así que basta
# con comprimir src/ (sin ingest.py, que va en su propio zip).
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/../.build/lambda.zip"
  excludes    = ["__pycache__", "ingest.py"]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}-processor"
  retention_in_days = 3
}

resource "aws_lambda_function" "processor" {
  function_name    = "${var.project_name}-processor"
  role             = data.aws_iam_role.lab.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 60
  memory_size      = var.lambda_memory

  # En la VPC para llegar a Neo4j por IP privada
  vpc_config {
    subnet_ids         = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.lambda.id]
  }

  environment {
    variables = {
      NEO4J_URL  = "http://${aws_instance.neo4j.private_ip}:7474"
      NEO4J_USER = "neo4j"
      NEO4J_PASS = random_password.neo4j.result
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}

# Kinesis -> Lambda: el servicio de Lambda lee los shards y nos entrega lotes
resource "aws_lambda_event_source_mapping" "kinesis" {
  event_source_arn  = data.aws_kinesis_stream.submissions.arn
  function_name     = aws_lambda_function.processor.arn
  starting_position = "TRIM_HORIZON"

  batch_size                         = var.batch_size
  maximum_batching_window_in_seconds = var.batching_window
  parallelization_factor             = var.parallelization_factor

  # Manejo de errores: reintenta, parte el lote a la mitad para aislar el registro
  # malo y, si se agotan los reintentos, manda el aviso a la DLQ y sigue avanzando.
  function_response_types        = ["ReportBatchItemFailures"]
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3
  maximum_record_age_in_seconds  = 3600

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }

  # Neo4j debe existir antes de empezar a consumir
  depends_on = [aws_instance.neo4j]

  # Si el stream se vuelve a crear (cambio de shards), el mapping también
  lifecycle {
    replace_triggered_by = [terraform_data.stream]
  }
}

# --- Lambda de ingesta: API Gateway -> Kinesis ---
# Fuera de la VPC: solo habla con Kinesis (endpoint público de AWS), arranca más
# rápido y no crea interfaces de red que retrasen el destroy.
data "archive_file" "ingest" {
  type        = "zip"
  source_file = "${path.module}/../src/ingest.py"
  output_path = "${path.module}/../.build/ingest.zip"
}

resource "aws_cloudwatch_log_group" "ingest" {
  name              = "/aws/lambda/${var.project_name}-ingest"
  retention_in_days = 3
}

resource "aws_lambda_function" "ingest" {
  function_name    = "${var.project_name}-ingest"
  role             = data.aws_iam_role.lab.arn
  runtime          = "python3.12"
  handler          = "ingest.handler"
  filename         = data.archive_file.ingest.output_path
  source_code_hash = data.archive_file.ingest.output_base64sha256
  timeout          = 10
  memory_size      = var.ingest_memory

  environment {
    variables = {
      STREAM_NAME = local.stream_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.ingest, terraform_data.stream]
}
