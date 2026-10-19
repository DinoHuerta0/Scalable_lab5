# Los outputs no solo se imprimen al final del apply: quedan guardados en el state y
# los scripts de tests/ los leen con `terraform output -json` (tests/common.py).
# Si borras uno de los que usan los tests, el siguiente apply lo quita del state y
# los scripts fallan. Los secretos van como sensitive: en pantalla salen como
# <sensitive>, pero `terraform output -raw <nombre>` y los scripts los leen igual.

# --- Lo que usas tú (también se imprime al terminar el apply) ---
output "api_url" {
  description = "URL base de la API (POST /submissions y /submissions/batch)"
  value       = aws_api_gateway_stage.v1.invoke_url
}

output "neo4j_browser" {
  description = "Neo4j Browser (usuario neo4j)"
  value       = "http://${aws_instance.neo4j.public_ip}:7474/browser/"
}

output "grafana_url" {
  description = "Grafana (usuario admin)"
  value       = "http://${aws_instance.neo4j.public_ip}:3000"
}

output "grafana_neo4j_url" {
  description = "URL que se pone en la fuente Neo4j de Grafana (red de Docker de la EC2)"
  value       = "bolt://neo4j:7687"
}

# Ruta del .pem sin comillas para que Terraform no agregue escapes (\")
# y el comando se pueda copiar y pegar directo en Git Bash / bash.
output "ssh_command" {
  description = "Comando para conectarte por SSH a la EC2 de Neo4j y Grafana (copiar y pegar en bash)"
  value       = "ssh -i ${abspath(local_sensitive_file.pem.filename)} ec2-user@${aws_instance.neo4j.public_ip}"
}

# --- Secretos (terraform output -raw <nombre>) ---
output "api_key" {
  description = "API key del juez (encabezado x-api-key). La leen producer.py y load_test.py"
  value       = aws_api_gateway_api_key.judge.value
  sensitive   = true
}

output "neo4j_password" {
  description = "Contraseña del usuario neo4j. La leen db.py, queries.py y metrics.py"
  value       = random_password.neo4j.result
  sensitive   = true
}

output "grafana_password" {
  description = "Contraseña del usuario admin de Grafana"
  value       = random_password.grafana.result
  sensitive   = true
}

# --- Los leen los scripts de tests/ (no cambiar los nombres) ---
output "profile" {
  description = "Perfil de AWS CLI (boto3 en tests/common.py)"
  value       = var.aws_profile
}

output "region" {
  description = "Región de AWS (boto3 en tests/common.py)"
  value       = var.aws_region
}

output "neo4j_url" {
  description = "API HTTP de Neo4j desde tu computadora (tests/common.py)"
  value       = "http://${aws_instance.neo4j.public_ip}:7474"
}

output "stream_name" {
  description = "Stream de Kinesis (producer.py y metrics.py)"
  value       = local.stream_name
}

output "api_name" {
  description = "Nombre de la API en API Gateway (métricas de CloudWatch en metrics.py)"
  value       = aws_api_gateway_rest_api.api.name
}

output "ingest_lambda_name" {
  description = "Lambda de ingesta (métricas de CloudWatch en metrics.py)"
  value       = aws_lambda_function.ingest.function_name
}

output "lambda_name" {
  description = "Lambda procesadora (métricas y logs en metrics.py)"
  value       = aws_lambda_function.processor.function_name
}

output "dlq_url" {
  description = "Cola SQS con los lotes que agotaron sus reintentos (metrics.py)"
  value       = aws_sqs_queue.dlq.url
}

output "consumer_config" {
  description = "Configuración del consumidor (la guarda metrics.py en experiments.csv)"
  value = {
    shards                 = var.shards
    batch_size             = var.batch_size
    batching_window        = var.batching_window
    parallelization_factor = var.parallelization_factor
    lambda_memory          = var.lambda_memory
    api_rate_limit         = var.api_rate_limit
    api_burst_limit        = var.api_burst_limit
  }
}
