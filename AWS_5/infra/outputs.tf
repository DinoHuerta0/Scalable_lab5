# Los scripts de tests/ leen api_url, api_key, stream_name, lambda_name,
# ingest_lambda_name, api_name, dlq_url, region, profile, neo4j_url,
# neo4j_password y consumer_config: no cambiar esos nombres.

output "api_url" {
  description = "URL base de la API (POST /submissions y /submissions/batch)"
  value       = aws_api_gateway_stage.v1.invoke_url
}

output "api_key" {
  description = "API key del juez (encabezado x-api-key)"
  value       = aws_api_gateway_api_key.judge.value
  sensitive   = true
}

output "api_name" {
  description = "Nombre de la API (dimensión ApiName en CloudWatch)"
  value       = aws_api_gateway_rest_api.api.name
}

output "ingest_lambda_name" {
  description = "Lambda que recibe las peticiones de la API y publica en Kinesis"
  value       = aws_lambda_function.ingest.function_name
}

output "stream_name" {
  description = "Stream de Kinesis donde publica el juez"
  value       = data.aws_kinesis_stream.submissions.name
}

output "lambda_name" {
  description = "Lambda que valida, limpia y carga en Neo4j"
  value       = aws_lambda_function.processor.function_name
}

output "dlq_url" {
  description = "Cola SQS con los lotes que agotaron sus reintentos"
  value       = aws_sqs_queue.dlq.url
}

output "region" {
  description = "Región de AWS"
  value       = var.aws_region
}

output "profile" {
  description = "Perfil de AWS CLI"
  value       = var.aws_profile
}

output "neo4j_url" {
  description = "API HTTP de Neo4j (la usan los scripts de tests/)"
  value       = "http://${aws_instance.neo4j.public_ip}:7474"
}

output "neo4j_browser" {
  description = "Neo4j Browser (usuario neo4j)"
  value       = "http://${aws_instance.neo4j.public_ip}:7474/browser/"
}

output "neo4j_password" {
  description = "Contraseña del usuario neo4j"
  value       = random_password.neo4j.result
  sensitive   = true
}

output "grafana_url" {
  description = "Grafana (usuario admin)"
  value       = "http://${aws_instance.neo4j.public_ip}:3000"
}

output "grafana_password" {
  description = "Contraseña del usuario admin de Grafana"
  value       = random_password.grafana.result
  sensitive   = true
}

output "grafana_neo4j_url" {
  description = "URL para configurar la fuente Neo4j dentro de Grafana"
  value       = "bolt://neo4j:7687"
}

# Ruta del .pem sin comillas para que Terraform no agregue escapes (\")
# y el comando se pueda copiar y pegar directo en Git Bash / bash.
output "ssh_command" {
  description = "Comando para conectarte por SSH a la EC2 de Neo4j y Grafana (copiar y pegar en bash)"
  value       = "ssh -i ${abspath(local_sensitive_file.pem.filename)} ec2-user@${aws_instance.neo4j.public_ip}"
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
