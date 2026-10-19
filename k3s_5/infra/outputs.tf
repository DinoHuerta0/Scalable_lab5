# Los outputs no solo se imprimen al final del apply: quedan guardados en el state y
# los scripts de tests/ los leen con `terraform output -json` (tests/common.py).
# Si borras uno de los que usan los tests, el siguiente apply lo quita del state y
# los scripts fallan. Los secretos van como sensitive: en pantalla salen como
# <sensitive>, pero `terraform output -raw <nombre>` y los scripts los leen igual.
#
# Todo se expone en la IP pública del servidor (k3s publica los Services LoadBalancer ahí).
# Dependen del despliegue (deploy.tf) para que, cuando aparezcan, ya todo responda.

# --- Lo que usas tú (también se imprime al terminar el apply) ---
output "api_url" {
  description = "URL base de la API en Kong (POST /submissions y /submissions/batch)"
  value       = "http://${aws_instance.server.public_ip}"
  depends_on  = [terraform_data.deploy]
}

output "neo4j_browser" {
  description = "Neo4j Browser (usuario neo4j)"
  value       = "http://${aws_instance.server.public_ip}:7474/browser/"
  depends_on  = [terraform_data.deploy]
}

output "grafana_url" {
  description = "Grafana (usuario admin)"
  value       = "http://${aws_instance.server.public_ip}:3000"
  depends_on  = [terraform_data.deploy]
}

output "grafana_neo4j_url" {
  description = "URL que se pone en la fuente Neo4j de Grafana (red interna de k3s)"
  value       = "bolt://neo4j.${local.namespace}.svc.cluster.local:7687"
}

output "prometheus_url" {
  description = "Prometheus (lo consulta metrics.py)"
  value       = "http://${aws_instance.server.public_ip}:9090"
  depends_on  = [terraform_data.deploy]
}

# Ruta del .pem sin comillas para que Terraform no agregue escapes (\")
# y el comando se pueda copiar y pegar directo en Git Bash / bash.
output "ssh_command" {
  description = "SSH al servidor de k3s, donde funciona kubectl (copiar y pegar en bash)"
  value       = "ssh -i ${abspath(local_sensitive_file.pem.filename)} ec2-user@${aws_instance.server.public_ip}"
}

output "ssh_command_data" {
  description = "SSH al nodo de datos (Kafka y Neo4j), solo para revisar su /var/log/bootstrap.log"
  value       = "ssh -i ${abspath(local_sensitive_file.pem.filename)} ec2-user@${aws_instance.data.public_ip}"
}

# --- Secretos (terraform output -raw <nombre>) ---
output "api_key" {
  description = "API key del juez (encabezado x-api-key). La leen producer.py y load_test.py"
  value       = random_password.api_key.result
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
output "neo4j_url" {
  description = "API HTTP de Neo4j desde tu computadora (tests/common.py)"
  value       = "http://${aws_instance.server.public_ip}:7474"
  depends_on  = [terraform_data.deploy]
}

output "topic_name" {
  description = "Tópico de envíos (producer.py y metrics.py)"
  value       = local.topic
}

output "dlq_topic" {
  description = "Tópico DLQ (metrics.py)"
  value       = local.dlq_topic
}

output "consumer_group" {
  description = "Grupo de consumidores del KafkaSource, para medir el lag (metrics.py)"
  value       = local.consumer_group
}

output "kong_service" {
  description = "Servicio de Kong en las métricas de Prometheus (metrics.py)"
  value       = local.kong_service
}

output "consumer_config" {
  description = "Configuración del consumidor (la guarda metrics.py en experiments.csv)"
  value = {
    partitions            = var.partitions
    consumers             = var.consumers
    ordering              = var.ordering
    max_poll_records      = var.max_poll_records
    batch_size            = var.batch_size
    batching_window       = var.batching_window
    processor_memory      = var.processor_memory
    processor_concurrency = var.processor_concurrency
    processor_max_scale   = var.processor_max_scale
    api_rate_limit        = var.api_rate_limit
  }
}
