# Destino de fallas: cuando un lote agota sus reintentos, el Event Source Mapping
# manda aquí los metadatos (shard, rango de secuencias) y el shard sigue avanzando.
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.project_name}-dlq"
  message_retention_seconds = 345600 # 4 días
}
