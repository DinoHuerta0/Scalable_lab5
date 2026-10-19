# --- Kafka: el "tópico" (equivale a Kinesis + la DLQ de SQS) ---
#
# Un solo broker de Apache Kafka en modo KRaft (k8s/kafka.yaml). deploy.sh crea:
#  - submissions:      cada envío del juez es un registro con llave = user_id, así los
#                      envíos de un usuario caen siempre en la misma partición y en orden.
#                      var.partitions hace el papel de los shards de Kinesis.
#  - submissions-dlq:  lo que la procesadora no pudo escribir tras sus reintentos. Lo
#                      escribe un KafkaSink, el destino de fallas del KafkaSource.
#
# Cambiar var.partitions borra y vuelve a crear el tópico (como cambiar shards en
# AWS_5). Haz `python db.py reset` antes de la siguiente prueba.

locals {
  namespace      = "judge"
  topic          = "submissions"
  dlq_topic      = "submissions-dlq"
  consumer_group = "judge-processor"
}
