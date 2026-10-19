variable "aws_profile" {
  description = "Perfil de AWS CLI a usar"
  type        = string
  default     = "academy"
}

variable "aws_region" {
  description = "Región de AWS donde se despliega todo"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Nombre base para etiquetar los recursos (distinto de AWS_5 para poder tener los dos desplegados)"
  type        = string
  default     = "judge-stream-k3s"
}

# --- EC2 con k3s (AWS Academy no permite más que large) ---
variable "instance_type" {
  description = "Tipo de instancia del servidor de k3s (Knative, Kong, funciones, Prometheus y Grafana)"
  type        = string
  default     = "t3.large" # 2 vCPU y 8 GB
}

variable "data_instance_type" {
  description = "Tipo de instancia del nodo de datos (Kafka y Neo4j)"
  type        = string
  default     = "t3.large" # 2 vCPU y 8 GB
}

variable "volume_size" {
  description = "Tamaño del disco de cada nodo en GB (imágenes, datos de Kafka y de Neo4j)"
  type        = number
  default     = 30
}

variable "allowed_ssh_cidr" {
  description = "Override opcional para SSH, la API, Neo4j Browser, Grafana y Prometheus. Si se deja en null, Terraform detecta tu IP pública actual y usa /32"
  type        = string
  default     = null
}

variable "knative_version" {
  description = "Versión de Knative Serving, Kourier, Eventing y eventing-kafka-broker (la misma de k3s_4)"
  type        = string
  default     = "knative-v1.16.0"
}

# --- Kafka (el "tópico") ---
variable "partitions" {
  description = "Particiones del tópico de envíos (equivalen a los shards de Kinesis). Cambiarlo recrea el tópico"
  type        = number
  default     = 1
}

# --- Consumidor (KafkaSource de Knative Eventing) ---
variable "consumers" {
  description = "Consumidores del KafkaSource en el grupo. Más consumidores que particiones no ayuda"
  type        = number
  default     = 1
}

variable "ordering" {
  description = "ordered: un evento a la vez por partición (como Lambda por shard). unordered: varios eventos en paralelo por partición"
  type        = string
  default     = "unordered"

  validation {
    condition     = contains(["ordered", "unordered"], var.ordering)
    error_message = "ordering debe ser ordered o unordered."
  }
}

variable "max_poll_records" {
  description = "Registros que el despachador de KafkaSource lee de Kafka por cada poll (en unordered limita los eventos en vuelo)"
  type        = number
  default     = 50
}

variable "batch_size" {
  description = "Máximo de envíos que la procesadora escribe en Neo4j en una sola transacción"
  type        = number
  default     = 100
}

variable "batching_window" {
  description = "Segundos que la procesadora espera para llenar el lote (0 = escribe lo que haya en cuanto termina el lote anterior)"
  type        = number
  default     = 0
}

variable "processor_memory" {
  description = "Límite de memoria de cada pod de la procesadora en MB (equivale a lambda_memory)"
  type        = number
  default     = 256
}

variable "processor_concurrency" {
  description = "Eventos simultáneos por pod de la procesadora; Knative agrega pods cuando se llenan"
  type        = number
  default     = 100
}

variable "processor_min_scale" {
  description = "Pods mínimos de la procesadora (0 = escala a cero como Lambda)"
  type        = number
  default     = 0
}

variable "processor_max_scale" {
  description = "Pods máximos de la procesadora (equivale al límite de concurrencia de Lambda)"
  type        = number
  default     = 4
}

# --- Kong (entrada del juez) ---
variable "api_rate_limit" {
  description = "Peticiones por segundo permitidas a la API key (plugin rate-limiting de Kong)"
  type        = number
  default     = 500
}

variable "api_quota_per_day" {
  description = "Peticiones por día permitidas a la API key"
  type        = number
  default     = 1000000
}

variable "ingest_memory" {
  description = "Límite de memoria de cada pod de ingesta en MB"
  type        = number
  default     = 256
}

variable "ingest_max_scale" {
  description = "Pods máximos de la ingesta"
  type        = number
  default     = 4
}

# --- Imágenes ---
variable "kafka_image" {
  description = "Imagen de Apache Kafka (modo KRaft, sin ZooKeeper)"
  type        = string
  default     = "apache/kafka:3.8.0"
}

variable "kong_image" {
  description = "Imagen de Kong Gateway (open source)"
  type        = string
  default     = "kong:3.8"
}

variable "neo4j_image" {
  description = "Imagen de Neo4j"
  type        = string
  default     = "neo4j:5.26-community"
}

variable "neo4j_heap" {
  description = "Heap de Neo4j"
  type        = string
  default     = "2G"
}

variable "neo4j_pagecache" {
  description = "Page cache de Neo4j"
  type        = string
  default     = "2G"
}

variable "grafana_image" {
  description = "Imagen de Grafana"
  type        = string
  default     = "grafana/grafana-oss:11.2.0"
}

variable "grafana_neo4j_plugin" {
  description = "Plugin de Grafana para usar Neo4j como fuente de datos"
  type        = string
  default     = "kniepdennis-neo4j-datasource"
}

variable "prometheus_image" {
  description = "Imagen de Prometheus"
  type        = string
  default     = "prom/prometheus:v2.54.1"
}

variable "kafka_exporter_image" {
  description = "Imagen de kafka-exporter (offsets y lag del consumidor para Prometheus)"
  type        = string
  default     = "danielqsj/kafka-exporter:v1.8.0"
}
