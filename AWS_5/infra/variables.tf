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
  description = "Nombre base para etiquetar los recursos"
  type        = string
  default     = "judge-stream"
}

# --- Kinesis (el "tópico") ---
variable "shards" {
  description = "Shards del stream. 1 shard = 1 MB/s o 1000 registros/s de escritura. Cambiarlo recrea el stream"
  type        = number
  default     = 1
}

# --- Consumidor (Event Source Mapping de la Lambda) ---
variable "batch_size" {
  description = "Registros por invocación de la Lambda (1-10000)"
  type        = number
  default     = 100
}

variable "batching_window" {
  description = "Segundos que espera el Event Source Mapping para llenar el lote (0-300)"
  type        = number
  default     = 1
}

variable "parallelization_factor" {
  description = "Lotes concurrentes por shard (1-10)"
  type        = number
  default     = 1
}

variable "lambda_memory" {
  description = "Memoria de la Lambda en MB (128-10240); solo valida, limpia y carga"
  type        = number
  default     = 256
}

# --- API Gateway (entrada del juez) ---
variable "api_rate_limit" {
  description = "Peticiones por segundo permitidas a la API key (usage plan)"
  type        = number
  default     = 500
}

variable "api_burst_limit" {
  description = "Ráfaga máxima de peticiones simultáneas permitida a la API key"
  type        = number
  default     = 1000
}

variable "api_quota_per_day" {
  description = "Peticiones por día permitidas a la API key"
  type        = number
  default     = 1000000
}

variable "ingest_memory" {
  description = "Memoria de la Lambda de ingesta en MB"
  type        = number
  default     = 256
}

# --- EC2 con Neo4j y Grafana ---
variable "instance_type" {
  description = "Tipo de instancia EC2 para Neo4j y Grafana"
  type        = string
  default     = "t3.medium" # 4 GB RAM: 1 GB heap + 1 GB page cache de Neo4j, más Grafana
}

variable "neo4j_image" {
  description = "Imagen Docker de Neo4j"
  type        = string
  default     = "neo4j:5.26-community"
}

variable "grafana_image" {
  description = "Imagen Docker de Grafana"
  type        = string
  default     = "grafana/grafana-oss:11.2.0"
}

variable "grafana_neo4j_plugin" {
  description = "Plugin de Grafana para usar Neo4j como fuente de datos"
  type        = string
  default     = "kniepdennis-neo4j-datasource"
}

variable "allowed_ssh_cidr" {
  description = "Override opcional para SSH, Neo4j Browser y Grafana. Si se deja en null, Terraform detecta tu IP pública actual y usa /32"
  type        = string
  default     = null
}
