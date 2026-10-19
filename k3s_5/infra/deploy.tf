# --- Despliegue del pipeline por SSH ---
#
# Después de crear las instancias, Terraform entra por SSH al servidor con el .pem que
# generó, sube src/ y k8s/ y corre scripts/deploy.sh.tftpl:
#   1. espera a que user_data instale la plataforma y a que el nodo de datos se una
#      (solo la primera vez);
#   2. construye la imagen de las funciones si cambió src/;
#   3. aplica los manifiestos de k8s/ con los valores de las variables;
#   4. crea los tópicos y ajusta el KafkaSource, Kong y el dashboard de Grafana.
#
# Se vuelve a correr cuando cambia cualquiera de los triggers, así que
#   terraform apply -var="partitions=2" -var="batch_size=50"
# ajusta el pipeline en uno o dos minutos sin recrear las EC2 (como en AWS_5, donde
# cambiar batch_size o los límites de la API no recreaba nada).
# Cuando el apply termina, todo está listo: no hace falta esperar a Neo4j.

locals {
  deploy_script = replace(templatefile("${local.scripts_dir}/deploy.sh.tftpl", {
    namespace             = local.namespace
    image                 = local.image
    topic                 = local.topic
    dlq_topic             = local.dlq_topic
    consumer_group        = local.consumer_group
    partitions            = var.partitions
    consumers             = var.consumers
    ordering              = var.ordering
    max_poll_records      = var.max_poll_records
    batch_size            = var.batch_size
    batching_window_ms    = floor(var.batching_window * 1000)
    processor_memory      = var.processor_memory
    processor_concurrency = var.processor_concurrency
    processor_threads     = local.processor_threads
    processor_min_scale   = var.processor_min_scale
    processor_max_scale   = var.processor_max_scale
    ingest_memory         = var.ingest_memory
    ingest_max_scale      = var.ingest_max_scale
    kafka_image           = var.kafka_image
    kong_image            = var.kong_image
    neo4j_image           = var.neo4j_image
    neo4j_heap            = var.neo4j_heap
    neo4j_pagecache       = var.neo4j_pagecache
    grafana_image         = var.grafana_image
    grafana_neo4j_plugin  = var.grafana_neo4j_plugin
    prometheus_image      = var.prometheus_image
    kafka_exporter_image  = var.kafka_exporter_image
    neo4j_password        = random_password.neo4j.result
    grafana_password      = random_password.grafana.result
    kong_config_b64       = base64encode(local.kong_yaml)
    kong_config_sha       = substr(sha1(local.kong_yaml), 0, 12)
    dashboard_b64         = base64gzip(jsonencode(local.grafana_dashboard))
  }), "\r\n", "\n")

  k8s_hash = sha1(join("", [for f in sort(fileset(local.k8s_dir, "*.yaml")) : filesha1("${local.k8s_dir}/${f}")]))
}

resource "terraform_data" "deploy" {
  triggers_replace = [
    aws_instance.server.id,
    aws_instance.data.id, # si se recrea el nodo de datos, Kafka y Neo4j se vuelven a desplegar
    sha1(local.deploy_script),
    local.k8s_hash,
    local.src_hash,
  ]

  connection {
    type        = "ssh"
    host        = aws_instance.server.public_ip
    user        = "ec2-user"
    private_key = tls_private_key.ssh.private_key_pem
    timeout     = "10m" # el servidor tarda un poco en aceptar SSH al arrancar
  }

  provisioner "remote-exec" {
    inline = ["rm -rf /tmp/lab && mkdir -p /tmp/lab/src /tmp/lab/k8s"]
  }

  provisioner "file" {
    source      = "${local.src_dir}/"
    destination = "/tmp/lab/src"
  }

  provisioner "file" {
    source      = "${local.k8s_dir}/"
    destination = "/tmp/lab/k8s"
  }

  provisioner "file" {
    content     = local.deploy_script
    destination = "/tmp/lab/deploy.sh"
  }

  provisioner "remote-exec" {
    inline = ["sudo bash /tmp/lab/deploy.sh"]
  }

  depends_on = [aws_security_group.node]
}
