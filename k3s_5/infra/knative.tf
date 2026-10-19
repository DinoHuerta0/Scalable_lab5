# --- Funciones en Knative (equivalen a las dos Lambdas) ---
#
#  - ingest (k8s/ingest.yaml): Kong -> Kafka. Siempre con al menos un pod, para que la
#    API no pague arranques en frío.
#  - processor (k8s/processor.yaml): la invoca el KafkaSource con un evento por petición
#    HTTP; junta los eventos que llegan juntos y escribe lotes en Neo4j. Escala a cero
#    cuando no hay tráfico, como Lambda.
#
# Las dos usan la misma imagen (src/Dockerfile) con distinto comando. Se construye en
# el nodo y se importa a k3s con el prefijo dev.local, para que Knative no la busque en
# un registro. La etiqueta es el hash de src/: si cambias el código, se reconstruye.

locals {
  src_files = sort(fileset(local.src_dir, "{*.py,Dockerfile,requirements.txt}"))
  src_hash  = sha1(join("", [for f in local.src_files : filesha1("${local.src_dir}/${f}")]))
  image     = "dev.local/${var.project_name}:${substr(local.src_hash, 0, 12)}"

  # gunicorn necesita al menos un hilo por evento en vuelo
  processor_threads = var.processor_concurrency + 16
}
