# Lab 5 · Ingesta en streaming a una base de grafos

> Sistemas Escalables — ITESO
> Autor(es): Victor Sebastian Sanchez Silva
> Fecha: 2026-10-18

Este laboratorio construye un pipeline de **ingesta en streaming** para un juez en línea tipo HackerRank, usando **infraestructura como código (IaC)** con **Terraform** sobre **AWS**. Cada envío calificado entra por una **API** protegida con API key, pasa por un **tópico**, se procesa en lotes solo para validar y limpiar, se guarda en una **base de datos de grafos (Neo4j)** y se visualiza en **Grafana**. El laboratorio contempla dos versiones del mismo pipeline: una con servicios administrados de AWS y otra open source sobre **Kubernetes (k3s)**.

---

## Tabla de contenidos
- [Objetivo del laboratorio](#objetivo-del-laboratorio)
- [Proyectos](#proyectos)
- [Comparación](#comparación)
- [Requisitos generales](#requisitos-generales)
- [Estructura del repositorio](#estructura-del-repositorio)
- [Uso rápido](#uso-rápido)
- [Limpieza de recursos](#limpieza-de-recursos)
- [Conclusiones](#conclusiones)
- [Referencias](#referencias)

---

## Objetivo del laboratorio
Recibir un volumen grande de peticiones, procesarlas desde un tópico y guardarlas en una base de datos con visualización, todo con Terraform y sin pasos manuales en la consola de AWS.

- Exponer una API para que el juez publique sus envíos, con API key y límites de tráfico.
- Publicar cada envío en un tópico (Kinesis o SQS).
- Procesar en lotes (Lambda, Kafka Streams o Flink) solo para validar y limpiar.
- Guardar en una base de datos de grafos y visualizar en Grafana.
- Atender muchas peticiones sin degradarse, y demostrarlo con una prueba de carga.
- Medir throughput, latencia de extremo a extremo y manejo de fallas variando la configuración.
- Destruir y recrear el entorno en cada sesión sin dejar recursos.

## Proyectos
Los dos proyectos cumplen el mismo objetivo; cambia la tecnología de cada etapa del pipeline.

### 1. [`AWS_5`](./AWS_5): API Gateway + Kinesis + Lambda + Neo4j + Grafana
Servicios administrados de AWS para la entrada, el tópico y el procesamiento; **Neo4j** y **Grafana** en Docker sobre una EC2 `t3.medium`.

- El juez (simulado con `producer.py` y `load_test.py`) calcula las huellas para plagio y manda cada envío a **API Gateway** con API key; un usage plan limita el tráfico y responde `429` si se excede.
- Una Lambda de ingesta publica en **Kinesis Data Streams**, con `user_id` como partition key.
- Otra **AWS Lambda**, conectada con un Event Source Mapping, solo valida, limpia y carga en Neo4j. Lo que falla de forma repetida va a una **DLQ (SQS)**.
- **Grafana** trae ya configurado el dashboard del pipeline (API Gateway, Kinesis, Lambdas y DLQ en CloudWatch) y se conecta a Neo4j para las analíticas.

### 2. [`k3s_5`](./k3s_5): Kong + Kafka + Knative + Neo4j + Grafana en k3s
El mismo pipeline solo con software open source, en un clúster de **k3s** de dos nodos `t3.large`: uno para las aplicaciones y otro para Kafka y Neo4j.

- **Kong** reemplaza a API Gateway: exige la API key en `x-api-key` y responde `429` si se pasa el límite.
- Una función de **Knative** publica cada envío en **Apache Kafka**, con `user_id` como llave.
- Un **KafkaSource** de Knative Eventing entrega cada registro a otra función de Knative, que junta los eventos en lotes, los valida, los limpia y los carga en Neo4j. Lo que falla de forma repetida va a un tópico DLQ.
- **Prometheus** reemplaza a CloudWatch, y **Grafana** trae ya configurado el dashboard del pipeline (Kong, Kafka, funciones y DLQ).

> Cada carpeta tiene su propio `README.md` con arquitectura, despliegue, verificación y problemas conocidos.

## Comparación
| Aspecto | `AWS_5` | `k3s_5` |
|---------|---------|---------|
| Enfoque | Servicios administrados de AWS | Open source sobre k3s |
| Entrada | API Gateway REST con API key y usage plan | Kong con `key-auth` y `rate-limiting` |
| Tópico | Kinesis Data Streams (1 a 4 shards) | Apache Kafka 3.8 en KRaft (N particiones) |
| Procesamiento | Lambda de ingesta + Lambda procesadora con Event Source Mapping | Knative Service de ingesta + KafkaSource + Knative Service procesador |
| Entrega al procesador | Lotes de hasta `batch_size` | Un evento por petición; la procesadora arma los lotes |
| DLQ | SQS (metadatos del lote fallido) | Tópico de Kafka con el evento completo (KafkaSink) |
| Base de datos | Neo4j 5 en Docker sobre EC2 | Neo4j 5 como pod en k3s |
| Visualización | Grafana con CloudWatch y Neo4j | Grafana con Prometheus y Neo4j |
| Cómo entra un envío | `POST /submissions` con `x-api-key` | Igual (sin key responde `401` en vez de `403`) |
| Tipo de instancia / recursos | 2 Lambdas de 256 MB, EC2 `t3.medium` | Dos EC2 `t3.large` (2 vCPU y 8 GB cada una): aplicaciones y datos |
| Puertos abiertos (solo tu IP) | 22, 3000, 7474, 7687 | 22, 80, 3000, 7474, 7687, 9090 |
| Tiempo hasta estar listo | 3 a 5 minutos | 10 a 15 minutos (1 o 2 al cambiar una variable) |
| Complejidad | Media | Alta |

**Cuándo conviene cada uno:**
- `AWS_5`: cuando no se quiere operar el broker ni los consumidores y el tráfico es variable. Lambda escala por shard y se paga por uso.
- `k3s_5`: cuando se quiere evitar la dependencia de un proveedor o correr el mismo pipeline en cualquier nube o en servidores propios. Se paga una instancia fija y se opera todo (Kafka, Neo4j, Knative), pero cada pieza se puede ajustar.

## Requisitos generales
Los requisitos específicos están en el README de cada carpeta.

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.5.
- [AWS CLI](https://aws.amazon.com/cli/) con el perfil `academy` (`aws sts get-caller-identity --profile academy`). En `AWS_5`, Terraform lo usa para crear el stream.
- Python 3.10 o superior. En `AWS_5`, con boto3 (`pip install -r AWS_5/tests/requirements.txt`); en `k3s_5` no hace falta instalar nada.
- **Bash**: en Windows, [Git Bash](https://git-scm.com/downloads); en Linux o Mac, la terminal normal.
- En Windows, **corre Terraform desde Git Bash**, no desde PowerShell ni CMD, para que `bash` apunte al de Git y no al de WSL.

## Estructura del repositorio
```
Lab_5/
├── AWS_5/             # API Gateway + Kinesis + Lambda + Neo4j + Grafana
│   ├── infra/         # Terraform (aquí se corren los comandos)
│   ├── scripts/       # user_data de la EC2 (Docker, Neo4j, Grafana)
│   ├── src/           # Código de las Lambdas (ingesta y procesadora)
│   ├── tests/         # Juez simulado, prueba de carga, métricas, consultas y Neo4j
│   ├── docs/          # Diagrama de arquitectura
│   └── README.md
├── k3s_5/             # Kong + Kafka + Knative + Neo4j + Grafana en k3s
│   ├── infra/         # Terraform (aquí se corren los comandos)
│   ├── k8s/           # Manifiestos de Kubernetes (Kafka, Neo4j, Kong, funciones, monitoreo)
│   ├── scripts/       # user_data del servidor y del nodo de datos, y deploy.sh (pipeline, por SSH)
│   ├── src/           # Código de las funciones de Knative (ingesta y procesadora)
│   ├── tests/         # Los mismos scripts de prueba que AWS_5
│   └── README.md
├── .gitattributes     # Finales de línea LF para los scripts que corren en Linux
├── .gitignore
└── README.md          # Este archivo
```

## Uso rápido
Entra a la carpeta `infra/` del proyecto que quieras desplegar:

```bash
cd AWS_5/infra   # o k3s_5/infra
terraform init
terraform apply
```

Después, desde `tests/`, espera a que Neo4j arranque, manda una prueba por la API y lanza la prueba de carga:

```bash
cd ../tests
python db.py wait
python producer.py --synthetic 5000 --rate 300 --plagio 0.02
python metrics.py --run-id <run_id que imprime el producer> --minutes 10
python queries.py
python load_test.py --steps 10,25,50,100 --duration 60
```

En `k3s_5`, cuando `terraform apply` termina el pipeline ya responde; `python db.py wait` regresa de inmediato.

Para conectarte por SSH a la EC2, desde `infra/`:

```bash
eval "$(terraform output -raw ssh_command)"
```

## Limpieza de recursos
Al terminar cada sesión, desde la carpeta `infra/` del proyecto desplegado:

```bash
terraform destroy
```

Esto borra la API Gateway, las Lambdas, el stream, la DLQ, la instancia, los Security Groups, el Key Pair y el `.pem` local, y elimina la huella de la IP en `~/.ssh/known_hosts`. En `AWS_5`, el Security Group de la Lambda tarda 20 a 40 minutos en borrarse; si `destroy` falla en ese recurso, espera y repítelo. En `k3s_5` todo vive dentro de las dos EC2, así que `destroy` tarda 1 o 2 minutos.

## Conclusiones
<!-- Ajusta con tus propias observaciones -->

- **La entrada no se degrada:** de 10 a 100 clientes simultáneos, la latencia p95 de la API se mantuvo entre 102 y 110 ms, sin errores `5xx`. Lo que pasó de 500 envíos/s (el límite del usage plan) se rechazó al instante con `429` en vez de volver lenta la API.
- **El throughput escala con los shards:** de unos 200 a unos 400 envíos/s al pasar de 1 a 2 shards, porque hay una Lambda procesadora por shard.
- **El cuello de botella es Neo4j en una sola `t3.medium`:** con 8 Lambdas escribiendo en paralelo (4 shards × 2), cada lote tardó unos 3 s en escribirse y el atraso del consumidor llegó a 4 minutos. Para crecer habría que ajustar el límite de la API a lo que procesa la base de datos o darle más recursos.
- **Kinesis entrega "al menos una vez"**, así que escribir con `MERGE` sobre llaves únicas hace la carga idempotente sin lógica extra.
- **La detección de plagio funcionó:** con programas distintos por usuario y huellas de 12 tokens, se detectaron las 92 copias inyectadas (recall de 1.0) sin falsos positivos.
<!-- k3s_5: agrega aquí la comparación con AWS_5 cuando tengas sus corridas -->
- **Las restricciones de AWS Academy cambian el diseño:** sin Neptune ni Managed Grafana se usaron Neo4j y Grafana en una EC2, y el bloqueo de `IncreaseStreamRetentionPeriod` obligó a crear el stream con el AWS CLI.

## Referencias
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)
- [Usar Lambda con Kinesis](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
- [Neo4j: API HTTP](https://neo4j.com/docs/http-api/current/)
- [Grafana](https://grafana.com/docs/grafana/latest/)
- [API Gateway: usage plans y API keys](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)

---

**Notas**
- No subas a Git los `.pem` ni los archivos `terraform.tfstate*`: el state contiene la llave privada, la API key y las contraseñas de Neo4j y Grafana. Ya están en el `.gitignore` del lab, junto con `.build/` y `__pycache__/`.
