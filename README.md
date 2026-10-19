# Lab 5 · Ingesta en streaming a una base de grafos

> Sistemas Escalables — ITESO
> Autor(es): Victor Sebastian Sanchez Silva
> Fecha: 2026-10-18

Este laboratorio construye un pipeline de **ingesta en streaming** para un juez en línea tipo HackerRank, usando **infraestructura como código (IaC)** con **Terraform** sobre **AWS**. Cada envío calificado entra por una **API** protegida con API key, pasa por un **tópico**, se procesa en lotes solo para validar y limpiar, se guarda en una **base de datos de grafos (Neo4j)** y se visualiza en **Grafana**. Hay dos versiones del mismo pipeline: una con servicios administrados de AWS y otra open source sobre **Kubernetes (k3s)**.

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
- Publicar cada envío en un tópico (Kinesis o SQS en AWS; Redpanda/Kafka en open source).
- Procesar en lotes (Lambda, Kafka Streams o Flink) solo para validar y limpiar.
- Guardar en una base de datos de grafos y visualizar en Grafana.
- Atender muchas peticiones sin degradarse, y demostrarlo con una prueba de carga.
- Medir throughput, latencia de extremo a extremo y manejo de fallas variando la configuración.
- Destruir y recrear el entorno en cada sesión sin dejar recursos.

## Proyectos
Ambos proyectos reciben el mismo evento del juez y producen el mismo grafo; cambia la tecnología de cada etapa del pipeline.

### 1. [`AWS_5`](./AWS_5): API Gateway + Kinesis + Lambda + Neo4j + Grafana
Servicios administrados de AWS para la entrada, el tópico y el procesamiento; **Neo4j** y **Grafana** en Docker sobre una EC2 `t3.medium`.

- El juez (simulado con `producer.py` y `load_test.py`) calcula las huellas para plagio y manda cada envío a **API Gateway** con API key; un usage plan limita el tráfico y responde `429` si se excede.
- Una Lambda de ingesta publica en **Kinesis Data Streams**, con `user_id` como partition key.
- Otra **AWS Lambda**, conectada con un Event Source Mapping, solo valida, limpia y carga en Neo4j. Lo que falla de forma repetida va a una **DLQ (SQS)**.
- **Grafana** trae ya configurado el dashboard del pipeline (API Gateway, Kinesis, Lambdas y DLQ en CloudWatch) y se conecta a Neo4j para las analíticas.

### 2. `k3s_5`: Redpanda + Knative + Neo4j + Grafana (pendiente)
Mismo pipeline en un clúster **k3s** sobre EC2, como en el Lab 4.

- **Redpanda** (compatible con Kafka) como tópico.
- **Knative** con KafkaSource como consumidor, en lugar de Lambda.
- **Neo4j** y **Grafana** dentro del clúster, con Prometheus en lugar de CloudWatch.

> Cada carpeta tiene su propio `README.md` con arquitectura, despliegue, verificación y problemas conocidos.

## Comparación
| Aspecto | `AWS_5` | `k3s_5` |
|---------|---------|---------|
| Enfoque | Servicios administrados de AWS | Open source sobre k3s |
| Entrada | API Gateway REST con API key y usage plan | Pendiente |
| Tópico | Kinesis Data Streams (shards) | Redpanda (particiones) |
| Procesamiento | Lambda de ingesta + Lambda procesadora con Event Source Mapping | Knative + KafkaSource |
| Base de datos | Neo4j 5 en Docker sobre EC2 | Neo4j 5 en k3s |
| Visualización | Grafana con CloudWatch y Neo4j | Grafana con Prometheus y Neo4j |
| Cómo entra un envío | `POST /submissions` con `x-api-key` | Pendiente |
| Recursos | 1 a 4 shards, 2 Lambdas de 256 MB, EC2 `t3.medium` | Pendiente |
| Puertos abiertos (solo tu IP) | 22, 3000, 7474, 7687 | Pendiente |
| Tiempo hasta estar listo | 3 a 5 minutos | Pendiente |
| Complejidad | Media | Pendiente |

**Cuándo conviene cada uno:**
- `AWS_5`: cuando no se quiere operar el broker ni los consumidores y el tráfico es variable. Lambda escala por shard y se paga por uso.
- `k3s_5`: cuando se necesita control total o portabilidad fuera de AWS, o el tráfico es constante y un clúster propio sale más barato.

## Requisitos generales
Los requisitos específicos están en el README de cada carpeta.

- [Terraform](https://developer.hashicorp.com/terraform/downloads) >= 1.5.
- [AWS CLI](https://aws.amazon.com/cli/) con el perfil `academy` (`aws sts get-caller-identity --profile academy`). En `AWS_5`, Terraform lo usa para crear el stream.
- Python 3.10 o superior con boto3 (`pip install -r AWS_5/tests/requirements.txt`).
- **Bash**: en Windows, [Git Bash](https://git-scm.com/downloads); en Linux o Mac, la terminal normal.
- En Windows, **corre Terraform desde Git Bash**, no desde PowerShell ni CMD, para que `bash` apunte al de Git y no al de WSL.

## Estructura del repositorio
```
Lab_5/
├── AWS_5/             # Kinesis + Lambda + Neo4j + Grafana
│   ├── infra/         # Terraform (aquí se corren los comandos)
│   ├── scripts/       # user_data de la EC2 (Docker, Neo4j, Grafana)
│   ├── src/           # Código de la Lambda
│   ├── tests/         # Juez simulado, métricas, consultas y administración de Neo4j
│   └── README.md
├── k3s_5/             # Versión open source (pendiente)
├── .gitattributes     # Finales de línea LF para los scripts que corren en Linux
├── .gitignore
└── README.md          # Este archivo
```

## Uso rápido
Entra a la carpeta `infra/` del proyecto que quieras desplegar:

```bash
cd AWS_5/infra
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

Para conectarte por SSH a la EC2 de Neo4j y Grafana, desde `infra/`:

```bash
eval "$(terraform output -raw ssh_command)"
```

## Limpieza de recursos
Al terminar cada sesión, desde la carpeta `infra/` del proyecto desplegado:

```bash
terraform destroy
```

Esto borra la API Gateway, las Lambdas, el stream, la DLQ, la instancia, los Security Groups, el Key Pair y el `.pem` local, y elimina la huella de la IP en `~/.ssh/known_hosts`. En `AWS_5`, el Security Group de la Lambda tarda 20 a 40 minutos en borrarse; si `destroy` falla en ese recurso, espera y repítelo.

## Conclusiones
<!-- Ajusta con tus propias observaciones; se completan con los experimentos E1-E7 y la versión k3s_5 -->

- Con 1 shard y una sola Lambda a la vez, el pipeline procesó unos **185–200 ev/s** contra los 300 ev/s que se publicaron. Kinesis no tuvo throttling: **el cuello de botella fue el consumidor**, y la latencia de extremo a extremo llegó a unos 10–11 s.
- **El throughput escala con los shards:** con 2 shards subió a unos **400 ev/s**, porque hay una Lambda procesadora por shard. Publicar más rápido de lo que se procesa forma cola y sube la latencia; por eso la API limita la entrada con un usage plan, que responde `429` en vez de degradar el pipeline.
- **La Lambda pasa casi todo su tiempo esperando a Neo4j** (unos 445 ms por lote contra 26 ms de validar y limpiar). Calcular las huellas en el juez la dejó ligera; el costo real está en la escritura en la base de datos.
- **Kinesis entrega "al menos una vez"**, así que escribir con `MERGE` sobre llaves únicas hace la carga idempotente sin lógica extra.
- **Las restricciones de AWS Academy cambian el diseño:** sin Neptune se usó Neo4j en EC2, y el bloqueo de `IncreaseStreamRetentionPeriod` obligó a crear el stream con el AWS CLI.
- **La detección de plagio depende de las huellas:** con k-gramas de 5 tokens casi todas se repetían entre programas distintos y el recall fue 0. Con programas sintéticos distintos por usuario y `k = 12`, una simulación local dio recall de 0.97–0.99 sin falsos positivos.

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
