# Ingesta en streaming de un juez en línea: API Gateway + Kinesis + Lambda + Neo4j + Grafana con Terraform

> Sistemas Escalables — ITESO
> Autor(es): Victor Sebastian Sanchez Silva
> Fecha: 2026-10-18

Despliega con Terraform un pipeline de streaming para un juez en línea tipo HackerRank. El juez manda cada envío calificado a una **API Gateway** protegida con **API key**. Una Lambda de ingesta lo publica en **Kinesis Data Streams**, otra **AWS Lambda** lo valida, lo limpia y lo carga en **Neo4j** (base de datos de grafos), y **Grafana** muestra el estado del pipeline y las analíticas. La versión open source del mismo pipeline estará en [`k3s_5`](../k3s_5).

---

## Tabla de contenidos
- [Objetivo](#objetivo)
- [Arquitectura](#arquitectura)
- [Tecnologías](#tecnologías)
- [Requisitos previos](#requisitos-previos)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Configuración](#configuración)
- [Despliegue / Ejecución](#despliegue--ejecución)
- [Verificación](#verificación)
- [Resultados](#resultados)
- [Limpieza de recursos](#limpieza-de-recursos)
- [Problemas conocidos](#problemas-conocidos)
- [Referencias](#referencias)

---

## Objetivo
Recibir un volumen grande de peticiones, procesarlas en streaming desde un tópico y guardarlas en una base de datos con visualización, todo con Terraform y sin pasos manuales en la consola de AWS.

- Exponer una **API** (`POST /submissions`) protegida con API key y con límites de tráfico.
- Publicar cada envío en un tópico (**Kinesis**), con la partición por usuario para conservar el orden de sus envíos.
- Procesarlos con **Lambda** solo para validar y limpiar, y cargarlos en una base de grafos (**Neo4j**).
- Mandar a una cola de fallas (**SQS**) lo que no se pueda procesar, sin frenar el resto.
- Visualizar el pipeline y los datos en **Grafana**.
- Demostrar que el despliegue **atiende muchas peticiones sin degradarse**, con una prueba de carga por escalones.
- Poder destruir y recrear el entorno en cada sesión sin dejar recursos.

## Arquitectura
El juez (en el lab, `tests/producer.py` o `tests/load_test.py`) calcula las huellas del código para detectar plagio y manda cada envío a la API con el encabezado `x-api-key`. API Gateway revisa la key y el límite de tráfico del usage plan, e invoca la **Lambda de ingesta**, que publica el envío en Kinesis con el `user_id` como partition key. El Event Source Mapping de Lambda lee los shards e invoca la **Lambda procesadora** con lotes: valida y limpia cada registro y escribe el lote completo en Neo4j con `MERGE`, que es idempotente porque Kinesis entrega "al menos una vez". Los registros inválidos se reintentan y, si siguen fallando, el aviso termina en la DLQ. Neo4j y Grafana corren en Docker dentro de una EC2; Grafana lee las métricas de CloudWatch y consulta Neo4j.

```
                                                                                     ┌──────── EC2 (t3.medium) ─────────┐
Juez ── POST + x-api-key ──► API Gateway ──► Lambda ──► Kinesis ── lotes ──► Lambda ─┼─► Neo4j ◄── bolt ── Grafana :3000 │
(producer.py,                (REST, usage    ingesta    (N shards,          procesadora     Docker (red "judge")         │
 load_test.py)                plan, 429)     (put_      key = user_id)      valida, limpia, └──────────────────────────────────┘
                                             record)                        carga (MERGE)       ▲                 │
                                                                              │                 │ tu IP /32       ▼
                                                                              ▼ tras 3 reintentos               CloudWatch
                                                                           SQS (DLQ)       Navegador, SSH,
                                                                                           scripts de tests/
```

Modelo del grafo que escribe la Lambda procesadora:
```
(:User)-[:SUBMITTED]->(:Submission)-[:FOR]->(:Problem)
(:Submission)-[:IN]->(:Language)
(:Submission)-[:GOT]->(:Verdict)
(:User)-[:SOLVED]->(:Problem)              al primer Accepted
(:Submission)-[:HAS]->(:Fingerprint)       huellas del código (plagio)
```

Recursos que crea Terraform:
- **API Gateway (REST)** `judge-stream-api`, stage `v1`, con las rutas `POST /submissions` y `POST /submissions/batch`. Ambas exigen API key y van por proxy a la Lambda de ingesta.
- **API key** del juez y **usage plan** con límite de peticiones por segundo, ráfaga y cuota diaria. Si se excede, la API responde `429`.
- **Lambda de ingesta** `judge-stream-ingest`, fuera de la VPC: recibe la petición y hace `put_record` / `put_records` a Kinesis.
- **Kinesis Data Streams** `judge-stream-submissions` en modo provisioned. Se crea con el AWS CLI porque Academy bloquea el recurso nativo (ver [Problemas conocidos](#problemas-conocidos)).
- **Lambda procesadora** `judge-stream-processor` en la VPC por defecto, con el **Event Source Mapping** hacia Kinesis (lotes, paralelismo, bisect y reporte de registros fallidos).
- **SQS** `judge-stream-dlq` como destino de los lotes que agotan sus reintentos.
- **Instancia EC2** `t3.medium` con la AMI más reciente de Amazon Linux 2023 y el rol `LabInstanceProfile` (para que Grafana lea CloudWatch). Al arrancar, `user_data` instala Docker y levanta Neo4j y Grafana.
- **Security Groups**: el de la Lambda procesadora (solo salida) y el de la EC2, que acepta el 7474 desde esa Lambda y el 7474, 7687, 3000 y 22 solo desde tu IP pública, detectada en cada `apply`.
- **Key Pair** generado automáticamente; el `.pem` se guarda en `infra/` con permisos restringidos a tu usuario.
- **Contraseñas aleatorias** para Neo4j y Grafana, y el **dashboard del pipeline** en Grafana, ya configurado.

### Contrato del evento
Lo que el juez manda a la API, un objeto JSON por envío (o un arreglo de hasta 500 en `/submissions/batch`):

```json
{"submission_id": "s123", "user_id": "u45", "problem_id": "p00001", "language": "Python",
 "verdict": "Wrong Answer", "cpu_ms": 120, "memory_kb": 9000, "code_size": 665,
 "date": 1500000000, "fps": ["0a3f9c21be47", "1b07e5d2c9aa"], "run_id": "opcional"}
```

| Campo | Obligatorio | Nota |
|---|---|---|
| `user_id` | Sí | Sin él la API responde `400`: es la partition key |
| `submission_id`, `problem_id`, `language`, `verdict` | Sí | Si falta alguno, la Lambda procesadora rechaza el registro y termina en la DLQ |
| `cpu_ms`, `memory_kb`, `code_size`, `date` | No | Se convierten a número; vacíos → `null` |
| `fps` | No | Huellas que calcula el juez (`tests/fingerprint.py`); sin ellas, el envío no entra a la detección de plagio |
| `run_id` | No | Etiqueta de la corrida, para `metrics.py` |

Respuestas de la API: `202` aceptado, `400` petición inválida, `403` sin API key válida, `429` límite del usage plan excedido, `503` Kinesis sin capacidad (reintentar). En `/submissions/batch`, la respuesta `202` trae `accepted` y la lista `failed` con los índices que hay que reintentar.

## Tecnologías
| Herramienta | Versión | Uso |
|-------------|---------|-----|
| Terraform | >= 1.5 | Infraestructura como código |
| AWS provider | 5.x | API Gateway, Lambda, Kinesis, SQS, EC2, Security Groups, Key Pair |
| archive / random / tls / local / http providers | 2.x / 3.x / 4.x / 2.x / 3.x | Empaquetar las Lambdas, contraseñas y API key, llave SSH, `.pem` y detectar tu IP |
| AWS API Gateway | REST, stage `v1` | Entrada del juez, API key y usage plan |
| AWS Lambda | Python 3.12 | Ingesta (API → Kinesis) y procesamiento (validar, limpiar y cargar) |
| AWS Kinesis Data Streams | provisioned | Tópico de ingesta |
| AWS SQS | — | DLQ |
| AWS EC2 | Amazon Linux 2023 | Host de Neo4j y Grafana (`t3.medium`) |
| Docker | repositorio de AL2023 | Contenedores de Neo4j y Grafana |
| Neo4j | 5.26 Community | Base de datos de grafos |
| Grafana | 11.2.0 OSS | Visualización (CloudWatch + plugin `kniepdennis-neo4j-datasource`) |
| Python + boto3 | 3.10+ / >= 1.34 | Scripts de prueba |
| Git Bash | — | Terminal para correr Terraform, SSH y los scripts |

## Requisitos previos
- Terraform instalado (`terraform --version`).
- AWS CLI configurado con el perfil `academy` (`aws sts get-caller-identity --profile academy`).
  Si no lo tienes: `aws configure --profile academy`. Terraform también usa el AWS CLI para crear el stream, así que debe estar en el PATH.
- Python 3.10 o superior (`python --version`) y las dependencias de prueba:
  ```bash
  pip install -r AWS_5/tests/requirements.txt
  ```
- **Bash**: en Windows, [Git Bash](https://git-scm.com/downloads); en Linux o Mac, la terminal normal. Los pasos automáticos de permisos del `.pem` y de limpieza de `known_hosts` usan `bash`.
- En Windows, **corre Terraform desde Git Bash**, no desde PowerShell ni CMD, para que `bash` apunte al de Git y no al de WSL.

## Estructura del proyecto
```
AWS_5/
├── infra/                     # Código Terraform (aquí se corren los comandos)
│   ├── versions.tf            # Versión de Terraform y de los providers
│   ├── providers.tf           # Provider de AWS con el perfil academy
│   ├── variables.tf           # Variables configurables
│   ├── outputs.tf             # URL y API key, stream, Lambdas, DLQ, URLs, contraseñas y SSH
│   ├── network.tf             # VPC por defecto, detección de tu IP y Security Groups
│   ├── apigw.tf               # API Gateway REST, rutas, API key y usage plan
│   ├── kinesis.tf             # Stream (creado con el AWS CLI) y su data source
│   ├── sqs.tf                 # DLQ
│   ├── neo4j.tf               # EC2 de Neo4j + Grafana, Key Pair, .pem y provisioners
│   ├── grafana.tf             # Contraseña de Grafana y dashboard del pipeline (CloudWatch)
│   └── lambda.tf              # Lambda procesadora (VPC + Event Source Mapping) y Lambda de ingesta
├── scripts/
│   └── user_data.sh.tftpl     # Instala Docker y levanta Neo4j y Grafana (ya configurado)
├── src/                       # Código de las Lambdas
│   ├── ingest.py              # Ingesta: API Gateway -> Kinesis
│   ├── handler.py             # Procesadora: valida, limpia y carga cada lote en Neo4j
│   └── neo4j_http.py          # Cliente HTTP de Neo4j con reintentos (también lo usan los tests)
├── tests/                     # Lo que ejecutas tú
│   ├── requirements.txt       # boto3
│   ├── common.py              # Outputs de Terraform y clientes de AWS, Neo4j y la API
│   ├── synthetic.py           # Generador de envíos: sesiones de intentos, plagio y corruptos
│   ├── fingerprint.py         # Huellas de código para plagio (lado del juez)
│   ├── producer.py            # Simula al juez: genera envíos, calcula huellas y los manda
│   ├── load_test.py           # Prueba de carga de la API por escalones de clientes
│   ├── db.py                  # Neo4j: wait (esperar), stats (conteos), reset (vaciar)
│   ├── metrics.py             # Latencia, throughput, API Gateway, CloudWatch y DLQ de una corrida
│   ├── queries.py             # Analíticas de grafo y detección de plagio con su recall
│   └── results/               # (generado) resultados de cada corrida y experiments.csv
└── README.md
```

## Configuración
Todas las variables tienen valor por defecto, así que no hace falta un archivo `.tfvars`.

| Variable | Descripción | Valor por defecto |
|----------|-------------|-------------------|
| `aws_profile` | Perfil de AWS CLI | `academy` |
| `aws_region` | Región de AWS | `us-east-1` |
| `project_name` | Prefijo para nombrar los recursos | `judge-stream` |
| `api_rate_limit` | Peticiones por segundo permitidas a la API key | `500` |
| `api_burst_limit` | Ráfaga máxima de peticiones permitida a la API key | `1000` |
| `api_quota_per_day` | Peticiones por día permitidas a la API key | `1000000` |
| `ingest_memory` | Memoria de la Lambda de ingesta en MB | `256` |
| `shards` | Shards del stream (cambiarlo recrea el stream) | `1` |
| `batch_size` | Registros por invocación de la Lambda procesadora | `100` |
| `batching_window` | Segundos que espera para llenar el lote | `1` |
| `parallelization_factor` | Lotes concurrentes por shard (1–10) | `1` |
| `lambda_memory` | Memoria de la Lambda procesadora en MB | `256` |
| `instance_type` | Tipo de la EC2 de Neo4j y Grafana | `t3.medium` |
| `neo4j_image` | Imagen Docker de Neo4j | `neo4j:5.26-community` |
| `grafana_image` | Imagen Docker de Grafana | `grafana/grafana-oss:11.2.0` |
| `grafana_neo4j_plugin` | Plugin de Neo4j para Grafana | `kniepdennis-neo4j-datasource` |
| `allowed_ssh_cidr` | IP autorizada para SSH, Neo4j Browser y Grafana. Si es `null`, se detecta tu IP pública y se usa `/32` | `null` |

Para cambiar un valor:
```bash
terraform apply -var="shards=4" -var="parallelization_factor=2"
```
Pasa todos los `-var` juntos: los que no pases vuelven a su valor por defecto. Los límites de la API, la memoria de las Lambdas, `batch_size`, `batching_window` y `parallelization_factor` se aplican sin recrear nada; `shards` recrea el stream; `instance_type`, las imágenes o cualquier cambio en `user_data.sh.tftpl` o `grafana.tf` recrean la EC2 (se pierden los datos de Neo4j).

La API key y las contraseñas de Neo4j y Grafana las genera Terraform; se leen con `terraform output -raw api_key`, `terraform output -raw neo4j_password` y `terraform output -raw grafana_password`.

### Capacidad del pipeline
Cada envío pesa unos **3 KB** (sobre todo por la lista de huellas), y cada shard acepta hasta 1 MB/s o 1000 registros/s de escritura. Por eso **un shard aguanta unos 340 envíos/s**. Para que el pipeline no se degrade, la tasa permitida en la API (`api_rate_limit`, en peticiones/s × envíos por petición) debe quedar por debajo de lo que aguantan los shards y la Lambda procesadora:

| Shards | Envíos/s que acepta Kinesis (aprox.) |
|---|---|
| 1 | 340 |
| 2 | 680 |
| 4 | 1,360 |

## Despliegue / Ejecución
```bash
# 1. Entrar a la carpeta de Terraform
cd AWS_5/infra

# 2. Inicializar (descarga los providers)
terraform init

# 3. (Opcional) Revisar qué se va a crear
terraform plan

# 4. Desplegar (confirma con yes)
terraform apply
```

Al terminar, Terraform muestra los outputs (la API key y las contraseñas salen como `<sensitive>`):

```
api_url           = "https://<id>.execute-api.us-east-1.amazonaws.com/v1"
dlq_url           = "https://sqs.us-east-1.amazonaws.com/<cuenta>/judge-stream-dlq"
grafana_neo4j_url = "bolt://neo4j:7687"
grafana_url       = "http://<IP_PUBLICA>:3000"
ingest_lambda_name = "judge-stream-ingest"
lambda_name       = "judge-stream-processor"
neo4j_browser     = "http://<IP_PUBLICA>:7474/browser/"
ssh_command       = "ssh -i C:/.../infra/judge-stream-key.pem ec2-user@<IP_PUBLICA>"
stream_name       = "judge-stream-submissions"
```

> Neo4j y Grafana tardan **2 a 4 minutos** extra en arrancar después de que la instancia aparece como creada, mientras `user_data` instala Docker y baja las imágenes. Si mandas datos antes, la Lambda procesadora falla y AWS reintenta solo. Para esperar con seguridad:
> ```bash
> cd ../tests
> python db.py wait
> ```

### Conectarse por SSH
Copia el texto de `ssh_command` (sin las comillas exteriores) y pégalo en Git Bash. O ejecútalo directamente desde `infra/`:

```bash
eval "$(terraform output -raw ssh_command)"
```

La primera vez te preguntará si confías en el servidor; responde `yes`.

### Configurar la fuente Neo4j en Grafana (una vez por despliegue)
```bash
terraform output grafana_url
terraform output -raw grafana_password; echo
terraform output -raw neo4j_password; echo
```
1. Entra a Grafana con el usuario `admin`.
2. Ve a **Connections → Data sources → Add data source → Neo4j**.
3. URL `bolt://neo4j:7687`, usuario `neo4j` y la contraseña de Neo4j.
4. **Save & test**.

El dashboard del pipeline (carpeta **Lab 5**) ya viene configurado con CloudWatch. Las consultas para los paneles de Neo4j están en [Resultados](#paneles-de-neo4j-en-grafana).

### Correr una prueba
Desde `tests/`:

```bash
# 1. Vaciar el grafo para medir solo esta corrida
python db.py reset

# 2. Publicar envíos por la API, como lo haría el juez
python producer.py --synthetic 5000 --rate 300 --plagio 0.02

# 3. Esperar 2-3 minutos (CloudWatch publica con retraso) y medir
python metrics.py --run-id <run_id> --minutes 10
python queries.py
```

- El producer manda por defecto a la API (`POST /submissions/batch`, 100 envíos por petición) con la API key. Con `--via kinesis` publica directo en Kinesis, sin pasar por la API.
- Imprime el **`run_id`** al empezar y al terminar (por ejemplo `run-20261018-180000`). También queda en `results/producer_<run_id>.json`, y puedes ponerle nombre con `--run-id e2-shards2`.
- Los envíos los genera `synthetic.py`: sesiones de 1 a 5 intentos por usuario y problema (por ejemplo CE → WA → AC), en Python, C++ y Java. `--plagio 0.02` inyecta copias disfrazadas y `--corrupt 0.001`, registros inválidos.
- Mientras corre, mira el dashboard **Lab 5 → Pipeline** en Grafana.
- `metrics.py` agrega una fila a `results/experiments.csv` para comparar corridas. Las métricas de CloudWatch no se filtran por corrida: usa `--minutes` para que la ventana cubra solo la tuya.

### Prueba de carga: muchas peticiones sin degradarse
`load_test.py` lanza escalones de clientes simultáneos contra la API. Cada cliente es un hilo con su propia conexión que manda peticiones con la API key una tras otra, y en cada escalón se mide throughput, latencia y errores.

```bash
# 1. Capacidad suficiente para la carga (ver Capacidad del pipeline)
terraform -chdir=../infra apply -var="shards=4" -var="parallelization_factor=2"

# 2. Vaciar el grafo y lanzar los escalones (10, 25, 50 y 100 clientes, 60 s cada uno)
python db.py reset
python load_test.py --steps 10,25,50,100 --duration 60

# 3. Latencia del pipeline completo por escalón (el script imprime estos comandos)
python metrics.py --run-id load-<fecha>-c50 --minutes 5
```

- Con `--mode batch --batch-size 25`, cada petición lleva 25 envíos: más envíos/s con las mismas peticiones.
- El resultado queda en `results/load-<fecha>.json` y `.csv`, con una fila por escalón: peticiones/s, envíos/s, latencia p50/p95/p99, % de error, `429` y `5xx`.
- **Qué demuestra que no se degrada:** al subir los clientes, el throughput sube o se estabiliza en el límite del usage plan, mientras la **p95 se mantiene plana** y la tasa de error baja. Si el tráfico pasa el límite, la API responde `429` de inmediato en vez de volverse lenta, y el resto de las peticiones conserva su latencia. Del lado del pipeline, `iterator_age_max_ms` no debe crecer sin control.
- Antes de subir mucho los clientes, revisa el límite de concurrencia de Lambda de tu cuenta (`aws lambda get-account-settings --profile academy`). Las dos Lambdas lo comparten y, si se alcanza, aparecen throttles (panel "Lambdas · throttles" en Grafana).

### Experimentos
Cambia la configuración desde `tests/`, vacía el grafo y repite:

```bash
terraform -chdir=../infra apply -var="shards=2" -var="batch_size=500"
python db.py reset
python producer.py --synthetic 20000 --rate 600 --run-id e3-b500
python metrics.py --run-id e3-b500 --minutes 10
```

| # | Pregunta | Cómo |
|---|---|---|
| E1 | Línea base | `shards=1`, `batch_size=100`, producer con 20k envíos a 300 ev/s |
| E2 | ¿Cuándo se satura un shard? | Sube `--rate` (300, 600, 1000) y mira `kinesis_throttled`; repite con `shards=2` y `4` |
| E3 | Tamaño de lote | `batch_size=10`, `100`, `500`: invocaciones, latencia p95, `iterator_age_max_ms` |
| E4 | Paralelismo contra contención en el grafo | `parallelization_factor=1`, `4`: throughput y `log_neo4j_retries` (deadlocks en nodos como `Language` o `Verdict`) |
| E5 | Memoria (costo) de la Lambda procesadora | `lambda_memory=128`, `256`, `512`: duración, latencia y GB-s |
| E6 | Fallas | `--corrupt 0.001`: mensajes en la DLQ y cómo un registro venenoso frena su shard |
| E7 | Calidad del detector de plagio | `--plagio 0.02`; `queries.py --threshold 0.5`, `0.6`, `0.8`: recall y falsos positivos |
| E8 | Muchas peticiones sin degradarse | `load_test.py` con `shards=1` y con `shards=4`; comparar p95, % de error y `iterator_age_max_ms` por escalón |
| E9 | Límite del usage plan | `api_rate_limit=100` contra `500`: cuántos `429` aparecen y si la latencia del resto se mantiene |

## Verificación
Corre los pasos 0 a 2 **dentro de la instancia**, después de entrar por SSH, y del 3 en adelante desde tu computadora (desde `tests/`). Van por capas, de adentro hacia afuera, para saber exactamente dónde falla algo.

**0. Ver el progreso de la instalación** (útil justo después del `apply`; sal con `Ctrl + C`)
```bash
sudo tail -f /var/log/cloud-init-output.log
```

**1. Que los contenedores estén corriendo** (`neo4j` y `grafana` en `Up`)
```bash
sudo docker ps
```

**2. Que Neo4j y Grafana respondan en la instancia**
```bash
curl -s http://localhost:7474
curl -s http://localhost:3000/api/health
```
Resultado esperado: un JSON de Neo4j con `neo4j_version` y otro de Grafana con `"database": "ok"`.

**3. Que tu computadora llegue a Neo4j**
```bash
python db.py wait
```
Resultado esperado: `Neo4j listo`. Abre también `grafana_url` en el navegador.

**4. Que la API rechace peticiones sin API key**
```bash
API_URL="$(terraform -chdir=../infra output -raw api_url)"
curl -s -o /dev/null -w "%{http_code}\n" -X POST "$API_URL/submissions" -d '{"user_id": "victor"}'
```
Resultado esperado: `403`.

**5. Mandar un envío de prueba con la API key**
```bash
API_KEY="$(terraform -chdir=../infra output -raw api_key)"
curl -s -X POST "$API_URL/submissions" -H "x-api-key: $API_KEY" -H "Content-Type: application/json" \
  -d '{"submission_id": "verif-001", "user_id": "victor", "problem_id": "p99999",
       "language": "Python", "verdict": "Accepted", "cpu_ms": 35, "memory_kb": 9200, "run_id": "verif"}'
```
Resultado esperado: `{"shard_id": "shardId-...", "sequence_number": "..."}` con código `202`.

**6. Que la Lambda procesadora lo haya cargado**
```bash
aws logs tail /aws/lambda/judge-stream-processor --since 5m --profile academy --region us-east-1
```
Resultado esperado: una línea como `{"records": 1, "written": 1, "invalid": 0, ...}`.

**7. Que el envío esté en el grafo**
```bash
python db.py stats
```
O en el Neo4j Browser (`terraform output neo4j_browser`):
```cypher
MATCH (u:User {id: "victor"})-[r]->(x) RETURN u, r, x
```
Resultado esperado: `victor` con `SUBMITTED` hacia `verif-001` y `SOLVED` hacia `p99999`.

**8. Que Grafana muestre el tráfico**: en el dashboard **Lab 5 → Pipeline** aparecen la petición en API Gateway, 1 registro entrante en Kinesis y 1 invocación de cada Lambda, con 1 a 3 minutos de retraso.

**9. Que la DLQ esté vacía** (salvo que hayas usado `--corrupt`)
```bash
aws sqs get-queue-attributes --queue-url "$(terraform -chdir=../infra output -raw dlq_url)" \
  --attribute-names ApproximateNumberOfMessages --profile academy --region us-east-1
```

| Resultado | Dónde está el problema |
|---|---|
| Falla el paso 1 | Instalación de Docker o de los contenedores (revisar el paso 0) |
| Pasa el 2, falla el 3 | Security Group: tu IP cambió desde el `apply` (vuelve a correr `terraform apply`) |
| El paso 4 no da `403` | La ruta no exige API key (revisar `api_key_required` en `apigw.tf`) |
| El paso 5 da `403` | API key mal copiada, o el usage plan no tiene asociada la key o el stage |
| El paso 5 da `502` o `500` | Error en la Lambda de ingesta: `aws logs tail /aws/lambda/judge-stream-ingest --since 5m ...` |
| El paso 5 da `503` | Kinesis sin capacidad: subir `shards` o bajar la tasa |
| Pasa el 5, no hay logs en el 6 | Event Source Mapping deshabilitado o Lambda procesadora sin crear |
| El log del 6 dice `invalid_record` | El evento no cumple el contrato; terminará en la DLQ |
| El log del 6 muestra error de conexión | Neo4j no ha terminado de arrancar o el Security Group Lambda → Neo4j |
| Pasa el 7, Grafana vacío en el 8 | Retraso de CloudWatch (esperar) o `LabInstanceProfile` sin permisos |

## Resultados
<!-- Agrega aquí tus capturas en docs/ (por ejemplo docs/grafana-pipeline.png, docs/load-test.png y docs/neo4j-grafo.png) y tus conclusiones -->

### Corridas con el producer directo a Kinesis (antes de agregar la API)
Datos sintéticos, `batch_size=100`, `parallelization_factor=1`, `lambda_memory=256`.

| Corrida | Shards | Envíos | Tasa enviada | Throughput del pipeline | Latencia p50 / p95 | Concurrencia Lambda | Throttling Kinesis |
|---|---|---|---|---|---|---|---|
| `run-20261013-124002` | 1 | 5,102 | 298 ev/s | 185 ev/s | 11.1 s / 12.2 s | 1 | 0 |
| `run-20261018-164414` | 1 | 5,102 | 298 ev/s | 201 ev/s | 9.5 s / 10.3 s | 1 | 0 |
| `e2-shards2` | 2 | 20,000 | 1,598 ev/s | 402 ev/s | 27.8 s / 34.5 s | 2 | 0 |

- **El throughput escala con los shards:** de unos 200 a unos 400 ev/s al pasar de 1 a 2 shards, porque hay una Lambda procesadora por shard.
- **La latencia sube cuando se publica más rápido de lo que se procesa:** en `e2-shards2` se mandaron 1,600 ev/s contra unos 400 de capacidad, así que se formó cola (`IteratorAge` de 22 s). Para que no se degrade, la entrada debe limitarse a la capacidad; eso hace ahora el usage plan de la API.
- **Casi todo el tiempo de la Lambda procesadora es escribir en Neo4j** (unos 400 ms por lote de 100). Calcular las huellas en el juez dejó la Lambda ligera.
- **Plagio: 0 de 102 copias detectadas** en esas corridas. La causa era el generador sintético anterior y los k-gramas de 5 tokens; se corrigió (ver [Problemas conocidos](#problemas-conocidos)). Con el generador nuevo y `k = 12`, una simulación local de la misma consulta de `queries.py` dio **recall de 0.97 a 0.99 y 0 falsos positivos** con 20 mil envíos. Hay que confirmarlo en AWS con `queries.py` (E7).

### Prueba de carga de la API
<!-- Pega aquí la tabla que imprime load_test.py (results/load-<fecha>.csv) -->

| Clientes | Peticiones/s | Envíos/s | p50 ms | p95 ms | p99 ms | % error | 429 | 5xx |
|---|---|---|---|---|---|---|---|---|
| 10 | | | | | | | | |
| 25 | | | | | | | | |
| 50 | | | | | | | | |
| 100 | | | | | | | | |

### Dashboard del pipeline en Grafana
| Panel | Métrica | Experimento |
|---|---|---|
| API Gateway · peticiones | `Count` | E8, E9 |
| API Gateway · latencia | `Latency` promedio y p95, `IntegrationLatency` | E8 |
| API Gateway · errores | `4XXError` (incluye 403 y 429), `5XXError` | E8, E9 |
| Lambdas · throttles | `Throttles` de ingesta y procesadora | E4, E8 |
| Registros entrantes vs leídos | `IncomingRecords`, `GetRecords.Records` | E1, E2 |
| Throttling de Kinesis | `WriteProvisionedThroughputExceeded` | E2 |
| Lambda procesadora · invocaciones y errores | `Invocations`, `Errors` | E3, E6 |
| Lambda procesadora · duración por lote | `Duration` promedio y máximo | E3, E5 |
| Atraso del consumidor | `IteratorAge` (Lambda y Kinesis) | E3, E4, E6, E8 |
| Lambda procesadora · ejecuciones concurrentes | `ConcurrentExecutions` | E4 |
| Bytes entrantes | `IncomingBytes` | E2 |
| DLQ | `ApproximateNumberOfMessagesVisible` | E6 |

### Paneles de Neo4j en Grafana
Crea un dashboard, agrega paneles con la fuente Neo4j y pega las consultas.

**Totales** (*Stat*)
```cypher
MATCH (s:Submission) WITH count(s) AS envios
MATCH (u:User) WITH envios, count(u) AS usuarios
MATCH (p:Problem) RETURN envios, usuarios, count(p) AS problemas
```

**Envíos ingeridos por minuto** (*Time series*)
```cypher
MATCH (s:Submission) WHERE s.ingested_ms IS NOT NULL
WITH datetime({epochMillis: s.ingested_ms - s.ingested_ms % 60000}) AS Time, count(*) AS envios
RETURN Time, envios ORDER BY Time
```

**Latencia de extremo a extremo** (*Stat*, ms)
```cypher
MATCH (s:Submission) WHERE s.arrival_ms IS NOT NULL
WITH s.ingested_ms - s.arrival_ms AS lat
RETURN percentileCont(lat, 0.5) AS p50_ms, percentileCont(lat, 0.95) AS p95_ms, max(lat) AS max_ms
```

**Envíos por veredicto** (*Bar chart*)
```cypher
MATCH (s:Submission)-[:GOT]->(v:Verdict)
RETURN v.name AS veredicto, count(*) AS envios ORDER BY envios DESC
```

**Aceptación por lenguaje** (*Table*)
```cypher
MATCH (l:Language)<-[:IN]-(s:Submission)
WITH l.name AS lenguaje, count(s) AS total,
     sum(CASE WHEN s.verdict = 'Accepted' THEN 1 ELSE 0 END) AS ac
WHERE total >= 50
RETURN lenguaje, total, round(100.0 * ac / total, 1) AS pct_ac ORDER BY total DESC
```

**Problemas más difíciles** (*Table*)
```cypher
MATCH (u:User)-[:SOLVED]->(p:Problem)
MATCH (u)-[:SUBMITTED]->(s:Submission)-[:FOR]->(p)
WITH u, p, collect(s) AS subs, min(CASE WHEN s.verdict = 'Accepted' THEN s.date END) AS first_ac
WITH p, size([x IN subs WHERE x.date <= first_ac]) AS intentos
RETURN p.id AS problema, count(*) AS resolvieron, round(avg(intentos), 2) AS intentos_promedio
ORDER BY intentos_promedio DESC LIMIT 10
```

**Parejas sospechosas de plagio** (*Table*)
```cypher
MATCH (f:Fingerprint)
WITH f, COUNT { (f)<-[:HAS]-() } AS df WHERE df >= 2 AND df <= 20
MATCH (a:Submission)-[:HAS]->(f)<-[:HAS]-(b:Submission)
WHERE a.id < b.id AND a.problem_id = b.problem_id AND a.user_id <> b.user_id
  AND a.n_fp >= 15 AND b.n_fp >= 15
WITH a, b, count(f) AS shared
WITH a, b, shared, toFloat(shared) / CASE WHEN a.n_fp < b.n_fp THEN a.n_fp ELSE b.n_fp END AS sim
WHERE sim >= 0.6
RETURN a.problem_id AS problema, a.user_id AS usuario_a, b.user_id AS usuario_b,
       shared AS huellas, round(sim, 3) AS similitud
ORDER BY similitud DESC LIMIT 50
```

## Limpieza de recursos
Al terminar cada sesión, desde `infra/`:

```bash
terraform destroy
```

Confirma con `yes`. Esto borra la API Gateway con su API key y usage plan, las dos Lambdas, el stream, la DLQ, la instancia, los Security Groups, el Key Pair y el `.pem` local, y elimina la huella de la IP en `~/.ssh/known_hosts`.

> El Security Group de la Lambda procesadora tarda **20 a 40 minutos** en borrarse, porque AWS libera con calma las interfaces de red de la Lambda en VPC. Si `destroy` se queda esperando o falla en ese recurso, espera y vuelve a correrlo. La Lambda de ingesta está fuera de la VPC, así que no causa esa espera. La VPC que aparece en `terraform state list` es solo una lectura de la VPC por defecto; no se borra.

Opcional, para no dejar rastro de la llave privada en el respaldo del state ni huellas viejas:
```bash
rm -f terraform.tfstate.backup ~/.ssh/known_hosts.old
```

## Problemas conocidos
| Problema | Causa | Solución |
|----------|-------|----------|
| Neptune no se puede usar | No está disponible en el Learner Lab | Neo4j Community en Docker sobre EC2 |
| `AccessDeniedException ... kinesis:IncreaseStreamRetentionPeriod` | La SCP de Academy lo bloquea y el recurso `aws_kinesis_stream` siempre lo llama al crear el stream | Crear el stream con el AWS CLI (`terraform_data` + `local-exec`) y leerlo con `data "aws_kinesis_stream"` |
| `destroy` tarda o falla en el Security Group de la Lambda | La Lambda en VPC deja interfaces de red que AWS libera en 20 a 40 minutos; mientras existan, el Security Group no se puede borrar | Esperar y repetir `terraform destroy`; esas interfaces no se pueden borrar a mano |
| La API responde `403` con la key correcta | La key no está asociada al usage plan o el stage no está en el plan | Revisar `aws_api_gateway_usage_plan_key` y `api_stages` en `apigw.tf`, y volver a aplicar |
| Muchos `429` en la prueba de carga | Se superó `api_rate_limit` / `api_burst_limit` del usage plan, o el límite de concurrencia de Lambda de la cuenta | Es el comportamiento esperado al pasar el límite. Para más throughput, subir los límites y los `shards`, o usar `--mode batch` |
| `503` o `failed` en las respuestas de la API | Kinesis rechaza escrituras: cada envío pesa unos 3 KB y un shard acepta 1 MB/s (unos 340 envíos/s) | Subir `shards` según la tabla de [Capacidad del pipeline](#capacidad-del-pipeline); el producer reintenta solo los fallidos |
| Recall de plagio en 0 | El generador sintético anterior salía de unas 24 líneas plantilla, y con k-gramas de 5 tokens casi todas las huellas se repetían entre programas distintos; el filtro de boilerplate (`--max-df`) las descartaba | Generador nuevo (`synthetic.py`, programas distintos por usuario) y `k = 12` por defecto en `fingerprint.py` |
| `UNPROTECTED PRIVATE KEY FILE` / `bad permissions` en Windows | El `.pem` hereda permisos de la carpeta y `file_permission` no aplica en NTFS | Provisioner que aplica `icacls` para dejar solo a tu usuario |
| `Access is denied` al recrear el `.pem` | `file_permission = "0400"` lo marcaba como solo lectura en Windows | Usar `0600` y dar `(F)` a tu usuario con `icacls`. Si el `.pem` viejo sigue en solo lectura: `chmod u+w infra/judge-stream-key.pem` |
| `Invalid -chdir option` | PowerShell parte `-chdir=../infra` en dos argumentos | Correr todo desde Git Bash (o poner el argumento entre comillas en PowerShell) |
| `metrics.py` da ceros | Se usó un `run_id` que no existe | Copiar el `run_id` que imprime el producer o `load_test.py`, o verlo en `results/` |
| La fuente Neo4j de Grafana desaparece | Recrear la EC2 crea un Grafana nuevo, y también borra los datos de Neo4j | Volver a configurar la fuente y recargar datos |
| La Lambda procesadora falla justo después del `apply` | Neo4j sigue arrancando dentro de la EC2 | Correr `python db.py wait` antes del producer; los lotes fallidos se reintentan solos |
| Aviso `post-quantum key exchange` al conectar | El cliente SSH de Git Bash es más nuevo que el servidor de Amazon Linux | Es solo un aviso; se puede ignorar |

## Referencias
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [API Gateway: usage plans y API keys](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)
- [API Gateway: throttling de peticiones](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-request-throttling.html)
- [Amazon Kinesis Data Streams: cuotas y límites](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
- [Usar Lambda con Kinesis](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
- [EC2 user data](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html)
- [Neo4j: API HTTP](https://neo4j.com/docs/http-api/current/)
- [Grafana: fuente de datos CloudWatch](https://grafana.com/docs/grafana/latest/datasources/aws-cloudwatch/)
- [Plugin Neo4j para Grafana](https://grafana.com/grafana/plugins/kniepdennis-neo4j-datasource/)
- Schleimer, Wilkerson y Aiken, *Winnowing: Local Algorithms for Document Fingerprinting* (SIGMOD 2003)

---

**Notas**
- No subas a Git el `.pem` ni los archivos `terraform.tfstate*`: el state contiene la llave privada, la API key y las contraseñas de Neo4j y Grafana. Ya están en el `.gitignore` del lab, junto con `.build/`.
- `tests/results/` sí se versiona: tiene las métricas de cada corrida para el reporte.
