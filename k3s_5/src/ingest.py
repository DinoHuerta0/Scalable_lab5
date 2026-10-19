"""Función de ingesta detrás de Kong: recibe envíos del juez y los publica en Kafka.

Equivale a la Lambda de ingesta de AWS_5 (ingest.py), con Kafka en lugar de Kinesis:

  POST /submissions        un envío (objeto JSON)        -> 1 registro
  POST /submissions/batch  hasta 500 envíos (arreglo)    -> 1 registro por envío
  GET  /metrics            métricas para Prometheus

Solo comprueba lo mínimo para publicar: que el body sea JSON y que cada envío traiga
user_id (es la llave del registro: los envíos de un usuario van a la misma partición
y conservan su orden). La validación y limpieza completas las hace la procesadora
(handler.py); lo que no pase termina en la DLQ.

A cada envío le agrega arrival_ms (cuándo entró a Kafka), que la procesadora guarda
para medir la latencia de extremo a extremo, como approximateArrivalTimestamp en Kinesis.

La API key la revisa Kong antes de llegar aquí (sin key -> 401).
Respuestas: 202 aceptado, 400 petición inválida, 503 Kafka sin capacidad (reintentar).
"""
import json
import os
import threading
import time

from confluent_kafka import KafkaException, Producer
from prometheus_client import Counter, Histogram, make_wsgi_app

TOPIC = os.environ["TOPIC"]
MAX_BATCH = 500
DELIVERY_TIMEOUT_S = 10

producer = Producer({
    "bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"],
    "acks": "all",
    "linger.ms": 5,                     # junta registros de varias peticiones (más throughput)
    "compression.type": "lz4",
    "message.timeout.ms": DELIVERY_TIMEOUT_S * 1000,
    "queue.buffering.max.messages": 200_000,
})

REQUESTS = Counter("ingest_requests_total", "Peticiones recibidas", ["route", "code"])
RECORDS = Counter("ingest_records_total", "Envíos publicados en Kafka", ["result"])
PUBLISH = Histogram("ingest_publish_seconds", "Tiempo hasta que Kafka confirma los envíos de una petición",
                    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
metrics_app = make_wsgi_app()


def _poll_loop():
    """Atiende las confirmaciones de Kafka (llama a los callbacks de entrega)."""
    while True:
        producer.poll(0.1)


threading.Thread(target=_poll_loop, daemon=True).start()


def publish(evs):
    """Publica los envíos y espera la confirmación de Kafka.

    Devuelve una lista con (partición, offset) por envío, o None si ese envío falló.
    """
    n = len(evs)
    results = [None] * n
    pending = [n]
    lock = threading.Lock()
    done = threading.Event()

    def finish():
        pending[0] -= 1
        if pending[0] == 0:
            done.set()

    def on_delivery(i):
        def cb(err, msg):
            with lock:
                if err is None:
                    results[i] = (msg.partition(), msg.offset())
                finish()
        return cb

    t0 = time.perf_counter()
    now_ms = int(time.time() * 1000)
    for i, ev in enumerate(evs):
        ev["arrival_ms"] = now_ms
        try:
            producer.produce(TOPIC, key=str(ev["user_id"]).encode(),
                             value=json.dumps(ev, ensure_ascii=False).encode(),
                             timestamp=now_ms, on_delivery=on_delivery(i))
        except (BufferError, KafkaException):
            with lock:
                finish()              # cola local llena o Kafka caído: ese envío falla
    if not done.wait(DELIVERY_TIMEOUT_S + 2):
        with lock:
            snapshot = list(results)  # lo que no se confirmó a tiempo cuenta como fallido
    else:
        snapshot = results
    PUBLISH.observe(time.perf_counter() - t0)
    ok = sum(r is not None for r in snapshot)
    RECORDS.labels("ok").inc(ok)
    RECORDS.labels("failed").inc(n - ok)
    return snapshot


def single(ev):
    if not isinstance(ev, dict) or not ev.get("user_id"):
        return 400, {"error": "se espera un objeto JSON con user_id"}
    r = publish([ev])[0]
    if r is None:
        return 503, {"error": "Kafka sin capacidad o no disponible, reintenta"}
    return 202, {"partition": r[0], "offset": r[1]}


def batch(evs):
    if not isinstance(evs, list) or not 1 <= len(evs) <= MAX_BATCH:
        return 400, {"error": f"se espera un arreglo JSON de 1 a {MAX_BATCH} envíos"}
    bad = [i for i, ev in enumerate(evs) if not isinstance(ev, dict) or not ev.get("user_id")]
    if bad:
        return 400, {"error": "envíos sin user_id", "indices": bad[:50]}
    results = publish(evs)
    failed = [i for i, r in enumerate(results) if r is None]
    if len(failed) == len(evs):
        return 503, {"error": "Kafka sin capacidad o no disponible, reintenta", "failed": failed}
    # 202 aunque fallen algunos: el cliente reintenta solo los índices de "failed"
    return 202, {"accepted": len(evs) - len(failed), "failed": failed}


_REASONS = {200: "OK", 202: "Accepted", 400: "Bad Request", 404: "Not Found",
            405: "Method Not Allowed", 503: "Service Unavailable"}


def _reply(start_response, status, body):
    data = json.dumps(body).encode()
    start_response(f"{status} {_REASONS[status]}",
                   [("Content-Type", "application/json"), ("Content-Length", str(len(data)))])
    return [data]


def app(environ, start_response):
    path = environ.get("PATH_INFO", "/")
    if path == "/metrics":
        return metrics_app(environ, start_response)
    if path == "/healthz":
        return _reply(start_response, 200, {"ok": True})
    if not path.startswith("/submissions"):
        return _reply(start_response, 404, {"error": "ruta no encontrada"})
    route = "batch" if path.rstrip("/").endswith("/batch") else "single"
    if environ.get("REQUEST_METHOD") != "POST":
        status, body = 405, {"error": "usa POST"}
    else:
        try:
            size = int(environ.get("CONTENT_LENGTH") or 0)
            data = json.loads(environ["wsgi.input"].read(size) or b"null")
        except (ValueError, UnicodeDecodeError):
            status, body = 400, {"error": "el body no es JSON válido"}
        else:
            status, body = batch(data) if route == "batch" else single(data)
    REQUESTS.labels(route, str(status)).inc()
    return _reply(start_response, status, body)
