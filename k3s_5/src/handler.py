"""Función procesadora: envíos del juez -> grafo en Neo4j.

Equivale a la Lambda procesadora de AWS_5 (handler.py): misma validación, misma
limpieza y mismo Cypher. Lo que cambia es cómo llegan los datos:

  - En AWS, el Event Source Mapping invoca la Lambda con un lote de registros.
  - Aquí, el KafkaSource de Knative Eventing manda cada registro como un CloudEvent
    en su propia petición HTTP (el body es el envío tal como lo publicó la ingesta).

Para no escribir en Neo4j un envío a la vez, la procesadora agrupa: cada petición
deja su envío en una cola en memoria y espera; un hilo escritor toma hasta BATCH_SIZE
envíos, los escribe en una sola transacción con UNWIND + MERGE y responde a todas
las peticiones del lote. Mientras escribe, se van juntando los siguientes, así que
el lote crece solo cuando hay carga. BATCHING_WINDOW_MS > 0 espera ese tiempo a que
el lote se llene (como maximum_batching_window_in_seconds).

Respuestas al KafkaSource:
  202  escrito en Neo4j
  400  envío inválido: no se reintenta, se va directo a la DLQ
  503  Neo4j falló: el KafkaSource reintenta y, si se agotan los reintentos, a la DLQ
MERGE hace la escritura idempotente: Kafka también entrega "al menos una vez".
"""
import datetime as dt
import json
import os
import re
import threading
import time

from prometheus_client import Counter, Gauge, Histogram, make_wsgi_app

from neo4j_http import Neo4j

REQUIRED = ("submission_id", "user_id", "problem_id", "language", "verdict")
MAX_FP = 400                            # tope de huellas por envío
FP_RE = re.compile(r"^[0-9a-f]{12}$")   # formato que genera el juez (48 bits en hex)

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))
BATCHING_WINDOW_S = int(os.environ.get("BATCHING_WINDOW_MS", "0")) / 1000
WAIT_TIMEOUT_S = 110                    # menos que timeoutSeconds del Knative Service

db = Neo4j(os.environ["NEO4J_URL"], os.environ["NEO4J_USER"], os.environ["NEO4J_PASS"])
_schema_ready = False

RECORDS = Counter("processor_records_total", "Envíos recibidos por resultado", ["result"])
BATCHES = Counter("processor_batches_total", "Lotes escritos en Neo4j")
BATCH_SIZES = Histogram("processor_batch_size", "Envíos por lote",
                        buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000))
NEO4J_SECONDS = Histogram("processor_neo4j_seconds", "Tiempo de escritura de un lote en Neo4j",
                          buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 4, 8, 16, 32))
NEO4J_RETRIES = Counter("processor_neo4j_retries_total", "Reintentos por errores transitorios de Neo4j")
FINGERPRINTS = Counter("processor_fingerprints_total", "Huellas escritas")
INFLIGHT = Gauge("processor_inflight", "Eventos esperando a que se escriba su lote")
metrics_app = make_wsgi_app()

SCHEMA = [
    "CREATE CONSTRAINT submission_id IF NOT EXISTS FOR (n:Submission) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (n:User) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT problem_id IF NOT EXISTS FOR (n:Problem) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT language_name IF NOT EXISTS FOR (n:Language) REQUIRE n.name IS UNIQUE",
    "CREATE CONSTRAINT verdict_name IF NOT EXISTS FOR (n:Verdict) REQUIRE n.name IS UNIQUE",
    "CREATE CONSTRAINT fingerprint_h IF NOT EXISTS FOR (n:Fingerprint) REQUIRE n.h IS UNIQUE",
]

UPSERT_SUBMISSIONS = """
UNWIND $rows AS r
MERGE (s:Submission {id: r.submission_id})
  ON CREATE SET s.arrival_ms = r.arrival_ms, s.ingested_ms = timestamp()
SET s.user_id = r.user_id, s.problem_id = r.problem_id,
    s.language = r.language, s.verdict = r.verdict,
    s.cpu_ms = r.cpu_ms, s.memory_kb = r.memory_kb, s.code_size = r.code_size,
    s.date = r.date, s.sent_ms = r.sent_ms, s.run_id = r.run_id,
    s.n_fp = size(r.fps), s.injected_from = r.injected_from
MERGE (u:User {id: r.user_id})
MERGE (p:Problem {id: r.problem_id})
MERGE (l:Language {name: r.language})
MERGE (v:Verdict {name: r.verdict})
MERGE (u)-[:SUBMITTED]->(s)
MERGE (s)-[:FOR]->(p)
MERGE (s)-[:IN]->(l)
MERGE (s)-[:GOT]->(v)
FOREACH (_ IN CASE WHEN r.verdict = 'Accepted' THEN [1] ELSE [] END |
  MERGE (u)-[:SOLVED]->(p))
"""

UPSERT_FINGERPRINTS = """
UNWIND $rows AS r
WITH r WHERE size(r.fps) > 0
MATCH (s:Submission {id: r.submission_id})
UNWIND r.fps AS h
MERGE (f:Fingerprint {h: h})
MERGE (s)-[:HAS]->(f)
"""


def ensure_schema():
    global _schema_ready
    if _schema_ready:
        return
    for q in SCHEMA:  # una por transacción: Neo4j no mezcla cambios de esquema
        try:
            db.run([(q, None)], max_attempts=3)
        except Exception as e:  # otro pod pudo crearla al mismo tiempo
            print(json.dumps({"schema_warning": str(e)[:200]}))
    _schema_ready = True


def _num(v):
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


def _clean_fps(v):
    """Huellas que manda el juez: deja solo las válidas, sin duplicados y con tope."""
    if not isinstance(v, list):
        return []
    return sorted({h for h in v if isinstance(h, str) and FP_RE.match(h)})[:MAX_FP]


def _ce_time_ms(value):
    """Atributo time del CloudEvent (timestamp del registro en Kafka), en ms."""
    try:
        return int(dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except (AttributeError, ValueError):
        return None


def parse(raw, ce_time=None):
    """Devuelve la fila para Neo4j o lanza ValueError si el evento es inválido."""
    try:
        ev = json.loads(raw)
    except Exception as e:
        raise ValueError(f"JSON inválido: {e}") from e
    if not isinstance(ev, dict):
        raise ValueError("el evento no es un objeto JSON")
    missing = [f for f in REQUIRED if not ev.get(f)]
    if missing:
        raise ValueError(f"faltan campos: {missing}")

    arrival = _num(ev.get("arrival_ms")) or _ce_time_ms(ce_time) or time.time() * 1000
    return {
        "submission_id": str(ev["submission_id"]),
        "user_id": str(ev["user_id"]),
        "problem_id": str(ev["problem_id"]),
        "language": str(ev["language"]),
        "verdict": str(ev["verdict"]),
        "cpu_ms": _num(ev.get("cpu_ms")),
        "memory_kb": _num(ev.get("memory_kb")),
        "code_size": _num(ev.get("code_size")),
        "date": _num(ev.get("date")),
        "sent_ms": _num(ev.get("sent_ms")),
        "run_id": ev.get("run_id"),
        "injected_from": ev.get("injected_from"),
        "arrival_ms": int(arrival),
        "fps": _clean_fps(ev.get("fps")),
    }


def write_batch(rows):
    """Escribe un lote en una sola transacción e imprime una línea JSON (como la Lambda)."""
    t0 = time.perf_counter()
    ensure_schema()
    retries_before = db.retries
    # Orden fijo por id: las transacciones concurrentes (de otros pods) toman bloqueos
    # en el mismo orden y hay menos deadlocks
    rows = sorted(rows, key=lambda r: r["submission_id"])
    db.run([(UPSERT_SUBMISSIONS, {"rows": rows}), (UPSERT_FINGERPRINTS, {"rows": rows})])
    elapsed = time.perf_counter() - t0
    retries = db.retries - retries_before
    fps = sum(len(r["fps"]) for r in rows)

    BATCHES.inc()
    BATCH_SIZES.observe(len(rows))
    NEO4J_SECONDS.observe(elapsed)
    NEO4J_RETRIES.inc(retries)
    FINGERPRINTS.inc(fps)
    print(json.dumps({
        "records": len(rows),
        "written": len(rows),
        "fingerprints": fps,
        "neo4j_ms": round(elapsed * 1000, 1),
        "neo4j_retries": retries,
    }), flush=True)


class _Item:
    __slots__ = ("row", "done", "error")

    def __init__(self, row):
        self.row = row
        self.done = threading.Event()
        self.error = None


class Batcher:
    """Junta los envíos de peticiones concurrentes y los escribe por lotes."""

    def __init__(self, write, size, window_s):
        self.write, self.size, self.window = write, max(1, size), max(0.0, window_s)
        self.cond = threading.Condition()
        self.pending = []
        threading.Thread(target=self._loop, daemon=True).start()

    def submit(self, row, timeout=WAIT_TIMEOUT_S):
        item = _Item(row)
        with self.cond:
            self.pending.append(item)
            self.cond.notify()
        if not item.done.wait(timeout):
            raise TimeoutError("el lote no se escribió a tiempo")
        if item.error:
            raise item.error

    def _take(self):
        with self.cond:
            while not self.pending:
                self.cond.wait()
            if self.window:
                deadline = time.monotonic() + self.window
                while len(self.pending) < self.size:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        break
                    self.cond.wait(left)
            items, self.pending = self.pending[:self.size], self.pending[self.size:]
            return items

    def _loop(self):
        while True:
            items = self._take()
            try:
                self.write([i.row for i in items])
                error = None
            except Exception as e:
                error = e
                print(json.dumps({"batch_error": str(e)[:300], "records": len(items)}), flush=True)
            for i in items:
                i.error = error
                i.done.set()


batcher = Batcher(write_batch, BATCH_SIZE, BATCHING_WINDOW_S)


def _reply(start_response, status, reason, body=b""):
    start_response(f"{status} {reason}", [("Content-Length", str(len(body)))])
    return [body]


def app(environ, start_response):
    if environ.get("PATH_INFO") == "/metrics":
        return metrics_app(environ, start_response)
    if environ.get("REQUEST_METHOD") != "POST":
        return _reply(start_response, 200, "OK", b"ok")

    size = int(environ.get("CONTENT_LENGTH") or 0)
    raw = environ["wsgi.input"].read(size)
    try:
        row = parse(raw, environ.get("HTTP_CE_TIME"))
    except ValueError as e:
        RECORDS.labels("invalid").inc()
        print(json.dumps({"invalid_record": environ.get("HTTP_CE_ID"), "error": str(e)[:200]}), flush=True)
        return _reply(start_response, 400, "Bad Request", str(e).encode())

    INFLIGHT.inc()
    try:
        batcher.submit(row)
    except Exception as e:
        RECORDS.labels("failed").inc()
        return _reply(start_response, 503, "Service Unavailable", str(e)[:200].encode())
    finally:
        INFLIGHT.dec()
    RECORDS.labels("written").inc()
    return _reply(start_response, 202, "Accepted")
