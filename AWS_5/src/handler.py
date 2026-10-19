"""Lambda consumidora de Kinesis: envíos del juez -> grafo en Neo4j.

La Lambda es ligera a propósito: solo valida, limpia y carga. El trabajo pesado
(calcular las huellas del código para plagio) lo hace el juez antes de publicar
el evento, porque ahí ya tiene el código y la CPU.

Por cada lote de registros:
  1. Decodifica y valida cada evento (JSON con los campos del envío).
  2. Limpia: tipos numéricos, vacíos -> null, huellas sin duplicados ni basura.
  3. Escribe todo el lote en una sola transacción con UNWIND + MERGE
     (MERGE hace la escritura idempotente: Kinesis entrega "al menos una vez").
  4. Reporta como fallidos los registros inválidos (ReportBatchItemFailures);
     tras agotar los reintentos terminan en la DLQ.
"""
import base64
import json
import os
import re
import time

from neo4j_http import Neo4j

REQUIRED = ("submission_id", "user_id", "problem_id", "language", "verdict")
MAX_FP = 400                            # tope de huellas por envío
FP_RE = re.compile(r"^[0-9a-f]{12}$")   # formato que genera el juez (48 bits en hex)

db = Neo4j(os.environ["NEO4J_URL"], os.environ["NEO4J_USER"], os.environ["NEO4J_PASS"])
_schema_ready = False

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
        except Exception as e:  # otra instancia pudo crearla al mismo tiempo
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


def parse(record):
    """Devuelve la fila para Neo4j o lanza ValueError si el evento es inválido."""
    k = record["kinesis"]
    try:
        ev = json.loads(base64.b64decode(k["data"]))
    except Exception as e:
        raise ValueError(f"JSON inválido: {e}") from e
    if not isinstance(ev, dict):
        raise ValueError("el evento no es un objeto JSON")
    missing = [f for f in REQUIRED if not ev.get(f)]
    if missing:
        raise ValueError(f"faltan campos: {missing}")

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
        "arrival_ms": int(float(k["approximateArrivalTimestamp"]) * 1000),
        "fps": _clean_fps(ev.get("fps")),
    }


def handler(event, context):
    t0 = time.perf_counter()
    ensure_schema()

    rows, failures = [], []
    for rec in event.get("Records", []):
        try:
            rows.append(parse(rec))
        except ValueError as e:
            seq = rec["kinesis"]["sequenceNumber"]
            failures.append({"itemIdentifier": seq})
            print(json.dumps({"invalid_record": seq, "error": str(e)[:200]}))
    t_parse = time.perf_counter()

    retries_before = db.retries
    if rows:
        # Orden fijo por id: las transacciones concurrentes toman bloqueos en el
        # mismo orden y hay menos deadlocks
        rows.sort(key=lambda r: r["submission_id"])
        db.run([(UPSERT_SUBMISSIONS, {"rows": rows}), (UPSERT_FINGERPRINTS, {"rows": rows})])
    t_db = time.perf_counter()

    print(json.dumps({
        "records": len(event.get("Records", [])),
        "written": len(rows),
        "invalid": len(failures),
        "fingerprints": sum(len(r["fps"]) for r in rows),
        "parse_ms": round((t_parse - t0) * 1000, 1),
        "neo4j_ms": round((t_db - t_parse) * 1000, 1),
        "neo4j_retries": db.retries - retries_before,
    }))
    return {"batchItemFailures": failures}
