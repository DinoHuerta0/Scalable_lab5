"""Lambda de ingesta detrás de API Gateway: recibe envíos del juez y los publica en Kinesis.

  POST /submissions        un envío (objeto JSON)        -> put_record
  POST /submissions/batch  hasta 500 envíos (arreglo)    -> put_records

Solo comprueba lo mínimo para publicar: que el body sea JSON y que cada envío
traiga user_id (es la partition key). La validación y limpieza completas las hace
la Lambda consumidora (handler.py); lo que no pase termina en la DLQ.

La API key la revisa API Gateway antes de invocar esta Lambda (sin key -> 403).
Respuestas: 202 aceptado, 400 petición inválida, 503 Kinesis sin capacidad.
"""
import base64
import json
import os

import boto3
from botocore.exceptions import ClientError

STREAM = os.environ["STREAM_NAME"]
MAX_BATCH = 500
kinesis = boto3.client("kinesis")


def _resp(status, body):
    return {"statusCode": status, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body)}


def _body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode()
    return json.loads(raw)


def _record(ev):
    return {"Data": json.dumps(ev, ensure_ascii=False).encode(), "PartitionKey": str(ev["user_id"])}


def _throttled(err):
    return err.response.get("Error", {}).get("Code") == "ProvisionedThroughputExceededException"


def single(ev):
    if not isinstance(ev, dict) or not ev.get("user_id"):
        return _resp(400, {"error": "se espera un objeto JSON con user_id"})
    try:
        r = kinesis.put_record(StreamName=STREAM, **_record(ev))
    except ClientError as e:
        if _throttled(e):
            return _resp(503, {"error": "Kinesis sin capacidad, reintenta"})
        raise
    return _resp(202, {"shard_id": r["ShardId"], "sequence_number": r["SequenceNumber"]})


def batch(evs):
    if not isinstance(evs, list) or not 1 <= len(evs) <= MAX_BATCH:
        return _resp(400, {"error": f"se espera un arreglo JSON de 1 a {MAX_BATCH} envíos"})
    bad = [i for i, ev in enumerate(evs) if not isinstance(ev, dict) or not ev.get("user_id")]
    if bad:
        return _resp(400, {"error": "envíos sin user_id", "indices": bad[:50]})
    r = kinesis.put_records(StreamName=STREAM, Records=[_record(ev) for ev in evs])
    failed = [i for i, rec in enumerate(r["Records"]) if "ErrorCode" in rec]
    if len(failed) == len(evs):
        return _resp(503, {"error": "Kinesis sin capacidad, reintenta", "failed": failed})
    # 202 aunque fallen algunos: el cliente reintenta solo los índices de "failed"
    return _resp(202, {"accepted": len(evs) - len(failed), "failed": failed})


def handler(event, context):
    try:
        data = _body(event)
    except (ValueError, UnicodeDecodeError):
        return _resp(400, {"error": "el body no es JSON válido"})
    if (event.get("resource") or event.get("path") or "").endswith("/batch"):
        return batch(data)
    return single(data)
