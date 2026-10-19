"""Mide una corrida: latencia de extremo a extremo, throughput y métricas de AWS.

  python metrics.py --run-id run-20261013-120000
  python metrics.py --run-id run-... --minutes 60

Fuentes:
  - Neo4j: por cada envío, arrival_ms (llegada a Kinesis) e ingested_ms (escrito
    en el grafo). Latencia = ingested - arrival. Ambos relojes son de AWS.
  - CloudWatch: métricas de API Gateway, Kinesis y las dos Lambdas en la ventana
    --minutes (no se filtran por run_id: usa una ventana que cubra solo tu corrida).
  - CloudWatch Logs Insights: el log JSON que imprime la Lambda en cada lote.
  - SQS: mensajes en la DLQ.
Agrega una fila a results/experiments.csv para comparar corridas.
"""
import argparse
import csv
import datetime as dt
import json
import time

from common import RESULTS_DIR, aws, neo4j, print_table, tf_outputs

LATENCY = """
MATCH (s:Submission) WHERE s.run_id = $run AND s.arrival_ms IS NOT NULL
WITH s, s.ingested_ms - s.arrival_ms AS lat
RETURN count(s), percentileCont(lat, 0.5), percentileCont(lat, 0.95),
       percentileCont(lat, 0.99), max(lat), min(s.arrival_ms), max(s.ingested_ms)
"""


def cw_stat(cw, namespace, metric, dims, stat, start, end):
    """Agrega una métrica de CloudWatch en la ventana: Sum = total, Average = promedio,
    Maximum y percentiles (p95, p99) = el peor minuto."""
    kw = {"ExtendedStatistics": [stat]} if stat.startswith("p") else {"Statistics": [stat]}
    resp = cw.get_metric_statistics(
        Namespace=namespace, MetricName=metric, StartTime=start, EndTime=end, Period=60,
        Dimensions=[{"Name": k, "Value": v} for k, v in dims.items()], **kw,
    )
    vals = [p["ExtendedStatistics"][stat] if stat.startswith("p") else p[stat]
            for p in resp["Datapoints"]]
    if not vals:
        return None
    if stat == "Sum":
        return round(sum(vals), 1)
    if stat == "Average":
        return round(sum(vals) / len(vals), 1)
    return round(max(vals), 1)


def logs_insights(logs, group, start, end):
    q = ("filter ispresent(neo4j_ms) | stats count(*) as batches, sum(records) as records, "
         "sum(written) as written, sum(invalid) as invalid, sum(fingerprints) as fingerprints, "
         "avg(records) as avg_batch, avg(parse_ms) as avg_parse_ms, avg(neo4j_ms) as avg_neo4j_ms, "
         "max(neo4j_ms) as max_neo4j_ms, sum(neo4j_retries) as neo4j_retries")
    qid = logs.start_query(logGroupName=group, startTime=int(start.timestamp()),
                           endTime=int(end.timestamp()), queryString=q)["queryId"]
    for _ in range(60):
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled"):
            break
        time.sleep(1)
    if not r.get("results"):
        return {}
    return {c["field"]: round(float(c["value"]), 1) for c in r["results"][0]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--minutes", type=int, default=30, help="ventana de CloudWatch hacia atrás")
    args = ap.parse_args()

    o = tf_outputs()
    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(minutes=args.minutes)
    res = {"run_id": args.run_id, **(o.get("consumer_config") or {})}

    # --- Neo4j: latencia y throughput reales
    n, p50, p95, p99, mx, first_arrival, last_ingest = neo4j().query(LATENCY, run=args.run_id)[0]
    res.update(written=n, lat_p50_ms=p50, lat_p95_ms=p95, lat_p99_ms=p99, lat_max_ms=mx)
    if n and last_ingest and first_arrival:
        span = (last_ingest - first_arrival) / 1000
        res.update(span_s=round(span, 1), throughput_ev_s=round(n / span, 1) if span else None)

    # --- CloudWatch
    cw = aws("cloudwatch")
    s = {"StreamName": o["stream_name"]}
    f = {"FunctionName": o["lambda_name"]}
    res.update(
        kinesis_incoming=cw_stat(cw, "AWS/Kinesis", "IncomingRecords", s, "Sum", start, end),
        kinesis_throttled=cw_stat(cw, "AWS/Kinesis", "WriteProvisionedThroughputExceeded", s, "Sum", start, end),
        lambda_invocations=cw_stat(cw, "AWS/Lambda", "Invocations", f, "Sum", start, end),
        lambda_errors=cw_stat(cw, "AWS/Lambda", "Errors", f, "Sum", start, end),
        lambda_duration_max_ms=cw_stat(cw, "AWS/Lambda", "Duration", f, "Maximum", start, end),
        lambda_concurrency_max=cw_stat(cw, "AWS/Lambda", "ConcurrentExecutions", f, "Maximum", start, end),
        iterator_age_max_ms=cw_stat(cw, "AWS/Lambda", "IteratorAge", f, "Maximum", start, end),
        lambda_throttles=cw_stat(cw, "AWS/Lambda", "Throttles", f, "Sum", start, end),
    )

    # --- API Gateway y Lambda de ingesta (la entrada del juez)
    if o.get("api_name"):
        api = {"ApiName": o["api_name"], "Stage": "v1"}
        ing = {"FunctionName": o["ingest_lambda_name"]}
        res.update(
            api_requests=cw_stat(cw, "AWS/ApiGateway", "Count", api, "Sum", start, end),
            api_latency_avg_ms=cw_stat(cw, "AWS/ApiGateway", "Latency", api, "Average", start, end),
            api_latency_p95_ms=cw_stat(cw, "AWS/ApiGateway", "Latency", api, "p95", start, end),
            api_4xx=cw_stat(cw, "AWS/ApiGateway", "4XXError", api, "Sum", start, end),
            api_5xx=cw_stat(cw, "AWS/ApiGateway", "5XXError", api, "Sum", start, end),
            ingest_invocations=cw_stat(cw, "AWS/Lambda", "Invocations", ing, "Sum", start, end),
            ingest_errors=cw_stat(cw, "AWS/Lambda", "Errors", ing, "Sum", start, end),
            ingest_throttles=cw_stat(cw, "AWS/Lambda", "Throttles", ing, "Sum", start, end),
            ingest_concurrency_max=cw_stat(cw, "AWS/Lambda", "ConcurrentExecutions", ing, "Maximum", start, end),
        )

    # --- Log de la Lambda (por lote)
    try:
        res.update({f"log_{k}": v for k, v in logs_insights(
            aws("logs"), f"/aws/lambda/{o['lambda_name']}", start, end).items()})
    except Exception as e:
        print(f"(Logs Insights no disponible: {e})")

    # --- DLQ
    attrs = aws("sqs").get_queue_attributes(
        QueueUrl=o["dlq_url"], AttributeNames=["ApproximateNumberOfMessages"])["Attributes"]
    res["dlq_messages"] = int(attrs["ApproximateNumberOfMessages"])

    print_table([[k, v] for k, v in res.items()], ["métrica", "valor"])
    print("\nNota: las métricas de CloudWatch tardan 1-3 min en aparecer completas.")

    RESULTS_DIR.mkdir(exist_ok=True)
    (RESULTS_DIR / f"metrics_{args.run_id}.json").write_text(json.dumps(res, indent=2))
    csv_path = RESULTS_DIR / "experiments.csv"
    rows, fields = [], []
    if csv_path.exists():  # conserva las filas anteriores y agrega columnas nuevas si hacen falta
        with open(csv_path, newline="") as fh:
            reader = csv.DictReader(fh)
            fields = list(reader.fieldnames or [])
            rows = list(reader)
    fields += [k for k in res if k not in fields]
    rows.append({k: res.get(k) for k in fields})
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"Guardado en results/metrics_{args.run_id}.json y results/experiments.csv")


if __name__ == "__main__":
    main()
