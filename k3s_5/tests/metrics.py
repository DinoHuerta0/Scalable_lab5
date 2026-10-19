"""Mide una corrida: latencia de extremo a extremo, throughput y métricas del pipeline.

  python metrics.py --run-id run-20261019-120000
  python metrics.py --run-id run-... --minutes 60

Fuentes:
  - Neo4j: por cada envío, arrival_ms (cuándo la ingesta lo publicó en Kafka) e
    ingested_ms (cuándo se escribió en el grafo). Latencia = ingested - arrival.
    Los dos relojes son del mismo nodo.
  - Prometheus (equivale a CloudWatch): Kong, la ingesta, Kafka (kafka-exporter),
    la procesadora y la DLQ en la ventana --minutes. No se filtran por run_id: usa
    una ventana que cubra solo tu corrida.
Agrega una fila a results/experiments.csv para comparar corridas. Las columnas que
equivalen a las de AWS_5 conservan el nombre (written, lat_*, throughput_ev_s,
api_*, dlq_messages); las demás llevan el prefijo kafka_, ingest_ o processor_.
"""
import argparse
import csv
import json
import time

from common import RESULTS_DIR, neo4j, print_table, prom_value, tf_outputs

LATENCY = """
MATCH (s:Submission) WHERE s.run_id = $run AND s.arrival_ms IS NOT NULL
WITH s, s.ingested_ms - s.arrival_ms AS lat
RETURN count(s), percentileCont(lat, 0.5), percentileCont(lat, 0.95),
       percentileCont(lat, 0.99), max(lat), min(s.arrival_ms), max(s.ingested_ms)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--minutes", type=int, default=30, help="ventana de Prometheus hacia atrás")
    args = ap.parse_args()

    o = tf_outputs()
    end = time.time()
    w = f"{args.minutes}m"
    res = {"run_id": args.run_id, **(o.get("consumer_config") or {})}

    # --- Neo4j: latencia y throughput reales
    n, p50, p95, p99, mx, first_arrival, last_ingest = neo4j().query(LATENCY, run=args.run_id)[0]
    res.update(written=n, lat_p50_ms=p50, lat_p95_ms=p95, lat_p99_ms=p99, lat_max_ms=mx)
    if n and last_ingest and first_arrival:
        span = (last_ingest - first_arrival) / 1000
        res.update(span_s=round(span, 1), throughput_ev_s=round(n / span, 1) if span else None)

    def q(expr, digits=1):
        try:
            return prom_value(expr, at=end, digits=digits)
        except Exception as e:
            print(f"(Prometheus: {e})")
            return None

    def total(metric, sel=""):
        return q(f"sum(increase({metric}{{{sel}}}[{w}]))", 0)

    def quantile(p, metric, sel="", scale=1):
        return q(f"{scale} * histogram_quantile({p}, sum by (le) (increase({metric}_bucket{{{sel}}}[{w}])))")

    def mean(metric, sel="", scale=1):
        return q(f"{scale} * sum(increase({metric}_sum{{{sel}}}[{w}])) / sum(increase({metric}_count{{{sel}}}[{w}]))")

    def peak(expr):
        return q(f"max_over_time(({expr})[{w}:15s])", 0)

    kong = f'service="{o["kong_service"]}"'
    topic = f'topic="{o["topic_name"]}"'
    group = f'consumergroup="{o["consumer_group"]}",{topic}'

    # --- Kong (la entrada del juez; equivale a API Gateway)
    res.update(
        api_requests=total("kong_http_requests_total", kong),
        api_latency_avg_ms=mean("kong_request_latency_ms", kong),
        api_latency_p95_ms=quantile(0.95, "kong_request_latency_ms", kong),
        api_401=total("kong_http_requests_total", f'{kong},code="401"'),
        api_429=total("kong_http_requests_total", f'{kong},code="429"'),
        api_4xx=total("kong_http_requests_total", f'{kong},code=~"4.."'),
        api_5xx=total("kong_http_requests_total", f'{kong},code=~"5.."'),
        ingest_published=total("ingest_records_total", 'result="ok"'),
        ingest_failed=total("ingest_records_total", 'result="failed"'),
        ingest_publish_p95_ms=quantile(0.95, "ingest_publish_seconds", scale=1000),
        ingest_pods_max=peak('count(up{job="ingest"} == 1) or vector(0)'),
    )

    # --- Kafka (equivale a Kinesis) y el lag del consumidor (equivale a IteratorAge)
    res.update(
        kafka_incoming=total("kafka_topic_partition_current_offset", topic),
        kafka_consumed=total("kafka_consumergroup_current_offset", group),
        consumer_lag_max=peak(f"sum(kafka_consumergroup_lag{{{group}}})"),
    )

    # --- Procesadora (equivale a la Lambda procesadora)
    res.update(
        processor_written=total("processor_records_total", 'result="written"'),
        processor_invalid=total("processor_records_total", 'result="invalid"'),
        processor_failed=total("processor_records_total", 'result="failed"'),
        processor_batches=total("processor_batches_total"),
        processor_avg_batch=mean("processor_batch_size"),
        processor_neo4j_avg_ms=mean("processor_neo4j_seconds", scale=1000),
        processor_neo4j_p95_ms=quantile(0.95, "processor_neo4j_seconds", scale=1000),
        processor_neo4j_retries=total("processor_neo4j_retries_total"),
        processor_pods_max=peak('count(up{job="processor"} == 1) or vector(0)'),
    )

    # --- DLQ: eventos en el tópico (total desde que se creó)
    res["dlq_messages"] = q(f'sum(kafka_topic_partition_current_offset{{topic="{o["dlq_topic"]}"}})', 0)

    print_table([[k, v] for k, v in res.items()], ["métrica", "valor"])
    print("\nNota: Prometheus lee las métricas cada 5 s; espera unos 30 s después de la corrida.")

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
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Guardado en results/metrics_{args.run_id}.json y results/experiments.csv")


if __name__ == "__main__":
    main()
