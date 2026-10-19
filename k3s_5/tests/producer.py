"""Producer: simula al juez en línea publicando envíos calificados.

  python producer.py --synthetic 20000 --rate 500
  python producer.py --synthetic 20000 --plagio 0.02 --corrupt 0.001

--rate       eventos/s objetivo (0 = lo más rápido posible)
--api-batch  envíos por petición a POST /submissions/batch (1-500)
--plagio     fracción de copias plagiadas inyectadas (verdad conocida para el detector)
--corrupt    fracción de registros corruptos para probar reintentos y DLQ
--fp-k/--fp-w  parámetros de winnowing (default 12 y 4)

Como haría un juez, calcula las huellas del código (fingerprint.py) y publica la
lista de huellas en lugar del código. Los envíos los genera synthetic.py.
Siempre entra por la API (Kong): Kafka no se expone fuera del nodo, así que no hay
equivalente a --via kinesis de AWS_5. Los registros rechazados se reintentan.
"""
import argparse
import json
import random
import time
import zlib

from common import AUTH_ERRORS, RESULTS_DIR, ApiClient, tf_outputs
from fingerprint import fingerprints
from synthetic import synthetic, with_injections


def encode(ev, run_id, k, w, stats):
    """Evento -> bytes JSON (lo que publicaría el juez)."""
    if ev.get("__corrupt__"):
        # El lote debe ser JSON válido: se manda un evento sin campos obligatorios
        return json.dumps({"user_id": ev["user_id"], "corrupto": True}).encode()
    ev = dict(ev, run_id=run_id)
    code = ev.pop("code", None)
    if code:
        t0 = time.perf_counter()
        ev["fps"] = fingerprints(code, ev.get("language", ""), k, w)   # trabajo del juez
        stats["fp_ms"] += (time.perf_counter() - t0) * 1000
    ev["sent_ms"] = int(time.time() * 1000)
    return json.dumps(ev, ensure_ascii=False).encode()


def send_api(client, datas, stats):
    """POST /submissions/batch; reintenta los registros fallidos y los 429/5xx."""
    delay = 0.2
    while datas:
        status, body, ms = client.post("/submissions/batch", b"[" + b",".join(datas) + b"]")
        stats["calls"] += 1
        stats["api_ms"].append(ms)
        if status == 202:
            failed = json.loads(body).get("failed", [])
            if not failed:
                return
            stats["throttled"] += len(failed)               # Kafka no confirmó esos registros
            datas = [datas[i] for i in failed]
        elif status in (429, 500, 502, 503, 504):
            stats["api_retries"] += 1                       # límite de Kong o error transitorio
        elif status in AUTH_ERRORS:
            raise SystemExit(f"HTTP {status}: falta la API key o no es válida (terraform output -raw api_key)")
        else:
            stats["other_errors"] += len(datas)
            print(f"  HTTP {status}: {body[:200]!r}")
            return
        time.sleep(delay)
        delay = min(delay * 2, 3.0)


def pct(values, p):
    if not values:
        return None
    v = sorted(values)
    return round(v[min(len(v) - 1, int(p / 100 * len(v)))], 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--synthetic", type=int, required=True, help="número de envíos a generar")
    ap.add_argument("--rate", type=float, default=0, help="eventos/s objetivo (0 = sin límite)")
    ap.add_argument("--api-batch", type=int, default=100, help="envíos por petición a la API (1-500)")
    ap.add_argument("--plagio", type=float, default=0.0)
    ap.add_argument("--corrupt", type=float, default=0.0)
    ap.add_argument("--run-id", default=time.strftime("run-%Y%m%d-%H%M%S"))
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--fp-k", type=int, default=12, help="tamaño del k-grama de tokens")
    ap.add_argument("--fp-w", type=int, default=4, help="ventana de winnowing")
    args = ap.parse_args()
    rng = random.Random(args.seed)

    # Prefijo por corrida: los submission_id no chocan entre corridas aunque no hagas reset
    prefix = f"s{zlib.crc32(args.run_id.encode()) & 0xffffff:06x}-"
    events = with_injections(synthetic(args.synthetic, prefix=prefix, seed=args.seed),
                             args.plagio, args.corrupt, rng)

    o = tf_outputs()
    client = ApiClient()
    max_batch = max(1, min(args.api_batch, 500))

    stats = {"sent": 0, "bytes": 0, "calls": 0, "throttled": 0, "api_retries": 0,
             "other_errors": 0, "plagio_injected": 0, "corrupt_injected": 0,
             "fp_ms": 0.0, "api_ms": []}
    print(f"Mandando por la API (run_id={args.run_id}, rate={args.rate or 'max'} ev/s)")
    t0 = last_report = time.time()
    batch = []

    def flush():
        nonlocal batch
        if batch:
            send_api(client, batch, stats)
            batch = []

    for ev in events:
        if ev.get("__corrupt__"):
            stats["corrupt_injected"] += 1
        elif ev.get("injected_from"):
            stats["plagio_injected"] += 1
        data = encode(ev, args.run_id, args.fp_k, args.fp_w, stats)
        if len(batch) == max_batch:
            flush()
        batch.append(data)
        stats["sent"] += 1
        stats["bytes"] += len(data)

        if args.rate:                       # no adelantarse al ritmo objetivo
            ahead = stats["sent"] / args.rate - (time.time() - t0)
            if ahead > 0:
                flush()
                time.sleep(ahead)
        if time.time() - last_report > 5:
            last_report = time.time()
            el = last_report - t0
            print(f"  {stats['sent']:>9,} enviados  {stats['sent'] / el:7.0f} ev/s  "
                  f"no confirmados={stats['throttled']}  reintentos_api={stats['api_retries']}", flush=True)
    flush()
    client.close()

    elapsed = time.time() - t0
    api_ms = stats.pop("api_ms")
    stats.update(run_id=args.run_id, via="api", topic=o["topic_name"], elapsed_s=round(elapsed, 2),
                 rate_target=args.rate, rate_real=round(stats["sent"] / elapsed, 1),
                 mb=round(stats["bytes"] / 2**20, 2), fp_ms_total=round(stats.pop("fp_ms"), 1),
                 fp_k=args.fp_k, fp_w=args.fp_w, source=f"synthetic:{args.synthetic}",
                 api_batch_p50_ms=pct(api_ms, 50), api_batch_p95_ms=pct(api_ms, 95),
                 consumer_config=o.get("consumer_config"))
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"producer_{args.run_id}.json"
    out.write_text(json.dumps(stats, indent=2))
    print(json.dumps(stats, indent=2))
    print(f"\nGuardado en {out}. Siguiente: python metrics.py --run-id {args.run_id}")


if __name__ == "__main__":
    main()
