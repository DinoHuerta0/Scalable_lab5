"""Prueba de carga de la API: muchos clientes simultáneos, por escalones.

  python load_test.py                                     # 10, 25, 50, 100 clientes, 60 s c/u
  python load_test.py --steps 10,50,100,200 --duration 60
  python load_test.py --mode batch --batch-size 25        # cada petición lleva 25 envíos

Cada cliente es un hilo con su propia conexión HTTPS (keep-alive) que manda
peticiones POST con la API key una tras otra, lo más rápido que la API responde
(carga en lazo cerrado). Por escalón mide:
  - peticiones/s y envíos/s aceptados (202),
  - latencia de la API vista por el cliente: p50, p95, p99 y máximo,
  - errores: 403 (API key), 429 (throttling de API Gateway o de Lambda), 5xx, excepciones.

"Sin degradarse" significa que al subir los clientes el throughput sube (o se
estabiliza en el límite configurado) mientras la p95 y la tasa de errores se
mantienen bajas. Cada escalón usa su propio run_id (<run-id>-c<clientes>), así que
`python metrics.py --run-id <run-id>-c50` mide la latencia del pipeline completo
(API -> Kinesis -> Lambda -> Neo4j) de ese escalón.

Los envíos se generan antes de cada escalón (synthetic.py + huellas) para que el
CPU del cliente no limite la medición.
"""
import argparse
import csv
import json
import threading
import time
import zlib

from common import RESULTS_DIR, ApiClient, print_table
from fingerprint import fingerprints
from synthetic import synthetic


def build_pool(size, seed):
    pool = []
    for ev in synthetic(size, seed=seed):
        code = ev.pop("code")
        ev["fps"] = fingerprints(code, ev["language"])
        pool.append(ev)
    return pool


def pct(v, p):
    if not v:
        return None
    v = sorted(v)
    return round(v[min(len(v) - 1, int(p / 100 * len(v)))], 1)


def run_step(clients, duration, pool, run_id, mode, batch_size, base_url=None, api_key=None):
    lock = threading.Lock()
    stats = {"requests": 0, "ok": 0, "events_ok": 0, "s403": 0, "s429": 0, "s4xx": 0,
             "s5xx": 0, "exceptions": 0, "lat": []}
    prefix = f"L{zlib.crc32(run_id.encode()) & 0xffffff:06x}-"
    deadline = time.time() + duration

    def worker(wid):
        client = ApiClient(base_url, api_key)
        n, lat, local = 0, [], {k: 0 for k in stats if k != "lat"}
        while time.time() < deadline:
            size = 1 if mode == "single" else batch_size
            evs = []
            for _ in range(size):
                base = pool[(wid * 7919 + n) % len(pool)]
                evs.append(dict(base, submission_id=f"{prefix}{wid}-{n}", run_id=run_id,
                                sent_ms=int(time.time() * 1000)))
                n += 1
            payload = json.dumps(evs[0] if mode == "single" else evs).encode()
            path = "/submissions" if mode == "single" else "/submissions/batch"
            local["requests"] += 1
            try:
                status, body, ms = client.post(path, payload)
            except Exception:
                local["exceptions"] += 1
                continue
            if status == 202:
                local["ok"] += 1
                lat.append(ms)
                if mode == "single":
                    local["events_ok"] += 1
                else:
                    local["events_ok"] += json.loads(body).get("accepted", 0)
            elif status == 403:
                local["s403"] += 1
                break                                   # sin API key no tiene caso seguir
            elif status == 429:
                local["s429"] += 1
            elif status >= 500:
                local["s5xx"] += 1
            else:
                local["s4xx"] += 1
        client.close()
        with lock:
            for k, v in local.items():
                stats[k] += v
            stats["lat"] += lat

    t0 = time.time()
    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.time() - t0
    lat = stats.pop("lat")
    errors = stats["requests"] - stats["ok"]
    return {
        "run_id": run_id, "clients": clients, "mode": mode,
        "batch_size": 1 if mode == "single" else batch_size, "duration_s": round(elapsed, 1),
        **stats,
        "rps": round(stats["requests"] / elapsed, 1),
        "events_per_s": round(stats["events_ok"] / elapsed, 1),
        "error_rate_pct": round(100 * errors / stats["requests"], 2) if stats["requests"] else None,
        "lat_p50_ms": pct(lat, 50), "lat_p95_ms": pct(lat, 95),
        "lat_p99_ms": pct(lat, 99), "lat_max_ms": round(max(lat), 1) if lat else None,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--steps", default="10,25,50,100", help="clientes simultáneos por escalón")
    ap.add_argument("--duration", type=int, default=60, help="segundos por escalón")
    ap.add_argument("--cooldown", type=int, default=10, help="pausa entre escalones (s)")
    ap.add_argument("--mode", choices=["single", "batch"], default="single")
    ap.add_argument("--batch-size", type=int, default=25)
    ap.add_argument("--pool", type=int, default=5000, help="envíos pregenerados que se reutilizan")
    ap.add_argument("--warmup", type=int, default=10, help="segundos de calentamiento (no se miden)")
    ap.add_argument("--run-id", default=time.strftime("load-%Y%m%d-%H%M%S"))
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--url", help="URL base de la API (default: terraform output api_url)")
    ap.add_argument("--api-key", help="API key (default: terraform output api_key)")
    args = ap.parse_args()
    steps = [int(x) for x in args.steps.split(",")]

    print(f"Generando {args.pool} envíos con huellas...")
    pool = build_pool(args.pool, args.seed)

    if args.warmup:
        print(f"Calentando {args.warmup} s con 5 clientes (arranque en frío de la Lambda)...")
        run_step(5, args.warmup, pool, f"{args.run_id}-warmup", args.mode, args.batch_size,
                 args.url, args.api_key)

    results = []
    for i, clients in enumerate(steps):
        print(f"Escalón {i + 1}/{len(steps)}: {clients} clientes durante {args.duration} s...", flush=True)
        r = run_step(clients, args.duration, pool, f"{args.run_id}-c{clients}", args.mode,
                     args.batch_size, args.url, args.api_key)
        results.append(r)
        print(f"  {r['rps']} pet/s  {r['events_per_s']} envíos/s  p95={r['lat_p95_ms']} ms  "
              f"errores={r['error_rate_pct']}%  (429={r['s429']}, 5xx={r['s5xx']})", flush=True)
        if r["s403"]:
            raise SystemExit("HTTP 403: falta la API key o no es válida (terraform output -raw api_key)")
        if i < len(steps) - 1:
            time.sleep(args.cooldown)

    print()
    print_table([[r["clients"], r["rps"], r["events_per_s"], r["lat_p50_ms"], r["lat_p95_ms"],
                  r["lat_p99_ms"], r["error_rate_pct"], r["s429"], r["s5xx"]] for r in results],
                ["clientes", "pet/s", "envíos/s", "p50 ms", "p95 ms", "p99 ms", "% error", "429", "5xx"])

    base = results[0]
    worst = max(results, key=lambda r: r["lat_p95_ms"] or 0)
    if base["lat_p95_ms"]:
        growth = round(100 * (worst["lat_p95_ms"] / base["lat_p95_ms"] - 1), 1)
        print(f"\np95: {base['lat_p95_ms']} ms con {base['clients']} clientes -> peor "
              f"{worst['lat_p95_ms']} ms con {worst['clients']} clientes ({growth:+}%). "
              f"Tasa de error máxima: {max(r['error_rate_pct'] or 0 for r in results)}%")

    RESULTS_DIR.mkdir(exist_ok=True)
    out_json = RESULTS_DIR / f"{args.run_id}.json"
    out_csv = RESULTS_DIR / f"{args.run_id}.csv"
    out_json.write_text(json.dumps({"args": vars(args), "steps": results}, indent=2))
    with open(out_csv, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)
    print(f"\nGuardado en {out_json} y {out_csv}")
    print("Latencia del pipeline por escalón (esperar 2-3 min):")
    for r in results:
        print(f"  python metrics.py --run-id {r['run_id']} --minutes {max(5, args.duration // 60 + 3)}")


if __name__ == "__main__":
    main()
