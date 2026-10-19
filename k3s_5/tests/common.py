"""Utilidades compartidas por los scripts de tests/.

Lee los outputs de Terraform (terraform -chdir=../infra output -json) y crea los
clientes de Neo4j (mismo cliente HTTP que la procesadora), de la API (Kong con API
key) y de Prometheus (consultas PromQL para metrics.py). Solo usa la biblioteca
estándar de Python: no hace falta instalar nada.
"""
import functools
import http.client
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RESULTS_DIR = HERE / "results"

sys.path.insert(0, str(ROOT / "src"))
from neo4j_http import Neo4j  # noqa: E402
from fingerprint import _KEYWORDS  # noqa: E402,F401  (tests/fingerprint.py, lo usa el producer)

# Kong responde 401 sin API key o con una inválida (API Gateway respondía 403)
AUTH_ERRORS = (401, 403)


@functools.lru_cache(maxsize=1)
def tf_outputs():
    out = subprocess.check_output(
        ["terraform", f"-chdir={ROOT / 'infra'}", "output", "-json"], text=True
    )
    return {k: v["value"] for k, v in json.loads(out).items()}


def neo4j():
    o = tf_outputs()
    return Neo4j(o["neo4j_url"], "neo4j", o["neo4j_password"])


def prom(expr, at=None, timeout=15):
    """Consulta instantánea a Prometheus. Devuelve [(etiquetas, valor), ...]."""
    params = {"query": expr}
    if at is not None:
        params["time"] = f"{at:.3f}"
    url = f"{tf_outputs()['prometheus_url']}/api/v1/query?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        payload = json.loads(resp.read())
    if payload.get("status") != "success":
        raise RuntimeError(payload.get("error", "consulta fallida"))
    return [(r["metric"], float(r["value"][1])) for r in payload["data"]["result"]]


def prom_value(expr, at=None, digits=1):
    """Primer valor de una consulta, redondeado; None si no hay datos (o es NaN)."""
    rows = prom(expr, at)
    if not rows or rows[0][1] != rows[0][1]:  # NaN != NaN
        return None
    return round(rows[0][1], digits)


class ApiClient:
    """Cliente HTTP de la API del pipeline con conexión persistente (keep-alive).

    Cada hilo de load_test.py usa su propio ApiClient: reutilizar la conexión evita
    pagar el handshake en cada petición y mide la latencia de la API, no la del
    cliente. Manda la API key en el encabezado x-api-key.
    """

    def __init__(self, base_url=None, api_key=None, timeout=30):
        if base_url is None or api_key is None:
            o = tf_outputs()
            base_url = base_url or o["api_url"]
            api_key = api_key if api_key is not None else o["api_key"]
        u = urllib.parse.urlsplit(base_url)
        self.scheme, self.host, self.prefix = u.scheme, u.netloc, u.path.rstrip("/")
        self.headers = {"Content-Type": "application/json", "x-api-key": api_key}
        self.timeout = timeout
        self.conn = None

    def _connect(self):
        cls = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        self.conn = cls(self.host, timeout=self.timeout)

    def post(self, path, payload):
        """Devuelve (status, body, latencia_ms). Reconecta una vez si se cayó la conexión."""
        for attempt in (1, 2):
            if self.conn is None:
                self._connect()
            t0 = time.perf_counter()
            try:
                self.conn.request("POST", self.prefix + path, body=payload, headers=self.headers)
                r = self.conn.getresponse()
                body = r.read()
                return r.status, body, (time.perf_counter() - t0) * 1000
            except (http.client.HTTPException, OSError):
                self.conn.close()
                self.conn = None
                if attempt == 2:
                    raise

    def close(self):
        if self.conn:
            self.conn.close()


def print_table(rows, headers):
    """Imprime una tabla simple en consola."""
    rows = [["" if v is None else str(v) for v in r] for r in rows]
    widths = [max([len(h)] + [len(r[i]) for r in rows]) for i, h in enumerate(headers)]
    line = "  ".join(h.ljust(w) for h, w in zip(headers, widths))
    print(line)
    print("-" * len(line))
    for r in rows:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths)))
    if not rows:
        print("(sin resultados)")
