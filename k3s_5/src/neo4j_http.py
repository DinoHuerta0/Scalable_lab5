"""Cliente mínimo de la API HTTP transaccional de Neo4j 5 (sin dependencias).

POST /db/neo4j/tx/commit ejecuta todas las sentencias en UNA transacción.
Los errores transitorios (deadlocks, bloqueos) se reintentan con backoff.
"""
import base64
import json
import random
import time
import urllib.error
import urllib.request


class Neo4jError(Exception):
    def __init__(self, errors):
        self.errors = errors
        super().__init__("; ".join(f"{e.get('code')}: {e.get('message')}" for e in errors))

    @property
    def transient(self):
        return all("TransientError" in e.get("code", "") for e in self.errors)


class Neo4j:
    def __init__(self, url, user, password, database="neo4j", timeout=50):
        self.endpoint = f"{url.rstrip('/')}/db/{database}/tx/commit"
        token = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=UTF-8",
        }
        self.timeout = timeout
        self.retries = 0  # reintentos acumulados (para métricas)

    def _post(self, statements):
        body = json.dumps({"statements": [
            {"statement": q, "parameters": p or {}} for q, p in statements
        ]}).encode()
        req = urllib.request.Request(self.endpoint, data=body, headers=self.headers, method="POST")
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            payload = json.loads(resp.read())
        if payload.get("errors"):
            raise Neo4jError(payload["errors"])
        return payload["results"]

    def run(self, statements, max_attempts=6):
        """statements: lista de (cypher, params). Devuelve una lista de filas por sentencia."""
        for attempt in range(1, max_attempts + 1):
            try:
                results = self._post(statements)
                return [[row["row"] for row in r["data"]] for r in results]
            except Neo4jError as e:
                if not e.transient or attempt == max_attempts:
                    raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                if attempt == max_attempts:
                    raise
            self.retries += 1
            time.sleep(min(2.0, 0.05 * 2 ** attempt) * (0.5 + random.random()))

    def query(self, cypher, **params):
        return self.run([(cypher, params)])[0]
