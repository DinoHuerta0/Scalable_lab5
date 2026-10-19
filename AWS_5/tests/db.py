"""Administración de Neo4j desde tu máquina.

  python db.py wait     # espera a que Neo4j responda (tras terraform apply)
  python db.py stats    # nodos y relaciones por tipo
  python db.py reset    # borra todo (entre experimentos)
"""
import sys
import time

from common import neo4j, print_table


def wait(db):
    for i in range(60):
        try:
            db.query("RETURN 1")
            print("Neo4j listo")
            return
        except Exception as e:
            print(f"  esperando Neo4j... ({type(e).__name__})")
            time.sleep(10)
    sys.exit("Neo4j no respondió")


def stats(db):
    nodes = db.query("MATCH (n) RETURN labels(n)[0] AS label, count(*) ORDER BY label")
    rels = db.query("MATCH ()-[r]->() RETURN type(r), count(*) ORDER BY type(r)")
    print_table(nodes, ["nodo", "cantidad"])
    print()
    print_table(rels, ["relación", "cantidad"])


def reset(db):
    # Borra por partes para no exceder la memoria de la transacción
    total = 0
    while True:
        n = db.query("MATCH (n) WITH n LIMIT 20000 DETACH DELETE n RETURN count(*)")[0][0]
        total += n
        if n == 0:
            break
        print(f"  borrados {total:,} nodos...")
    print(f"Listo: {total:,} nodos borrados (las constraints se conservan)")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "stats"
    {"wait": wait, "stats": stats, "reset": reset}[cmd](neo4j())
