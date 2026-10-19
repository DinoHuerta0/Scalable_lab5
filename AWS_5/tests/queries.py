"""Consultas de grafo sobre los envíos del juez (lo que justifica usar Neo4j).

  python queries.py                    # todas
  python queries.py --only plagio --threshold 0.7
  python queries.py --user u123456789  # recomendación para un usuario concreto

Cada consulta reporta su tiempo de ejecución (útil para el reporte).
"""
import argparse
import json
import time

from common import RESULTS_DIR, neo4j, print_table

Q = {}

Q["compile_errors"] = ("Problemas con más Compile Error, por lenguaje", """
MATCH (:Verdict {name: 'Compile Error'})<-[:GOT]-(s:Submission)-[:FOR]->(p:Problem),
      (s)-[:IN]->(l:Language)
RETURN p.id, l.name, count(*) AS ce ORDER BY ce DESC LIMIT 10
""", ["problema", "lenguaje", "compile_errors"])

Q["lenguajes"] = ("Veredictos por lenguaje", """
MATCH (l:Language)<-[:IN]-(s:Submission)
WITH l, count(s) AS total,
     sum(CASE WHEN s.verdict = 'Accepted' THEN 1 ELSE 0 END) AS ac,
     sum(CASE WHEN s.verdict = 'Compile Error' THEN 1 ELSE 0 END) AS ce
WHERE total >= 50
RETURN l.name, total, round(100.0 * ac / total, 1), round(100.0 * ce / total, 1)
ORDER BY total DESC LIMIT 15
""", ["lenguaje", "envíos", "%AC", "%CE"])

Q["intentos"] = ("Problemas más difíciles: intentos promedio hasta el primer Accepted", """
MATCH (u:User)-[:SOLVED]->(p:Problem)
MATCH (u)-[:SUBMITTED]->(s:Submission)-[:FOR]->(p)
WITH u, p, collect(s) AS subs,
     min(CASE WHEN s.verdict = 'Accepted' THEN s.date END) AS first_ac
WITH p, u, size([x IN subs WHERE x.date <= first_ac]) AS attempts
WITH p, count(u) AS solvers, avg(attempts) AS avg_attempts
WHERE solvers >= 5
RETURN p.id, solvers, round(avg_attempts, 2) ORDER BY avg_attempts DESC LIMIT 10
""", ["problema", "usuarios que lo resolvieron", "intentos promedio"])

Q["trayectorias"] = ("Trayectorias usuario-problema (veredictos en orden)", """
MATCH (u:User)-[:SOLVED]->(p:Problem)
MATCH (u)-[:SUBMITTED]->(s:Submission)-[:FOR]->(p)
WITH u, p, s ORDER BY s.date
WITH u, p, collect(s.verdict) AS path
WHERE size(path) >= 4
RETURN u.id, p.id, reduce(t = '', v IN path | t + CASE WHEN t = '' THEN '' ELSE ' -> ' END +
  CASE v WHEN 'Accepted' THEN 'AC' WHEN 'Wrong Answer' THEN 'WA' WHEN 'Compile Error' THEN 'CE'
         WHEN 'Runtime Error' THEN 'RE' WHEN 'Time Limit Exceeded' THEN 'TLE'
         WHEN 'Memory Limit Exceeded' THEN 'MLE' ELSE v END)
LIMIT 8
""", ["usuario", "problema", "trayectoria"])

Q["recomendacion"] = ("Recomendación: problemas que resolvieron usuarios parecidos", """
OPTIONAL MATCH (given:User {id: $user})
CALL {
  MATCH (u:User)-[r:SOLVED]->()
  WITH u, count(r) AS solved ORDER BY solved DESC LIMIT 1
  RETURN u AS top
}
WITH coalesce(given, top) AS me
MATCH (me)-[:SOLVED]->(:Problem)<-[:SOLVED]-(other:User)-[:SOLVED]->(rec:Problem)
WHERE other <> me AND NOT (me)-[:SOLVED]->(rec)
RETURN me.id, rec.id, count(DISTINCT other) AS score ORDER BY score DESC LIMIT 5
""", ["usuario", "problema recomendado", "usuarios similares que lo resolvieron"])

Q["velocidad"] = ("Tiempo de CPU mediano de soluciones Accepted por problema y lenguaje", """
MATCH (p:Problem)<-[:FOR]-(s:Submission)-[:GOT]->(:Verdict {name: 'Accepted'}),
      (s)-[:IN]->(l:Language)
WHERE s.cpu_ms >= 0
WITH p, l, count(s) AS n, percentileCont(s.cpu_ms, 0.5) AS med
WHERE n >= 10
RETURN p.id, l.name, n, med ORDER BY p.id, med LIMIT 30
""", ["problema", "lenguaje", "n", "cpu_ms mediana"])

# Dos envíos del mismo problema, de usuarios distintos, que comparten huellas poco
# comunes (df <= max_df descarta el boilerplate que todos escriben).
# sim = huellas compartidas / huellas del envío más chico (containment).
PLAGIO = """
MATCH (f:Fingerprint)
WITH f, COUNT { (f)<-[:HAS]-() } AS df
WHERE df >= 2 AND df <= $max_df
MATCH (a:Submission)-[:HAS]->(f)<-[:HAS]-(b:Submission)
WHERE a.id < b.id AND a.problem_id = b.problem_id AND a.user_id <> b.user_id
  AND a.n_fp >= $min_fp AND b.n_fp >= $min_fp
WITH a, b, count(f) AS shared
WITH a, b, shared, toFloat(shared) / CASE WHEN a.n_fp < b.n_fp THEN a.n_fp ELSE b.n_fp END AS sim
WHERE sim >= $threshold
RETURN a.id, a.user_id, b.id, b.user_id, a.problem_id, shared, round(sim, 3) AS sim,
       coalesce(a.injected_from, '') + coalesce(b.injected_from, '') AS injected
ORDER BY sim DESC
"""

INJECTED = """
MATCH (c:Submission) WHERE c.injected_from IS NOT NULL
MATCH (o:Submission {id: c.injected_from})
RETURN c.id, o.id, o.user_id
"""


def timed(db, cypher, **params):
    t0 = time.perf_counter()
    rows = db.query(cypher, **params)
    return rows, round((time.perf_counter() - t0) * 1000, 1)


def run_plagio(db, args, report):
    print("\n## Detección de plagio")
    rows, ms = timed(db, PLAGIO, max_df=args.max_df, min_fp=args.min_fp, threshold=args.threshold)
    print_table([r[:7] for r in rows[:20]],
                ["envío A", "usuario A", "envío B", "usuario B", "problema", "huellas", "sim"])
    print(f"{len(rows)} parejas sospechosas  ({ms} ms)")

    # Evaluación contra las copias inyectadas por el producer (verdad conocida)
    inj_rows = db.query(INJECTED)
    injected = {tuple(sorted((c, o))) for c, o, _ in inj_rows}
    author = {c: ou for c, _, ou in inj_rows}          # copia -> autor del código original
    found = {tuple(sorted((r[0], r[2]))) for r in rows}
    user_of = {sid: u for r in rows for sid, u in ((r[0], r[1]), (r[2], r[3]))}
    entry = {"pairs": len(rows), "ms": ms, "threshold": args.threshold, "max_df": args.max_df}
    if injected:
        tp = len(found & injected)
        # Las demás parejas: si las dos partes vienen del mismo código original (la copia
        # contra otro intento del autor, o dos copias del mismo original) también son
        # plagio; si no, es un falso positivo.
        related = false_pos = 0
        for pair in found - injected:
            roots = {author.get(sid, user_of[sid]) for sid in pair}
            if len(roots) == 1:
                related += 1
            else:
                false_pos += 1
        entry.update(injected=len(injected), detected_injected=tp,
                     recall=round(tp / len(injected), 3),
                     related_pairs=related, false_positives=false_pos)
        print(f"Copias inyectadas: {len(injected)}  detectadas: {tp}  recall={entry['recall']}")
        print(f"Otras parejas del mismo código original (también plagio): {related}  "
              f"falsos positivos: {false_pos}")
    report["plagio"] = entry


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="nombre de una consulta: " + ", ".join(list(Q) + ["plagio"]))
    ap.add_argument("--user", help="usuario para la recomendación (default: el que más ha resuelto)")
    ap.add_argument("--threshold", type=float, default=0.6, help="similitud mínima para plagio")
    ap.add_argument("--max-df", type=int, default=20, help="ignora huellas presentes en más envíos")
    ap.add_argument("--min-fp", type=int, default=15, help="ignora envíos con pocas huellas (código trivial)")
    args = ap.parse_args()

    db = neo4j()
    report = {}
    for name, (title, cypher, headers) in Q.items():
        if args.only and args.only != name:
            continue
        print(f"\n## {title}")
        rows, ms = timed(db, cypher, user=args.user)
        print_table(rows, headers)
        print(f"({ms} ms)")
        report[name] = {"ms": ms, "rows": rows}

    if not args.only or args.only == "plagio":
        run_plagio(db, args, report)

    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"queries_{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nGuardado en {out}")


if __name__ == "__main__":
    main()
