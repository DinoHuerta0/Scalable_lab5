"""Generador sintético de envíos de un juez en línea (lo usan producer.py y load_test.py).

Simula sesiones reales: un usuario elige un problema y manda de 1 a 5 intentos
(por ejemplo CE -> WA -> AC). Cada usuario escribe su propio programa, generado
con una gramática (asignaciones, if, for, while, expresiones anidadas), así que
dos soluciones honestas del mismo problema comparten muy pocas huellas. Entre
intentos del mismo usuario el código solo cambia un poco.

Además puede inyectar:
  - copias plagiadas: el código Accepted de otro usuario con variables renombradas,
    líneas en blanco y líneas basura (verdad conocida para medir el detector);
  - registros corruptos (JSON roto) para probar reintentos y DLQ.
"""
import random
import re
import time
import uuid

from fingerprint import _KEYWORDS

LANGUAGES = ["Python"] * 6 + ["C++"] * 3 + ["Java"]
FAIL_VERDICTS = (["Wrong Answer"] * 45 + ["Compile Error"] * 20 + ["Runtime Error"] * 15
                 + ["Time Limit Exceeded"] * 15 + ["Memory Limit Exceeded"] * 5)
ATTEMPTS = [1] * 40 + [2] * 25 + [3] * 18 + [4] * 10 + [5] * 7

OPS = ["+", "-", "*", "%", "<<", ">>", "&", "|", "^"]
CMPS = ["<", ">", "<=", ">=", "==", "!="]
FUNCS = ["abs", "max", "min", "len", "sum", "sorted"]
NAMES = ["n", "m", "k", "a", "b", "c", "x", "y", "s", "t", "res", "ans", "cnt", "acc",
         "arr", "nums", "dp", "memo", "tot", "best", "cur", "val", "lo", "hi", "mid"]


class _Gen:
    """Programa aleatorio en estilo Python o C (C++/Java)."""

    def __init__(self, rng, style):
        self.r, self.style = rng, style
        self.vars = rng.sample(NAMES, rng.randint(4, 8))
        self.ops = OPS + (["//"] if style == "py" else ["/"])

    def expr(self, d):
        r = self.r
        p = r.random()
        if d <= 0 or p < 0.25:
            q = r.random()
            if q < 0.55:
                return r.choice(self.vars)
            if q < 0.8:
                return str(r.randint(0, 1000))
            return f"{r.choice(self.vars)}[{self.expr(0)}]"
        if p < 0.7:
            return f"{self.expr(d - 1)} {r.choice(self.ops)} {self.expr(d - 1)}"
        if p < 0.85:
            args = ", ".join(self.expr(d - 1) for _ in range(r.randint(1, 2)))
            return f"{r.choice(FUNCS)}({args})"
        return f"({self.expr(d - 1)})"

    def cond(self):
        c = f"{self.expr(1)} {self.r.choice(CMPS)} {self.expr(1)}"
        if self.r.random() < 0.25:
            joiner = " and " if self.style == "py" else " && "
            c += joiner + f"{self.expr(1)} {self.r.choice(CMPS)} {self.expr(1)}"
        return c

    def stmt(self, depth, ind):
        r, py = self.r, self.style == "py"
        sp = "    " * ind
        end = "" if py else ";"
        v = r.choice(self.vars)
        kind = r.random() if depth < 2 else r.random() * 0.55
        if kind < 0.3:
            return [f"{sp}{v} = {self.expr(2)}{end}"]
        if kind < 0.45:
            return [f"{sp}{v} {r.choice(self.ops)}= {self.expr(1)}{end}"]
        if kind < 0.55:
            return [f"{sp}print({self.expr(1)})" if py else f"{sp}cout << {self.expr(1)} << endl;"]
        if kind < 0.7:
            head = f"if {self.cond()}:" if py else f"if ({self.cond()}) {{"
            body = self.block(depth + 1, ind + 1)
            return [sp + head] + body + ([] if py else [sp + "}"])
        if kind < 0.85:
            i = r.choice(["i", "j", "idx", "p", "q"])
            head = (f"for {i} in range({self.expr(1)}):" if py
                    else f"for (int {i} = 0; {i} < {self.expr(1)}; {i}++) {{")
            body = self.block(depth + 1, ind + 1)
            return [sp + head] + body + ([] if py else [sp + "}"])
        head = f"while {self.cond()}:" if py else f"while ({self.cond()}) {{"
        body = self.block(depth + 1, ind + 1) + [f"{sp}    {v} -= 1{end}"]
        return [sp + head] + body + ([] if py else [sp + "}"])

    def block(self, depth, ind):
        out = []
        for _ in range(self.r.randint(1, 3)):
            out += self.stmt(depth, ind)
        return out

    def program(self):
        py = self.style == "py"
        a, b = self.vars[0], self.vars[1]
        if py:
            head = [f"{a} = int(input())", f"{b} = list(map(int, input().split()))"]
        else:
            head = ["#include <bits/stdc++.h>", "using namespace std;", "int main() {",
                    f"    long long {a}; cin >> {a};"]
        ind = 0 if py else 1
        body = []
        for _ in range(self.r.randint(8, 22)):
            body += self.stmt(0, ind)
        tail = [f"print({self.vars[-1]})"] if py else [f"    cout << {self.vars[-1]} << endl;", "}"]
        return "\n".join(head + body + tail)


def _style(language):
    return "py" if language.lower().startswith("python") else "c"


def mutate(code, rng):
    """Siguiente intento del mismo usuario: cambia unos números y a veces una línea."""
    lines = code.split("\n")
    for _ in range(rng.randint(1, 3)):
        i = rng.randrange(len(lines))
        lines[i] = re.sub(r"\b\d+\b", lambda m: str(rng.randint(0, 1000)), lines[i], count=1)
    if rng.random() < 0.3 and len(lines) > 6:
        del lines[rng.randrange(2, len(lines) - 1)]
    return "\n".join(lines)


def synthetic(n, users=2000, problems=50, prefix="syn", seed=None):
    """Genera n envíos agrupados en sesiones usuario-problema."""
    rng = random.Random(seed)
    t = int(time.time()) - n * 60
    k = 0
    while k < n:
        user = f"u{rng.randint(1, users):06d}"
        problem = f"p{rng.randint(1, problems):05d}"
        language = rng.choice(LANGUAGES)
        code = _Gen(rng, _style(language)).program()
        attempts = rng.choice(ATTEMPTS)
        solved = rng.random() < 0.75
        for a in range(attempts):
            if k >= n:
                return
            last = a == attempts - 1
            verdict = "Accepted" if (last and solved) else rng.choice(FAIL_VERDICTS)
            if a:
                code = mutate(code, rng)
            t += rng.randint(30, 900)
            yield {
                "submission_id": f"{prefix}{k:09d}",
                "user_id": user,
                "problem_id": problem,
                "language": language,
                "verdict": verdict,
                "cpu_ms": rng.randint(5, 2000) if verdict != "Compile Error" else None,
                "memory_kb": rng.randint(3000, 65000) if verdict != "Compile Error" else None,
                "code_size": len(code),
                "date": t,
                "code": code,
            }
            k += 1


# ---------------------------------------------------------------- inyección

_IDENT = re.compile(r"\b[A-Za-z_]\w*\b")


def obfuscate(code, language, rng=random):
    """Copia "disfrazada": renombra variables, mete líneas en blanco y líneas basura."""
    mapping = {}

    def rename(m):
        w = m.group(0)
        if w in _KEYWORDS:
            return w
        return mapping.setdefault(w, "v_" + uuid.uuid4().hex[:6])

    lines = _IDENT.sub(rename, code).split("\n")
    end = "" if _style(language) == "py" else ";"
    for _ in range(rng.randint(1, 4)):
        lines.insert(rng.randint(1, len(lines) - 1), "")
    for _ in range(rng.randint(1, 3)):
        sp = "" if end == "" else "    "
        lines.insert(rng.randint(3, len(lines) - 1), f"{sp}v_{uuid.uuid4().hex[:4]} = {rng.randint(0, 99)}{end}")
    return "\n".join(lines)


def with_injections(events, plagio=0.0, corrupt=0.0, rng=random):
    """Intercala copias plagiadas y registros corruptos en el flujo de eventos."""
    pool, users = [], []
    for ev in events:
        yield ev
        if len(users) < 50_000:
            users.append(ev["user_id"])
        if ev.get("verdict") == "Accepted" and ev.get("code"):
            if len(pool) < 5000:
                pool.append(ev)
            else:
                pool[rng.randrange(5000)] = ev
        if plagio and pool and rng.random() < plagio:
            src = rng.choice(pool)
            thief = rng.choice(users)
            if thief != src["user_id"]:
                copy = dict(src)
                copy.update(
                    submission_id="sP" + uuid.uuid4().hex[:10],
                    user_id=thief,
                    code=obfuscate(src["code"], src["language"], rng),
                    date=int(src["date"]) + rng.randint(60, 86400),
                    injected_from=src["submission_id"],
                )
                copy["code_size"] = len(copy["code"])
                yield copy
        if corrupt and rng.random() < corrupt:
            yield {"__corrupt__": True, "user_id": ev["user_id"]}
