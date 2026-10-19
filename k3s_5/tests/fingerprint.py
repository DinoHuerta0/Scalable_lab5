"""Huellas de código para detectar plagio (winnowing, la idea de MOSS).

Lo ejecuta el JUEZ (en el lab, producer.py) antes de publicar el evento, no la
Lambda: el juez ya tiene el código y la CPU, y así el evento lleva solo las huellas.

1. Quita comentarios y tokeniza el código.
2. Normaliza: identificadores -> V, números -> N, cadenas -> S. Así, renombrar
   variables o cambiar constantes no cambia la huella.
3. Calcula el hash de cada k-grama de tokens (k = 12 por defecto: tras normalizar
   quedan pocos símbolos distintos, y con k-gramas cortos casi todas las huellas
   se repiten entre programas distintos y el filtro de boilerplate las descarta).
4. Winnowing: en cada ventana de w hashes se queda con el mínimo. Garantiza que
   cualquier coincidencia de al menos w + k - 1 tokens comparta al menos una huella.
"""
import hashlib
import re

# Comentarios estilo C (C, C++, Java, JS...). "#" quita también #include (boilerplate)
_COMMENTS_C = re.compile(r"/\*.*?\*/|//[^\n]*|#[^\n]*", re.DOTALL)
# Comentarios y docstrings de Python ("//" en Python es división entera, no comentario)
_COMMENTS_PY = re.compile(r'#[^\n]*|"""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\'')

_TOKENS = re.compile(
    r'"(?:\\.|[^"\\\n])*"'           # cadena "..."
    r"|'(?:\\.|[^'\\\n])*'"          # cadena '...'
    r"|[A-Za-z_]\w*"                 # identificador o palabra reservada
    r"|\d+(?:\.\d+)?"                # número
    r"|==|!=|<=|>=|&&|\|\||<<|>>|\+\+|--|->|\+=|-=|\*=|/=|//|\*\*"
    r"|[^\s\w]"                      # cualquier otro símbolo
)

# Palabras reservadas comunes (Python, C, C++, Java, JS). Se conservan tal cual
# porque describen la estructura del programa.
_KEYWORDS = frozenset("""
if else elif for while do break continue return def class lambda yield import from as
try except finally raise with pass in not and or is None True False global nonlocal
int long short float double char bool boolean void unsigned signed const static struct
switch case default new delete public private protected this using namespace template
typename auto sizeof true false null nullptr catch throw throws final extends implements
interface package string String var let function print println printf scanf cin cout
input range len map list dict set
""".split())


def normalize(code: str, language: str = "") -> list[str]:
    is_py = language.lower().startswith("python")
    code = (_COMMENTS_PY if is_py else _COMMENTS_C).sub(" ", code)
    out = []
    for tok in _TOKENS.findall(code):
        c = tok[0]
        if c in "\"'":
            out.append("S")
        elif c.isdigit():
            out.append("N")
        elif c.isalpha() or c == "_":
            out.append(tok if tok in _KEYWORDS else "V")
        else:
            out.append(tok)
    return out


def _h(s: str) -> str:
    # 48 bits en hex: suficiente para evitar colisiones y cabe en cualquier tipo
    return hashlib.blake2b(s.encode(), digest_size=6).hexdigest()


def fingerprints(code: str, language: str = "", k: int = 12, w: int = 4,
                 max_fp: int = 400) -> list[str]:
    tokens = normalize(code, language)
    if len(tokens) < k:
        return []
    hashes = [_h(" ".join(tokens[i:i + k])) for i in range(len(tokens) - k + 1)]
    if len(hashes) <= w:
        return sorted(set(hashes))[:max_fp]

    selected = set()
    for i in range(len(hashes) - w + 1):
        selected.add(min(hashes[i:i + w]))
    # Si hay demasiadas, se toman las de hash menor: muestreo consistente entre envíos
    return sorted(selected)[:max_fp]
