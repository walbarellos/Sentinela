"""
haEmet — Normalização de dados brutos do TSE
"""
import re
import unicodedata
import hashlib
from typing import Optional


def normalize_cpf(raw: str) -> Optional[str]:
    """
    Retorna CPF como 11 dígitos ou None se inválido.
    CPF mascarado do TSE (***123456**) → retorna 'parcial:123456'
    para permitir linkagem cruzada posterior.
    """
    if not raw:
        return None

    # Extrai dígitos e asteriscos separados
    digits = re.sub(r"[^\d]", "", raw)
    stars = raw.count("*")

    # Mascarado
    if stars > 0:
        if len(digits) == 6:
            return f"parcial:{digits}"
        return None

    if len(digits) != 11:
        return None

    # Rejeita sequências triviais
    if len(set(digits)) == 1:
        return None

    # Valida dígitos verificadores
    def calc_dv(d: str, n: int) -> int:
        total = sum(int(d[i]) * (n + 1 - i) for i in range(n))
        rem = (total * 10) % 11
        return 0 if rem == 10 else rem

    if calc_dv(digits, 9) != int(digits[9]):
        return None
    if calc_dv(digits, 10) != int(digits[10]):
        return None

    return digits


def normalize_name(raw: str) -> str:
    """Uppercase + remove acentos + squash espaços."""
    if not raw:
        return ""
    # Remove acentos via NFD decomposition
    nfd = unicodedata.normalize("NFD", raw)
    ascii_str = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    upper = ascii_str.upper()
    # Mantém só alfanumérico, espaço e hífen
    clean = re.sub(r"[^A-Z0-9 \-]", "", upper)
    return re.sub(r" +", " ", clean).strip()


def normalize_currency(raw: str) -> float:
    """Converte '1.234.567,89' → 1234567.89"""
    if not raw or raw.strip() in ("#NULO#", "", "-"):
        return 0.0
    try:
        return float(raw.replace(".", "").replace(",", "."))
    except ValueError:
        return 0.0


def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def jaro_winkler(a: str, b: str) -> float:
    """Similaridade de nomes para entity resolution."""
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0

    match_dist = max(len(a), len(b)) // 2 - 1

    a_matched = [False] * len(a)
    b_matched = [False] * len(b)
    matches = 0
    transpositions = 0

    for i in range(len(a)):
        start = max(0, i - match_dist)
        end = min(len(b) - 1, i + match_dist)
        for j in range(start, end + 1):
            if b_matched[j] or a[i] != b[j]:
                continue
            a_matched[i] = b_matched[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len(a)):
        if not a_matched[i]:
            continue
        while not b_matched[k]:
            k += 1
        if a[i] != b[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len(a) + matches / len(b) +
            (matches - transpositions / 2) / matches) / 3

    prefix = 0
    for i in range(min(len(a), len(b), 4)):
        if a[i] == b[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * 0.1 * (1 - jaro)
