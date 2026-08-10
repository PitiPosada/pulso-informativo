"""
Deduplicación de noticias.

Dos capas:
1. Exacta: hash del título normalizado + link. Atrapa la misma noticia
   re-scrapeada de la misma fuente.
2. Difusa: similitud de texto (SequenceMatcher) contra las noticias ya
   guardadas en las últimas N horas. Atrapa la MISMA noticia cubierta
   por distintos medios con titulares distintos (el caso más común
   entre fuentes nacionales e internacionales).
"""

import hashlib
import re
import unicodedata
from difflib import SequenceMatcher

UMBRAL_SIMILITUD = 0.82  # 0-1, qué tan parecidos deben ser los títulos para considerarse duplicados


def normalizar_titulo(titulo: str) -> str:
    """minúsculas, sin tildes, sin puntuación, espacios colapsados."""
    t = titulo.lower().strip()
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"[^\w\s]", "", t)
    t = re.sub(r"\s+", " ", t)
    return t.strip()


def hash_titulo(titulo: str) -> str:
    return hashlib.sha256(normalizar_titulo(titulo).encode()).hexdigest()


def es_duplicado(titulo_nuevo: str, titulos_normalizados_existentes: list[str]) -> bool:
    """
    Compara el título nuevo (ya normalizado) contra una lista de títulos
    normalizados ya guardados. True si alguno supera el umbral de similitud.
    """
    t_nuevo = normalizar_titulo(titulo_nuevo)
    for t_existente in titulos_normalizados_existentes:
        ratio = SequenceMatcher(None, t_nuevo, t_existente).ratio()
        if ratio >= UMBRAL_SIMILITUD:
            return True
    return False
