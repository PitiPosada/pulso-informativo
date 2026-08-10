"""
Base de datos central del pipeline Pulso Informativo.
Todos los módulos (monitor, clasificador/generador, publicador) leen y
escriben sobre esta misma tabla usando el campo `estado` como máquina
de estados:

    nueva -> aprobada -> generada -> publicada
                                   -> programada -> publicada
"""

import sqlite3
from contextlib import contextmanager

DB_PATH = "pulso_informativo.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS noticias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    titulo TEXT NOT NULL,
    link TEXT NOT NULL UNIQUE,
    fuente TEXT NOT NULL,
    resumen TEXT,
    imagen_url TEXT,
    fecha_publicacion TEXT,
    fecha_detectada TEXT NOT NULL DEFAULT (datetime('now')),

    -- dedup
    hash_titulo TEXT NOT NULL,          -- hash normalizado del título (exacto)
    titulo_normalizado TEXT NOT NULL,   -- para comparación difusa (fuzzy)

    -- clasificación / generación
    categoria TEXT,                      -- ultima_hora | viral_mundial | comun_matutina
    plantilla_usada TEXT,
    imagen_generada_path TEXT,
    descripcion_humanizada TEXT,
    hashtags TEXT,

    -- flujo de aprobación / publicación
    estado TEXT NOT NULL DEFAULT 'nueva',  -- nueva|descartada|aprobada|generada|programada|publicada
    fecha_programada TEXT,
    fecha_publicada TEXT,
    telegram_msg_id INTEGER,

    -- para el batching del monitor (evita re-notificar lo ya avisado)
    notificada INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_hash_titulo ON noticias(hash_titulo);
CREATE INDEX IF NOT EXISTS idx_estado ON noticias(estado);
CREATE INDEX IF NOT EXISTS idx_notificada ON noticias(notificada);

CREATE TABLE IF NOT EXISTS meta (
    clave TEXT PRIMARY KEY,
    valor TEXT
);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)


def get_meta(clave: str, default=None):
    with get_conn() as conn:
        fila = conn.execute("SELECT valor FROM meta WHERE clave = ?", (clave,)).fetchone()
    return fila["valor"] if fila else default


def set_meta(clave: str, valor):
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO meta (clave, valor) VALUES (?, ?) "
            "ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor",
            (clave, str(valor)),
        )


if __name__ == "__main__":
    init_db()
    print(f"Base de datos lista en {DB_PATH}")
