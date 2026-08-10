from database import get_conn

with get_conn() as conn:
    filas = conn.execute(
        "SELECT id, titulo, categoria, estado FROM noticias WHERE estado = 'aprobada'"
    ).fetchall()

if not filas:
    print("No hay noticias aprobadas todavía.")
else:
    for f in filas:
        print(f"[{f['id']}] ({f['categoria']}) {f['titulo']}")
