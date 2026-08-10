"""
Monitor de noticias.

- Recorre todas las fuentes en paralelo cada CICLO_SEGUNDOS.
- Guarda en la base de datos SOLO lo que no esté duplicado (hash exacto
  + similitud difusa contra lo detectado en las últimas 48h).
- Cada INTERVALO_NOTIFICACION_MIN minutos, junta todo lo nuevo desde el
  último aviso y manda UN mensaje por noticia a Telegram con botones
  inline (👍 aprobar / 👎 descartar), marcándolas como notificadas.

Requiere las variables de entorno:
    TELEGRAM_BOT_TOKEN
    TELEGRAM_CHAT_ID
"""

import os
import sys
import time
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import feedparser
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from database import get_conn, init_db, get_meta, set_meta
from dedup import hash_titulo, normalizar_titulo, es_duplicado
from fuentes import FUENTES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("monitor")

CICLO_SEGUNDOS = 5 * 60          # cada cuánto se revisan las fuentes (5 min)
INTERVALO_NOTIFICACION_MIN = 20  # cada cuánto se manda el resumen a Telegram (15-20 min)
VENTANA_DEDUP_HORAS = 48         # contra cuántas horas atrás se compara para el dedup difuso

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
CLAVE_OFFSET = "telegram_ultimo_update_id"


# ---------------------------------------------------------------- scraping

def fetch_fuente(fuente: dict) -> list[dict]:
    """Descarga y parsea una fuente RSS. Nunca lanza excepción hacia afuera."""
    items = []
    try:
        feed = feedparser.parse(fuente["url"])
        for entry in feed.entries:
            imagen, video = _extraer_media(entry)
            items.append({
                "titulo": entry.get("title", "").strip(),
                "link": entry.get("link", "").strip(),
                "resumen": entry.get("summary", "")[:500],
                "imagen_url": imagen,
                "video_url": video,
                "fecha_publicacion": entry.get("published", ""),
                "fuente": fuente["nombre"],
            })
    except Exception as e:
        log.warning(f"Fallo al leer {fuente['nombre']}: {e}")
    return items


def _extraer_media(entry) -> tuple[str | None, str | None]:
    """Devuelve (imagen_url, video_url) extrayendo ambos del RSS si existen."""
    imagen = None
    video = None

    for mc in entry.get("media_content", []):
        tipo = (mc.get("type") or "").lower()
        if "video" in tipo and not video:
            video = mc.get("url")
        elif "image" in tipo and not imagen:
            imagen = mc.get("url")

    for e in entry.get("enclosures", []):
        tipo = (e.get("type") or "").lower()
        if "video" in tipo and not video:
            video = e.get("href") or e.get("url")
        elif "image" in tipo and not imagen:
            imagen = e.get("href") or e.get("url")

    for l in entry.get("links", []):
        tipo = (l.get("type") or "").lower()
        if not video and "video" in tipo:
            video = l.get("href")
        elif not imagen and tipo.startswith("image/"):
            imagen = l.get("href")

    if not imagen and "media_thumbnail" in entry and entry.media_thumbnail:
        imagen = entry.media_thumbnail[0].get("url")

    return imagen or None, video or None


def recolectar_todas_las_fuentes() -> list[dict]:
    resultados = []
    with ThreadPoolExecutor(max_workers=min(10, len(FUENTES))) as ex:
        futuros = {ex.submit(fetch_fuente, f): f for f in FUENTES}
        for fut in as_completed(futuros):
            resultados.extend(fut.result())
    return resultados


# ---------------------------------------------------------------- dedup + guardado

def guardar_si_no_duplicada(item: dict) -> bool:
    """Devuelve True si se guardó como noticia nueva, False si era duplicada."""
    h = hash_titulo(item["titulo"])
    t_norm = normalizar_titulo(item["titulo"])

    with get_conn() as conn:
        # 1) dedup exacta (hash o mismo link)
        existe = conn.execute(
            "SELECT id FROM noticias WHERE hash_titulo = ? OR link = ?", (h, item["link"])
        ).fetchone()
        if existe:
            return False

        # 2) dedup difusa contra lo detectado en la ventana reciente
        desde = (datetime.now() - timedelta(hours=VENTANA_DEDUP_HORAS)).isoformat()
        recientes = conn.execute(
            "SELECT titulo_normalizado FROM noticias WHERE fecha_detectada >= ?", (desde,)
        ).fetchall()
        titulos_recientes = [r["titulo_normalizado"] for r in recientes]

        if es_duplicado(item["titulo"], titulos_recientes):
            return False

        conn.execute(
            """INSERT INTO noticias
               (titulo, link, fuente, resumen, imagen_url, fecha_publicacion,
                hash_titulo, titulo_normalizado)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (item["titulo"], item["link"], item["fuente"], item["resumen"],
             item["imagen_url"], item["fecha_publicacion"], h, t_norm),
        )
        return True


# ---------------------------------------------------------------- guardar multimedia aprobada

def descargar_media(url, destino: str) -> str | None:
    """Descarga un archivo (imagen/video) con headers de navegador reales.

    Muchos CDN (Infobae, por ejemplo) bloquean descargas directas por hotlink
    ("access denied"). Enviar un User-Agent + Referer normales lo evita.
    Devuelve la ruta relativa guardada, o None si falla.
    """
    if not url:
        return None
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0 Safari/537.36"),
            "Referer": "https://www.google.com/",
            "Accept": "*/*",
        })
        os.makedirs(os.path.dirname(destino), exist_ok=True)
        with urllib.request.urlopen(req, timeout=20) as resp, open(destino, "wb") as f:
            f.write(resp.read())
        log.info(f"Multimedia guardada: {destino}")
        return destino
    except Exception as e:
        log.warning(f"No se pudo descargar multimedia {url}: {e}")
        return None


def guardar_media_noticia(noticia_id: int) -> None:
    """Descarga imagen y/o video de una noticia aprobada a media/YYYY-MM-DD/."""
    with get_conn() as conn:
        n = conn.execute("SELECT * FROM noticias WHERE id = ?", (noticia_id,)).fetchone()
        if not n:
            return
        fecha = (n["fecha_aprobada"] or n["fecha_detectada"] or "")[:10] or "sin-fecha"
        carpeta = os.path.join("media", fecha)
        media_guardada = None
        media_tipo = None

        if n["imagen_url"]:
            destino = os.path.join(carpeta, f"{noticia_id}_img{_extension(n['imagen_url'], 'jpg')}")
            if descargar_media(n["imagen_url"], destino):
                media_guardada, media_tipo = destino, "imagen"
        if not media_guardada and n["video_url"]:
            destino = os.path.join(carpeta, f"{noticia_id}_vid{_extension(n['video_url'], 'mp4')}")
            if descargar_media(n["video_url"], destino):
                media_guardada, media_tipo = destino, "video"

        if media_guardada:
            conn.execute(
                "UPDATE noticias SET media_path = ?, media_tipo = ? WHERE id = ?",
                (media_guardada.replace("\\", "/"), media_tipo, noticia_id),
            )


def _extension(url: str | None, defecto: str) -> str:
    if not url:
        return f".{defecto}"
    camino = url.split("?")[0].rstrip("/")
    ext = camino.rsplit(".", 1)[-1]
    return f".{ext}" if len(ext) in (3, 4) and ext.isalnum() else f".{defecto}"


def generar_reporte_diario() -> None:
    """Escribe aprobadas/YYYY-MM-DD.md con las noticias aprobadas de cada día.

    Se ejecuta al final de cada ciclo del workflow, para que el archivo quede
    dentro del repo y puedas verlo en el navegador o descargarlo.
    """
    with get_conn() as conn:
        aprobadas = conn.execute(
            "SELECT * FROM noticias WHERE estado = 'aprobada' ORDER BY fecha_aprobada ASC"
        ).fetchall()

    por_dia: dict[str, list] = {}
    for n in aprobadas:
        dia = (n["fecha_aprobada"] or n["fecha_detectada"] or "")[:10] or "sin-fecha"
        por_dia.setdefault(dia, []).append(n)

    for dia, noticias in por_dia.items():
        lineas = [f"# Noticias aprobadas - {dia}", ""]
        for n in noticias:
            categoria = n["categoria"] or "sin categoría"
            media = f"[{n['media_tipo']}]({n['media_path']})" if n["media_path"] else "(sin multimedia)"
            extra = []
            if n["imagen_url"]:
                extra.append(f"Imagen URL: {n['imagen_url']}")
            if n["video_url"]:
                extra.append(f"Video URL: {n['video_url']}")
            lineas += [
                f"## [{n['titulo']}]({n['link']})",
                f"- Fuente: {n['fuente']}",
                f"- Categoría: {categoria}",
                f"- Media: {media}",
                *[f"- {e}" for e in extra],
                f"- Detectada: {n['fecha_detectada']}",
                "",
            ]
        ruta = os.path.join("aprobadas", f"{dia}.md")
        os.makedirs("aprobadas", exist_ok=True)
        with open(ruta, "w", encoding="utf-8") as f:
            f.write("\n".join(lineas))
        log.info(f"Reporte diario actualizado: {ruta}")


# ---------------------------------------------------------------- notificación Telegram

def escape_markdown(texto: str) -> str:
    """Escapa caracteres especiales de Markdown para que Telegram no rompa el mensaje."""
    if not texto:
        return texto
    import re as re_mod
    return re_mod.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", texto)


def limpiar_resumen(resumen: str | None, max_chars: int = 300) -> str:
    """Convierte el resumen RSS (HTML) en texto plano legible para Telegram."""
    if not resumen:
        return ""
    import html as html_mod
    import re as re_mod
    texto = re_mod.sub(r"<[^>]+>", " ", resumen)          # quita etiquetas HTML
    texto = html_mod.unescape(texto)                      # &aacute; -> á
    texto = re_mod.sub(r"\s+", " ", texto).strip()        # colapsa espacios
    if len(texto) > max_chars:
        texto = texto[:max_chars].rsplit(" ", 1)[0] + "…"
    return texto


async def notificar_pendientes():
    """Manda a Telegram todo lo detectado desde el último aviso (notificada=0)."""
    if not bot:
        log.warning("TELEGRAM_BOT_TOKEN no configurado, se omite notificación")
        return

    with get_conn() as conn:
        pendientes = conn.execute(
            "SELECT * FROM noticias WHERE notificada = 0 ORDER BY fecha_detectada ASC"
        ).fetchall()

        if not pendientes:
            log.info("Sin noticias nuevas en este ciclo de notificación")
            return

        log.info(f"Notificando {len(pendientes)} noticias nuevas")
        for n in pendientes:
            fuente = escape_markdown(n["fuente"])
            titulo = escape_markdown(n["titulo"])
            resumen = limpiar_resumen(n["resumen"])
            if resumen:
                texto = (f"*{fuente}*\n"
                         f"{titulo}\n\n"
                         f"_{escape_markdown(resumen)}_")
            else:
                texto = f"*{fuente}*\n{titulo}"
            teclado = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("🔴 Última hora", callback_data=f"cat:ultima_hora:{n['id']}"),
                    InlineKeyboardButton("🌐 Viral mundial", callback_data=f"cat:viral_mundial:{n['id']}"),
                ],
                [
                    InlineKeyboardButton("🗞️ Común/matutina", callback_data=f"cat:comun_matutina:{n['id']}"),
                    InlineKeyboardButton("👎 Descartar", callback_data=f"descartar:{n['id']}"),
                ],
            ])
            try:
                if n["imagen_url"]:
                    # Telegram a veces rechaza la URL de la imagen (formato no soportado,
                    # requiere headers, expiró, etc.) — si falla, caemos a texto plano
                    # para no perder la noticia.
                    try:
                        await bot.send_photo(
                            chat_id=TELEGRAM_CHAT_ID, photo=n["imagen_url"], caption=texto,
                            parse_mode=ParseMode.MARKDOWN, reply_markup=teclado,
                        )
                    except Exception as e:
                        log.warning(f"No se pudo enviar imagen de noticia {n['id']} ({e}), mando solo texto")
                        await bot.send_message(
                            chat_id=TELEGRAM_CHAT_ID, text=texto,
                            parse_mode=ParseMode.MARKDOWN, reply_markup=teclado,
                        )
                else:
                    await bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID, text=texto,
                        parse_mode=ParseMode.MARKDOWN, reply_markup=teclado,
                    )
            except Exception as e:
                log.error(f"No se pudo notificar noticia {n['id']}: {e}")
                continue

            conn.execute("UPDATE noticias SET notificada = 1 WHERE id = ?", (n["id"],))
            await asyncio.sleep(0.1)  # evita el flood control de Telegram con lotes grandes


# ---------------------------------------------------------------- botones aprobar/descartar

async def procesar_respuestas():
    """Revisa si tocaste algún botón 👍/👎 y actualiza el estado.

    El offset se guarda en la base de datos para no reprocesar los mismos
    clics entre ciclos. En modo local esto no importa porque se corre de
    forma continua, pero en GitHub Actions cada ejecución es un proceso nuevo.
    """
    if not bot:
        return
    ultimo_update_id = int(get_meta(CLAVE_OFFSET, "0"))

    try:
        # timeout=30: long-polling. Telegram mantiene la conexión abierta y responde
        # EN CUANTO llega un clic, en vez de que tengamos que estar preguntando
        # a cada rato — así el aviso llega casi al instante sin bajar CICLO_SEGUNDOS.
        updates = await bot.get_updates(offset=ultimo_update_id + 1, timeout=30,
                                         allowed_updates=["callback_query"])
    except Exception as e:
        log.warning(f"No se pudieron leer respuestas de Telegram: {e}")
        return

    for update in updates:
        ultimo_update_id = update.update_id
        cq = update.callback_query
        if not cq or not cq.data:
            continue

        partes = cq.data.split(":")

        if partes[0] == "cat" and len(partes) == 3:
            _, categoria, noticia_id = partes
            nuevo_estado = "aprobada"
        elif partes[0] == "descartar" and len(partes) == 2:
            _, noticia_id = partes
            categoria = None
            nuevo_estado = "descartada"
        else:
            continue

        with get_conn() as conn:
            if categoria:
                conn.execute(
                    "UPDATE noticias SET estado = ?, categoria = ?, fecha_aprobada = ? WHERE id = ?",
                    (nuevo_estado, categoria, datetime.now().isoformat(timespec="seconds"), noticia_id),
                )
            else:
                conn.execute(
                    "UPDATE noticias SET estado = ?, fecha_aprobada = ? WHERE id = ?",
                    (nuevo_estado, datetime.now().isoformat(timespec="seconds"), noticia_id),
                )

        if nuevo_estado == "aprobada":
            guardar_media_noticia(noticia_id)

        etiquetas_categoria = {
            "ultima_hora": "🔴 Última hora",
            "viral_mundial": "🌐 Viral mundial",
            "comun_matutina": "🗞️ Común/matutina",
        }
        etiqueta = etiquetas_categoria.get(categoria, "❌ Descartada")
        try:
            await cq.answer(etiqueta)
            confirmacion = f"\n\n*━━━━━━━━━━━━*\n*{etiqueta}*"
            # reply_markup=None quita los botones para que no parezca que falta clasificar
            if cq.message.photo:
                await cq.edit_message_caption(
                    caption=f"{cq.message.caption}{confirmacion}",
                    parse_mode=ParseMode.MARKDOWN, reply_markup=None,
                )
            else:
                await cq.edit_message_text(
                    text=f"{cq.message.text}{confirmacion}",
                    parse_mode=ParseMode.MARKDOWN, reply_markup=None,
                )
        except Exception as e:
            log.warning(f"No se pudo confirmar el botón para noticia {noticia_id}: {e}")

        log.info(f"Noticia {noticia_id} -> {nuevo_estado} ({categoria or 'sin categoría'})")

    if updates:
        set_meta(CLAVE_OFFSET, ultimo_update_id)


# ---------------------------------------------------------------- loop principal

def ciclo_scraping():
    items = recolectar_todas_las_fuentes()
    nuevas = sum(1 for item in items if item["titulo"] and item["link"] and guardar_si_no_duplicada(item))
    log.info(f"Ciclo de scraping: {len(items)} leídas, {nuevas} nuevas guardadas")


def hilo_botones():
    """Corre en segundo plano, independiente del scraping, escuchando los
    clics de 👍/👎 con long-polling (respuesta casi instantánea)."""
    async def bucle():
        while True:
            try:
                await procesar_respuestas()
            except Exception as e:
                log.warning(f"Error escuchando botones: {e}")
                await asyncio.sleep(3)  # evita un loop de errores muy rápido si algo falla

    asyncio.run(bucle())


def run_una_vez():
    """Un solo ciclo: scrapea, notifica lo pendiente y procesa botones.

    Es el modo usado por GitHub Actions (cada ejecución = un ciclo).
    Termina solo; el cron de GitHub se encarga de volver a ejecutarlo.
    """
    init_db()
    log.info("Ciclo único iniciado")

    ciclo_scraping()
    asyncio.run(procesar_respuestas())
    generar_reporte_diario()
    asyncio.run(notificar_pendientes())

    log.info("Ciclo único completado")


def main():
    init_db()
    log.info("Monitor iniciado")

    if bot:
        threading.Thread(target=hilo_botones, daemon=True).start()
        log.info("Escucha de botones (aprobar/descartar) corriendo en segundo plano")

    ultimo_aviso = datetime.now()
    while True:
        ciclo_scraping()

        if datetime.now() - ultimo_aviso >= timedelta(minutes=INTERVALO_NOTIFICACION_MIN):
            asyncio.run(notificar_pendientes())
            ultimo_aviso = datetime.now()

        time.sleep(CICLO_SEGUNDOS)


if __name__ == "__main__":
    if os.environ.get("RUN_UNA_VEZ") == "1":
        run_una_vez()
    else:
        main()