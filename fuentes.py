"""
Lista de fuentes RSS. Agrega o quita las que quieras — el monitor las
recorre todas por igual, en paralelo.
"""

FUENTES = [
    # --- Colombia / nacionales ---
    {"nombre": "El Tiempo", "url": "https://www.eltiempo.com/rss/portada.xml"},
    {"nombre": "El Espectador", "url": "https://www.elespectador.com/arc/outboundfeeds/rss/"},
    {"nombre": "Semana", "url": "https://www.semana.com/arc/outboundfeeds/rss/"},
    {"nombre": "Caracol Radio", "url": "https://caracol.com.co/rss/portada.xml"},
    {"nombre": "RCN Radio", "url": "https://www.rcnradio.com/feed"},

    # --- Internacionales (español) ---
    {"nombre": "BBC Mundo", "url": "https://feeds.bbci.co.uk/mundo/rss.xml"},
    {"nombre": "CNN en Español", "url": "https://cnnespanol.cnn.com/feed/"},
    {"nombre": "Infobae", "url": "https://www.infobae.com/arc/outboundfeeds/rss/"},

    # --- Internacionales (inglés, para viral mundial) ---
    {"nombre": "Reuters World", "url": "https://feeds.reuters.com/Reuters/worldNews"},
    {"nombre": "AP Top News", "url": "https://apnews.com/apf-topnews?format=rss"},
]
