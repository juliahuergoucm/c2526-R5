import io
import os
import time
import random
import requests
import pandas as pd
import numpy as np
from collections import defaultdict
from minio import Minio

BASE_URL = "https://api.setlist.fm/rest/1.0"
SEARCH_SETLISTS_URL = f"{BASE_URL}/search/setlists"

API_KEY = os.getenv("SETLIST_API_KEY")
if not API_KEY:
    raise ValueError("No se encontró la variable de entorno SETLIST_API_KEY")

headers = {"x-api-key": API_KEY, "Accept": "application/json"}

DEFAULT_ENDPOINT = "minio.fdi.ucm.es"
DEFAULT_BUCKET   = "pd1"
MAX_PAGINAS_API  = 500


# ─────────────────────────────────────────────
#  MinIO
# ─────────────────────────────────────────────
def minio_client(access_key, secret_key, endpoint=DEFAULT_ENDPOINT):
    return Minio(endpoint, access_key=access_key, secret_key=secret_key)


def upload_df_parquet(access_key, secret_key, object_name, df,
                      endpoint=DEFAULT_ENDPOINT, bucket=DEFAULT_BUCKET):
    c = minio_client(access_key, secret_key, endpoint)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    c.put_object(bucket, object_name, buf, length=buf.getbuffer().nbytes)


# --- FUNCIONES DE APOYO PARA PARADAS DE METRO ---

def cargar_paradas_df():
    url = "https://data.ny.gov/api/views/39hk-dx4f/rows.csv?accessType=DOWNLOAD"
    try:
        df = pd.read_csv(url)
        columnas_utiles = ['Stop Name', 'Daytime Routes', 'GTFS Longitude', 'GTFS Latitude']
        df_limpio = df[columnas_utiles].copy()
        df_limpio = df_limpio.rename(columns={
            'Stop Name': 'nombre',
            'Daytime Routes': 'lineas',
            'GTFS Longitude': 'lon',
            'GTFS Latitude': 'lat'
        })
        return df_limpio
    except Exception as e:
        print(f"Error descargando paradas: {e}")
        return None

def obtener_paradas_afectadas(coords, df_paradas, max_metros=500):
    if not coords or None in coords or df_paradas is None or df_paradas.empty:
        return []
    
    lon_evento, lat_evento = coords
    lat1, lon1 = np.radians(lat_evento), np.radians(lon_evento)
    lat2, lon2 = np.radians(df_paradas['lat']), np.radians(df_paradas['lon'])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    r = 6371000 
    
    distancias = c * r
    cercanas = df_paradas[distancias <= max_metros]
    return [(row['nombre'], row['lineas']) for _, row in cercanas.iterrows()]

def convertir_fecha(fecha_str):
    try:
        return pd.to_datetime(fecha_str, format="%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        return fecha_str

def fusionar_lista_estaciones(lista_tuplas):
    if not isinstance(lista_tuplas, list) or not lista_tuplas:
        return []
    estaciones_fusionadas = defaultdict(set)
    for nombre, lineas in lista_tuplas:
        estaciones_fusionadas[nombre].update(str(lineas).split())
    return [(nombre, " ".join(sorted(lineas_set))) for nombre, lineas_set in estaciones_fusionadas.items()]


# --- LÓGICA DE SETLIST.FM OPTIMIZADA ---

def request_with_retry(session, url, params=None, timeout=30, max_retries=8, base_sleep=2.0):
    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else base_sleep * (2 ** (attempt - 1)) + random.uniform(1, 2)
                print(f"  [Límite API] Esperando {wait:.2f}s...")
                time.sleep(wait)
                continue
                
            if 500 <= r.status_code < 600:
                wait = base_sleep * (2 ** (attempt - 1)) + random.uniform(0.5, 1.5)
                print(f"  [Error {r.status_code}] Reintentando en {wait:.2f}s...")
                time.sleep(wait)
                continue
                
            if r.status_code >= 400:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
                
            return r
            
        except requests.exceptions.RequestException as e:
            wait = base_sleep * (2 ** (attempt - 1)) + random.uniform(1, 3)
            print(f"  [Fallo de Red] {type(e).__name__}. Reintentando en {wait:.2f}s...")
            time.sleep(wait)

    raise RuntimeError(f"No se pudo completar la petición tras {max_retries} reintentos.")

def fetch_all_setlists_nyc_2025(pagina_inicio=1) -> list[dict]:
    all_items = []
    page, total = pagina_inicio, None
    params_base = {"cityName": "New York", "stateCode": "NY", "countryCode": "US", "year": 2025}

    with requests.Session() as session:
        session.headers.update(headers)

        while True:
            params = {**params_base, "p": page}
            r = request_with_retry(session, SEARCH_SETLISTS_URL, params=params)
            payload = r.json()
            batch = payload.get("setlist", [])
            all_items.extend(batch)

            if total is None:
                total = int(payload.get("total", 0))

            print(f"Página {page} -> acumulado: {len(all_items)}/{total}")

            if len(all_items) >= total or not batch or page >= MAX_PAGINAS_API:
                if page >= MAX_PAGINAS_API:
                    print(f"  [Límite API] Alcanzado el máximo de {MAX_PAGINAS_API} páginas.")
                break

            page += 1
            time.sleep(3 + random.uniform(1, 2))

    return all_items

def to_dataframe(setlists: list[dict], df_paradas) -> pd.DataFrame:
    rows = []
    for s in setlists:
        venue = s.get("venue", {}) or {}
        city = venue.get("city", {}) or {}
        coords = city.get("coords", {}) or {}
        artist = s.get("artist", {}) or {}

        lon, lat = coords.get("long"), coords.get("lat")
        paradas = []
        if lon and lat:
            paradas_raw = obtener_paradas_afectadas([lon, lat], df_paradas)
            paradas = fusionar_lista_estaciones(paradas_raw)

        rows.append({
            "fecha_inicio": convertir_fecha(s.get("eventDate")),
            "nombre_evento": artist.get("name"),
            "venue_name": venue.get("name"),
            "lat": lat,
            "lng": lon,
            "paradas_afectadas": paradas
        })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    artistas_ny_2025 = [
        "Taylor Swift","Dua Lipa", "Gracie Abrams", "Tate McRae", "Benson Boone", 
        "Chappell Roan", "Mary J. Blige", "Sabrina Carpenter", "Katy Perry",
        "Deftones", "Ghost", "Avril Lavigne", "Pierce The Veil", 
        "Shinedown", "Eric Clapton", "Nick Cave & The Bad Seeds",
        "The Black Keys", "Pulp", "Bloc Party", "Alabama Shakes",
        "Oasis", "Cage The Elephant",
        "Tyler, The Creator", "Kali Uchis", "Muni Long", "Eladio Carrión", 
        "Rod Wave", "Kendrick Lamar", "SZA", "Chris Brown",
        "Chris Stapleton", "Hardy", "Lainey Wilson", "Tyler Childers",
        "Phish", "King Gizzard & the Lizard Wizard",
        "Christian Nodal", "Los Tigres Del Norte", "Alejandro Fernández", "Shakira",
        "Vybz Kartel", "Bounty Killer", "Capleton", "Shenseea",
        "Andrea Bocelli", "Hans Zimmer", "Bruno Mars","Ariana Grande","Beyoncé","Billie Eilish","Post Malone",         
        "Olivia Rodrigo","Bruce Springsteen","J. Cole","Bad Bunny","Karol G","Linkin Park","Metallica",         
        "Stray Kids","The Weeknd","Justin Timberlake","Adele","Ed Sheeran","Lady Gaga"               
    ]

    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
    assert MINIO_ACCESS_KEY is not None, "Falta MINIO_ACCESS_KEY"
    assert MINIO_SECRET_KEY is not None, "Falta MINIO_SECRET_KEY"

    print("Cargando base de datos de paradas de metro...")
    df_metro = cargar_paradas_df()

    print("Iniciando descarga de setlists...")
    setlists = fetch_all_setlists_nyc_2025(pagina_inicio=3)
    
    
    df_completo = to_dataframe(setlists, df_metro)

    df = df_completo[df_completo['nombre_evento'].isin(artistas_ny_2025)].copy()
    
    mapeo_horarios = {
        "Madison Square Garden": "19:30",
        "Barclays Center": "19:30",
        "UBS Arena": "19:30",
        "Forest Hills Stadium": "18:00",
        "Kings Theatre": "20:00",
        "Brooklyn Paramount": "20:00",
        "Amazura Concert Hall": "21:00",
        "Great Lawn (Central Park)": "16:00",
        "Flushing Meadows Park": "13:00",
        "Under the K Bridge": "18:00",
        "Lincoln Center": "19:30"
    }

    df['hora_inicio'] = df['venue_name'].map(mapeo_horarios).fillna("20:00")
    df['hora_salida_estimada'] = df['hora_inicio'].apply(
        lambda x: (pd.Timestamp(x) + pd.Timedelta(hours=3)).strftime("%H:%M")
    )
    df['score'] = 1.0
    df['nombre_evento'] = "Concierto: " + df['nombre_evento']

    df = df.drop(columns=["lat", "lng", "venue_name"]).reset_index(drop=True)
    df = df.sort_values(by=["fecha_inicio", "hora_inicio"]).reset_index(drop=True)

    if df.empty:
        print("No hay eventos para subir.")
        exit()

    print("Subiendo archivos a MinIO...")
    subidos = 0
    for fecha, df_dia in df.groupby("fecha_inicio", sort=True):
        if df_dia.empty:
            continue

        df_dia = df_dia.reset_index(drop=True)
        object_name = f"grupo5/raw/eventos_nyc/dia={fecha}/eventos_concierto_{fecha}.parquet"

        try:
            upload_df_parquet(MINIO_ACCESS_KEY, MINIO_SECRET_KEY, object_name, df_dia)
            print(f"Subido: {DEFAULT_BUCKET}/{object_name} (número de filas: {len(df_dia)})")
            subidos += 1
        except Exception as e:
            print(f"Error subiendo {object_name}: {e}")

    print(f"\nTerminado. {subidos} archivos subidos a MinIO.")