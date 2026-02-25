import os
import time
import random
import requests
import pandas as pd
import numpy as np
from collections import defaultdict

BASE_URL = "https://api.setlist.fm/rest/1.0"
SEARCH_SETLISTS_URL = f"{BASE_URL}/search/setlists"

API_KEY = os.getenv("SETLIST_API_KEY")
if not API_KEY:
    raise ValueError("No se encontró la variable de entorno SETLIST_API_KEY")

headers = {"x-api-key": API_KEY, "Accept": "application/json"}

# --- FUNCIONES DE APOYO PARA PARADAS DE METRO (DEL SEGUNDO ARCHIVO) ---

def cargar_paradas_df():
    """Descarga el CSV del metro de NY y lo prepara como DataFrame."""
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
    """Calcula la distancia Haversine y devuelve paradas a menos de max_metros."""
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

def fusionar_lista_estaciones(lista_tuplas):
    """Fusiona líneas con el mismo nombre de estación."""
    if not isinstance(lista_tuplas, list) or not lista_tuplas:
        return []
    estaciones_fusionadas = defaultdict(set)
    for nombre, lineas in lista_tuplas:
        estaciones_fusionadas[nombre].update(str(lineas).split())
    return [(nombre, " ".join(sorted(lineas_set))) for nombre, lineas_set in estaciones_fusionadas.items()]

# --- LÓGICA ORIGINAL DE SETLIST.FM ---

def request_with_retry(url, headers, params=None, timeout=30, max_retries=8, base_sleep=1.0):
    for attempt in range(1, max_retries + 1):
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600:
            time.sleep(base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.5))
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
        return r
    raise RuntimeError(f"No se pudo completar la petición tras {max_retries} reintentos.")

def fetch_all_setlists_nyc_2025(page_sleep=1.2) -> list[dict]:
    all_items = []
    page, total = 1, None
    params_base = {"cityName": "New York", "stateCode": "NY", "countryCode": "US", "year": 2025}

    while True:
        params = {**params_base, "p": page}
        r = request_with_retry(SEARCH_SETLISTS_URL, headers=headers, params=params)
        payload = r.json()
        batch = payload.get("setlist", [])
        all_items.extend(batch)
        if total is None:
            total = int(payload.get("total", 0))
        print(f"Página {page} -> acumulado: {len(all_items)}/{total}")
        if len(all_items) >= total or not batch: break
        page += 1
        time.sleep(page_sleep)
    return all_items

def to_dataframe(setlists: list[dict], df_paradas) -> pd.DataFrame:
    rows = []
    for s in setlists:
        venue = s.get("venue", {}) or {}
        city = venue.get("city", {}) or {}
        coords = city.get("coords", {}) or {}
        artist = s.get("artist", {}) or {}

        # Coordenadas para el cálculo de paradas
        lon, lat = coords.get("long"), coords.get("lat")
        paradas = []
        if lon and lat:
            paradas_raw = obtener_paradas_afectadas([lon, lat], df_paradas)
            paradas = fusionar_lista_estaciones(paradas_raw)

        rows.append({
            "fecha_inicio": s.get("eventDate"), 
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

    print("Cargando base de datos de paradas de metro...")
    df_metro = cargar_paradas_df()

    print("Iniciando descarga de setlists...")
    setlists = fetch_all_setlists_nyc_2025(page_sleep=1.2)
    
    # Pasamos df_metro a la función para procesar las paradas durante la creación del DF
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
    df['hora_salida_estimada'] = df['hora_inicio'].apply(lambda x: pd.Timestamp(x) + pd.Timedelta(hours=3))   
    df['score'] = 1.0
    df['nombre_evento'] = "Concierto: " + df['nombre_evento']

    # Limpieza final de columnas
    df = df.drop(columns=["lat", "lng", "venue_name"]).reset_index(drop=True)

    print(df.head(10))
    
    if not df.empty:
        df.to_csv("setlistfm_conciertos_con_metro_2025.csv", index=False, encoding="utf-8")
        print(f"\nGuardado: setlistfm_conciertos_con_metro_2025.csv con {len(df)} registros.")