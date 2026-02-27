import sys

from Extraccion_clima_historico import ingest_climas_historicos

def ingest_clima_historico(start_date, end_date):
    try:
        ingest_climas_historicos(start_date, end_date)
    except Exception as exc:
        raise RuntimeError("Error en la extracción de datos históricos.")
    print("Todo el clima histórico se cargó de manera correcta.")
