from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

# CDR
TELEFONIA_DIR   = DATA / "telefonia"
MIN_PINGS_DIA   = 5 # Mínimo de pings por día para considerar al usuario
RM_FRACCION_MIN = 0.80 # Mínima fracción de viajes para considerar un usuario dentro de la RM

# Coordenadas RM
LAT_MIN, LAT_MAX = -34.3, -33.0
LON_MIN, LON_MAX = -71.1, -70.2

# Viajes Bip
VIAJES_DIR        = DATA / "viajes"
VIAJES_PARQUET_DIR = VIAJES_DIR / "parquet"

# GTFS / Paraderos
GTFS_DIR      = DATA / "gtfs"
DTPM_XLSX     = GTFS_DIR / "paradas" / "2026-03-21_consolidado_Registro-Paradas_anual.xlsx"
STOPS_TXT     = GTFS_DIR / "stops.txt"
TRIPS_TXT     = GTFS_DIR / "trips.txt"
STOP_TIMES_TXT = GTFS_DIR / "stop_times.txt"
PARADEROS_CSV = GTFS_DIR / "paradas" / "paraderos_coords.csv"

COLUMNAS_VIAJES_ELIMINAR = [
    "Unnamed: 100",
    "mediahora_inicio_viaje", "mediahora_fin_viaje",
    "mediahora_bajada_1", "mediahora_bajada_2",
    "mediahora_bajada_3", "mediahora_bajada_4",
    "mediahora_inicio_viaje_hora", "mediahora_fin_viaje_hora",
    "op_1era_etapa", "op_2da_etapa", "op_3era_etapa", "op_4ta_etapa",
    "tv3", "tc3", "tv4", "tviaje", "tviaje2", "egreso",
    "proposito", "tv1", "tc1", "te1", "tv2", "tc2", "te2", "te3",
]
