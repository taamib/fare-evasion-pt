from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA = ROOT / "data"

# CDR
TELEFONIA   = DATA / "telefonia_por_usuario"
MIN_PINGS_DIA   = 5 # Mínimo de pings por día para considerar al usuario
RM_FRACCION_MIN = 0.80 # Mínima fracción de viajes para considerar un usuario dentro de la RM

# Coordenadas RM
LAT_MIN_RM, LAT_MAX_RM = -34.3, -33.0
LON_MIN_RM, LON_MAX_RM = -71.1, -70.2

# Viajes Bip
VIAJES = DATA / "viajes" 
VIAJES_BIP = VIAJES / "parquet"

# GTFS / Paraderos
GTFS    = DATA / "gtfs"
DTPM    = GTFS / "paradas" / "2026-03-21_consolidado_Registro-Paradas_anual.xlsx"
STOPS     = GTFS / "stops.txt"
TRIPS     = GTFS / "trips.txt"
STOP_TIMES = GTFS / "stop_times.txt"
PARADEROS = GTFS / "paradas" / "paraderos_coords.csv"

# Análisis espacial y socioeconómico
COMUNAS_GEOJSON  = DATA / "comunas_chile.json.zip"   # bajado de GADM (todas las comunas Chile)
POBREZA_COMUNAL  = DATA / "pobreza_comunal.xlsx"     # tasa de pobreza por comuna (SAE 2022)

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
