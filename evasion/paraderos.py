#Construcción del diccionario de paraderos (código → lat/lon).

# Los datos Bip identifican los paraderos con códigos del tipo L-26-12-30-PO
# que no tienen coordenadas directas. Este módulo los resuelve

import unicodedata
import pandas as pd
from pyproj import Transformer

from .config import DTPM_XLSX, STOPS_TXT, TRIPS_TXT, STOP_TIMES_TXT, PARADEROS_CSV

LINEAS_METRO = ["L1", "L2", "L3", "L4", "L4A", "L5", "L6", "MTR", "MTN"] 

ALIASES = {
    "CAL Y CANTO"           : "PUENTE CAL Y CANTO",
    "PARQUE OHIGGINS"       : "PARQUE O'HIGGINS",
    "RONDIZONNI"            : "RONDIZZONI",
    "PLAZA MAIPU"           : "PLAZA DE MAIPU",
    "ESTACION ALAMEDA"      : "ESTACION CENTRAL",
    "UNION LATINO AMERICANA": "UNION LATINOAMERICANA",
}


def normalizar(texto):
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")

# Carga paraderos de buses desde DTPM, estaciones de metro/metrotren desde GTFS, y aliases manuales
def cargar_buses():
    df = pd.read_excel(DTPM_XLSX)
    df.columns = [
        "orden", "codigo_ts", "codigo_usuario", "sentido", "variante", "un",
        "codigo_paradero", "codigo_paradero_usuario", "comuna", "eje",
        "desde", "hacia", "x", "y", "nombre", "zona_paga", "excepciones",
    ]
    df["x"] = pd.to_numeric(df["x"], errors="coerce")
    df["y"] = pd.to_numeric(df["y"], errors="coerce")
    df = df.dropna(subset=["x", "y", "codigo_paradero"]).copy()

    # Convertir coordenadas de UTM (EPSG:32719) a lat/lon (EPSG:4326) ya que DTPM da coordenadas en UTM
    transformer = Transformer.from_crs("EPSG:32719", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(df["x"].values, df["y"].values)
    df["lat"] = lat
    df["lon"] = lon

    return (
        df.groupby("codigo_paradero")
        .agg(lat=("lat", "first"), lon=("lon", "first"), nombre=("nombre", "first"))
        .reset_index()
        .rename(columns={"codigo_paradero": "paradero"})
    )


# Carga estaciones de metro/metrotren desde datos GTFS
def cargar_metro():
    trips      = pd.read_csv(TRIPS_TXT)
    stop_times = pd.read_csv(STOP_TIMES_TXT)
    stops      = pd.read_csv(STOPS_TXT)

    trips_metro = trips[trips["route_id"].astype(str).isin(LINEAS_METRO)]
    stops_metro = (
        stop_times[stop_times["trip_id"].isin(trips_metro["trip_id"])]
        .merge(stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]], on="stop_id")
        [["stop_id", "stop_name", "stop_lat", "stop_lon"]]
        .drop_duplicates("stop_id")
        .copy()
    )
    stops_metro["paradero"] = (
        stops_metro["stop_name"]
        .str.split(r"[Dd]irecci", regex=True).str[0]
        .str.split(r"\(Anden",   regex=True).str[0]
        .str.strip()
        .apply(normalizar)
    )
    return (
        stops_metro[["paradero", "stop_lat", "stop_lon", "stop_name"]]
        .rename(columns={"stop_lat": "lat", "stop_lon": "lon", "stop_name": "nombre"})
        .drop_duplicates("paradero")
    )


def cargar_aliases(base):
    coords = base.set_index("paradero")
    rows = []
    for nombre_viaje, nombre_gtfs in ALIASES.items():
        if nombre_gtfs in coords.index:
            rows.append({
                "paradero": nombre_viaje,
                "lat"     : coords.loc[nombre_gtfs, "lat"],
                "lon"     : coords.loc[nombre_gtfs, "lon"],
                "nombre"  : nombre_gtfs,
            })
    return pd.DataFrame(rows)


def construir_diccionario(guardar=True):
    print("Cargando paraderos de buses (DTPM)...", flush=True)
    buses = cargar_buses()
    print(f"  Buses      : {len(buses):,}")

    print("Cargando estaciones de metro/metrotren (GTFS)...", flush=True)
    metro = cargar_metro()
    print(f"  Metro/MTR  : {len(metro):,}")

    aliases = cargar_aliases(pd.concat([buses, metro]))
    print(f"  Aliases    : {len(aliases):,}")

    diccionario = (
        pd.concat([buses, metro, aliases], ignore_index=True)
        .drop_duplicates("paradero")
    )
    print(f"  Total      : {len(diccionario):,}")

    if guardar:
        diccionario.to_csv(PARADEROS_CSV, index=False)
        print(f"  Guardado en: {PARADEROS_CSV}")

    return diccionario


def cargar_diccionario():
    return pd.read_csv(PARADEROS_CSV)[["paradero", "lat", "lon"]].drop_duplicates("paradero")
