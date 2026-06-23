import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from evasion.config import COMUNAS_GEOJSON


def estimar_hogar(cdr: pd.DataFrame) -> pd.DataFrame:
    # Supuesto: entre 22h y 7h la gente está en su casa. El hogar es la mediana de
    # lat/lon de los pings nocturnos de cada usuario
    hora = pd.to_datetime(cdr["timestamp"]).dt.hour
    pings_noche = cdr[(hora >= 22) | (hora < 7)].copy()
    print(f"  Pings nocturnos: {len(pings_noche):,} de {len(cdr):,} total ({100*len(pings_noche)/len(cdr):.1f}%)")

    hogar = (
        pings_noche
        .groupby("user_id")[["lat", "lon"]]
        .median()
        .reset_index()
        .rename(columns={"lat": "lat_hogar", "lon": "lon_hogar"})
    )
    print(f"  Usuarios con hogar estimado: {len(hogar):,}")
    return hogar


def cargar_comunas_rm() -> gpd.GeoDataFrame:
    # Carga el GeoJSON de GADM y deja solo las comunas de la RM
    # NAME_1 = región ("SantiagoMetropolitan"), NAME_3 = comuna
    print("  Cargando comunas de GADM...")
    comunas = gpd.read_file(COMUNAS_GEOJSON)
    comunas_rm = comunas[comunas["NAME_1"] == "SantiagoMetropolitan"].copy()
    comunas_rm = comunas_rm[["NAME_3", "geometry"]].rename(columns={"NAME_3": "comuna_gadm"})
    comunas_rm = comunas_rm.set_crs("EPSG:4326", allow_override=True)
    print(f"  Comunas RM cargadas: {len(comunas_rm)}")
    return comunas_rm.reset_index(drop=True)


def asignar_comuna(hogar: pd.DataFrame, comunas_rm: gpd.GeoDataFrame) -> pd.DataFrame:
    # Point-in-polygon: a qué comuna cae el hogar de cada usuario. Los que caen fuera
    # de la RM quedan con comuna_gadm = NaN.
    geometria = [Point(lon, lat) for lat, lon in zip(hogar["lat_hogar"], hogar["lon_hogar"])]
    hogar_geo = gpd.GeoDataFrame(hogar.copy(), geometry=geometria, crs="EPSG:4326")

    resultado = gpd.sjoin(hogar_geo, comunas_rm, how="left", predicate="within")
    resultado = resultado.drop(columns=["index_right", "geometry"], errors="ignore")

    n_asignados = resultado["comuna_gadm"].notna().sum()
    n_total = len(resultado)
    print(f"  Usuarios con comuna asignada: {n_asignados:,} / {n_total:,} ({100*n_asignados/n_total:.1f}%)")
    return pd.DataFrame(resultado)
