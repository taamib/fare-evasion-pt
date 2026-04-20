# Carga y filtrado de datos CDR (registros de telefonía móvil)
import dask.dataframe as dd
import pandas as pd

from .config import (
    TELEFONIA_DIR,
    MIN_PINGS_DIA,
    RM_FRACCION_MIN,
    LAT_MIN, LAT_MAX,
    LON_MIN, LON_MAX,
)

# Carga los archivos parquet de CDR con Dask y elimina el super-usuario (ruido, millones de registros)
def cargar_cdr(path=None):
    if path is None:
        path = TELEFONIA_DIR
    ddf = dd.read_parquet(str(path))
    ddf["end_absolutetime_ts"] = dd.to_datetime(ddf["end_absolutetime_ts"], errors="coerce")
    ddf = ddf.dropna(subset=["end_absolutetime_ts"])
    ddf["fecha"] = ddf["end_absolutetime_ts"].dt.date

    counts = ddf.groupby("hashed_imsi_CENIA").size().compute()
    super_user = counts.idxmax()
    return ddf[ddf["hashed_imsi_CENIA"] != super_user].compute()


# Filtra usuarios que no tienen la mayoría de sus viajes dentro de la RM
def filtrar_rm(df, fraccion_min=RM_FRACCION_MIN):
    dentro_rm = (
        df["end_geo_lat"].between(LAT_MIN, LAT_MAX) &
        df["end_geo_lon"].between(LON_MIN, LON_MAX)
    )
    fraccion = df.assign(en_rm=dentro_rm).groupby("hashed_imsi_CENIA")["en_rm"].mean()
    usuarios_rm = fraccion[fraccion >= fraccion_min].index
    return df[df["hashed_imsi_CENIA"].isin(usuarios_rm)].copy()


# Filtra días con pocos pings, que no permiten identificar viajes realistas 
def filtrar_dias_buenos(df, min_pings=MIN_PINGS_DIA):
    pings_x_dia = df.groupby(["hashed_imsi_CENIA", "fecha"]).size().rename("n_pings")
    dias_buenos = (
        pings_x_dia[pings_x_dia >= min_pings]
        .reset_index()[["hashed_imsi_CENIA", "fecha"]]
    )
    return df.merge(dias_buenos, on=["hashed_imsi_CENIA", "fecha"], how="inner")


# Pipeline completo:
# carga → elimina super-usuario → filtra RM → filtra días por ping mínimo
def procesar_cdr(path=None):
    print("Cargando CDR...", flush=True)
    df = cargar_cdr(path)
    print(f"  Registros cargados : {len(df):,}")

    df = filtrar_rm(df)
    print(f"  Tras filtro RM     : {df['hashed_imsi_CENIA'].nunique():,} usuarios")

    df = filtrar_dias_buenos(df)
    print(f"  Tras filtro días   : {df['hashed_imsi_CENIA'].nunique():,} usuarios, {len(df):,} registros")

    return df
