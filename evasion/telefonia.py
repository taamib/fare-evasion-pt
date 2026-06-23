import glob
import pandas as pd
from pathlib import Path

from .config import (
    TELEFONIA,
    MIN_PINGS_DIA,
    RM_FRACCION_MIN,
    LAT_MIN_RM, LAT_MAX_RM,
    LON_MIN_RM, LON_MAX_RM,
)


def procesar_cdr(path=None) -> pd.DataFrame:
    if path is None:
        path = TELEFONIA

    archivos = sorted(glob.glob(str(Path(path) / "*.parquet")))
    print(f"Archivos CDR encontrados: {len(archivos)}")

    #Cargar archivos de a uno 
    filtered = []  # acá vamos acumulando los archivos filtrados

    for archivo in archivos:
        df_archivo = pd.read_parquet(archivo)

        # Marcar qué filas están dentro del rectángulo de la RM
        dentro_rm = (
            df_archivo["lat"].between(LAT_MIN_RM, LAT_MAX_RM) &
            df_archivo["lon"].between(LON_MIN_RM, LON_MAX_RM)
        )

        # Para cada usuario, calcular la fracción de pings que están en la RM
        df_archivo["en_rm"] = dentro_rm
        fraccion_en_rm_por_usuario = df_archivo.groupby("user_id")["en_rm"].mean()

        # Quedarse solo con los usuarios que tengan +80% pings en RM
        usuarios_de_rm = fraccion_en_rm_por_usuario[fraccion_en_rm_por_usuario >= RM_FRACCION_MIN].index
        df_filtrado = df_archivo[df_archivo["user_id"].isin(usuarios_de_rm)].copy()
        df_filtrado = df_filtrado.drop(columns=["en_rm"])  # ya no la necesitamos

        filtered.append(df_filtrado)

    # Juntar todos los archivos filtrados en un solo DataFrame
    df = pd.concat(filtered, ignore_index=True)
    print(f"Registros tras carga inicial: {len(df):,}")
    print(f"Usuarios tras carga inicial: {df['user_id'].nunique():,}")
    print(f"Usuarios tras filtro fracción RM (>={RM_FRACCION_MIN*100:.0f}%): {df['user_id'].nunique():,}")

    # Filtrar por días con al menos 5 pings
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["fecha"] = df["timestamp"].dt.date

    # Contar cuántos pings tiene cada usuario en cada día
    pings_por_usuario_dia = df.groupby(["user_id", "fecha"]).size().reset_index(name="n_pings")

    # Filtrar los pares (usuario, fecha) que superan el mínimo de 5 pings por dia
    dias_validos = pings_por_usuario_dia[pings_por_usuario_dia["n_pings"] >= MIN_PINGS_DIA]
    dias_validos = dias_validos[["user_id", "fecha"]]  # solo necesitamos las llaves

    # Hacer un merge para quedarnos solo con las filas de días válidos
    df = df.merge(dias_validos, on=["user_id", "fecha"], how="inner")

    print(f"Usuarios tras filtro días (>={MIN_PINGS_DIA} pings/día): {df['user_id'].nunique():,}")
    print(f"Registros finales: {len(df):,}")

    return df


