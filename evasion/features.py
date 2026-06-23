# Cálculo de features por usuario CDR para el clasificador.
#
# 6 features:
#   1. n_pings               total de registros CDR del usuario
#   2. n_dias_activos        días distintos con al menos un ping
#   3. frac_cerca_paradero   fracción de pings a <=200m de algún paradero
#   4. n_paraderos_distintos paraderos distintos por los que pasó
#   5. frac_manana           fracción de pings entre 6h y 12h
#   6. frac_tarde            fracción de pings entre 17h y 21h

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

RADIO_CERCA_METROS = 200  # un ping cuenta como "cerca" si está a <= este radio de un paradero
R_TIERRA = 6_371_000      # metros

def calcular_features(cdr: pd.DataFrame, paraderos: pd.DataFrame) -> pd.DataFrame:
    cdr = cdr.copy()

    cdr["ts"]   = pd.to_datetime(cdr["timestamp"])
    cdr["hora"] = cdr["ts"].dt.hour

    # n_pings: registros CDR por usuario
    n_pings = (
        cdr.groupby("user_id").size()
        .rename("n_pings")
        .reset_index()
    )

    # n_dias_activos: días distintos con al menos un ping
    n_dias_activos = (
        cdr.groupby("user_id")["fecha"].nunique()
        .rename("n_dias_activos")
        .reset_index()
    )

    # Paradero más cercano a cada ping, con BallTree (misma lógica que matching.py)
    coords_paraderos_rad = np.radians(paraderos[["lat", "lon"]].values)
    arbol = BallTree(coords_paraderos_rad, metric="haversine")

    coords_cdr_rad = np.radians(cdr[["lat", "lon"]].values)
    distancias_rad, indices = arbol.query(coords_cdr_rad, k=1)
    distancias_m = distancias_rad[:, 0] * R_TIERRA
    indices      = indices[:, 0]

    cdr["cerca_paradero"] = distancias_m <= RADIO_CERCA_METROS
    cdr["idx_paradero"]   = np.where(cdr["cerca_paradero"], indices, -1)  # -1 = ninguno cerca

    # frac_cerca_paradero: fracción de pings cerca de algún paradero
    frac_cerca = (
        cdr.groupby("user_id")["cerca_paradero"].mean()
        .rename("frac_cerca_paradero")
        .reset_index()
    )

    # n_paraderos_distintos: paraderos distintos visitados
    pings_cerca = cdr[cdr["idx_paradero"] >= 0]
    n_paraderos = (
        pings_cerca.groupby("user_id")["idx_paradero"].nunique()
        .rename("n_paraderos_distintos")
        .reset_index()
    )

    # frac_manana / frac_tarde: horas pico de transporte en Santiago (6-12h y 17-21h)
    cdr["es_manana"] = (cdr["hora"] >= 6)  & (cdr["hora"] < 12)
    cdr["es_tarde"]  = (cdr["hora"] >= 17) & (cdr["hora"] < 21)

    frac_manana = (
        cdr.groupby("user_id")["es_manana"].mean()
        .rename("frac_manana")
        .reset_index()
    )
    frac_tarde = (
        cdr.groupby("user_id")["es_tarde"].mean()
        .rename("frac_tarde")
        .reset_index()
    )

    usuarios = cdr[["user_id"]].drop_duplicates()
    features = usuarios
    for df_feature in [n_pings, n_dias_activos, frac_cerca, n_paraderos,
                       frac_manana, frac_tarde]:
        features = features.merge(df_feature, on="user_id", how="left")

    # Quien no pasó cerca de ningún paradero queda con NaN; lo dejamos en 0
    features["n_paraderos_distintos"] = (
        features["n_paraderos_distintos"].fillna(0).astype(int)
    )

    print(f"  Features calculadas para {len(features):,} usuarios")
    return features.reset_index(drop=True)


def agregar_grupos(features: pd.DataFrame, usuarios_grupos: pd.DataFrame) -> pd.DataFrame:
    # Pega la columna "grupo" (de crossday.py). Quien no tiene grupo nunca coincidió
    # con una validación Bip, así que queda como sin_uso_aparente
    grupos = (
        usuarios_grupos[["usuario", "grupo"]]
        .rename(columns={"usuario": "user_id"})
    )

    resultado = features.merge(grupos, on="user_id", how="left")
    resultado["grupo"] = resultado["grupo"].fillna("sin_uso_aparente")

    return resultado
