# Análisis de autocorrelación espacial de la tasa de no-validación por comuna

import pandas as pd
import geopandas as gpd
from libpysal.weights import KNN
from esda.moran import Moran, Moran_Local

from evasion.hogar import cargar_comunas_rm


def cargar_geometria_con_tasas(evasion_por_comuna: pd.DataFrame) -> gpd.GeoDataFrame:
    # Pega a cada comuna su polígono (su forma en el mapa) y su tasa de no-validación
    comunas_rm = cargar_comunas_rm()
    geo = comunas_rm.merge(
        evasion_por_comuna[["comuna_gadm", "nombre_pobreza", "tasa_no_validacion"]],
        on="comuna_gadm",
        how="inner",
    )
    print(f"  Comunas con geometría y tasa: {len(geo)}")
    return geo.reset_index(drop=True)


def construir_pesos(geo: gpd.GeoDataFrame, k: int = 5):
    # Para medir "vecindad espacial" primero hay que definir quién es vecino de quién
    # KNN: cada comuna se conecta con las k comunas cuyo centro está más cerca
    w = KNN.from_dataframe(geo, k=k)
    w.transform = "r"
    print(f"  Vecindad KNN con k={k} (cada comuna tiene {k} vecinos)")
    return w


def moran_global(geo: gpd.GeoDataFrame, w) -> Moran:
    # Calcula el Moran's I global: ¿hay patrón espacial en la tasa, sí o no?
    # permutations=999: para el p-valor, baraja las tasas al azar 999 veces y compara
    # el Moran real contra esos azares. Si el real supera a casi todos, el patrón es significativo 
    y = geo["tasa_no_validacion"].values
    moran = Moran(y, w, permutations=999, seed=42)
    print(f"\n  Moran's I global = {moran.I:.3f}")
    print(f"  p-valor          = {moran.p_sim:.4f}  "
          f"{'(autocorrelación significativa)' if moran.p_sim < 0.05 else '(NO significativo)'}")
    return moran


def moran_local(geo: gpd.GeoDataFrame, w) -> Moran_Local:
    # LISA: el mismo análisis pero por comuna, para ver DÓNDE están los patrones
    y = geo["tasa_no_validacion"].values
    return Moran_Local(y, w, permutations=999, seed=42)


# Qué significa cada cuadrante de LISA
#   Alto-Alto: tasa alta rodeada de tasas altas (foco caliente)
#   Bajo-Bajo: tasa baja rodeada de tasas bajas (zona fría)
#   Alto-Bajo y Bajo-Alto: comuna que no se parece a sus vecinas (outlier)
ETIQUETAS_LISA = {
    1: "Alto-Alto (foco caliente)",
    2: "Bajo-Alto (outlier)",
    3: "Bajo-Bajo (zona fría)",
    4: "Alto-Bajo (outlier)",
}


def clasificar_lisa(geo: gpd.GeoDataFrame, lisa, alpha: float = 0.05) -> gpd.GeoDataFrame:
    # Le pone a cada comuna su etiqueta LISA (foco caliente, zona fría, etc)
    geo = geo.copy()
    significativo = lisa.p_sim < alpha
    geo["lisa_sig"] = significativo
    geo["cuadrante"] = [
        ETIQUETAS_LISA[q] if sig else "No significativo"
        for q, sig in zip(lisa.q, significativo)
    ]
    print("\n  Clasificación LISA por comuna:")
    print(geo["cuadrante"].value_counts().to_string())
    return geo
