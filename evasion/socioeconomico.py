import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import unicodedata
import pandas as pd
from scipy import stats

from evasion.config import POBREZA_COMUNAL


# Utilidad de normalización 
def _normalizar(nombre: str) -> str:
    # Convierte un nombre de comuna a una clave de matching sin ambigüedad:
    # minúsculas, sin tildes/diéresis, sin espacios.
    # Ejemplos:
    #   "La Pintana"  → "lapintana"
    #   "LaPintana"   → "lapintana"
    #   "Ñuñoa"       → "nunoa"
    #   "Pudahuel"    → "pudahuel"
    if not isinstance(nombre, str):
        return ""
    sin_tildes = unicodedata.normalize("NFD", nombre)
    sin_tildes = "".join(c for c in sin_tildes if unicodedata.category(c) != "Mn")
    return sin_tildes.lower().replace(" ", "").replace("-", "")


# Cargar pobreza
def cargar_pobreza() -> pd.DataFrame:
    # Lee el Excel de pobreza comunal SAE 2022
    df = pd.read_excel(POBREZA_COMUNAL, header=1)

    # Renombrar columnas por posición 
    df = df.rename(columns={
        df.columns[0]: "CUT",
        df.columns[2]: "nombre_pobreza",
        df.columns[5]: "tasa_pobreza",
    })

    # Quedarnos solo con las tres columnas que necesitamos
    df = df[["CUT", "nombre_pobreza", "tasa_pobreza"]].copy()

    # Filtrar filas válidas: CUT debe ser numérico y estar en rango RM
    df = df[pd.to_numeric(df["CUT"], errors="coerce").notna()].copy()
    df["CUT"] = df["CUT"].astype(int)
    df = df[(df["CUT"] >= 13001) & (df["CUT"] <= 13999)].copy()

    # Agregar clave de matching normalizada
    df["clave_match"] = df["nombre_pobreza"].apply(_normalizar)

    print(f"  Comunas RM en datos de pobreza: {len(df)}")
    return df.reset_index(drop=True)


# Calcular tasa de no-validación por comuna

def calcular_tasa_por_comuna(
    features_clasificados: pd.DataFrame,
    hogar: pd.DataFrame,
    min_usuarios: int = 100,
) -> pd.DataFrame:
    # Calcula, para cada comuna, la tasa de no-validación según la lógica inversa.

    # Población: solo los que el modelo dice que se mueven como transeúnte de transporte
    transit = features_clasificados[features_clasificados["patron_similar"] == True].copy()

    # Marcar a los no-validadores: parecen transeúnte pero no validan consistentemente
    transit["no_valida"] = transit["grupo"] != "validador_consistente"

    # Unir con datos de hogar para saber la comuna de cada usuario
    transit = transit.merge(hogar[["user_id", "comuna_gadm"]], on="user_id", how="inner")

    # Usuarios sin comuna asignada (fuera de RM o sin pings nocturnos) los descartamos
    transit = transit[transit["comuna_gadm"].notna()]

    # Calcular tasa por comuna
    agrupado = transit.groupby("comuna_gadm").agg(
        n_parece_transito = ("no_valida", "count"),   # toda la población de la comuna
        n_no_valida       = ("no_valida", "sum"),     # los que no validan
    ).reset_index()

    agrupado["tasa_no_validacion"] = agrupado["n_no_valida"] / agrupado["n_parece_transito"]

    # Agregar clave de matching
    agrupado["clave_match"] = agrupado["comuna_gadm"].apply(_normalizar)

    # Filtrar comunas con pocos usuarios (estimación poco confiable)
    antes = len(agrupado)
    agrupado = agrupado[agrupado["n_parece_transito"] >= min_usuarios]
    print(f"  Comunas con >= {min_usuarios} usuarios: {len(agrupado)} de {antes}")

    return agrupado.reset_index(drop=True)


# Correlación con pobreza 
def correlacionar_con_pobreza(
    tasa_comunal: pd.DataFrame,
    pobreza: pd.DataFrame,
) -> tuple[pd.DataFrame, float, float]:
    # Une la tasa de no-validación con los datos de pobreza
    # Calcula la correlación de Spearman entre ambas variables

    df = tasa_comunal.merge(pobreza, on="clave_match", how="inner")

    n_antes = len(tasa_comunal)
    n_despues = len(df)
    n_sin_match = n_antes - n_despues
    if n_sin_match > 0:
        sin_match = set(tasa_comunal["clave_match"]) - set(pobreza["clave_match"])
        print(f"  ADVERTENCIA: {n_sin_match} comunas sin match en pobreza: {sorted(sin_match)}")

    print(f"  Comunas en análisis final: {n_despues}")

    r, p = stats.spearmanr(df["tasa_no_validacion"], df["tasa_pobreza"])

    print("\n  Correlación Spearman:")
    print(f"    r = {r:.3f}")
    print(f"    p = {p:.4f}  {'(significativo p<0.05)' if p < 0.05 else '(NO significativo)'}")

    return df, r, p
