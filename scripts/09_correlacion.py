import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from evasion.socioeconomico import cargar_pobreza, calcular_tasa_por_comuna, correlacionar_con_pobreza

DATA = Path(__file__).parent.parent / "data"

# Cargar resultados del clasificador y hogares
print("Cargando datos...")
features = pd.read_parquet(DATA / "features_clasificados.parquet")
hogar    = pd.read_parquet(DATA / "hogar.parquet")
print(f"  {len(features):,} usuarios clasificados")
print(f"  {len(hogar):,} usuarios con hogar estimado")

# Cargar datos de pobreza SAE 2022
print("\nCargando datos de pobreza...")
pobreza = cargar_pobreza()

# Calcular tasa de no-validación por comuna
print("\nCalculando tasa de no-validación por comuna...")
tasa_comunal = calcular_tasa_por_comuna(features, hogar, min_usuarios=100)

# Correlación Spearman con pobreza
print("\nCalculando correlación con pobreza...")
df_merged, r, p = correlacionar_con_pobreza(tasa_comunal, pobreza)

# Guardar el DataFrame unificado para las visualizaciones
df_merged.to_parquet(DATA / "evasion_por_comuna.parquet", index=False)
print(f"\nGuardado en data/evasion_por_comuna.parquet")
print(f"Columnas: {list(df_merged.columns)}")

# Mostrar tabla resumen
print("\nTop 10 comunas por tasa de no-validación:")
print(
    df_merged[["nombre_pobreza", "n_parece_transito", "tasa_no_validacion", "tasa_pobreza"]]
    .sort_values("tasa_no_validacion", ascending=False)
    .head(10)
    .to_string(index=False)
)
