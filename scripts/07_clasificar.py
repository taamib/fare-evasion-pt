import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from evasion.clasificador import entrenar_y_evaluar, aplicar_a_todos

DATA = Path(__file__).parent.parent / "data"

# Cargar features calculadas en el paso anterior
print("Cargando features...")
features = pd.read_parquet(DATA / "features.parquet")
print(f"  {len(features):,} usuarios")

# Entrenar el modelo y evaluarlo
print("\nEntrenando clasificador...")
modelo, imputer = entrenar_y_evaluar(features)

# Aplicar el modelo a todos los usuarios
print("\nAplicando modelo a todos los usuarios...")
resultado = aplicar_a_todos(features, modelo, imputer)

# Guardar resultado completo
resultado.to_parquet(DATA / "features_clasificados.parquet", index=False)
print(f"\nGuardado en data/features_clasificados.parquet")
print(f"Columnas: {list(resultado.columns)}")

# Guardar importancias de features para el gráfico
from evasion.clasificador import FEATURES
importancias = pd.DataFrame({
    "feature": FEATURES,
    "importancia": modelo.feature_importances_,
}).sort_values("importancia", ascending=False)
importancias.to_csv(DATA / "feature_importances.csv", index=False)
print(f"\nGuardado en data/feature_importances.csv")
