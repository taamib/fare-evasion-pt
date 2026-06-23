import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from evasion.telefonia import procesar_cdr
from evasion.paraderos import cargar_diccionario
from evasion.features import calcular_features, agregar_grupos

DATA = Path(__file__).parent.parent / "data"

# Cargar CDR completo
print("Cargando CDR...")
cdr = procesar_cdr()
print(f"  {cdr['user_id'].nunique():,} usuarios, {len(cdr):,} registros")

# Cargar diccionario de paraderos
print("Cargando paraderos...")
paraderos = cargar_diccionario()
print(f"  {len(paraderos):,} paraderos")

# Calcular las 6 features por usuario
print("Calculando features...")
features = calcular_features(cdr, paraderos)

# Cargar los grupos asignados en crossday (validador_consistente, transit_sin_match, etc.)
print("Agregando grupos...")
usuarios_grupos = pd.read_parquet(DATA / "usuarios_grupos.parquet")
features = agregar_grupos(features, usuarios_grupos)

# Distribución de grupos
print("\nUsuarios por grupo:")
print(features["grupo"].value_counts())

# Guardar
features.to_parquet(DATA / "features.parquet", index=False)
print(f"\nGuardado en data/features.parquet")
print(f"Columnas: {list(features.columns)}")
