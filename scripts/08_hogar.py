import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from evasion.telefonia import procesar_cdr
from evasion.hogar import estimar_hogar, cargar_comunas_rm, asignar_comuna

DATA = Path(__file__).parent.parent / "data"

# Cargar CDR (mismo que usamos en los pasos anteriores)
print("Cargando CDR...")
cdr = procesar_cdr()
print(f"  {len(cdr):,} pings de {cdr['user_id'].nunique():,} usuarios")

# Paso 1: estimar hogar usando pings nocturnos
print("\nEstimando hogares...")
hogar = estimar_hogar(cdr)

# Paso 2: cargar comunas de la RM
print("\nCargando comunas RM...")
comunas_rm = cargar_comunas_rm()

# Paso 3: asignar a cada usuario su comuna según donde está su hogar estimado
print("\nAsignando comunas...")
hogar_con_comuna = asignar_comuna(hogar, comunas_rm)

# Guardar
hogar_con_comuna.to_parquet(DATA / "hogar.parquet", index=False)
print(f"\nGuardado en data/hogar.parquet")
print(f"Columnas: {list(hogar_con_comuna.columns)}")
print(f"\nTop 10 comunas por cantidad de usuarios:")
print(hogar_con_comuna["comuna_gadm"].value_counts().head(10).to_string())
