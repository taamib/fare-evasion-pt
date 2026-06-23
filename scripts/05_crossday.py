import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from evasion.crossday import agregar_id_tarjeta, contar_dias_por_par, etiquetar_usuarios

DATA = Path(__file__).parent.parent / "data"

# Cargar candidatos ya calculados
candidatos = pd.read_parquet(DATA / "candidatos.parquet")
print(f"Candidatos cargados: {len(candidatos):,}")

# Paso 1: agregar id_tarjeta a cada candidato
candidatos = agregar_id_tarjeta(candidatos)

# Paso 2: contar cuántos días coincidió cada par (usuario, tarjeta)
pares = contar_dias_por_par(candidatos)
print("\nDías juntos por par:")
print(pares["n_dias_juntos"].value_counts().sort_index())

# Paso 3: etiquetar cada usuario con su grupo
usuarios = etiquetar_usuarios(candidatos, pares)
print("\nUsuarios por grupo:")
print(usuarios["grupo"].value_counts())

# Guardar
usuarios.to_parquet(DATA / "usuarios_grupos.parquet", index=False)
print("\nGuardado en data/usuarios_grupos.parquet")
