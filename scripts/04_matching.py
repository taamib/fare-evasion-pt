import sys
import datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from evasion.telefonia import procesar_cdr
from evasion.matching import generar_candidatos

# Cargar CDR completo y filtrar solo nov 1-7 (los días que tiene Bip)
cdr = procesar_cdr()
cdr = cdr[cdr["fecha"].between(datetime.date(2023, 11, 1), datetime.date(2023, 11, 7))].copy()
print(f"CDR filtrado Noviembre 1-7: {cdr['user_id'].nunique():,} usuarios, {len(cdr):,} registros")

# Generar candidatos
candidatos = generar_candidatos(cdr)

# Guardar
salida = Path(__file__).parent.parent / "data" / "candidatos.parquet"
candidatos.to_parquet(salida, index=False)
print(f"Guardado en {salida}")
