import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from evasion.telefonia import procesar_cdr

cdr = procesar_cdr()
print(f"Usuarios: {cdr['user_id'].nunique():,}")
print(f"Registros: {len(cdr):,}")
