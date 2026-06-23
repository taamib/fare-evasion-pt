import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from evasion.paraderos import construir_diccionario

dic = construir_diccionario(guardar=True)
print(f"Paraderos guardados: {len(dic):,}")
