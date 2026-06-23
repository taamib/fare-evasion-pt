import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from evasion.viajes import convertir_gz_a_parquet

convertir_gz_a_parquet()
