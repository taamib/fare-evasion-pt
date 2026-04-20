# Procesamiento de archivos de viajes
# convertir de CSV.gz a Parquet, agregar coordenadas, y cargar como Dask DataFrame

import unicodedata
import glob
import os

import pandas as pd
import dask.dataframe as dd

from .config import VIAJES_DIR, VIAJES_PARQUET_DIR, COLUMNAS_VIAJES_ELIMINAR
from .paraderos import cargar_diccionario


def normalizar(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).upper().strip()
    texto = unicodedata.normalize("NFD", texto)
    return "".join(c for c in texto if unicodedata.category(c) != "Mn")

# Convierte los archivos de viajes de CSV.gz a Parquet, agregando coordenadas de paraderos 
def convertir_gz_a_parquet(viajes_dir=None, parquet_dir=None):
    if viajes_dir is None:
        viajes_dir = VIAJES_DIR
    if parquet_dir is None:
        parquet_dir = VIAJES_PARQUET_DIR

    os.makedirs(parquet_dir, exist_ok=True)
    paraderos = cargar_diccionario()
    sub_coords = paraderos.rename(columns={"paradero": "sub_norm", "lat": "lat_subida", "lon": "lon_subida"})
    baj_coords = paraderos.rename(columns={"paradero": "baj_norm", "lat": "lat_bajada", "lon": "lon_bajada"})

    archivos = sorted(glob.glob(str(viajes_dir / "*.viajes.csv.gz")))
    print(f"Archivos a convertir: {len(archivos)}")

    for archivo in archivos:
        fecha = os.path.basename(archivo)[:10]
        out = parquet_dir / f"{fecha}.parquet"

        if out.exists():
            print(f"  {fecha}: ya existe, saltando", flush=True)
            continue

        mb = os.path.getsize(archivo) / 1e6
        print(f"\nConvirtiendo {fecha} ({mb:.0f} MB gz)...", flush=True)
        df = pd.read_csv(archivo, compression="gzip", sep="|")
        print(f"  Leído: {len(df):,} filas", flush=True)

        cols_drop = [c for c in COLUMNAS_VIAJES_ELIMINAR if c in df.columns]
        df = df.drop(columns=cols_drop)

        df["sub_norm"] = df["paradero_subida_1"].apply(normalizar)
        df["baj_norm"] = df["paradero_bajada_1"].apply(normalizar)
        df = df.merge(sub_coords, on="sub_norm", how="left")
        df = df.merge(baj_coords, on="baj_norm", how="left")
        df = df.drop(columns=["sub_norm", "baj_norm"])

        df.to_parquet(out, index=False, engine="pyarrow")
        tam = os.path.getsize(out) / 1e6
        print(f"  Guardado: {fecha}.parquet ({tam:.0f} MB)  subida {df['lat_subida'].notna().mean()*100:.1f}%", flush=True)

    print("\nConversión completa.")


def cargar_viajes(parquet_dir=None):
    if parquet_dir is None:
        parquet_dir = VIAJES_PARQUET_DIR
    return dd.read_parquet(str(parquet_dir))
