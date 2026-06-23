import os
import glob
import pandas as pd

from .config import VIAJES, VIAJES_BIP, COLUMNAS_VIAJES_ELIMINAR
from .paraderos import cargar_diccionario, normalizar

# Convierte csv.gz a parquet y agrega coords
def convertir_gz_a_parquet() -> None:
    os.makedirs(VIAJES_BIP, exist_ok=True)

    paraderos = cargar_diccionario()

    paraderos_subida = paraderos.rename(columns={
        "paradero": "sub_norm",
        "lat": "lat_subida",
        "lon": "lon_subida",
    })
    paraderos_bajada = paraderos.rename(columns={
        "paradero": "baj_norm",
        "lat": "lat_bajada",
        "lon": "lon_bajada",
    })

    # Buscar todos los archivos CSV comprimidos en la carpeta de viajes
    archivos = sorted(glob.glob(str(VIAJES / "*.viajes.csv.gz")))
    print(f"Archivos a convertir: {len(archivos)}")

    for archivo in archivos:
        # El nombre del archivo empieza con la fecha asi que la extraemos
        fecha = os.path.basename(archivo)[:10] 
        archivo_salida = VIAJES_BIP / f"{fecha}.parquet"

        # Si ya convertimos este día antes, no lo volvemos a hacer
        if archivo_salida.exists():
            print(f"  {fecha}: ya existe, saltando")
            continue

        print(f"  Convirtiendo {fecha}...")

        df = pd.read_csv(archivo, compression="gzip", sep="|")
        print(f"    Filas leídas: {len(df):,}")

        columnas_a_eliminar = [col for col in COLUMNAS_VIAJES_ELIMINAR if col in df.columns]
        df = df.drop(columns=columnas_a_eliminar)

        # Normalizar nombres paraderos
        df["sub_norm"] = df["paradero_subida_1"].apply(normalizar)
        df["baj_norm"] = df["paradero_bajada_1"].apply(normalizar)

        # Agregar coordenadas de subida 
        df = df.merge(paraderos_subida, on="sub_norm", how="left")

        # Agregar coordenadas de bajada
        df = df.merge(paraderos_bajada, on="baj_norm", how="left")

        # Eliminar las columnas auxiliares de normalización, ya no las necesitamos
        df = df.drop(columns=["sub_norm", "baj_norm"])

        df.to_parquet(archivo_salida, index=False, engine="pyarrow")

        pct_con_coords = df["lat_subida"].notna().mean() * 100
        print(f"    Guardado. Subida con coords: {pct_con_coords:.1f}%")

    print("Conversión completa.")
