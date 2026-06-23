import glob
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.neighbors import BallTree

from .config import VIAJES_BIP

RADIO_METROS  = 100  
VENTANA_MINUTOS = 3  

# Busca pares cercanos en espacio y tiempo usando un BallTree 
def generar_candidatos(cdr: pd.DataFrame, radio_m: int = RADIO_METROS, ventana_min: int = VENTANA_MINUTOS) -> pd.DataFrame:
    cdr = cdr.copy().reset_index(drop=True)
    cdr["ts"]    = pd.to_datetime(cdr["timestamp"])
    cdr["fecha"] = cdr["ts"].dt.date
    cdr = cdr.dropna(subset=["lat", "lon", "ts"])

    radio_en_radianes = radio_m / 6_371_000

    CHUNK = 200_000

    archivos_bip = sorted(glob.glob(str(VIAJES_BIP / "*.parquet")))
    print(f"Días Bip a procesar: {len(archivos_bip)} | Radio: {radio_m}m | Ventana: ±{ventana_min}min")
    todos_los_resultados = []

    for archivo in archivos_bip:
        fecha_str = Path(archivo).stem             
        fecha     = pd.to_datetime(fecha_str).date()

        cdr_dia = cdr[cdr["fecha"] == fecha]
        if cdr_dia.empty:
            print(f"{fecha_str}: sin pings CDR, saltando")
            continue

        viajes_dia = pd.read_parquet(archivo, columns=["lat_subida", "lon_subida", "tiempo_subida_1"])
        viajes_dia["ts_subida"] = pd.to_datetime(viajes_dia["tiempo_subida_1"], errors="coerce")
        viajes_dia = viajes_dia.dropna(subset=["lat_subida", "lon_subida", "ts_subida"]).reset_index(drop=True)

        print(f"\n{fecha_str} — CDR: {len(cdr_dia):,} pings | Bip: {len(viajes_dia):,} viajes")

        # Construir BallTree con pings CDR
        coords_cdr_en_radianes = np.radians(cdr_dia[["lat", "lon"]].values)
        arbol = BallTree(coords_cdr_en_radianes, metric="haversine")

        # Guardamos los índices globales del CDR para recuperar user_id después
        indices_globales_cdr = cdr_dia.index.to_numpy()

        n_viajes = len(viajes_dia)

        for inicio in range(0, n_viajes, CHUNK):
            chunk = viajes_dia.iloc[inicio : inicio + CHUNK]

            coords_bip_en_radianes = np.radians(chunk[["lat_subida", "lon_subida"]].values)

            # query_radius: para cada punto Bip, devuelve los pings CDR
            # que están dentro del radio. Devuelve listas de listas.
            # idx_list[i] = array de índices CDR cercanos al viaje i
            # dist_list[i] = array de distancias (en radianes) a esos pings
            idx_list, dist_list = arbol.query_radius(
                coords_bip_en_radianes, r=radio_en_radianes, return_distance=True
            )

            cuantos_vecinos = np.array([len(x) for x in idx_list])

            if cuantos_vecinos.sum() == 0:
                continue

            # Aplanamos arrays de arrays. Ejemplo:
            #   idx_list   = [[2, 5], [8], []] (viaje 0 -> CDR 2 y 5; viaje 1 -> CDR 8; viaje 2 -> nadie)
            #   cuantos    = [2, 1, 0]
            #   posicion_bip = [0, 0, 1] (repetimos el índice del viaje según cuántos vecinos tiene)
            #   indices_cdr  = [2, 5, 8] (los vecinos aplanados)
            posicion_bip = np.repeat(np.arange(len(chunk)), cuantos_vecinos)
            indices_cdr  = np.concatenate(idx_list).astype(int)
            distancias_m = np.concatenate(dist_list) * 6_371_000  

            pares = pd.DataFrame({
                "idx_bip"    : chunk.index[posicion_bip],   
                "idx_cdr"    : indices_globales_cdr[indices_cdr], 
                "distancia_m": distancias_m,
            })

            # Filtro temporal
            pares["ts_bip"] = viajes_dia.loc[pares["idx_bip"], "ts_subida"].values
            pares["ts_cdr"] = cdr.loc[pares["idx_cdr"], "ts"].values
            pares["delta_min"] = (pares["ts_cdr"] - pares["ts_bip"]).dt.total_seconds() / 60

            pares = pares[pares["delta_min"].abs() <= ventana_min]

            if pares.empty:
                continue

            pares["usuario"] = cdr.loc[pares["idx_cdr"], "user_id"].values
            pares["fecha"]   = fecha_str

            todos_los_resultados.append(
                pares[["fecha", "usuario", "idx_cdr", "idx_bip", "distancia_m", "delta_min"]]
            )

            print(f"  chunk {inicio // CHUNK + 1}: {len(pares):,} candidatos")

        del viajes_dia

    if not todos_los_resultados:
        print("Sin candidatos encontrados.")
        return pd.DataFrame(columns=["fecha", "usuario", "idx_cdr", "idx_bip", "distancia_m", "delta_min"])

    candidatos = pd.concat(todos_los_resultados, ignore_index=True)
    print(f"\nTotal candidatos: {len(candidatos):,}")
    return candidatos
