# Cruce espacio-temporal entre trazas CDR y viajes Bip.

# Genera pares candidatos (ping CDR, viaje Bip) donde el usuario CDR
# estaba cerca del paradero de subida en un momento similar a la validación.

import glob
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.neighbors import BallTree

from .config import VIAJES_PARQUET_DIR

RADIO_M_DEFAULT     = 300  # metros alrededor del paradero de subida
VENTANA_MIN_DEFAULT = 10   # minutos ± alrededor de la hora de validación

# Distancia vectorizada entre dos arrays de lat/lon
def haversine_vectorized(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi    = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return R * 2 * np.arcsin(np.sqrt(a))


# Genera candidatos de cruce cdr y bip
def generar_candidatos(cdr, parquet_dir=None, radio_m=RADIO_M_DEFAULT, ventana_min=VENTANA_MIN_DEFAULT):
    if parquet_dir is None:
        parquet_dir = VIAJES_PARQUET_DIR

    archivos = sorted(glob.glob(str(Path(parquet_dir) / "*.parquet")))
    print(f"Archivos de viajes: {len(archivos)} | Radio: {radio_m} m | Ventana: ±{ventana_min} min", flush=True)

    # Preparar CDR una sola vez
    cdr = cdr.copy().reset_index(drop=True)
    cdr['ts']    = pd.to_datetime(cdr['end_absolutetime_ts'])
    cdr['fecha'] = cdr['ts'].dt.date
    cdr = cdr.dropna(subset=['end_geo_lat', 'end_geo_lon', 'ts'])

    radio_rad  = radio_m / 6_371_000
    CHUNK      = 200_000
    resultados = []

    # Dia a día
    for archivo in archivos:
        fecha_str = Path(archivo).stem        
        fecha     = pd.to_datetime(fecha_str).date()

        cdr_dia = cdr[cdr['fecha'] == fecha] # filtrar cdr para este dia
        if cdr_dia.empty:
            print(f"{fecha_str}: sin pings CDR, saltando", flush=True)
            continue

        # Cargar solo este día de viajes
        viajes_dia = pd.read_parquet(archivo, columns=['lat_subida', 'lon_subida', 'tiempo_subida_1']) # solo columnas necesarias
        viajes_dia['ts_subida'] = pd.to_datetime(viajes_dia['tiempo_subida_1'], errors='coerce')
        viajes_dia = viajes_dia.dropna(subset=['lat_subida', 'lon_subida', 'ts_subida']).reset_index(drop=True)

        print(f"\n{fecha_str} — CDR: {len(cdr_dia):,} pings | Bip: {len(viajes_dia):,} viajes", flush=True)

        # BallTree sobre CDR del día 
        cdr_coords     = np.radians(cdr_dia[['end_geo_lat', 'end_geo_lon']].values) # lat/lon a radianes
        tree           = BallTree(cdr_coords, metric='haversine')
        cdr_idx_global = cdr_dia.index.to_numpy()

        n = len(viajes_dia)
        for start in range(0, n, CHUNK):
            chunk      = viajes_dia.iloc[start:start + CHUNK]
            bip_coords = np.radians(chunk[['lat_subida', 'lon_subida']].values)

            idx_list, dist_list = tree.query_radius(bip_coords, r=radio_rad, return_distance=True)

            # Aplanar resultados y calcular distancias reales
            lengths = np.array([len(x) for x in idx_list])
            if lengths.sum() == 0:
                continue

            bip_pos    = np.repeat(np.arange(len(chunk)), lengths)
            cdr_local  = np.concatenate(idx_list).astype(int)
            distancias = np.concatenate(dist_list) * 6_371_000

            df_pares = pd.DataFrame({
                'idx_bip'    : chunk.index[bip_pos],
                'idx_cdr'    : cdr_idx_global[cdr_local],
                'distancia_m': distancias,
            })

            # Filtro temporal
            df_pares['ts_bip'] = viajes_dia.loc[df_pares['idx_bip'], 'ts_subida'].values
            df_pares['ts_cdr'] = cdr.loc[df_pares['idx_cdr'], 'ts'].values
            df_pares['delta_min'] = (
                (df_pares['ts_cdr'] - df_pares['ts_bip']).dt.total_seconds() / 60
            )
            df_pares = df_pares[df_pares['delta_min'].abs() <= ventana_min]

            if not df_pares.empty:
                df_pares['usuario'] = cdr.loc[df_pares['idx_cdr'], 'hashed_imsi_CENIA'].values
                df_pares['fecha']   = fecha_str
                resultados.append(df_pares[['fecha', 'usuario', 'idx_cdr', 'idx_bip', 'distancia_m', 'delta_min']])

            print(f"  {fecha_str} chunk {start//CHUNK+1}: {len(df_pares):,} candidatos", flush=True)

        del viajes_dia  # liberar RAM antes del siguiente día

    if not resultados:
        print("Sin candidatos encontrados.")
        return pd.DataFrame(columns=['fecha', 'usuario', 'idx_cdr', 'idx_bip', 'distancia_m', 'delta_min'])

    candidatos = pd.concat(resultados, ignore_index=True)
    print(f"\nTotal candidatos: {len(candidatos):,}")
    return candidatos
