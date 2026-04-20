import pandas as pd
import os

ARCHIVO_ENTRADA_GZ = '2023-11-01.viajes.csv.gz'
# Nombre para el nuevo archivo de salida CSV
ARCHIVO_SALIDA_CSV = 'viajes_limpios_muestra.csv'

pd.set_option('display.max_columns', 105) 
pd.set_option('display.width', 1000) 

try:
    print(f"Cargando archivo: {os.path.basename(ARCHIVO_ENTRADA_GZ)}...")
    df_viajes = pd.read_csv(
        ARCHIVO_ENTRADA_GZ, 
        compression='gzip', 
        sep='|',
        # Asumiendo que el archivo de viajes es muy grande, leer solo las primeras 1000 filas para una muestra
        nrows=1000
    )
    print(f"Carga exitosa. Filas cargadas: {len(df_viajes)}")

    # Opcional: Mostrar los nuevos nombres de columna para verificar
    # print(df_viajes_limpio.columns.tolist())

    # Convertir valores nulos/guiones a NaN de Python
    # El archivo usa '-' para valores faltantes, lo reemplazamos por NaN para manejo uniforme
    # df_viajes = df_viajes.replace('-', pd.NA)
    
    # Conversión de Tipos: Tiempo (Crucial)
    # Convertir las columnas de tiempo a formato datetime
    # tiempo_cols = ['tiempo_inicio_viaje', 'tiempo_fin_viaje']
    # for col in tiempo_cols:
    #     df_viajes[col] = pd.to_datetime(df_viajes[col], errors='coerce')
    # print("Columnas de tiempo convertidas a formato datetime.")

    # Conversión de Tipos: Distancias y Duración (Crucial para features)
    # Convertir distancias y duraciones de object a float
    # dist_cols = ['tviaje', 'distancia_eucl', 'distancia_ruta', 'tviaje2']
    # for col in dist_cols:
    #     # Reemplazamos los NaN recién creados a 0 antes de la conversión, o manejamos los errores
    #     df_viajes[col] = pd.to_numeric(df_viajes[col], errors='coerce').fillna(0)
    # print("Columnas de distancia/duración convertidas a formato numérico.")


    # Exportar a csv limpio
    print(f"Exportando a {ARCHIVO_SALIDA_CSV}...")
    df_viajes.to_csv(ARCHIVO_SALIDA_CSV, index=False, encoding='utf-8', sep=';')

    columnas_a_eliminar = [
        # Ruido / Columnas ya identificadas
        'Unnamed: 100', 
        # Columnas Redundantes o Agregadas
        'mediahora_inicio_viaje',
        'mediahora_fin_viaje',
        'mediahora_bajada_1',
        'mediahora_bajada_2',
        'mediahora_bajada_3',
        'mediahora_bajada_4',
        'mediahora_inicio_viaje_hora',
        'mediahora_fin_viaje_hora',
        'op_1era_etapa', 'op_2da_etapa',
        'op_3era_etapa', 'op_4ta_etapa',
        'tv3', 'tc3', 'tv4', 'tviaje', 'tviaje2', 'egreso',
        'proposito', 'tv1', 'tc1', 'te1', 'tv2', 'tc2', 'te2', 'te3'
        # Puedes añadir más columnas según tu criterio tras la revisión visual...
    ]

    df_viajes_limpio = df_viajes.drop(columns=columnas_a_eliminar)

    print(f"Columnas originales: {len(df_viajes.columns)}")
    print(f"Columnas después de la limpieza: {len(df_viajes_limpio.columns)}")
    print("¡Exportación completada! El archivo ya está listo para ser visualizado.")

except Exception as e:
    print(f"Ocurrió un error durante la ejecución: {e}")