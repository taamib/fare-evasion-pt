import dask.dataframe as dd
import pandas as pd
import numpy as np

# Files
input_file = '2023-11-01.viajes.csv.gz'
output_file = 'viajes_limpios.parquet' 

# Columns to drop
drop_cols = [
    'Unnamed: 100',
    'id_tarjeta',
    'periodo_inicio_viaje', 
    'periodo_fin_viaje',
    'netapassinbajada', 
    'ultimaetapaconbajada',
    'tipo_transporte_2', 'tipo_transporte_3', 'tipo_transporte_4', 
    'srv_2', 'srv_3', 'srv_4',
    'paradero_bajada_2', 'paradero_bajada_3', 'paradero_bajada_4', 
    'zona_subida_2', 'zona_subida_3', 'zona_subida_4', 
    'zona_bajada_2', 'zona_bajada_3', 'zona_bajada_4', 
    'periodo_bajada_1', 'periodo_bajada_2', 'periodo_bajada_3', 'periodo_bajada_4',
    'contrato', 
    'mediahora_inicio_viaje',
    'mediahora_fin_viaje',
    'mediahora_bajada_1',
    'mediahora_bajada_2',
    'mediahora_bajada_3',
    'mediahora_bajada_4',
    'mediahora_inicio_viaje_hora',
    'mediahora_fin_viaje_hora',
    'op_1era_etapa', 'op_2da_etapa', 'op_3era_etapa', 'op_4ta_etapa',
    'tv3', 'tc3', 'tv4', 'tviaje', 'tviaje2', 'egreso',
    'proposito', 'tv1', 'tc1', 'te1', 'tv2', 'tc2', 'te2', 'te3',
    'dt1', 'dveh_ruta1', 'dveh_euc1', 
    'dt2', 'dveh_ruta2', 'dveh_euc2', 
    'dt3', 'dveh_ruta3', 'dveh_euc3', 
    'dveh_ruta4', 'dveh_euc4', 
    'dtfinal', 'dveh_rutafinal', 'dveh_eucfinal', 
    'tipo_corte_etapa_viaje', 
    'entrada', 'te0'
]

# Columns that need type conversion
time_cols = ['tiempo_inicio_viaje', 'tiempo_fin_viaje']
num_cols = ['distancia_eucl', 'distancia_ruta', 'n_etapas']

try:
    temp_df = pd.read_csv(input_file, compression='gzip', sep='|', nrows=1000, na_values='-')
    
    # Ensure key columns have known types
    dtype_meta = {col: 'object' for col in temp_df.columns}
    
    # Set known types for key columns
    for col in time_cols:
        dtype_meta[col] = 'object' 
    for col in num_cols:
        dtype_meta[col] = np.float64 
        
    # Load the full file using the types
    ddf_viajes = dd.read_csv(
        input_file, 
        compression='gzip', 
        sep='|',
        dtype=dtype_meta, 
        na_values='-',
        blocksize='100MB' # Large file
    )
    
    print("DataFrame loaded")
    
    # Drop columns
    ddf_viajes_clean = ddf_viajes.drop(columns=drop_cols, errors='ignore')

    # Date conversion
    for col in time_cols:
        ddf_viajes_clean[col] = dd.to_datetime(ddf_viajes_clean[col], errors='coerce')
        
    # Numeric conversion
    for col in num_cols:
        ddf_viajes_clean[col] = dd.to_numeric(ddf_viajes_clean[col], errors='coerce').fillna(0)
        
    # Define target variable Y
    # Target (Y=1) is 'Validated Trip': n_etapas > 0 (at least one bip!)
    # This defines the positive class
    ddf_viajes_clean['is_validated_Y'] = (ddf_viajes_clean['n_etapas'] > 0).astype(int)

    # Export cleaned data
    ddf_viajes_clean.to_parquet(output_file, write_metadata_file=True, compute=True)
    
    print("Clean data expored!")

except Exception as e:
    print(f"An error occurred during execution: {e}")

# Cargar una muestra en Pandas para inspección visual
df_sample = pd.read_parquet(output_file, engine='pyarrow').head(10)

# Mostrar la lista de columnas (debería ser menor que 101)
print(df_sample.columns.tolist())
    
# Mostrar los tipos de datos (verificar datetime64 y float64)
print(df_sample.dtypes)

# Mostrar la nueva columna Y y las columnas clave para el matching
print(df_sample[['tiempo_inicio_viaje', 'distancia_eucl', 'n_etapas', 'is_validated_Y']])
