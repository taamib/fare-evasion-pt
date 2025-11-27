import dask.dataframe as dd
import pandas as pd

# Files
input_file = 'CENIA_20231101_000000000808.parquet'
output_file = 'telefonia_limpios.parquet'

# Read a sample Parquet file to inspect its structure
df_telefonia_sample = dd.read_parquet(input_file)

# print("Columnas de Telefonía:", df_telefonia_sample.columns)
# print("Tipos de datos:", df_telefonia_sample.dtypes)
# print(df_telefonia_sample.head())

# Rename columns
columns = {
    'hashed_imsi_CENIA': 'user_id',
    'end_absolutetime_ts': 'timestamp',
    'end_geo_lat': 'latitud',
    'end_geo_lon': 'longitud',
    'code_region': 'region'
}

df_telefonia_newcolumns = df_telefonia_sample.rename(columns=columns)

# Drop comuna column
df_telefonia_clean = df_telefonia_newcolumns.drop(columns=['code_commune'])

# Filter by region (RM is 13)
df_telefonia_clean_rm = df_telefonia_clean[df_telefonia_clean['region'] == 13]

# Drop region column since we know its RM
df_rm_final = df_telefonia_clean_rm.drop(columns=['region'])

# Export cleaned data
df_rm_final.to_parquet(
    output_file, 
    write_metadata_file=True, 
    overwrite=True, 
    compression='snappy',
    compute=True
)

print("Clean telefonia data exported! ")

df_sample = pd.read_parquet(output_file, engine='pyarrow').head(10)

# Show columns
print(df_sample.columns.tolist())

# Show data types
print(df_sample.dtypes)


