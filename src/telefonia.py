# Ejemplo de inspección (usando pandas para una MUESTRA local)
import pandas as pd
import os
# Reemplaza 'ruta/al/archivo.parquet' con la ruta real de tu muestra
script_dir = os.path.dirname(os.path.abspath(__file__))
df_telefonia_sample = pd.read_parquet(os.path.join(script_dir, 'CENIA_20231101_000000000808.parquet'))

print("Fecha más antigua:", df_telefonia_sample['end_absolutetime_ts'].min())
print("Fecha más reciente:", df_telefonia_sample['end_absolutetime_ts'].max())

print("Cantidad de registros en la muestra:", len(df_telefonia_sample))
print("Cantidad de usuarios únicos:", df_telefonia_sample['hashed_imsi_CENIA'].nunique())

# Contar registros por usuario
registros_por_usuario = df_telefonia_sample.groupby('hashed_imsi_CENIA').size().reset_index(name='cantidad_registros')

# Ordenar por cantidad de registros (de mayor a menor)
registros_por_usuario = registros_por_usuario.sort_values('cantidad_registros', ascending=False)

print("Usuarios con más registros:")
print(registros_por_usuario.head(10))

# Filtrar solo usuarios con más de 1 registro
usuarios_multiples = registros_por_usuario[registros_por_usuario['cantidad_registros'] > 1]
print(f"\nUsuarios con más de 1 registro: {len(usuarios_multiples)}")

# Eliminar el usuario con más de 150k registros (el súper usuario)
super_usuario = registros_por_usuario.iloc[0]['hashed_imsi_CENIA']
print(f"Eliminando súper usuario con {registros_por_usuario.iloc[0]['cantidad_registros']} registros")
df_limpio = df_telefonia_sample[df_telefonia_sample['hashed_imsi_CENIA'] != super_usuario].copy()
print(f"Registros después de limpiar: {len(df_limpio)} (se eliminaron {len(df_telefonia_sample) - len(df_limpio)})")

# Crear columna de hora (bloque de 1 hora)
df_limpio['fecha_hora'] = df_limpio['end_absolutetime_ts'].dt.floor('h')  # Agrupa por hora

# Contar registros por usuario y por hora
registros_por_hora = df_limpio.groupby(['hashed_imsi_CENIA', 'fecha_hora']).size().reset_index(name='cantidad_en_hora')

print("\nCantidad de registros por usuario y hora:")
print(registros_por_hora.head(20))
# Filtrar solo bloques con 5+ registros en la misma hora
candidatos_evasion = registros_por_hora[registros_por_hora['cantidad_en_hora'] >= 5]

print(f"\nCandidatos para evasión (5+ registros en 1 hora): {len(candidatos_evasion)}")
print(candidatos_evasion.head(20))

# usuario1 = df_telefonia_sample['hashed_imsi_CENIA'].iloc[0]

# viajes_usuario1 = df_telefonia_sample[df_telefonia_sample['hashed_imsi_CENIA'] == usuario1]
# viajes_usuario1 = viajes_usuario1.sort_values('end_absolutetime_ts')
# print(f"\nTotal de viajes para el usuario {usuario1}: {len(viajes_usuario1)}")
# print(viajes_usuario1[['end_absolutetime_ts']].head(10))

#Se concluye que es noviembre del 2023 

# df_telefonia_sample['fecha'] = df_telefonia_sample['end_absolutetime_ts'].dt.date
# df_telefonia_sample['hora'] = df_telefonia_sample['end_absolutetime_ts'].dt.time
# df_telefonia_sample = df_telefonia_sample.sort_values(['fecha', 'hora'])
# print(df_telefonia_sample[['fecha', 'hora', 'end_absolutetime_ts']].head(10))