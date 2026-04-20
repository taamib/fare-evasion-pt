import dask.dataframe as dd
import os

# 1. Definir la ruta a la carpeta donde tienes varios archivos .parquet
# Nota: Puedes usar '*' para que lea todos los que coincidan con el patrón
folder_path = r'telefonia\datos' 

# 2. Cargar los datos con Dask
# Esto es instantáneo porque Dask no lee los datos aún, solo inspecciona la estructura
ddf = dd.read_parquet(folder_path)

# 3. Limpieza: Quitar al "Súper Usuario" (que ya identificaste como ruido)
# Contar registros por usuario
usuarios_counts = ddf.groupby('hashed_imsi_CENIA').size().compute().reset_index(name='cantidad')
super_user_id = usuarios_counts.loc[usuarios_counts['cantidad'].idxmax(), 'hashed_imsi_CENIA']
print(f"El súper usuario identificado tiene {usuarios_counts['cantidad'].max()} registros")
print(f"ID: {super_user_id}")
ddf = ddf[ddf['hashed_imsi_CENIA'] != super_user_id]

# 4. Crear la columna de hora (redondeando el timestamp)
# En Dask, usamos .dt para acceder a funciones de tiempo
ddf['fecha_hora'] = ddf['end_absolutetime_ts'].dt.floor('h')

# 5. Agrupar por usuario y por hora
# Esto le dice a Dask: "Busca en TODOS los archivos los registros de cada persona"
registros_por_hora = ddf.groupby(['hashed_imsi_CENIA', 'fecha_hora']).size().reset_index()
registros_por_hora.columns = ['hashed_imsi_CENIA', 'fecha_hora', 'cantidad_en_hora']

# 6. Filtrar candidatos (5+ registros en la misma hora)
candidatos = registros_por_hora[registros_por_hora['cantidad_en_hora'] >= 5]

# 7. ¡RECIÉN AQUÍ EJECUTAMOS! 
# .compute() le dice a Dask que procese todo lo anterior y nos devuelva el resultado en un Pandas DF
print("Calculando candidatos en todos los archivos...")
resultado_final = candidatos.compute()

print(f"¡Encontramos {len(resultado_final)} potenciales trayectorias de viaje!")
print(resultado_final.head(20))