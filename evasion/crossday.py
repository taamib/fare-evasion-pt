import pandas as pd

from .config import VIAJES_BIP

# Recuperamos id_tarjeta para c/ candidato usando idx_bip
def agregar_id_tarjeta(candidatos: pd.DataFrame) -> pd.DataFrame:
    resultados = []

    for fecha in sorted(candidatos["fecha"].unique()):
        viajes_dia = pd.read_parquet(
            VIAJES_BIP / f"{fecha}.parquet",
            columns=["lat_subida", "lon_subida", "tiempo_subida_1", "id_tarjeta"]
        )

        viajes_dia["ts"] = pd.to_datetime(viajes_dia["tiempo_subida_1"], errors="coerce")
        viajes_dia = viajes_dia.dropna(subset=["lat_subida", "lon_subida", "ts"]).reset_index(drop=True)

        # Filtrar candidatos de este día y recuperar la tarjeta usando idx_bip
        candidatos_dia = candidatos[candidatos["fecha"] == fecha].copy()
        candidatos_dia["id_tarjeta"] = viajes_dia.loc[candidatos_dia["idx_bip"], "id_tarjeta"].values

        resultados.append(candidatos_dia)

    return pd.concat(resultados, ignore_index=True)

# Para cada par CDR/Bip contar en cuántos días distintos coincidieron
def contar_dias_por_par(candidatos: pd.DataFrame) -> pd.DataFrame:
    pares = (
        candidatos
        .groupby(["usuario", "id_tarjeta"])["fecha"]
        .nunique()
        .reset_index(name="n_dias_juntos")
    )
    return pares

# Para c/ usuario CDR quedarse con el max dias que coincidió con alguna tarjeta
# Luego etiquetar cada usuario según su comportamiento
def etiquetar_usuarios(candidatos: pd.DataFrame, pares: pd.DataFrame) -> pd.DataFrame:
    n_dias_max = (
        pares
        .groupby("usuario")["n_dias_juntos"]
        .max()
        .reset_index(name="n_dias_max_tarjeta")
    )

    # Contar cuántas tarjetas distintas vio cada usuario en los candidatos
    n_tarjetas = (
        candidatos
        .groupby("usuario")["id_tarjeta"]
        .nunique()
        .reset_index(name="n_tarjetas")
    )

    # Unir todo
    usuarios = candidatos[["usuario"]].drop_duplicates()
    usuarios = usuarios.merge(n_dias_max, on="usuario", how="left")
    usuarios = usuarios.merge(n_tarjetas, on="usuario", how="left")
    usuarios["n_dias_max_tarjeta"] = usuarios["n_dias_max_tarjeta"].fillna(0).astype(int)
    usuarios["n_tarjetas"]         = usuarios["n_tarjetas"].fillna(0).astype(int)

    # Etiquetar cada usuario según su comportamiento:
    #
    # validador_consistente   -> coincidió con la misma tarjeta en 3+ días distintos
    #                           muy probablemente paga con esa tarjeta
    #
    # sin_uso_aparente        -> nunca apareció cerca de ninguna validación
    #                           probablemente no usa transporte público
    #
    # coincidencia_intermedia -> coincidió en exactamente 2 días, ambiguo
    #
    # transit_sin_match       -> tiene coincidencias pero ninguna consistente
    #                           usa transporte pero sin tarjeta identificable
    #                           acá pueden estar los que no validan

    def asignar_grupo(fila: pd.Series) -> str:
        if fila["n_dias_max_tarjeta"] >= 3:
            return "validador_consistente"
        if fila["n_tarjetas"] == 0:
            return "sin_uso_aparente"
        if fila["n_dias_max_tarjeta"] == 2:
            return "coincidencia_intermedia"
        return "transit_sin_match"

    usuarios["grupo"] = usuarios.apply(asignar_grupo, axis=1)
    return usuarios
