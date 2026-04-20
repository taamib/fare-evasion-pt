# Medición de Evasión en Transporte Público de la Región Metropolitana

Trabajo de título de Ingeniería Civil en Computación, Universidad de Chile (FCFM).

Estima la **tasa de evasión** en el sistema de transporte público de Santiago (RED) combinando dos fuentes de datos complementarias:

- **Registros CDR** (Call Detail Records): trazas espacio-temporales de usuarios de telefonía móvil, usados como proxy de movilidad real.
- **Registros de validación Bip**: viajes detectados por el sistema de cobro (tarjeta Bip), que solo captura pasajeros que pagan.

La hipótesis central es que usuarios con trazas CDR consistentes con una ruta de transporte, pero sin registro Bip correspondiente, son candidatos a evasión.

---

## Metodología

El pipeline tiene cinco etapas:

```
CDR (telefonía)  ──┐
                   ├──► Pares candidatos ──► SWS ──► Clasificación ──► Tasa de evasión
Bip (validación) ──┘                         ▲
                                             │
                                        GTFS (rutas)
```

1. **Preprocesamiento CDR**: filtrado de usuarios con cobertura suficiente en la Región Metropolitana.
2. **Preprocesamiento Bip**: conversión de registros de validación a formato eficiente (Parquet) con coordenadas georreferenciadas.
3. **Generación de candidatos** *(en desarrollo)*: cruce espacio-temporal entre pings CDR y paraderos de subida Bip.
4. **Space-Weighted Similarity (SWS)** *(en desarrollo)*: métrica que compara la traza CDR interpolada con la geometría oficial de la ruta GTFS, siguiendo Gong et al. (2020).
5. **Clasificación** *(pendiente)*: modelo supervisado (scikit-learn) entrenado sobre viajes validados para identificar evasores.

---

## Estructura del repositorio

```
prevwork/
├── evasion/                    # Paquete principal
│   ├── __init__.py
│   ├── config.py               # Rutas y parámetros globales
│   ├── telefonia.py            # Carga y filtrado de datos CDR
│   ├── paraderos.py            # Diccionario de paraderos con coordenadas
│   ├── viajes.py               # Conversión y carga de registros Bip
│   ├── matching.py             # Cruce CDR × Bip (en desarrollo)
│   └── sws.py                  # Métrica SWS (en desarrollo)
├── notebooks/
│   └── exploracion.ipynb       # Análisis exploratorio y decisiones metodológicas
├── data/                       # NO incluida en el repositorio (ver abajo)
├── requirements.txt
└── README.md
```

---

## Datos requeridos (no incluidos en el repo)

Los datos son de acceso restringido. La estructura esperada en `data/` es:

```
data/
├── telefonia/                  # Archivos Parquet de registros CDR
│   └── *.parquet
├── viajes/
│   ├── *.viajes.csv.gz         # Archivos originales de validación Bip (por día)
│   └── parquet/                # Generado automáticamente por convertir_gz_a_parquet()
│       └── YYYY-MM-DD.parquet
└── gtfs/
    ├── stops.txt               # GTFS RED: paradas
    ├── trips.txt               # GTFS RED: viajes
    ├── stop_times.txt          # GTFS RED: horarios
    └── paradas/
        ├── 2026-03-21_consolidado_Registro-Paradas_anual.xlsx   # DTPM: paraderos de buses
        └── paraderos_coords.csv     # Generado automáticamente por construir_diccionario()
```

### Fuentes

| Datos | Fuente | Descripción |
|-------|--------|-------------|
| CDR | Operadora de telefonía (anonimizado) | Pings de antena por usuario, semana del 1–7 nov 2023 |
| Registros Bip | DTPM / RED Metropolitana | Validaciones de tarjeta por viaje, misma semana |
| GTFS RED | [datos.gob.cl](https://datos.gob.cl) | Rutas, paradas y horarios del sistema RED |
| Paraderos DTPM | DTPM | Consolidado anual de paradas de buses con coordenadas UTM |

---

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## Uso

### Preparar los datos (una sola vez)

```python
from evasion.paraderos import construir_diccionario
from evasion.viajes import convertir_gz_a_parquet

construir_diccionario()     # genera paraderos_coords.csv (~12.200 paraderos)
convertir_gz_a_parquet()    # convierte los .gz a Parquet con coordenadas
```

### Procesar CDR

```python
from evasion.telefonia import procesar_cdr

cdr = procesar_cdr()
# Salida: DataFrame con ~96.700 registros de ~6.700 usuarios filtrados
```

### Cargar viajes Bip

```python
from evasion.viajes import cargar_viajes

viajes = cargar_viajes()    # Dask DataFrame lazy, ~18.2M viajes
```

---

## Estado actual

| Módulo | Estado |
|--------|--------|
| `config.py` | Completo |
| `telefonia.py` | Completo |
| `paraderos.py` | Completo |
| `viajes.py` | Completo |
| `matching.py` | Pendiente |
| `sws.py` | Pendiente |
| Clasificación | Pendiente |
| Visualizaciones | Pendiente |

---

## Referencias

- Gong, L., et al. (2020). *High-performance spatiotemporal trajectory matching across heterogeneous data sources*. Future Generation Computer Systems.
