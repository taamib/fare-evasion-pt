# Validez del cruce de trazas de telefonía móvil (CDR) y validaciones Bip para caracterizar la no-validación tarifaria en Santiago

Trabajo de título de Ingeniería Civil en Computación, Universidad de Chile (FCFM).
Profesor guía: Eduardo Graells-Garrido.

Cruza dos fuentes de datos a nivel de usuario individual en el transporte público de Santiago
(sistema RED):

- CDR (Call Detail Records): trazas espacio-temporales de telefonía móvil, usadas como proxy de movilidad.
- Registros de validación Bip (DTPM): viajes pagados con tarjeta.

Idea central: un usuario que se mueve como pasajero de transporte pero sin registro de
validación consistente es candidato a no-validación. El trabajo construye el método,
caracteriza los patrones de movilidad y temporales de los grupos, y evalúa la validez del
método para el análisis socioespacial.

Hallazgo principal: la asociación aparente entre no-validación y pobreza comunal es un
artefacto del sesgo de cobertura del CDR, no una relación real (ver notebook 03). El método no
permite medir la dimensión socioeconómica de la no-validación sin sesgo.

---

## Resultados principales

El trabajo entrega tres resultados:

1. Método. El emparejamiento CDR-Bip y el clasificador funcionan: el clasificador distingue
   patrones de uso de transporte con F1 cercano a 0,92 (notebooks 02 y 03).
2. Caracterización. Los validadores confirmados concentran su actividad en la mañana,
   alrededor de las 8h, como un viaje habitual al trabajo; los probables no-validadores se
   mueven más tarde y de forma más repartida durante el día. Es una comparación interna entre grupos, no depende del cruce
   socioeconómico y no la afecta el sesgo de cobertura (notebook 04).
3. Validez. La asociación aparente entre no-validación y pobreza comunal (r ≈ 0,33) es un
   artefacto del sesgo de cobertura del CDR. La tasa depende de la cobertura, la cobertura es
   menor en las comunas de menores ingresos, y una simulación sin diferencias reales de evasión reproduce
   la correlación. Con CDR de un solo operador no se puede medir la dimensión socioeconómica
   sin sesgo (notebook 03).

---

## Pipeline

Los scripts en `scripts/` se ejecutan en orden. Cada uno usa lo que produjo el anterior.

| # | Script | Qué hace | Produce |
|---|--------|----------|---------|
| 01 | `construir_paraderos.py` | Diccionario código→coordenadas (buses DTPM en UTM + metro GTFS) | `paraderos_coords.csv` |
| 02 | `convertir_viajes.py` | Convierte Bip `.csv.gz` a Parquet con coordenadas | `data/viajes/parquet/` |
| 03 | `procesar_cdr.py` | Carga y filtra CDR (≥80% en RM, ≥5 pings/día) | DataFrame CDR |
| 04 | `matching.py` | Cruce espacio-temporal CDR↔Bip (BallTree, 100 m, ±3 min) | `candidatos.parquet` |
| 05 | `crossday.py` | Co-ocurrencia entre días → 4 grupos de usuarios | `usuarios_grupos.parquet` |
| 06 | `features.py` | 6 características de movilidad por usuario | `features.parquet` |
| 07 | `clasificar.py` | Random Forest (validador vs sin-uso) aplicado a todos | `features_clasificados.parquet` |
| 08 | `hogar.py` | Estima hogar (pings nocturnos) y asigna comuna | `hogar.parquet` |
| 09 | `correlacion.py` | Tasa de no-validación por comuna + correlación con pobreza | `evasion_por_comuna.parquet` |

Los pasos 01-09 construyen los datos. Las figuras del informe se generan aparte con los
scripts de `scripts/figuras/` (mapa, heatmap temporal, scatter de correlación, importancia de
features y la prueba de antenas); no son parte del pipeline y se corren cuando se quieran
regenerar las figuras. El análisis y los resultados viven en los notebooks.

## Notebooks

Los notebooks de `notebooks/` contienen el análisis, los resultados y las figuras con su
explicación:

- `01_exploracion`: registro de la fase exploratoria inicial.
- `02_validacion_decisiones`: justificación de cada decisión metodológica (filtros del CDR,
  radio del matching, umbral del clasificador, vecindad espacial, etc.).
- `03_resumen_hallazgo`: el hallazgo de validez, con la prueba del sesgo de cobertura
  (correlación parcial, submuestra bien cubierta, densidad de antenas y una simulación) y el
  diagnóstico del clasificador.
- `04_caracterizacion`: perfil de movilidad y patrones temporales de los grupos.

---

## Estructura

```
tesis/
├── evasion/                  # Paquete: lógica reutilizable
│   ├── config.py             # Rutas y parámetros
│   ├── telefonia.py          # Carga/filtrado CDR
│   ├── paraderos.py          # Diccionario de paraderos
│   ├── viajes.py             # Conversión de registros Bip
│   ├── matching.py           # Cruce CDR×Bip (BallTree)
│   ├── crossday.py           # Etiquetado de usuarios
│   ├── features.py           # Cálculo de features
│   ├── clasificador.py       # Random Forest
│   ├── hogar.py              # Estimación de hogar y comuna
│   ├── socioeconomico.py     # Tasa por comuna y correlación
│   └── espacial.py           # Moran's I y LISA
├── scripts/                  # Pipeline ejecutable (01-09): construye los datos
│   └── figuras/              # Genera las figuras del informe 
├── notebooks/
│   ├── 01_exploracion.ipynb              # Registro de la fase exploratoria
│   ├── 02_validacion_decisiones.ipynb    # Justificación de cada decisión
│   ├── 03_resumen_hallazgo.ipynb         # Validez y sesgo de cobertura (hallazgo central)
│   └── 04_caracterizacion.ipynb          # Perfil de movilidad y temporal de los grupos
├── figuras/                  # Salidas gráficas
├── data/                     # No incluida (acceso restringido)
├── requirements.txt
└── README.md
```

---

## Datos requeridos (no incluidos en el repo)

Estructura esperada en `data/`:

```
data/
├── telefonia_por_usuario/    # CDR en Parquet
├── viajes/                   # Bip: .csv.gz originales y parquet/ generado
├── gtfs/                     # GTFS RED + consolidado DTPM de paraderos
├── comunas_chile.json.zip    # Geometrías comunales (GADM)
└── pobreza_comunal.xlsx      # Tasa de pobreza por comuna (SAE 2022)
```

| Datos | Fuente | Descripción |
|-------|--------|-------------|
| CDR | CENIA (anonimizado) | Pings por usuario, noviembre 2023, Región Metropolitana |
| Bip | DTPM / RED | Validaciones por viaje, 1–7 nov 2023 |
| GTFS RED + paraderos DTPM | DTPM | Rutas, paradas y coordenadas de paraderos |
| Pobreza comunal | SAE 2022 | Tasa de pobreza por ingresos por comuna |

---

## Instalación

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Uso

Ejecutar los scripts en orden desde la raíz del proyecto, con el entorno activado:

```bash
python scripts/01_construir_paraderos.py
python scripts/02_convertir_viajes.py
...
python scripts/09_correlacion.py
```

Los pasos 01–02 preparan los datos (una sola vez). Del 03 al 09 corren el pipeline. Las
figuras se regeneran cuando se quieran con los scripts de `scripts/figuras/`, por ejemplo:

```bash
python scripts/figuras/scatter_correlacion.py
python scripts/figuras/antenas_cobertura.py
```


## Referencias

Método y datos:

- Gong, X., et al. (2020). High-performance spatiotemporal trajectory matching across
  heterogeneous data sources. Future Generation Computer Systems. (Considerado; ver notebook
  02 para por qué no se aplicó SWS.)
- De Montjoye, Y.-A., et al. (2013). Unique in the Crowd: The privacy bounds of human
  mobility. Scientific Reports.

Sesgo de cobertura del CDR (sustenta el hallazgo de validez):

- Wesolowski, A., et al. (2013). The impact of biases in mobile phone ownership on estimates
  of human mobility. Journal of the Royal Society Interface.
- Ricciato, F., et al. (2017). Beyond the single-operator, CDR-only paradigm. Pervasive and
  Mobile Computing.
- Zhao, Z., et al. (2016). Understanding the bias of call detail records in human mobility
  research. International Journal of Geographical Information Science.
- SUBTEL (2023). Informe del Sector Telecomunicaciones. Penetración móvil en Chile (133 líneas
  por 100 habitantes).
