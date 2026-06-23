# Clasificador de patrones de movilidad.
#
# Entrena un Random Forest para distinguir dos grupos con etiqueta clara
# (validador_consistente vs sin_uso_aparente) por sus 6 features CDR, y lo aplica a
# todos los usuarios. Lo que predice es "patrón de movilidad similar a un validador",
# no "es evasor".

import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import StratifiedKFold, cross_validate

# Deben coincidir con las columnas que genera features.py
FEATURES = [
    "n_pings",
    "n_dias_activos",
    "frac_cerca_paradero",
    "n_paraderos_distintos",
    "frac_manana",
    "frac_tarde",
]

# Umbral de probabilidad para marcar "patrón similar a validador". 0.45 maximiza el F1.
UMBRAL_PROBA = 0.45


def entrenar_y_evaluar(features: pd.DataFrame) -> tuple:
    # Entrena con los dos grupos etiquetados, evalúa con validación cruzada y retorna
    # el modelo y el imputer listos para aplicar.

    # Solo los grupos con etiqueta clara; los ambiguos se predicen después
    entrenamiento = features[
        features["grupo"].isin(["validador_consistente", "sin_uso_aparente"])
    ].copy()

    n_val = (entrenamiento["grupo"] == "validador_consistente").sum()
    n_sin = (entrenamiento["grupo"] == "sin_uso_aparente").sum()
    print("  Datos de entrenamiento:")
    print(f"    validador_consistente (clase 1): {n_val:,}")
    print(f"    sin_uso_aparente      (clase 0): {n_sin:,}")

    X = entrenamiento[FEATURES].values
    y = (entrenamiento["grupo"] == "validador_consistente").astype(int).values

    kfold = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    # class_weight balancea las clases (hay muchos más sin_uso que validadores)
    modelo = RandomForestClassifier(
        n_estimators=300,
        max_depth=12,
        class_weight="balanced",
        random_state=42,
    )
    rf_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("clf", modelo),
    ])

    # Baselines para saber si el F1 del modelo es bueno o el problema es fácil
    baselines = {
        "Dummy (mayoritaria)":   DummyClassifier(strategy="most_frequent"),
        "Dummy (azar estrat.)":  DummyClassifier(strategy="stratified", random_state=42),
        "Regresión logística":   Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
        ]),
        "Random Forest":         rf_pipe,
    }

    print("\n  F1 (validación cruzada, 5 folds) frente a baselines:")
    for nombre, estimador in baselines.items():
        f1 = cross_validate(estimador, X, y, cv=kfold, scoring=["f1"])["test_f1"].mean()
        print(f"    {nombre:<22} F1 = {f1:.3f}")

    # Random Forest en detalle (las métricas que reportamos)
    resultados_cv = cross_validate(
        rf_pipe, X, y,
        cv=kfold,
        scoring=["accuracy", "precision", "recall", "f1"],
    )
    print("\n  Random Forest en detalle (validación cruzada, 5 folds):")
    print(f"    Accuracy:  {resultados_cv['test_accuracy'].mean():.3f}  ± {resultados_cv['test_accuracy'].std():.3f}")
    print(f"    Precision: {resultados_cv['test_precision'].mean():.3f}  ± {resultados_cv['test_precision'].std():.3f}")
    print(f"    Recall:    {resultados_cv['test_recall'].mean():.3f}  ± {resultados_cv['test_recall'].std():.3f}")
    print(f"    F1:        {resultados_cv['test_f1'].mean():.3f}  ± {resultados_cv['test_f1'].std():.3f}")

    # Modelo final con el 100% de los datos etiquetados. Acá el imputer se ajusta sobre
    # todo el set a propósito: es el modelo que se aplica, no el que se evalúa.
    imputer = SimpleImputer(strategy="median")
    X_imp = imputer.fit_transform(X)
    modelo.fit(X_imp, y)

    print("\n  Importancia de features (de mayor a menor):")
    importancias = sorted(
        zip(FEATURES, modelo.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    for nombre, valor in importancias:
        barra = "█" * int(valor * 40)
        print(f"    {nombre:<25} {valor:.3f}  {barra}")

    return modelo, imputer


def aplicar_a_todos(features: pd.DataFrame, modelo, imputer) -> pd.DataFrame:
    # Aplica el modelo a todos los usuarios
    X_todos = imputer.transform(features[FEATURES].values)

    # columna 1 = probabilidad de "validador_consistente"
    probabilidades = modelo.predict_proba(X_todos)

    resultado = features.copy()
    resultado["proba_patron_validador"] = probabilidades[:, 1]
    resultado["patron_similar"]         = resultado["proba_patron_validador"] >= UMBRAL_PROBA

    print("\n  Predicción por grupo (¿cuántos tienen patrón similar a validador?):")
    orden = ["validador_consistente", "coincidencia_intermedia",
             "transit_sin_match", "sin_uso_aparente"]
    for grupo in orden:
        sub = resultado[resultado["grupo"] == grupo]
        if len(sub) == 0:
            continue
        n_similar = sub["patron_similar"].sum()
        pct = 100 * n_similar / len(sub)
        print(f"    {grupo:<28}  {n_similar:>5} / {len(sub):>5}  ({pct:.1f}%)")

    return resultado
