"""
TRANSFORM.PY
------------------------------------------------------------------------------
Módulo de Limpieza y Normalización de Datos.

Este módulo aplica reglas de negocio y corrección de codificación (Mojibake).
Utiliza una estrategia de mapeo nativo de Python (map_elements) para la limpieza
intensiva de texto, lo cual ofrece mayor estabilidad frente a diccionarios de
reemplazo extensos (>500 reglas) que causarían 'Stack Overflow' en el optimizador
de consultas de Polars.
"""

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def limpiar_texto_python(texto: str | None) -> str | None:
    """
    Aplica secuencialmente el diccionario maestro de reemplazos sobre una cadena.

    Esta función se ejecuta a nivel de fila (Python GIL) para garantizar la
    estabilidad de memoria durante la aplicación de cientos de reglas regex/string.

    Args:
        texto (str | None): Cadena de texto original con posibles artefactos.

    Returns:
        str | None: Cadena saneada o None si la entrada es nula.
    """
    if texto is None:
        return None

    texto_limpio = texto

    # Iteración sobre el diccionario maestro de reglas forenses.
    # El orden de aplicación está determinado por la definición en cleaning_rules.py
    for sucio, limpio in REEMPLAZOS_MOJIBAKE.items():
        if sucio in texto_limpio:
            texto_limpio = texto_limpio.replace(sucio, limpio)

    return texto_limpio


def transform_sat_batch(df: pl.DataFrame) -> pl.DataFrame:
    """
    Orquesta las transformaciones aplicables a un lote de datos SAT.

    Fases:
    1. Normalización Estándar: Eliminación de espacios y conversión a mayúsculas (Motor Polars).
    2. Saneamiento de Mojibake: Corrección de codificación en nombres (Motor Python Nativo).

    Args:
        df (pl.DataFrame): DataFrame con tipos de datos crudos (String).

    Returns:
        pl.DataFrame: DataFrame transformado y limpio.
    """
    # Fase 1: Normalización de alto rendimiento (Polars Native)
    # Se aplica a todas las columnas de tipo String (Utf8)
    df = df.with_columns(
        pl.col(pl.Utf8).str.strip_chars().str.to_uppercase().name.keep()
    )

    # Fase 2: Limpieza Quirúrgica de Nombres (Python Native)
    # Se utiliza map_elements con return_dtype explícito para delegar el procesamiento
    if "ReceptorNombre" in df.columns:
        df = df.with_columns(
            pl.col("ReceptorNombre")
            .map_elements(limpiar_texto_python, return_dtype=pl.Utf8)
            .alias("ReceptorNombre")
        )

    # Se aplica la misma lógica para el Emisor si la columna existe
    if "EmisorNombre" in df.columns:
        df = df.with_columns(
            pl.col("EmisorNombre")
            .map_elements(limpiar_texto_python, return_dtype=pl.Utf8)
            .alias("EmisorNombre")
        )

    return df
