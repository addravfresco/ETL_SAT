"""
TRANSFORM.PY - MÓDULO DE SANEAMIENTO Y NORMALIZACIÓN DINÁMICA
------------------------------------------------------------------------------
Este módulo orquesta la limpieza profunda de los datos, especializándose en la
corrección de artefactos de codificación (Mojibake) y la estandarización de
campos de texto para los Anexos 1A a 7G.

Estrategia de Procesamiento Híbrido:
    1. Motor Polars (Nativo): Ejecuta normalizaciones masivas de alta velocidad
       como el truncado de espacios (Trim) y conversión a mayúsculas.
    2. Motor Python (Fila a Fila): Gestiona la limpieza quirúrgica de Mojibake
       mediante un diccionario maestro de reglas. Se utiliza 'map_elements' para
       evitar el desbordamiento de pila (Stack Overflow) en el planificador de
       Polars cuando los diccionarios de reemplazo son extensos (>500 reglas).

Detección Dinámica:
    Identifica automáticamente columnas sensibles (Nombres, Conceptos, Puestos)
    para aplicar el saneamiento sin requerir configuraciones manuales por anexo.
"""

from typing import Optional

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def limpiar_texto_python(texto: Optional[str]) -> Optional[str]:
    """
    Aplica el catálogo maestro de reglas forenses sobre una cadena de texto.
    Diseñada para estabilizar caracteres corruptos derivados de inconsistencias
    entre UTF-8 y CP1252.

    Args:
        texto (Optional[str]): Cadena original con posibles artefactos.

    Returns:
        Optional[str]: Texto saneado o None si la entrada es nula.
    """
    if texto is None:
        return None

    texto_limpio = texto

    # Iteración exhaustiva sobre el diccionario de evidencias Mojibake
    for sucio, limpio in REEMPLAZOS_MOJIBAKE.items():
        if sucio in texto_limpio:
            texto_limpio = texto_limpio.replace(sucio, limpio)

    return texto_limpio


def transform_sat_batch(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ejecuta el pipeline de transformación sobre un lote de datos SAT.

    Fases:
        1. Normalización Global: Estandariza todas las columnas de texto (Utf8)
           eliminando espacios y forzando mayúsculas.
        2. Saneamiento Forense: Aplica la corrección de Mojibake exclusivamente
           en columnas descriptivas detectadas por palabras clave.

    Args:
        df (pl.DataFrame): Lote de datos crudos (Strings).

    Returns:
        pl.DataFrame: DataFrame normalizado y saneado.
    """
    # Fase 1: Normalización de alto rendimiento utilizando el motor nativo de Polars
    df = df.with_columns(
        pl.col(pl.Utf8).str.strip_chars().str.to_uppercase().name.keep()
    )

    # Fase 2: Identificación Automática de Columnas Sensibles
    # Buscamos columnas descriptivas que suelen contener errores de codificación
    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "PUESTO", "DEPARTAMENTO"]

    cols_a_limpiar = [
        col for col in df.columns if any(key in col.upper() for key in palabras_clave)
    ]

    # Aplicación de limpieza quirúrgica mediante el motor de Python (estabilidad de memoria)
    if cols_a_limpiar:
        df = df.with_columns(
            [
                pl.col(c)
                .map_elements(limpiar_texto_python, return_dtype=pl.Utf8)
                .alias(c)
                for c in cols_a_limpiar
            ]
        )

    return df
