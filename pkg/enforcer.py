"""
ENFORCER.PY - MÓDULO DE INTEGRIDAD ESTRUCTURAL Y TIPADO DINÁMICO
------------------------------------------------------------------------------
Este módulo es responsable de la normalización técnica de los DataFrames.
Implementa una estrategia de 'Inferencia por Patrones' que permite adaptar
automáticamente el esquema de cualquier Anexo (1A a 7G) a los tipos nativos
requeridos por SQL Server.

Funcionalidades Principales:
    1. Schema Enforcement: Convierte cadenas de texto en objetos Datetime
       y Float64 basados en palabras clave detectadas en los encabezados.
    2. Sanitización de Metadatos: Normaliza los nombres de las columnas
       eliminando caracteres especiales y diacríticos para garantizar
       la compatibilidad con el motor de base de datos.
    3. Resiliencia de Datos: Utiliza casteo no estricto (strict=False) para
       transformar anomalías de datos en valores nulos (NULL), evitando
       la interrupción del pipeline masivo.
"""

from typing import Any, Dict

import polars as pl


def aplicar_tipos_seguros(
    df: pl.DataFrame, reglas_globales: Dict[str, Any]
) -> pl.DataFrame:
    """
    Analiza la estructura del lote y aplica conversiones de tipo basadas en
    la semántica del nombre de cada columna.

    Estrategia de Transformación:
        - Fechas: Recorte ISO (YYYY-MM-DD HH:MM:SS) y conversión a Datetime.
        - Financieros: Limpieza de espacios y casteo a Float64 (Decimal en SQL).
        - Cantidades: Identificación de métricas de volumen y días.

    Args:
        df (pl.DataFrame): Lote de datos en formato Utf8.
        reglas_globales (Dict[str, Any]): Reservado para mapeos explícitos opcionales.

    Returns:
        pl.DataFrame: DataFrame con tipos de datos normalizados.
    """
    if df.height == 0:
        return df

    expresiones_casteo = []

    for col in df.columns:
        col_up = col.upper()

        # --- INFERENCIA DINÁMICA DE TIPOS ---

        # Patrón 1: Identificación de Atributos Temporales
        if "FECHA" in col_up:
            # Se normaliza la longitud a 19 caracteres para estandarizar el parseo
            expresiones_casteo.append(
                pl.col(col).str.slice(0, 19).str.to_datetime(strict=False).alias(col)
            )

        # Patrón 2: Identificación de Atributos Financieros (Precision Decimal)
        elif any(
            key in col_up
            for key in [
                "TOTAL",
                "IMPORTE",
                "SUBTOTAL",
                "DESCUENTO",
                "TRASLADOS",
                "RETENIDOS",
                "MONTO",
                "VALOR",
                "SALDO",
            ]
        ):
            expresiones_casteo.append(
                pl.col(col).str.strip_chars().cast(pl.Float64, strict=False).alias(col)
            )

        # Patrón 3: Identificación de Métricas de Volumen y Días
        elif "DIAS" in col_up or "CANTIDAD" in col_up:
            expresiones_casteo.append(
                pl.col(col).str.strip_chars().cast(pl.Float64, strict=False).alias(col)
            )

    # Aplicación masiva de transformaciones de esquema
    return df.with_columns(expresiones_casteo) if expresiones_casteo else df


def estandarizar_nombres_columnas(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normaliza los encabezados del DataFrame para eliminar inconsistencias
    propias de los archivos fuente del SAT.

    Proceso de Limpieza:
        1. Eliminación de espacios perimetrales y conversión a minúsculas.
        2. Remoción de comillas y caracteres de escape.
        3. Sustitución de espacios por guiones bajos y remoción de puntos.
        4. Traducción de caracteres con acento a su equivalente ASCII.

    Args:
        df (pl.DataFrame): DataFrame con encabezados originales.

    Returns:
        pl.DataFrame: DataFrame con identificadores de columna sanitizados.
    """
    mapping_acentos = str.maketrans("áéíóú", "aeiou")
    nuevas_columnas = []

    for col in df.columns:
        # Normalización semántica de identificadores
        clean_name = col.strip().lower()
        clean_name = clean_name.replace('"', "").replace("'", "")
        clean_name = clean_name.replace(" ", "_").replace(".", "")
        clean_name = clean_name.translate(mapping_acentos)
        nuevas_columnas.append(clean_name)

    return df.rename(dict(zip(df.columns, nuevas_columnas)))
