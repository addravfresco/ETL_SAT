"""
ENFORCER.PY - MODULO SAT
Asegura la integridad estructural de los lotes mediante la aplicacion de esquemas
estrictos y la normalizacion de metadatos de columnas.
"""

from typing import Any, Dict

import polars as pl


def aplicar_tipos_seguros(df: pl.DataFrame, reglas: Dict[str, Any]) -> pl.DataFrame:
    """
    Normaliza los tipos de datos en el DataFrame segun un esquema predefinido.
    Utiliza una estrategia de casteo no estricto para preservar la ejecucion.
    Parametros:
        df (pl.DataFrame): Lote de datos objetivo.
        reglas (dict): Mapeo de {columna: tipo_polars}.
    Salida:
        pl.DataFrame: Lote con tipos de datos normalizados.
    """
    if df.height == 0:
        return df

    expresiones_casteo = []
    # Normalizacion de mapa para resolver colisiones de nomenclatura
    col_map = {c.lower().strip().replace('"', ""): c for c in df.columns}

    for col_objetivo, tipo_dato in reglas.items():
        # Busqueda insensible a mayusculas/minusculas
        col_real = col_map.get(col_objetivo.lower().replace('"', ""))

        if col_real:
            # Los errores de parseo se transforman en null para auditoria posterior
            # strict=False evita que el programa se detenga por datos corruptos
            expresiones_casteo.append(
                pl.col(col_real).cast(tipo_dato, strict=False).alias(col_real)
            )

    return df.with_columns(expresiones_casteo) if expresiones_casteo else df


def estandarizar_nombres_columnas(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normaliza los encabezados del DataFrame para asegurar compatibilidad tecnica.
    Proceso: Eliminacion de espacios, caracteres de escape y diacriticos.
    Parametros:
        df (pl.DataFrame): DataFrame original.
    Salida:
        pl.DataFrame: DataFrame con encabezados estandarizados.
    """
    mapping_acentos = str.maketrans("áéíóú", "aeiou")
    nuevas_columnas = []

    for col in df.columns:
        clean_name = col.strip().lower()
        clean_name = clean_name.replace('"', "").replace("'", "")
        clean_name = clean_name.replace(" ", "_").replace(".", "")
        clean_name = clean_name.translate(mapping_acentos)
        nuevas_columnas.append(clean_name)

    return df.rename(dict(zip(df.columns, nuevas_columnas)))
