import polars as pl
from pkg.globals import *
from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE

def clean_text_field(col_name: str) -> pl.Expr:
    """
    Normaliza campos de texto: convierte a mayusculas, elimina espacios 
    perimetrales y sustituye caracteres especiales segun el diccionario de limpieza.
    Parametros:
        col_name (str): Nombre de la columna a procesar.
    Salida:
        pl.Expr: Expresion de Polars con las reglas aplicadas.
    """
    expr = pl.col(col_name).str.to_uppercase().str.strip_chars()
    
    for patron, reemplazo in REEMPLAZOS_MOJIBAKE.items():
        expr = expr.str.replace(patron, reemplazo, literal=True)
    
    return expr.alias(col_name)

def fix_monetary_values(cols_dinero: list) -> list:
    """
    Normaliza campos numericos sustituyendo valores nulos por cero.
    Parametros:
        cols_dinero (list): Lista de nombres de columnas monetarias.
    Salida:
        list: Lista de expresiones pl.Expr para transformacion selectiva.
    """
    return [pl.col(col).fill_null(0.0) for col in cols_dinero]

def standardize_dates(cols_fechas: list) -> list:
    """
    Prepara campos de fecha eliminando espacios y normalizando cadenas vacias a nulos.
    Parametros:
        cols_fechas (list): Lista de nombres de columnas de tipo fecha.
    Salida:
        list: Lista de expresiones pl.Expr.
    """
    return [
        pl.col(col).cast(pl.Utf8).str.strip_chars().replace("", None) 
        for col in cols_fechas
    ]

def transform_sat_batch(df: pl.DataFrame) -> pl.DataFrame:
    """
    Orquestador de transformaciones para lotes de datos del SAT.
    Ejecuta la limpieza de texto, moneda y fechas en un solo paso de computo.
    Parametros:
        df (pl.DataFrame): Lote de datos original.
    Salida:
        pl.DataFrame: Lote de datos procesado segun reglas de negocio.
    """
    cols_texto = ['EmisorNombre', 'ReceptorNombre', 'CondicionesDePago', 'Serie', 'Folio']
    cols_dinero = [
        'Descuento', 'SubTotal', 'Total', 'TrasladosIVA', 'TrasladosIEPS', 
        'TotalImpuestosTrasladados', 'RetenidosIVA', 'RetenidosISR', 
        'TotalImpuestosRetenidos', 'TipoCambio'
    ]
    cols_fechas = ['FechaEmision', 'FechaCertificacion', 'FechaCancelacion']

    return df.with_columns([
        *[clean_text_field(c) for c in cols_texto if c in df.columns],
        *fix_monetary_values([c for c in cols_dinero if c in df.columns]),
        *standardize_dates([c for c in cols_fechas if c in df.columns])
    ])