"""
CONFIG_MASTER.PY - CATÁLOGO MAESTRO DE METADATOS Y REGLAS DINÁMICAS
------------------------------------------------------------------------------
Este módulo actúa como el diccionario central de activos del SAT. Define la
cartografía de datos para los Anexos 1A hasta 7G, orquestando el destino
de las bases de datos y las reglas de tipado técnico (Schema Enforcement).

Estrategia de Tipado:
    Se utiliza un enfoque de 'Casteo Heurístico'. Las reglas aquí definidas
    permiten que el motor de transformación identifique columnas críticas
    (Fechas y Moneda) para realizar conversiones seguras desde texto crudo
    hacia tipos nativos de SQL Server (DATETIME2, DECIMAL).
"""

import polars as pl

# Identificador de periodo para versionamiento de tablas en SQL Server
SUFIJO = "_2025_1S"

# ----------------------------------------------------------------------------
# 1. MAPEO DE ENTIDADES (TABLAS Y RUTAS)
# ----------------------------------------------------------------------------
CONFIG_TABLAS = {
    "1A": {
        "db": "SAT_V2",
        "table": "ANEXO_1A",
        "file": "GERG_AECF_1891_Anexo1A-QA.txt",
    },
    "2B": {"db": "SAT_V2", "table": "ANEXO_2B", "file": "GERG_AECF_1891_Anexo2B.csv"},
    "3C": {
        "db": "SAT_Nómina_V2",
        "table": "ANEXO_3C",
        "file": "GERG_AECF_1891_Anexo3C.csv",
    },
    "4D": {
        "db": "SAT_Nómina_V2",
        "table": "ANEXO_4D",
        "file": "GERG_AECF_1891_Anexo4D.csv",
    },
    "5E": {
        "db": "SAT_Nómina_V2",
        "table": "ANEXO_5E",
        "file": "GERG_AECF_1891_Anexo5E.csv",
    },
    "6F": {
        "db": "SAT_Nómina_V2",
        "table": "ANEXO_6F",
        "file": "GERG_AECF_1891_Anexo6F.csv",
    },
    "7G": {
        "db": "SAT_Nómina_V2",
        "table": "ANEXO_7G",
        "file": "GERG_AECF_1891_Anexo7G.csv",
    },
}

# ----------------------------------------------------------------------------
# 2. REGLAS DE TIPADO DINÁMICO (ESQUEMA GLOBAL)
# ----------------------------------------------------------------------------
REGLAS_DINAMICAS = {
    # Identificadores Estructurales
    "UUID": pl.Utf8,
    "EMISORRFC": pl.Utf8,
    "RECEPTORRFC": pl.Utf8,
    # Cronología y Estampas de Tiempo (DateTime Parsing)
    "FECHAEMISION": pl.Datetime,
    "FECHACERTIFICACION": pl.Datetime,
    "FECHACANCELACION": pl.Datetime,
    "FECHAPAGO": pl.Datetime,
    "FECHAINICIALPAGO": pl.Datetime,
    "FECHAFINALPAGO": pl.Datetime,
    "RECEPTORFECHAINICIORELLABORAL": pl.Datetime,
    # Variables Financieras (Precision Decimal)
    "DESCUENTO": pl.Float64,
    "SUBTOTAL": pl.Float64,
    "TOTAL": pl.Float64,
    "TRASLADOSIVA": pl.Float64,
    "TRASLADOSIEPS": pl.Float64,
    "TOTALIMPUESTOSTRASLADADOS": pl.Float64,
    "RETENIDOSIVA": pl.Float64,
    "RETENIDOSISR": pl.Float64,
    "TOTALIMPUESTOSRETENIDOS": pl.Float64,
    "TIPOCAMBIO": pl.Float64,
    "CONCEPTOCANTIDAD": pl.Float64,
    "CONCEPTOVALORUNITARIO": pl.Float64,
    "CONCEPTOIMPORTE": pl.Float64,
    "NUMDIASPAGADOS": pl.Float64,
    "TOTALPERCEPCIONES": pl.Float64,
    "TOTALDEDUCCIONES": pl.Float64,
    "TOTALOTROSPAGOS": pl.Float64,
    "PERCEPCIONESTOTALGRAVADO": pl.Float64,
    "PERCEPCIONESTOTALEXENTO": pl.Float64,
    "TOTALOTRASDEDUCCIONES": pl.Float64,
    "NOMINATOTALIMPUESTOSRETENIDOS": pl.Float64,
    "EMISORENTIDADSNCFMONTORECURSOPROPIO": pl.Float64,
    # Atributos Específicos de Percepciones y Deducciones
    "PERCEPCIONIMPORTEGRAVADO": pl.Float64,
    "PERCEPCIONIMPORTEEXENTO": pl.Float64,
    "DEDUCCIONESIMPORTE": pl.Float64,
    "PERCEPCIONESTOTALSUELDOS": pl.Float64,
    "PERCEPCIONESTOTALSEPARACIONINDEMNIZACION": pl.Float64,
    "PERCEPCIONESTOTALJUBILACIONPENSIONRETIRO": pl.Float64,
    "JUBILACIONPENSIONRETIROTOTALUNAEXHIBICION": pl.Float64,
    "JUBILACIONPENSIONRETIROTOTALPARCIALIDAD": pl.Float64,
    "JUBILACIONPENSIONRETIROMONTODIARIO": pl.Float64,
    "JUBILACIONPENSIONRETIROINGRESOACUMULABLE": pl.Float64,
    "JUBILACIONPENSIONRETIROINGRESONOACUMULABLE": pl.Float64,
    "SEPARACIONINDEMNIZACIONTOTALPAGADO": pl.Float64,
    "SEPARACIONINDEMNIZACIONULTIMOSUELDOMENSORD": pl.Float64,
    "SEPARACIONINDEMNIZACIONINGRESOACUMULABLE": pl.Float64,
    "SEPARACIONINDEMNIZACIONINGRESONOACUMULABLE": pl.Float64,
    "IMPORTE": pl.Float64,
    "SUBSIDIOCAUSADO": pl.Float64,
}


# ----------------------------------------------------------------------------
# 3. MÉTODOS DE ACCESO A METADATOS
# ----------------------------------------------------------------------------
def obtener_meta(id_anexo: str) -> dict:
    """
    Recupera la configuración técnica para un identificador de anexo dado.

    Args:
        id_anexo (str): Código de la tabla (ej. '1A', '3C').

    Returns:
        dict: Diccionario con nombres de DB, tablas y archivos formateados.

    Raises:
        ValueError: Si el ID solicitado no existe en CONFIG_TABLAS.
    """
    meta = CONFIG_TABLAS.get(id_anexo)

    if not meta:
        raise ValueError(
            f"Fallo de Configuración: El Anexo '{id_anexo}' no está definido "
            "en el catálogo maestro de CONFIG_TABLAS."
        )

    return {
        "database": meta["db"],
        "table_name": f"{meta['table']}{SUFIJO}",
        "file_name": meta["file"],
    }
