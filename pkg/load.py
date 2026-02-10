"""
LOAD.PY - MOTOR DE PERSISTENCIA DINÁMICA EN SQL SERVER
------------------------------------------------------------------------------
Este módulo gestiona el ciclo de vida de la carga de datos (Load) en el
ecosistema SQL Server. Implementa una arquitectura de 'DDL Just-in-Time',
creando estructuras de tabla optimizadas bajo demanda para cualquier Anexo.

Capacidades Principales:
    1. Conectividad Adaptativa: Soporta routing dinámico hacia múltiples
       bases de datos (SAT_V2, SAT_Nómina_V2).
    2. Creación Estructural Inteligente: Infiere longitudes de negocio (RFC,
       UUID, CURP) y tipos de precisión decimal basándose en metadatos.
    3. Carga de Alto Rendimiento: Utiliza 'fast_executemany' de pyodbc para
       maximizar el throughput de inserción en lotes masivos.
    4. Integridad de Datos: Implementa verificaciones de existencia previas
       para evitar colisiones de Identificadores Únicos (UUID).
"""

from typing import Optional

import polars as pl
import pyodbc

from pkg.globals import (
    DB_DRIVER,
    DB_PASSWORD,
    DB_SERVER,
    DB_TRUSTED,
    DB_USER,
    SAT_DB_NAME,
)


def get_sql_connection(db_name: Optional[str] = None) -> pyodbc.Connection:
    """
    Establece una sesión con el servidor SQL priorizando el direccionamiento dinámico.

    Args:
        db_name (Optional[str]): Nombre de la base de datos objetivo.

    Returns:
        pyodbc.Connection: Objeto de conexión activa.
    """
    database = db_name if db_name else SAT_DB_NAME
    auth = (
        "Trusted_Connection=yes;"
        if DB_TRUSTED.upper() in ["YES", "TRUE", "1"]
        else f"UID={DB_USER};PWD={DB_PASSWORD};"
    )

    conn_str = (
        f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={database};"
        f"{auth}Encrypt=no;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def check_if_exists(cursor: pyodbc.Cursor, table_name: str, uuid_value: str) -> bool:
    """
    Verifica la pre-existencia de un registro para mantener la unicidad del lote.

    Args:
        cursor (pyodbc.Cursor): Cursor de base de datos activo.
        table_name (str): Tabla de destino.
        uuid_value (str): Identificador UUID a validar.

    Returns:
        bool: True si el registro ya reside en la base de datos.
    """
    try:
        query = f"SELECT TOP 1 1 FROM {table_name} WHERE UUID = ?"
        cursor.execute(query, (uuid_value,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def create_table_dynamic(cursor: pyodbc.Cursor, table_name: str, df: pl.DataFrame):
    """
    Orquesta la creación de la estructura de tabla basándose en patrones de
    negocio y tipos de datos nativos de Polars.

    Lógica de Inferencia:
        - Prioridad 1: Longitudes fijas de negocio (RFC, UUID, CURP).
        - Prioridad 2: Tipos temporales (DATETIME2).
        - Prioridad 3: Tipos financieros (DECIMAL) con escala adaptativa.
        - Fallback: Almacenamiento elástico (VARCHAR(MAX)).
    """
    column_defs = []

    # Diccionario de semántica de negocio para definición de longitudes óptimas
    business_lengths = {
        "RFC": "VARCHAR(13)",
        "UUID": "VARCHAR(36) NOT NULL PRIMARY KEY",
        "CURP": "VARCHAR(18)",
        "MONEDA": "VARCHAR(10)",
        "TIPO": "VARCHAR(10)",
        "METODOPAGO": "VARCHAR(5)",
        "FORMAPAGO": "VARCHAR(5)",
        "SERIE": "VARCHAR(50)",
        "FOLIO": "VARCHAR(50)",
        "NUMEMPLEADO": "VARCHAR(50)",
        "BANCO": "VARCHAR(10)",
        "CAMBIO": "DECIMAL(18,4)",
    }

    for col in df.columns:
        col_up = col.upper()
        dtype = df[col].dtype

        # A. Validación por Semántica de Negocio
        found_length = next(
            (val for key, val in business_lengths.items() if key in col_up), None
        )

        if found_length:
            column_defs.append(f"[{col}] {found_length}")

        # B. Validación de Estampas de Tiempo
        elif dtype == pl.Datetime:
            column_defs.append(f"[{col}] DATETIME2(0)")

        # C. Validación de Tipos Numéricos y Financieros
        elif dtype == pl.Float64:
            # Escala extendida para tipos de cambio, estándar para montos
            precision = "18,4" if "CAMBIO" in col_up else "18,2"
            column_defs.append(f"[{col}] DECIMAL({precision})")

        # D. Almacenamiento de Texto Generalista
        else:
            column_defs.append(f"[{col}] VARCHAR(MAX)")

    # Construcción de DDL con Indexación automática por RFC
    ddl = (
        f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}') "
        f"BEGIN CREATE TABLE [dbo].[{table_name}] ({', '.join(column_defs)}); "
        f"IF EXISTS (SELECT 1 FROM sys.columns WHERE name LIKE '%RFC%' AND object_id = OBJECT_ID('{table_name}')) "
        f"CREATE INDEX IX_{table_name}_RFC ON {table_name} (EmisorRFC); END"
    )

    cursor.execute(ddl)


def upload_to_sql_blindado(
    df: pl.DataFrame, table_name: str, db_name: Optional[str] = None
) -> bool:
    """
    Ejecuta la inserción masiva de datos con una política de 'Carga Blindada'.

    Args:
        df (pl.DataFrame): Lote de datos normalizado.
        table_name (str): Tabla destino.
        db_name (Optional[str]): Base de datos destino específica.

    Returns:
        bool: Estado de éxito de la operación.
    """
    if df.is_empty():
        return False

    conn = get_sql_connection(db_name)
    try:
        cursor = conn.cursor()
        # Optimización de throughput para pyodbc
        cursor.fast_executemany = True

        # Asegurar infraestructura de tabla previa a la carga
        create_table_dynamic(cursor, table_name, df)
        conn.commit()

        # Validación de integridad para evitar colisiones de PK en el lote
        primer_uuid = df["UUID"][0]
        if check_if_exists(cursor, table_name, primer_uuid):
            return False

        # Preparación de sentencia de inserción atómica
        cols = df.columns
        placeholders = ",".join(["?"] * len(cols))
        insert_sql = (
            f"INSERT INTO {table_name} ([{'],['.join(cols)}]) VALUES ({placeholders})"
        )

        # Ingesta masiva
        rows = list(df.iter_rows())
        cursor.executemany(insert_sql, rows)
        conn.commit()

        return True
    finally:
        conn.close()
