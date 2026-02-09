import gc
import time

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


def get_sql_connection(db_name=None):
    """
    Establece la conexion con SQL Server utilizando los parametros de configuracion globales.
    Parametros:
        db_name (str): Nombre de la base de datos (opcional).
    Salida:
        pyodbc.Connection: Objeto de conexion activa.
    """
    database = db_name if db_name else SAT_DB_NAME
    auth = (
        "Trusted_Connection=yes;"
        if DB_TRUSTED.upper() in ["YES", "TRUE", "1"]
        else f"UID={DB_USER};PWD={DB_PASSWORD};"
    )
    conn_str = f"DRIVER={DB_DRIVER};SERVER={DB_SERVER};DATABASE={database};{auth}Encrypt=no;TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)


def check_if_exists(cursor, table_name, uuid_value):
    """
    Consulta la existencia de un identificador unico en la tabla destino.
    Parametros:
        cursor (pyodbc.Cursor): Cursor de la conexion activa.
        table_name (str): Nombre de la tabla a consultar.
        uuid_value (str): Valor del UUID a buscar.
    Salida:
        bool: True si el registro existe, False en caso contrario.
    """
    try:
        query = f"SELECT TOP 1 1 FROM {table_name} WHERE UUID = ?"
        cursor.execute(query, (uuid_value,))
        return cursor.fetchone() is not None
    except Exception:
        return False


def create_table_sat(cursor, table_name):
    """
    Ejecuta el DDL para la creacion de la tabla y sus indices si no existen.
    Parametros:
        cursor (pyodbc.Cursor): Cursor de la conexion activa.
        table_name (str): Nombre de la tabla a crear.
    """
    ddl = f"""
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}')
    BEGIN
        CREATE TABLE [dbo].[{table_name}] (
            [UUID] VARCHAR(36) NOT NULL PRIMARY KEY,
            [FechaEmision] DATETIME2(0),
            [Descuento] DECIMAL(18,2),
            [SubTotal] DECIMAL(18,2),
            [Total] DECIMAL(18,2),
            [TrasladosIVA] DECIMAL(18,2),
            [TrasladosIEPS] DECIMAL(18,2),
            [TotalImpuestosTrasladados] DECIMAL(18,2),
            [RetenidosIVA] DECIMAL(18,2),
            [RetenidosISR] DECIMAL(18,2),
            [TotalImpuestosRetenidos] DECIMAL(18,2),
            [TipoCambio] DECIMAL(18,4),
            [Moneda] VARCHAR(10),
            [TipoDeComprobante] VARCHAR(5),
            [FechaCertificacion] DATETIME2(0),
            [EmisorRFC] VARCHAR(13), 
            [ReceptorRFC] VARCHAR(13),
            [Serie] VARCHAR(50),
            [Folio] VARCHAR(50),
            [FormaPago] VARCHAR(10),
            [NoCertificado] VARCHAR(30),
            [CondicionesDePago] VARCHAR(MAX),
            [MetodoPago] VARCHAR(10),
            [LugarExpedicion] VARCHAR(10),
            [EmisorRegimenFiscal] VARCHAR(10),
            [ReceptorNombre] VARCHAR(300),
            [EmisorNombre] VARCHAR(300),
            [FechaCancelacion] DATETIME2(0) NULL
        );
        CREATE INDEX IX_{table_name}_Emisor ON {table_name} (EmisorRFC);
        CREATE INDEX IX_{table_name}_Receptor ON {table_name} (ReceptorRFC);
    END
    """
    cursor.execute(ddl)


def upload_to_sql_blindado(df: pl.DataFrame, table_name: str):
    """
    Realiza la carga masiva de datos mediante micro-lotes y confirmacion de transaccion.
    Parametros:
        df (pl.DataFrame): Conjunto de datos a insertar.
        table_name (str): Tabla destino en la base de datos.
    Salida:
        bool: True si la insercion fue exitosa, False si el lote fue omitido.
    """
    if df.is_empty():
        return False

    conn = get_sql_connection()
    try:
        cursor = conn.cursor()
        cursor.fast_executemany = True

        create_table_sat(cursor, table_name)
        conn.commit()

        primer_uuid = df["UUID"][0]
        if check_if_exists(cursor, table_name, primer_uuid):
            return False

        cols = df.columns
        placeholders = ",".join(["?"] * len(cols))
        insert_sql = (
            f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"
        )

        rows = list(df.iter_rows())

        cursor.executemany(insert_sql, rows)
        conn.commit()

        time.sleep(0.1)
        del rows
        gc.collect()

        return True

    except Exception as e:
        print(f"Error en carga masiva: {e}")
        raise e
    finally:
        conn.close()
