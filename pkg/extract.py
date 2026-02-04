import polars as pl
from pkg.globals import *

def get_sat_reader(ruta_archivo, batch_size=50000, schema=None):
    """
    Crea un objeto de lectura por lotes (batched) para archivos CSV/TXT.
    Parametros:
        ruta_archivo (str): Ruta local o de red del archivo fuente.
        batch_size (int): Cantidad de registros por lote.
        schema (dict): Definicion de tipos de datos para las columnas.
    Salida:
        pl.BatchedCsvReader: Objeto para iteracion de datos masivos.
    """
    return pl.read_csv_batched(
        ruta_archivo,
        separator="|",
        encoding="utf8-lossy",
        batch_size=batch_size,
        schema_overrides=schema,
        ignore_errors=True,
        truncate_ragged_lines=True
    )

def get_sample_data(reader, num_batches=1):
    """
    Extrae una muestra inicial de lotes para validacion de estructura.
    Parametros:
        reader (pl.BatchedCsvReader): Lector inicializado.
        num_batches (int): Numero de lotes a extraer.
    Salida:
        pl.DataFrame: Datos de la muestra o None si no hay datos.
    """
    chunks = reader.next_batches(num_batches)
    return chunks[0] if chunks else None