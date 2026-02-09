"""
EXTRACT.PY
------------------------------------------------------------------------------
Módulo de Ingestión de Datos Híbrida.

Este módulo implementa una estrategia de lectura que combina el manejo de archivos
nativo de Python (io/itertools) con el motor de parseo de Polars.
Su objetivo principal es garantizar la continuidad del flujo de datos (Stream)
incluso ante la presencia de bytes corruptos o secuencias de codificación mixta
que detendrían a un lector CSV estándar.
"""

import io
import itertools

import polars as pl


class HybridSatReader:
    """
    Adaptador de lectura que bufferiza líneas de texto crudo utilizando los
    manejadores de codificación permisivos de Python ('replace') y alimenta
    a Polars con fragmentos de memoria saneados.

    Attributes:
        f (file object): Descriptor de archivo abierto con estrategia de reemplazo de errores.
        batch_size (int): Cantidad de líneas a procesar por iteración de lectura.
        header (str): Cabecera CSV preservada para reconstruir el contexto en cada lote.
    """

    def __init__(self, file_path: str, batch_size: int):
        """
        Inicializa el lector de archivos con codificación 'cp1252' y estrategia de recuperación.

        Args:
            file_path (str): Ruta absoluta al archivo fuente.
            batch_size (int): Número de filas a leer por lote.
        """
        # Se utiliza 'errors="replace"' para sustituir bytes ilegibles por el caracter
        # de reemplazo Unicode (\ufffd) sin interrumpir la ejecución.
        self.f = open(file_path, "r", encoding="cp1252", errors="replace")
        self.batch_size = batch_size

        try:
            self.header = next(self.f)
        except StopIteration:
            self.header = ""

    def next_batches(self, n: int = 1) -> list[pl.DataFrame] | None:
        """
        Recupera el siguiente bloque de líneas y lo convierte en un DataFrame de Polars.

        El proceso realiza una limpieza intermedia de codificación para asegurar
        que el buffer de memoria sea compatible con el parser C++ de Polars.

        Args:
            n (int): Número de lotes a recuperar (Mantenido por compatibilidad de interfaz).

        Returns:
            list[pl.DataFrame] | None: Lista conteniendo el DataFrame del lote actual,
                                       o None si se ha alcanzado el EOF (Fin de Archivo).
        """
        # Lectura eficiente de N líneas usando iteradores para minimizar impacto en RAM
        lines = list(itertools.islice(self.f, self.batch_size))

        if not lines:
            self.f.close()
            return None

        chunk_str = self.header + "".join(lines)

        # Re-codificación Crítica:
        # El caracter de reemplazo Unicode (\ufffd) generado por Python no existe en cp1252.
        # Se fuerza una nueva codificación con reemplazo ('?') para evitar errores de
        # 'charmap codec' al pasar el buffer a Polars.
        chunk_bytes = chunk_str.encode("cp1252", errors="replace")

        # Parseo del stream de bytes saneado
        df = pl.read_csv(
            io.BytesIO(chunk_bytes),
            separator="|",
            encoding="cp1252",
            ignore_errors=True,
            truncate_ragged_lines=True,
            # Se fuerza la inferencia como String para evitar errores de tipo
            # previos a la etapa de limpieza y transformación.
            infer_schema_length=0,
        )

        return [df]


def get_sat_reader(
    file_path: str, batch_size: int = 50000, schema=None
) -> HybridSatReader:
    """
    Función factoría para instanciar el HybridSatReader.

    Args:
        file_path (str): Ruta al archivo.
        batch_size (int): Tamaño del lote.
        schema (dict, optional): Esquema (no utilizado en modo híbrido texto).

    Returns:
        HybridSatReader: Instancia configurada del lector.
    """
    return HybridSatReader(file_path, batch_size)
