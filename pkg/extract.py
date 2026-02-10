"""
EXTRACT.PY - MÓDULO DE INGESTIÓN DE DATOS HÍBRIDA (RESILIENTE)
------------------------------------------------------------------------------
Este módulo implementa una arquitectura de lectura en dos etapas para
estabilizar la ingesta de archivos masivos del SAT (Anexos 1A a 7G).

Estrategia Técnica:
    1. Saneamiento Nativo (Python): Utiliza manejadores de archivos con
       estrategia de error 'replace' para neutralizar bytes corruptos o
       secuencias de codificación mixta (UTF-8/CP1252).
    2. Parseo de Alto Rendimiento (Polars): Ingiere fragmentos de memoria
       saneados forzando el tipo 'String' (Utf8) para todas las columnas,
       delegando el tipado final a la etapa de 'Enforcement'.

Ventaja Arquitectónica:
    Evita colapsos en el motor C++ de Polars ante inconsistencias de
    codificación, garantizando la continuidad del flujo de datos (Stream).
"""

import io
import itertools
from typing import List, Optional

import polars as pl


class HybridSatReader:
    """
    Adaptador de lectura bufferizada que actúa como puente entre el sistema
    de archivos y el motor de Polars.
    """

    def __init__(self, file_path: str, batch_size: int):
        """
        Inicializa el descriptor de archivo con políticas de recuperación.

        Args:
            file_path (str): Ruta absoluta o relativa al recurso.
            batch_size (int): Volumen de filas a procesar por iteración.
        """
        # Apertura permisiva para evitar excepciones por 'charmap' codecs
        self.f = open(file_path, "r", encoding="cp1252", errors="replace")
        self.batch_size = batch_size

        # Preservación de la cabecera para reconstrucción de contexto en chunks
        try:
            self.header = next(self.f)
        except StopIteration:
            self.header = ""

    def next_batches(self, n: int = 1) -> Optional[List[pl.DataFrame]]:
        """
        Extrae el siguiente lote de datos y lo transforma en un DataFrame.

        Implementa una re-codificación de seguridad para asegurar que el
        buffer de memoria sea compatible con el parser de Polars.

        Returns:
            Optional[List[pl.DataFrame]]: Lista con el lote procesado o
                                          None si se alcanza el fin del archivo.
        """
        # Extracción eficiente de líneas vía iteradores (Lazy Load)
        lines = list(itertools.islice(self.f, self.batch_size))

        if not lines:
            self.f.close()
            return None

        # Reconstrucción del fragmento de memoria
        chunk_str = self.header + "".join(lines)

        # Saneamiento de bytes crítico para estabilidad del parser
        chunk_bytes = chunk_str.encode("cp1252", errors="replace")

        # Ingesta masiva forzando esquema neutro (Utf8)
        df = pl.read_csv(
            io.BytesIO(chunk_bytes),
            separator="|",
            encoding="cp1252",
            ignore_errors=True,
            truncate_ragged_lines=True,
            # Se desactiva la inferencia para permitir tipado dinámico posterior
            infer_schema_length=0,
        )

        return [df]


def get_sat_reader(
    file_path: str, batch_size: int = 50000, schema=None
) -> HybridSatReader:
    """
    Función factoría para la instanciación estandarizada del lector híbrido.

    Args:
        file_path (str): Ubicación del archivo fuente.
        batch_size (int): Tamaño del lote (default: 50,000 registros).
        schema: Reservado para compatibilidad con interfaces futuras.

    Returns:
        HybridSatReader: Instancia configurada para el procesamiento masivo.
    """
    return HybridSatReader(file_path, batch_size)
