"""
REPORTS.PY - SISTEMA DE TELEMETRÍA, AUDITORÍA Y CONTROL DE CALIDAD
------------------------------------------------------------------------------
Este módulo gestiona la observabilidad del pipeline ETL. Supervisa el rendimiento
del motor de procesamiento y ejecuta auditorías heurísticas para detectar
degradación en la calidad de la información.

Capacidades de Auditoría:
    1. Telemetría de Rendimiento: Cálculo de throughput (filas/seg) y latencia.
    2. Detección de Mojibake: Identificación de artefactos de codificación mediante
       patrones regex optimizados.
    3. Validación Estructural: Monitoreo de integridad (nulos) y detección de
       mutilación de cadenas (texto excesivamente corto).
    4. Persistencia Forense: Generación de logs detallados con muestras de evidencia
       para la mejora continua de las reglas de limpieza.
"""

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Set

import polars as pl

from pkg.globals import LOG_DIR


def imprimir_barra_progreso(
    actual: int, total: int, prefijo: str = "", longitud: int = 40
):
    """
    Renderiza una barra de progreso visual en la consola para monitoreo interactivo.
    """
    if total == 0:
        return
    porcentaje = "{0:.1f}".format(100 * (actual / float(total)))
    llenado = int(longitud * actual // total)
    barra = "█" * llenado + "-" * (longitud - llenado)
    sys.stdout.write(f"\r{prefijo} |{barra}| {porcentaje}%")
    sys.stdout.flush()


class ETLReport:
    """
    Controlador de telemetría y auditoría dinámica para procesos multitabla.
    """

    def __init__(self):
        """
        Inicializa contadores de rendimiento y configuraciones de auditoría.
        """
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0
        self.alerts_mojibake = 0
        self.alerts_length = 0
        self.alerts_nulls = 0
        self.samples_mojibake: Set[str] = set()
        self.samples_length: Set[str] = set()

        # Patrón heurístico para identificar fallos de encoding (Latin-1 vs UTF-8)
        self.regex_mojibake = (
            r"[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿Ðð]"
        )
        self.min_text_length = 3

    def audit_batch(self, df: pl.DataFrame):
        """
        Ejecuta una auditoría de calidad selectiva sobre columnas descriptivas.

        Lógica de Seguridad de Tipos:
            Filtra automáticamente columnas que coincidan con keywords descriptivas
            pero asegura que el análisis solo se aplique si el tipo es pl.Utf8,
            evitando errores de ejecución en columnas ya tipadas (Float/Datetime).
        """
        keywords = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "PUESTO", "DEPARTAMENTO"]

        # Selección dinámica de columnas candidatas para auditoría de texto
        cols_audit = [
            c
            for c in df.columns
            if any(k in c.upper() for k in keywords) and df[c].dtype == pl.Utf8
        ]

        if not cols_audit:
            return

        for col in cols_audit:
            # 1. Monitoreo de Integridad (Valores Nulos)
            n_nulls = df.filter(pl.col(col).is_null()).height
            if n_nulls > 0:
                self.alerts_nulls += n_nulls

            df_valid = df.filter(pl.col(col).is_not_null())
            if df_valid.height == 0:
                continue

            # 2. Análisis de Corrupción de Caracteres (Mojibake)
            suspect_mojibake = df_valid.filter(
                pl.col(col).str.contains(self.regex_mojibake)
            )
            if suspect_mojibake.height > 0:
                self.alerts_mojibake += suspect_mojibake.height
                # Recolección de evidencia única para el reporte final
                samples = (
                    suspect_mojibake.select(col).unique().head(5).to_series().to_list()
                )
                self.samples_mojibake.update([f"[{col}] {s}" for s in samples])

            # 3. Análisis de Mutilación Estructural
            suspect_len = df_valid.filter(
                pl.col(col).str.len_chars() < self.min_text_length
            )
            if suspect_len.height > 0:
                self.alerts_length += suspect_len.height
                samples = suspect_len.select(col).unique().head(3).to_series().to_list()
                self.samples_length.update([f"[{col}] {s}" for s in samples])

    def update_metrics(self, rows_count: int):
        """
        Actualiza los contadores globales y reporta velocidad de procesamiento.
        """
        self.total_rows += rows_count
        self.total_batches += 1
        elapsed = time.time() - self.start_time
        speed = self.total_rows / (elapsed + 1e-6)
        print(
            f"[INFO] Telemetry -> Rows: {self.total_rows:,.0f} | Speed: {speed:,.0f} rows/sec"
        )

    def generate_final_report(
        self,
        id_anexo: str = "UNKNOWN",
        status: str = "SUCCESS",
        error_details: str = "",
    ) -> Path:
        """
        Genera el artefacto final de auditoría en disco.

        Returns:
            Path: Ruta del archivo de log generado.
        """
        total_duration = (time.time() - self.start_time) / 60
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"AUDIT_{id_anexo}_{timestamp}.txt"
        report_path = LOG_DIR / log_filename

        lines = [
            "=" * 80,
            f"SAT ETL AUDIT REPORT - ANEXO {id_anexo}",
            "=" * 80,
            f"Execution Status: {status}",
            f"Total Duration:   {total_duration:.2f} minutes",
            f"Processed Rows:   {self.total_rows:,.0f}",
            "-" * 80,
            "QUALITY SUMMARY",
            "-" * 80,
            f"Encoding Alerts (Mojibake): {self.alerts_mojibake}",
            f"Integrity Alerts (Nulls):    {self.alerts_nulls}",
            f"Mutilation Alerts (Short):   {self.alerts_length}",
            "=" * 80,
        ]

        if self.samples_mojibake:
            lines.append("\n[EVIDENCE] MOJIBAKE SAMPLES DETECTED:")
            lines.extend([f"  > {s}" for s in sorted(list(self.samples_mojibake))[:50]])

        if error_details:
            lines.append(f"\n[CRITICAL ERROR DETAILS]\n{error_details}")

        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            return report_path
        except Exception as e:
            print(f"[ERROR] No se pudo escribir el reporte de auditoría: {e}")
            return Path("ERROR")
