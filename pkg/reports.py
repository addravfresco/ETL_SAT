"""
REPORTS.PY - MÓDULO DE TELEMETRÍA Y AUDITORÍA
Este módulo gestiona la generación de reportes de ejecución, monitoreo de rendimiento
y auditoría de calidad de datos en tiempo real.
"""

import sys
import time
from datetime import datetime

import polars as pl

from pkg.globals import LOG_DIR


def imprimir_barra_progreso(actual, total, prefijo="", longitud=40):
    """
    Renderiza una barra de progreso textual en la salida estándar (stdout).
    Útil para visualizar el avance de operaciones largas sin saturar el log.

    Args:
        actual (int): Contador actual de progreso.
        total (int): Total objetivo (100%).
        prefijo (str): Texto descriptivo a mostrar antes de la barra.
        longitud (int): Cantidad de caracteres que ocupará la barra visual.
    """
    if total == 0:
        return

    porcentaje = "{0:.1f}".format(100 * (actual / float(total)))
    llenado = int(longitud * actual // total)
    # Utilizamos caracteres ASCII estándar para compatibilidad universal
    barra = "█" * llenado + "-" * (longitud - llenado)

    # \r permite sobrescribir la línea actual en la terminal
    sys.stdout.write(f"\r{prefijo} |{barra}| {porcentaje}%")
    sys.stdout.flush()


class ETLReport:
    """
    Controlador central de métricas y auditoría.

    Responsabilidades:
    1. Calcular velocidad de procesamiento (Throughput).
    2. Detectar anomalías en datos de texto (Encoding, Truncamiento).
    3. Registrar evidencias de errores para corrección posterior.
    4. Persistir el resumen de ejecución en disco.
    """

    def __init__(self):
        """
        Inicializa contadores de métricas y estructuras de almacenamiento de evidencias.
        """
        # Métricas temporales
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0

        # Contadores de Calidad de Datos
        self.alerts_mojibake = 0
        self.alerts_length = 0
        self.alerts_nulls = 0

        # Repositorios de Evidencia (Set garantiza unicidad de las muestras)
        self.samples_mojibake = set()
        self.samples_length = set()

        # Configuración de Detección Heurística
        # Regex para identificar artefactos de codificación (Latin-1 vs UTF-8)
        self.regex_mojibake = (
            r"[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿Ðð]"
        )

        # Umbral mínimo de caracteres para considerar un texto válido
        self.min_text_length = 3

    def audit_batch(self, df: pl.DataFrame, col_audit: str):
        """
        Ejecuta validaciones de calidad sobre un lote de datos en memoria.

        Args:
            df (pl.DataFrame): Lote de datos a auditar.
            col_audit (str): Columna objetivo para análisis de texto (ej. Razón Social).
        """
        if col_audit not in df.columns:
            return

        # 1. Auditoría de Integridad (Valores Nulos)
        n_nulls = df.filter(pl.col(col_audit).is_null()).height
        if n_nulls > 0:
            self.alerts_nulls += n_nulls

        # Filtrar registros válidos para análisis de contenido
        df_valid = df.filter(pl.col(col_audit).is_not_null())
        if df_valid.height == 0:
            return

        # 2. Detección de Corrupción de Encoding (Mojibake)
        suspect_mojibake = df_valid.filter(
            pl.col(col_audit).str.contains(self.regex_mojibake)
        )
        n_moji = suspect_mojibake.height

        if n_moji > 0:
            self.alerts_mojibake += n_moji
            # Capturar muestra representativa (Top 10 únicos) para análisis forense
            samples = (
                suspect_mojibake.select(col_audit)
                .unique()
                .head(10)
                .to_series()
                .to_list()
            )
            self.samples_mojibake.update(samples)

        # 3. Detección de Anomalías Estructurales (Longitud Insuficiente)
        suspect_len = df_valid.filter(
            pl.col(col_audit).str.len_chars() < self.min_text_length
        )
        n_len = suspect_len.height

        if n_len > 0:
            self.alerts_length += n_len
            samples = (
                suspect_len.select(col_audit).unique().head(5).to_series().to_list()
            )
            self.samples_length.update(samples)

        # Registro en consola solo si existen anomalías (Log por Excepción)
        if n_moji > 0 or n_len > 0 or n_nulls > 0:
            print(
                f"[WARN] Batch Quality Issue -> Nulls: {n_nulls} | Mojibake: {n_moji} | ShortStr: {n_len}"
            )

    def update_metrics(self, rows_count):
        """
        Actualiza los contadores globales de progreso y calcula la velocidad.

        Args:
            rows_count (int): Número de filas procesadas exitosamente en el lote actual.
        """
        self.total_rows += rows_count
        self.total_batches += 1

        elapsed = time.time() - self.start_time
        # Evitar división por cero
        speed = self.total_rows / (elapsed + 0.0001)

        print(
            f"[INFO] Progress -> Rows: {self.total_rows:,.0f} | Batches: {self.total_batches} | Speed: {speed:,.0f} rows/sec"
        )

    def generate_final_report(self, status="SUCCESS", error_details=""):
        """
        Genera el artefacto final de log con todas las estadísticas y evidencias recolectadas.

        Args:
            status (str): Estado final de la ejecución (SUCCESS, FAILED).
            error_details (str): Detalle técnico del error en caso de fallo.

        Returns:
            Path: Ruta del archivo generado.
        """
        end_time = time.time()
        total_duration = (end_time - self.start_time) / 60
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"ETL_AUDIT_LOG_{timestamp}.txt"
        report_path = LOG_DIR / log_filename

        # Construcción del reporte
        lines = []
        lines.append("=" * 80)
        lines.append("SAT ETL PROCESS - EXECUTION REPORT")
        lines.append("=" * 80)
        lines.append(f"Timestamp:      {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"Final Status:   {status}")
        lines.append(f"Duration (min): {total_duration:.2f}")
        lines.append(f"Total Rows:     {self.total_rows:,.0f}")
        lines.append(f"Total Batches:  {self.total_batches}")
        lines.append("-" * 80)
        lines.append("DATA QUALITY METRICS")
        lines.append("-" * 80)
        lines.append(f"Encoding Alerts (Mojibake):   {self.alerts_mojibake}")
        lines.append(
            f"Length Alerts (<{self.min_text_length} chars):      {self.alerts_length}"
        )
        lines.append(f"Null Value Alerts:            {self.alerts_nulls}")
        lines.append("=" * 80)

        # Sección de Evidencia Forense
        if self.samples_mojibake:
            lines.append(
                "\n[EVIDENCE] DETECTED ENCODING ARTIFACTS (Action: Update cleaning_rules.py):"
            )
            lines.append("-" * 60)
            sorted_samples = sorted(list(self.samples_mojibake))
            # Limitamos la salida para evitar archivos de log excesivamente grandes
            for sample in sorted_samples[:100]:
                lines.append(f'    "{sample}": "FIX_ME",')

            if len(self.samples_mojibake) > 100:
                lines.append(
                    f"\n    ... and {len(self.samples_mojibake) - 100} more items truncated."
                )

        if self.samples_length:
            lines.append(
                "\n[EVIDENCE] SUSPICIOUSLY SHORT STRINGS (Potential Mutilation):"
            )
            lines.append("-" * 60)
            sorted_samples = sorted(list(self.samples_length))
            for sample in sorted_samples[:50]:
                lines.append(f"    Value: '{sample}'")

        # Reporte de Errores Críticos
        if error_details:
            lines.append("\n" + "!" * 80)
            lines.append("CRITICAL FAILURE DETAILS")
            lines.append("!" * 80)
            lines.append(error_details)

        # Persistencia en disco
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            print(f"\n[INFO] Audit log successfully saved to: {report_path}")
        except OSError as e:
            print(f"[ERROR] Failed to write audit log: {e}")

        return report_path
