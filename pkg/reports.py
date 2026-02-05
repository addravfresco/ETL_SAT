import time
import polars as pl
from datetime import datetime
from pkg.globals import LOG_DIR

class ETLReport:
    """
    Gestiona metricas de rendimiento y auditoria de calidad de datos.
    """
    def __init__(self, total_expected=None):
        """
        Inicializa cronometro y contadores de registros y alertas.
        """
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0
        
        # Contadores de auditoria tecnica
        self.mojibake_alerts = 0
        self.mutilation_alerts = 0
        
        # Patrones de deteccion
        self.regex_mojibake = r'[?ÃÂƒ†‡‰‹›ŒŽ‘’“”•–—˜™š›œžŸ¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]'
        self.mutilaciones_criticas = ["MAR", "SILV", "PATRIC", "EL", "FO", "MU", "CA", "RO", "SA"]

    def audit_batch(self, df: pl.DataFrame, col_evaluar: str):
        """
        Analiza la integridad del lote y registra alertas si detecta anomalias.
        Parametros:
            df (pl.DataFrame): Lote de datos procesados.
            col_evaluar (str): Nombre de la columna de texto a auditar.
        """
        if col_evaluar not in df.columns:
            return

        # Filtrado de anomalias
        n_moji = df.filter(pl.col(col_evaluar).str.contains(self.regex_mojibake)).height
        n_mut  = df.filter(pl.col(col_evaluar).is_in(self.mutilaciones_criticas)).height

        if n_moji > 0 or n_mut > 0:
            self.mojibake_alerts += n_moji
            self.mutilation_alerts += n_mut
            print(f"Alerta de Calidad - Mojibake: {n_moji} | Mutilaciones: {n_mut}")

    def update(self, rows_in_batch):
        """
        Actualiza metricas de rendimiento generales.
        Parametros:
            rows_in_batch (int): Numero de registros procesados.
        """
        self.total_rows += rows_in_batch
        self.total_batches += 1
        elapsed = time.time() - self.start_time
        speed = self.total_rows / (elapsed + 0.001)
        print(f"Filas: {self.total_rows:,.0f} | Lote: {self.total_batches} | Velocidad: {speed:,.0f} f/s")

    def generate_final_log(self, status="EXITOSO", error_msg=""):
        """
        Genera archivo de auditoria incluyendo el resumen de calidad.
        Parametros:
            status (str): Estado final.
            error_msg (str): Detalle del error si aplica.
        Salida:
            Path: Ruta del log generado.
        """
        end_time = time.time()
        duration_min = (end_time - self.start_time) / 60
        report_path = LOG_DIR / f"REPORTE_SAT_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"
        
        lineas = [
            "="*60,
            "       REPORTE DE AUDITORIA - ETL SAT MODULAR",
            "="*60,
            f"Fecha Finalizacion: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Estado del Proceso:  {status}",
            f"Tiempo Total:        {duration_min:.2f} minutos",
            f"Total Filas:         {self.total_rows:,.0f}",
            f"Total Lotes:         {self.total_batches}",
            "-"*60,
            "       DIAGNOSTICO DE CALIDAD DE DATOS",
            "-"*60,
            f"Alertas Mojibake:    {self.mojibake_alerts}",
            f"Alertas Mutilacion:  {self.mutilation_alerts}",
            "="*60
        ]
        
        if error_msg:
            lineas.append(f"ERROR DETECTADO: {error_msg}")
            lineas.append("="*60)
        
        try:
            with open(report_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lineas))
            print(f"\nReporte de auditoria guardado en: {report_path}")
        except Exception as e:
            print(f"Error al generar archivo de log: {e}")
            
        return report_path