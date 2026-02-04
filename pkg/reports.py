import time
from datetime import datetime
from pkg.globals import LOG_DIR

class ETLReport:
    """
    Clase encargada de la gestion de metricas y generacion de reportes de auditoria.
    """
    def __init__(self, total_expected=None):
        """
        Inicializa los contadores y el temporizador del proceso.
        """
        self.start_time = time.time()
        self.total_rows = 0
        self.total_batches = 0

    def update(self, rows_in_batch):
        """
        Actualiza las metricas de rendimiento basadas en el lote procesado.
        Parametros:
            rows_in_batch (int): Numero de registros procesados en el lote actual.
        """
        self.total_rows += rows_in_batch
        self.total_batches += 1
        elapsed = time.time() - self.start_time
        speed = self.total_rows / (elapsed + 0.001)
        print(f"Filas: {self.total_rows:,.0f} | Lote: {self.total_batches} | Velocidad: {speed:,.0f} f/s")

    def generate_final_log(self, status="EXITOSO", error_msg=""):
        """
        Genera un archivo de texto con el resumen tecnico de la ejecucion.
        Parametros:
            status (str): Estado final de la ejecucion (EXITOSO/FALLIDO).
            error_msg (str): Descripcion del error en caso de fallo.
        Salida:
            Path: Ruta del archivo de reporte generado.
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
            "-"*60
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