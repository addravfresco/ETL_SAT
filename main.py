"""
MAIN.PY
------------------------------------------------------------------------------
Orquestador Principal del Proceso ETL SAT.
Ejecuta el pipeline de extracción híbrida, transformación nativa y carga SQL.
"""

import os
import sys
import traceback

# Configuración de salida: Forzar flush inmediato para visibilidad de logs.
# Se utiliza 'getattr' o bloque try para compatibilidad con linters estáticos.
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore
except AttributeError:
    pass

try:
    # Módulos del paquete ETL
    from pkg.enforcer import aplicar_tipos_seguros
    from pkg.extract import get_sat_reader
    from pkg.globals import REGLAS_TIPOS, RUTA_COMPLETA_SAT
    from pkg.load import upload_to_sql_blindado
    from pkg.reports import ETLReport
    from pkg.transform import transform_sat_batch
except ImportError as e:
    print(f"[FATAL] Error crítico de inicialización de módulos: {e}")
    sys.exit(1)


def main():
    """
    Función de entrada principal (Entry Point).
    Gestiona el ciclo de vida del ETL y el manejo de excepciones de alto nivel.
    """
    print(">>> INICIANDO SISTEMA ETL SAT [MODO PRODUCCIÓN]")

    # 1. Validación de entorno
    if not RUTA_COMPLETA_SAT.exists():
        print(f"[ERROR] Archivo fuente no accesible: {RUTA_COMPLETA_SAT}")
        return

    # 2. Inicialización de métricas
    file_size_gb = os.path.getsize(RUTA_COMPLETA_SAT) / (1024**3)
    processed_batches = 0
    report = ETLReport()

    print(f"[INFO] Fuente: {RUTA_COMPLETA_SAT} | Tamaño: {file_size_gb:.2f} GB")
    print("-" * 80)

    try:
        # 3. Inicialización del Lector Híbrido
        # Se define un batch_size de 50k para balancear uso de RAM y I/O de red.
        reader = get_sat_reader(str(RUTA_COMPLETA_SAT), batch_size=50000)
        print("[INFO] Stream de lectura iniciado. Comenzando procesamiento...")

        while True:
            # --- FASE A: EXTRACCIÓN (Extract) ---
            chunks = reader.next_batches(1)

            # Control de flujo: EOF o Lote Vacío termina el proceso
            if chunks is None or len(chunks) == 0:
                print("\n[INFO] Fin de archivo (EOF) alcanzado satisfactoriamente.")
                break

            df_batch = chunks[0]
            processed_batches += 1
            row_count = len(df_batch)

            # Feedback de progreso en consola
            print(f"[LOTE {processed_batches:04d}] Regs: {row_count:<6} | ", end="")

            # --- FASE B: TRANSFORMACIÓN (Transform) ---
            # Limpieza de mojibake utilizando motor Python (evita Stack Overflow de Polars)
            df_processed = transform_sat_batch(df_batch)
            print("Transform -> ", end="")

            # --- FASE C: APLICACIÓN DE TIPOS (Enforce) ---
            # Coerción estricta de tipos SQL
            df_final = aplicar_tipos_seguros(df_processed, REGLAS_TIPOS)
            print("Tipado -> ", end="")

            # --- FASE D: AUDITORÍA Y CARGA (Audit & Load) ---
            report.audit_batch(df_final, "ReceptorNombre")

            # Inserción atómica en base de datos
            upload_success = upload_to_sql_blindado(df_final, "FACTURAS_CABECERA_V2")

            if upload_success:
                print("SQL: OK")
                report.update_metrics(len(df_final))
            else:
                print("SQL: OMITIDO (Duplicados)")

            # Pausa técnica mínima para permitir flush de I/O si es necesario
            # time.sleep(0.01)

    except KeyboardInterrupt:
        print("\n[WARN] Proceso interrumpido manualmente por el usuario.")
    except Exception as e:
        print(
            f"\n[CRITICAL ERROR] Excepción no controlada en lote {processed_batches}."
        )
        print(f"Detalle: {e}")
        print("-" * 40)
        traceback.print_exc()

        # Generación de reporte forense
        report.generate_final_report(status="FAILED", error_details=str(e))
        sys.exit(1)

    # 4. Finalización y Reporteo
    print("-" * 80)
    log_path = report.generate_final_report(status="SUCCESS")
    print(f"[INFO] Proceso finalizado. Log de auditoría: {log_path}")


if __name__ == "__main__":
    main()
