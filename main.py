"""
MAIN.PY - ORQUESTADOR SUPREMO DEL PIPELINE ETL SAT
------------------------------------------------------------------------------
Este es el punto de entrada principal del sistema. Su función es coordinar el
flujo de datos a través de las cuatro fases críticas del pipeline:
Extracción, Transformación, Aplicación de Tipos (Enforcement) y Carga.

Arquitectura Multitabla:
    El sistema utiliza una lógica de despacho dinámico basada en argumentos
    de línea de comandos. Esto permite procesar cualquier anexo (1A a 7G)
    utilizando el mismo núcleo de código, inyectando metadatos específicos
    en tiempo de ejecución.

Ciclo de Vida:
    1. Ingesta: Lectura bufferizada desde la unidad de red.
    2. Saneamiento: Corrección quirúrgica de Mojibake y normalización de texto.
    3. Validación: Coerción de tipos mediante inferencia de patrones.
    4. Persistencia: Carga atómica masiva en SQL Server con auditoría forense.
"""

import os
import sys
import traceback

# Configuración del entorno de salida para visualización de logs en tiempo real
try:
    if hasattr(sys.stdout, "reconfigure"):
        # El comentario # type: ignore silencia la advertencia de Pylance
        sys.stdout.reconfigure(line_buffering=True)  # type: ignore
except AttributeError:
    pass

try:
    # Integración de componentes del núcleo ETL
    from pkg.config_master import REGLAS_DINAMICAS, obtener_meta
    from pkg.enforcer import aplicar_tipos_seguros
    from pkg.extract import get_sat_reader
    from pkg.globals import SAT_RAW_DIR
    from pkg.load import upload_to_sql_blindado
    from pkg.reports import ETLReport
    from pkg.transform import transform_sat_batch
except ImportError as e:
    print(f"[FATAL] Error en la infraestructura de módulos: {e}")
    sys.exit(1)


def main() -> None:
    print("DEBUG: Entrando a la función main")
    id_anexo = sys.argv[1].upper() if len(sys.argv) > 1 else "1A"
    """
    Orquestador del ciclo de vida del proceso ETL.

    Uso desde terminal:
        python main.py [ID_ANEXO]
        Ejemplo: python main.py 3C
    """

    # 1. Resolución de Identidad y Metadatos
    # Se captura el argumento de consola; por defecto se procesa el Anexo 1A
    id_anexo = sys.argv[1].upper() if len(sys.argv) > 1 else "1A"

    try:
        meta = obtener_meta(id_anexo)
    except ValueError as e:
        print(f"[ERROR DE CONFIGURACIÓN] {e}")
        return

    print(f">>> INICIANDO SISTEMA ETL SAT: ANEXO {id_anexo} [PROD]")
    print(f">>> CONTEXTO: DB {meta['database']} | TABLA {meta['table_name']}")

    # 2. Validación de Disponibilidad de Recursos
    ruta_archivo = SAT_RAW_DIR / meta["file_name"]
    if not ruta_archivo.exists():
        print(f"[ERROR] Recurso fuente no localizado: {ruta_archivo}")
        return

    # 3. Preparación de Telemetría
    file_size_gb = os.path.getsize(ruta_archivo) / (1024**3)
    processed_batches = 0
    report = ETLReport()

    print(f"[INFO] Ingesta: {meta['file_name']} | Volumen: {file_size_gb:.2f} GB")
    print("-" * 80)

    try:
        # 4. Inicialización del Stream de Lectura Híbrida
        reader = get_sat_reader(str(ruta_archivo), batch_size=50000)
        print("[INFO] Stream activo. Procesando flujo de datos...")

        while True:
            # --- FASE A: EXTRACCIÓN (Extract) ---
            chunks = reader.next_batches(1)

            # Condición de salida: Fin de flujo de datos
            if chunks is None or len(chunks) == 0:
                print("\n[INFO] Flujo finalizado exitosamente (EOF).")
                break

            df_batch = chunks[0]
            processed_batches += 1
            row_count = len(df_batch)

            print(f"[LOTE {processed_batches:04d}] Regs: {row_count:<6} | ", end="")

            # --- FASE B: TRANSFORMACIÓN (Transform) ---
            # Aplicación de limpieza de Mojibake y normalización de texto
            df_processed = transform_sat_batch(df_batch)
            print("Transform -> ", end="")

            # --- FASE C: APLICACIÓN DE TIPOS (Enforce) ---
            # Coerción dinámica de tipos basada en patrones globales
            df_final = aplicar_tipos_seguros(df_processed, REGLAS_DINAMICAS)
            print("Tipado -> ", end="")

            # --- FASE D: AUDITORÍA Y PERSISTENCIA (Audit & Load) ---
            # Ejecución de auditoría forense sobre el lote tipado
            report.audit_batch(df_final)

            # Carga atómica con direccionamiento dinámico de DB y Tabla
            upload_success = upload_to_sql_blindado(
                df_final, table_name=meta["table_name"], db_name=meta["database"]
            )

            if upload_success:
                print("SQL: OK")
                report.update_metrics(len(df_final))
            else:
                print("SQL: OMITIDO (Colisión de PK)")

    except KeyboardInterrupt:
        print("\n[WARN] Operación cancelada por el operador.")
    except Exception as e:
        print(f"\n[CRITICAL ERROR] Fallo sistémico en lote {processed_batches}.")
        print(f"Detalle Técnico: {e}")
        print("-" * 40)
        traceback.print_exc()


if __name__ == "__main__":
    main()
