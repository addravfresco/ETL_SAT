"""
EJECUTAR_TODO.PY - ORQUESTADOR DE CARGA MASIVA MULTITABLA SAT
------------------------------------------------------------------------------
Este script automatiza la ejecución secuencial del pipeline ETL para todos
los anexos definidos en el catálogo maestro (Anexos 1A a 7G).

Estrategia de Estabilidad y Seguridad:
    1. Ingesta UNC: Valida la disponibilidad de los recursos en la ruta de red
       servidor.
    2. Aislamiento de Memoria: Ejecuta cada anexo en un proceso independiente
       vía 'subprocess', forzando la liberación de RAM al finalizar cada tabla.
    3. Gestión de Espacio: Implementa un ciclo de 'Auto-Limpieza' post-lote que
       elimina archivos temporales y rastros del antivirus (.adv, .tmp) en la
       unidad de trabajo, protegiendo el espacio en disco.
    4. Resiliencia: Ante fallos en un anexo específico, el sistema registra el
       incidente y continúa con la siguiente tabla de la cola.
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

# Importación de metadatos y rutas globales
try:
    from pkg.config_master import CONFIG_TABLAS, obtener_meta
    from pkg.globals import SAT_RAW_DIR, TEMP_DIR
except ImportError as e:
    print(f"[FATAL] Error al importar dependencias del paquete pkg: {e}")
    sys.exit(1)


def limpiar_directorio_trabajo(ruta_temp: Path):
    """
    Elimina archivos residuales y bloqueos de caché generados por el motor
    de datos o el agente de seguridad (Antivirus).
    """
    print(f"[LIMPIEZA] Purgando archivos temporales en: {ruta_temp}...")

    # Iteración sobre el contenido del directorio de intercambio
    for item in ruta_temp.iterdir():
        try:
            if item.is_file():
                item.unlink()  # Eliminación de archivos
            elif item.is_dir():
                shutil.rmtree(item)  # Eliminación de subcarpetas
        except Exception:
            # Si un archivo está bloqueado por el antivirus, se ignora
            # para no detener el flujo principal.
            pass


def procesar_todo_el_periodo():
    """
    Orquesta la ejecución secuencial de la jornada de carga para todos
    los anexos configurados.
    """
    print("=" * 80)
    print("SISTEMA DE ORQUESTACIÓN SAT - INICIANDO CARGA MASIVA 2025")
    print("=" * 80)
    print(f"[ORIGEN UNC]: {SAT_RAW_DIR}")
    print(f"[TRABAJO]:    {TEMP_DIR}")

    start_total = time.time()

    # Recuperación de la lista de tareas desde el catálogo maestro
    ANEXOS = list(CONFIG_TABLAS.keys())
    resumen = []

    for anexo in ANEXOS:
        print(f"\n{'-' * 40}")
        print(f"[ORQUESTADOR] Iniciando ciclo para Anexo {anexo}...")

        try:
            # 1. Validación dinámica de metadatos y existencia de archivo
            meta = obtener_meta(anexo)
            ruta_archivo = SAT_RAW_DIR / meta["file_name"]

            if not ruta_archivo.exists():
                print(f"[⚠️ SALTO] Archivo fuente no localizado: {meta['file_name']}")
                resumen.append((anexo, "⏭️ SALTADO (Archivo no existe)"))
                continue

            # 2. Ejecución del proceso ETL independiente (Aislamiento de RAM)
            # sys.executable asegura que se use el mismo entorno virtual (.venv)
            resultado = subprocess.run([sys.executable, "main.py", anexo], check=True)

            if resultado.returncode == 0:
                resumen.append((anexo, "✅ ÉXITO"))

        except subprocess.CalledProcessError:
            print(
                f"[❌ ERROR] El proceso main.py reportó un fallo para el Anexo {anexo}."
            )
            resumen.append((anexo, "❌ FALLÓ (Error de ejecución)"))

        except Exception as e:
            print(f"[⚠️ AVISO] Error inesperado en orquestador para {anexo}: {e}")
            resumen.append((anexo, "⚠️ ERROR TÉCNICO"))

        finally:
            # 3. Ciclo de Higiene de Disco: Ejecución tras cada anexo para
            # prevenir que el antivirus consuma el almacenamiento.
            limpiar_directorio_trabajo(TEMP_DIR)

    # --- REPORTE CONSOLIDADO DE FIN DE JORNADA ---
    duracion = (time.time() - start_total) / 60
    print("\n" + "=" * 80)
    print("RESUMEN DE CARGA MASIVA - CONSOLIDADO FINAL")
    print("-" * 80)
    for idx, (anexo, status) in enumerate(resumen, 1):
        print(f"  {idx}. Anexo {anexo:<4} : {status}")
    print("-" * 80)
    print(f"Tiempo Total de Operación: {duracion:.2f} minutos")
    print("=" * 80)


if __name__ == "__main__":
    procesar_todo_el_periodo()
