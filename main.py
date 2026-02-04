"""
File:           main.py
Author:         Alan Damián Domínguez Romero
Last Updated:   2026-02-03
Version:        1.0
Description:    Script principal para el proceso ETL del SAT para Facturas.
                Realiza la extraccion de por lotes, transformacion de caracteres 
                especiales y carga validada en SQL Server.
Dependencies:   polars, pyodbc, python-dotenv
Usage:          Ejecutar 'python main.py' con acceso a la unidad V: y servidor SQL.
"""

import time
import sys
import os
from pkg.globals import *
from pkg.extract import get_sat_reader
from pkg.transform import transform_sat_batch
from pkg.load import upload_to_sql_blindado
from pkg.reports import ETLReport

def imprimir_barra_progreso(actual, total, prefijo='', longitud=40):
    """
    Genera una representacion visual del progreso en la terminal.
    Parametros:
        actual (int): Valor de iteracion actual.
        total (int): Valor objetivo de iteraciones.
        prefijo (str): Texto descriptivo previo a la barra.
        longitud (int): Tamaño visual de la barra de progreso.
    """
    porcentaje = "{0:.1f}".format(100 * (actual / float(total)))
    llenado = int(longitud * actual // total)
    barra = '█' * llenado + '-' * (longitud - llenado)
    sys.stdout.write(f'\r{prefijo} |{barra}| {porcentaje}%')
    sys.stdout.flush()

def main():
    """
    Funcion principal que orquesta el flujo de extraccion, transformacion y carga.
    Mantiene la simplicidad del flujo mediante el uso de modulos en 'pkg'.
    """
    archivo_fuente = RUTA_COMPLETA_SAT 
    tabla_destino = "FACTURAS_CABECERA_V2"
    reporte = ETLReport()
    
    if not archivo_fuente.exists():
        print(f"Error: No se localizo el archivo fuente en la ruta: {archivo_fuente}")
        return
    
    tamano_total = os.path.getsize(archivo_fuente)
    lotes_analizados = 0

    print(f"Iniciando procesamiento: {archivo_fuente.name}")
    print(f"Tamano del archivo: {tamano_total / (1024**3):.2f} GB")
    
    try:
        # Inicializacion del lector batched de Polars
        reader = get_sat_reader(str(archivo_fuente), batch_size=50000)

        while True:
            chunks = reader.next_batches(1)
            if not chunks: 
                break
            
            df_lote = chunks[0]
            lotes_analizados += 1
            
            # Transformacion y limpieza (Rescate de caracteres especiales)
            df_procesado = transform_sat_batch(df_lote)
            
            # Persistencia con validacion de redundancia (Smart Resume)
            resultado_carga = upload_to_sql_blindado(df_procesado, tabla_destino)
            
            if resultado_carga is True:
                # Incremento de metricas en registros nuevos
                reporte.update(len(df_procesado))
            else:
                # Barra de progreso durante el escaneo de datos existentes
                imprimir_barra_progreso(lotes_analizados, 2000, prefijo='Analizando registros')
            
            # Pausa de estabilizacion para infraestructura (Discos G/H)
            if lotes_analizados % 20 == 0:
                time.sleep(5)

        # Cierre de auditoria y generacion de log
        reporte.generate_final_log(status="EXITOSO")
        print("\nProceso finalizado correctamente.")

    except Exception as e:
        reporte.generate_final_log(status="FALLIDO", error_msg=str(e))
        print(f"\nError en ejecucion principal: {e}")

if __name__ == "__main__":
    main()