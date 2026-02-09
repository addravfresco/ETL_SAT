"""
PROFILE_DATA.PY
------------------------------------------------------------------------------
Radiografía de Datos REAL (Agnóstica).
Lee el archivo fuente original y analiza TODAS las columnas que encuentre,
sin depender de configuraciones externas.
"""

import time

import polars as pl

from pkg.extract import get_sat_reader
from pkg.globals import RUTA_COMPLETA_SAT

# Configuración
MUESTRA_LOTES = 20  # 20 lotes de 50k = 1 Millón de filas
BATCH_SIZE = 50000


def main():
    print("🚀 INICIANDO PERFILADO DE DATOS ORIGINALES (RAW)...")
    print(f"📂 Fuente: {RUTA_COMPLETA_SAT}")
    print(
        f"⏱️  Esto leerá los primeros {MUESTRA_LOTES * BATCH_SIZE / 1_000_000:.1f} millones de filas tal cual vienen."
    )

    start_time = time.time()

    # Instanciamos el lector (Este lee el TXT original byte a byte)
    reader = get_sat_reader(str(RUTA_COMPLETA_SAT), batch_size=BATCH_SIZE)

    # Leemos el primer lote para descubrir las columnas reales del archivo
    first_batch = reader.next_batches(1)
    if not first_batch:
        print("❌ El archivo está vacío o no se pudo leer.")
        return

    df_sample = first_batch[0]
    columnas_reales = df_sample.columns
    print(
        f"🔍 Columnas detectadas en el archivo ({len(columnas_reales)}): {columnas_reales}"
    )

    # Inicializamos estadísticas para las columnas detectadas
    global_stats = {
        col: {"max_len": 0, "nulls": 0, "examples": set(), "dirty_chars": 0}
        for col in columnas_reales
    }

    # Reiniciamos el lector para empezar desde cero (opcional, pero más limpio)
    reader = get_sat_reader(str(RUTA_COMPLETA_SAT), batch_size=BATCH_SIZE)

    total_rows_processed = 0
    batches_leidos = 0

    try:
        while batches_leidos < MUESTRA_LOTES:
            chunks = reader.next_batches(1)
            if not chunks:
                break

            df = chunks[0]
            current_rows = len(df)
            total_rows_processed += current_rows

            # Iteramos sobre las columnas que REALMENTE vienen en el archivo
            for col in columnas_reales:
                if col in df.columns:
                    stats = global_stats[col]

                    # 1. Análisis de Texto (Longitud y Suciedad)
                    # Forzamos interpretación como string para medir longitud real
                    serie_str = df[col].cast(pl.Utf8)

                    batch_max = serie_str.str.len_bytes().max()
                    if batch_max is not None and batch_max > stats["max_len"]:
                        stats["max_len"] = batch_max

                    # Buscamos caracteres peligrosos en columnas clave (RFCs/Montos)
                    # (Ejemplo: Guiones en RFC o $ en Montos)
                    if "RFC" in col.upper():
                        stats["dirty_chars"] += serie_str.str.contains(r"[- /]").sum()
                    elif "TOTAL" in col.upper() or "IMPORTE" in col.upper():
                        stats["dirty_chars"] += serie_str.str.contains(r"[\$,]").sum()

                    # 2. Conteo de Nulos
                    stats["nulls"] += df[col].null_count()

                    # 3. Muestreo (Solo valores no nulos y no vacíos)
                    sample = serie_str.str.strip_chars().drop_nulls()
                    sample = sample.filter(sample != "").head(3).to_list()
                    stats["examples"].update(sample)

            batches_leidos += 1
            print(f"\r⏳ Escaneando: {total_rows_processed} filas...", end="")

    except KeyboardInterrupt:
        print("\n🛑 Detenido por usuario.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

    # --- REPORTE FINAL ---
    print("\n\n" + "=" * 145)
    print(
        f"{' ':2} {'CAMPO REAL (TXT)':<30} | {'MAX LEN':<8} | {'% NULL':<8} | {'ALERTA':<10} | {'EJEMPLOS REALES'}"
    )
    print("=" * 145)

    safe_total = total_rows_processed if total_rows_processed > 0 else 1

    for col in columnas_reales:
        stats = global_stats[col]
        pct_null = (stats["nulls"] / safe_total) * 100

        lista_ejemplos = list(stats["examples"])[:3]
        ejemplos_str = ", ".join([str(x)[:35] for x in lista_ejemplos])

        len_str = str(stats["max_len"])

        # --- Lógica de Diagnóstico Automático ---
        alert_msg = ""
        prefix = "  "

        # 1. Alerta RFC Sucio
        if "RFC" in col.upper() and stats["dirty_chars"] > 0:
            alert_msg = "SUCIO (-/ )"
            prefix = "🔴"
        # 2. Alerta RFC Largo
        elif "RFC" in col.upper() and stats["max_len"] > 13:
            alert_msg = "LARGO >13"
            prefix = "🔴"
        # 3. Alerta Monto Sucio
        elif ("TOTAL" in col.upper() or "SUB" in col.upper()) and stats[
            "dirty_chars"
        ] > 0:
            alert_msg = "TEXTO ($,)"
            prefix = "🔴"
        # 4. Alerta Texto Gigante
        elif stats["max_len"] > 300:
            alert_msg = "TRUNCAR"
            prefix = "⚠️ "

        print(
            f"{prefix} {col:<30} | {len_str:<8} | {pct_null:5.1f}%   | {alert_msg:<10} | {ejemplos_str}"
        )

    duration = time.time() - start_time
    print("=" * 145)
    print(f"✅ Análisis completado en {duration:.2f} segundos.")


if __name__ == "__main__":
    main()
