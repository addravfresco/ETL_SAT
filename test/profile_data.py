from pathlib import Path

from pkg.extract import get_sat_reader

# --- CONFIGURACIÓN DE RUTAS ---
RUTA_BASE = Path(r"V:\SAT")
ARCHIVOS = [
    "GERG_AECF_1891_Anexo1A-QA.txt",
    "GERG_AECF_1891_Anexo2B.csv",
    "GERG_AECF_1891_Anexo3C.csv",
    "GERG_AECF_1891_Anexo4D.csv",
    "GERG_AECF_1891_Anexo5E.csv",
    "GERG_AECF_1891_Anexo6F.csv",
    "GERG_AECF_1891_Anexo7G.csv",
]


def profile_all_tables():
    print("🚀 INICIANDO PERFILADO MULTITABLA - PERIODO: 2025_1S")
    print("=" * 100)

    for nombre_archivo in ARCHIVOS:
        ruta_completa = RUTA_BASE / nombre_archivo
        if not ruta_completa.exists():
            print(f"❌ Saltando: {nombre_archivo} (No encontrado)")
            continue

        print(f"\n🔍 ANALIZANDO: {nombre_archivo}")

        # El lector híbrido ya maneja el encoding y errores de bytes
        reader = get_sat_reader(str(ruta_completa), batch_size=50000)
        chunk = reader.next_batches(1)

        if chunk:
            df = chunk[0]
            columnas = df.columns
            total_filas = len(df)

            print(f"✅ Columnas detectadas: {len(columnas)}")
            print(f"📊 Muestra inicial: {total_filas:,} filas")

            # Análisis de tipos y nulos
            for col in columnas:
                # Calculamos el % de nulos usando LaTeX para la representación
                # $\text{null\_pct} = \frac{\text{null\_count}}{\text{total\_rows}} \times 100$
                nulls = df[col].null_count()
                null_pct = (nulls / total_filas) * 100

                # Identificación de tipos sugeridos
                tipo_real = df[col].dtype
                ejemplos = df[col].drop_nulls().head(2).to_list()

                print(
                    f"   - {col:<25} | {str(tipo_real):<10} | Nulls: {null_pct:>5.1f}% | Ex: {ejemplos}"
                )

        print("-" * 100)


if __name__ == "__main__":
    profile_all_tables()
