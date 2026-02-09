"""
GLOBALS.PY - MODULO SAT
Contiene variables globales, rutas de sistema y configuraciones de entorno.
"""

import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

# 1. CARGA DE CONFIGURACION DE ENTORNO
load_dotenv()

# 2. DEFINICION DE RUTAS ESTRUCTURALES
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
TEMP_DIR = BASE_DIR / "temp_processing"

# Inicializacion de directorios criticos
for folder in [LOG_DIR, TEMP_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# 3. RUTAS DE ACCESO A DATOS (UNIDAD DE RED V:)
SAT_RAW_DIR = Path(r"V:\SAT")
ARCHIVO_SAT_NOMBRE = "GERG_AECF_1891_Anexo1A-QA.txt"
RUTA_COMPLETA_SAT = SAT_RAW_DIR / ARCHIVO_SAT_NOMBRE

# 4. PARAMETROS DE CONEXION SQL SERVER
DB_SERVER = os.getenv("DB_SERVER", "LOCALHOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TRUSTED = os.getenv("DB_TRUSTED", "NO")
DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
SAT_DB_NAME = os.getenv("DB_NAME", "SAT_MASTER_V2")

# 5. CONFIGURACION DE RENDIMIENTO (POLARS ENGINE)
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

# Redireccion de desbordamiento de memoria (Swap) para proteccion de Unidad C:
os.environ["POLARS_TEMP_DIR"] = str(TEMP_DIR)
os.environ["TMPDIR"] = str(TEMP_DIR)
os.environ["TEMP"] = str(TEMP_DIR)
os.environ["TMP"] = str(TEMP_DIR)

# --- DEFINICIÓN DE ESQUEMAS Y TIPOS
REGLAS_TIPOS = {
    "UUID": pl.Utf8,
    "Descuento": pl.Float64,
    "SubTotal": pl.Float64,
    "Total": pl.Float64,
    "TrasladosIVA": pl.Float64,
    "TrasladosIEPS": pl.Float64,
    "TotalImpuestosTrasladados": pl.Float64,
    "RetenidosIVA": pl.Float64,
    "RetenidosISR": pl.Float64,
    "TotalImpuestosRetenidos": pl.Float64,
    "TipoCambio": pl.Float64,
}
