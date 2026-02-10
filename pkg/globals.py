"""
GLOBALS.PY - CONFIGURACIÓN CENTRALIZADA Y ENTORNO DE EJECUCIÓN
------------------------------------------------------------------------------
Este módulo orquesta la infraestructura base del sistema ETL. Gestiona la
carga de variables de entorno, la inicialización de directorios temporales
para el manejo de grandes volúmenes de datos (Big Data) y define los
parámetros de conectividad para SQL Server.

Lógica de Seguridad de Memoria:
    Implementa el redireccionamiento de variables de entorno (TMP/TEMP)
    para asegurar que el motor de Polars realice el intercambio de datos
    (Swap) en directorios específicos, protegiendo la integridad y el
    espacio en el disco del sistema local (Unidad C:).
"""

import os
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

# ----------------------------------------------------------------------------
# 1. CONFIGURACIÓN DE ENTORNO Y RUTAS BASE
# ----------------------------------------------------------------------------

# Carga de secretos y configuraciones desde archivo .env
load_dotenv()

# Resolución de rutas estructurales del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
TEMP_DIR = BASE_DIR / "temp_processing"

# Asegurar la existencia de la infraestructura de directorios necesaria
for folder in [LOG_DIR, TEMP_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# Ruta base para la ingesta de datos en la unidad de red institucional
SAT_RAW_DIR = Path(r"\\sia\AECF\DGATIC\LOTA\Bases de Datos\SAT")

# ----------------------------------------------------------------------------
# 2. PARÁMETROS DE CONECTIVIDAD SQL SERVER
# ----------------------------------------------------------------------------

# Configuración del servidor y credenciales (Inyectadas vía Env)
DB_SERVER = os.getenv("DB_SERVER", "LOCALHOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_TRUSTED = os.getenv("DB_TRUSTED", "NO")
DB_DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")

# Base de datos objetivo principal
SAT_DB_NAME = os.getenv("DB_NAME", "SAT_V2")

# ----------------------------------------------------------------------------
# 3. OPTIMIZACIÓN DEL MOTOR POLARS Y GESTIÓN DE MEMORIA
# ----------------------------------------------------------------------------

# Configuración de visualización y depuración de DataFrames
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

DISCO_TRABAJO = Path(r"D:\SAT\ETL_TEMP")
DISCO_TRABAJO.mkdir(parents=True, exist_ok=True)

# Redireccionamiento de directorios temporales de bajo nivel.
# Es crítico para procesos que superan la capacidad de la memoria RAM (Out-of-Core).
os.environ["POLARS_TEMP_DIR"] = str(DISCO_TRABAJO)
os.environ["TMPDIR"] = str(DISCO_TRABAJO)
os.environ["TEMP"] = str(DISCO_TRABAJO)
os.environ["TMP"] = str(DISCO_TRABAJO)

os.environ["POLARS_MAX_THREADS"] = "4"

"""
Nota sobre la evolución del sistema:
    La lógica de tipado estático REGLAS_TIPOS ha sido migrada hacia 
    'config_master.py' y 'enforcer.py' para permitir el soporte multitabla 
    y la inferencia de tipos por patrones.
"""
