
# ETL SAT-FACTURAS V1

Repositorio especializado en el procesamiento masivo de comprobantes fiscales provenientes del SAT (volúmenes de 50GB+). Este sistema implementa una arquitectura modular de alto rendimiento enfocada en la integridad de los datos y la protección de la infraestructura de red y almacenamiento.

## Estructura del Proyecto

ETL-SAT/
├── main.py              # Orquestador principal y punto de entrada.
├── requirements.txt     # Librerías mínimas necesarias para ejecución.
├── .gitignore           # Definición de exclusiones para el control de versiones.
├── logs/                # Directorio de reportes de auditoría generados.
├── temp_processing/     # Directorio para gestión de memoria temporal (Swap).
└── pkg/                 # Paquete que contiene la lógica modular.
    ├── __init__.py      # Identificador de paquete de Python.
    ├── globals.py       # Variables globales, rutas y configuración SQL.
    ├── extract.py       # Funciones de extracción batched (Polars).
    ├── transform.py     # Reglas de normalización y rescate de caracteres.
    ├── load.py          # Lógica de persistencia e idempotencia.
    ├── reports.py       # Gestión de métricas y logs técnicos.
    └── cleaning_rules.py# Diccionarios técnicos para limpieza de Mojibake.

## Estructura del Proyecto
1. Idempotencia y Smart Resume
El sistema incluye una lógica de verificación de duplicados mediante UUID. Esto permite que el proceso pueda ser interrumpido y reanudado en cualquier momento. El script identificará automáticamente el último registro cargado exitosamente en SQL Server y omitirá los datos ya procesados sin duplicar información.

2. Protección de Infraestructura
Diseñado para operar en entornos con restricciones de hardware, el flujo incluye:

Gestión de Transacciones: Uso de commits frecuentes para evitar el desbordamiento del Log de Transacciones (.ldf) en la unidad de red.

Pausas de Estabilización: Intervalos de espera programados para permitir la descompresión de buffers en discos de escritura y lectura.

Optimización de Memoria: Uso del motor Polars para procesamiento en streaming, evitando la saturación de la memoria RAM.

3. Normalización de Datos (Rescate de la Ñ)
Implementa un motor de limpieza basado en diccionarios para corregir errores de codificación (Mojibake), asegurando el rescate de caracteres especiales como la "Ñ" y acentos en campos críticos como nombres de emisores y receptores.

## Configuración y Uso
1. Requisitos Previos

Python 3.10+

Acceso a la Unidad donde esten las tablas

SQL Server con permisos de escritura.

2. Instalación

Clonar el repositorio.

Crear un entorno virtual: python -m venv .venv

Activar el entorno e instalar dependencias:

pip install -r requirements.txt

Configurar las variables de entorno en el archivo .env.

3. Ejecución

Para iniciar el proceso de carga masiva, ejecute:

python main.py


## Auditoría
Al finalizar cada ejecución, o ante un error crítico, el sistema genera automáticamente un archivo .txt en la carpeta logs/ detallando:

Estado final del proceso.

Cantidad total de registros insertados.

Tiempo total de ejecución y velocidad promedio (filas/segundo).