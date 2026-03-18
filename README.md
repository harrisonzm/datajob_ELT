# datajob_ETL
this is a technical interview done for the role of data engineer

## 🚀 Inicio Rápido

### Para Evaluadores (Entrevista Técnica)

Ejecuta el pipeline completo con un solo comando:

```powershell
.\evaluate.ps1
```

Este script ejecuta automáticamente:
1. Inicia PostgreSQL en Docker
2. Extracción de datos
3. Tests de extracción
4. Configuración de dbt (profiles.yml)
5. Transformaciones dbt
6. Tests de transformaciones
7. Tests de calidad de datos

**Prerequisito**: Asegúrate de que `data_jobs.csv` esté en el directorio raíz.

### Para Desarrollo

¿Primera vez usando el proyecto? Consulta la [Guía de Inicio Rápido](QUICKSTART.md)

## Características

- **Pipeline ETL completo**: Extracción, transformación y carga de datos de empleos
- **Procesamiento paralelizado**: Aprovecha múltiples cores de CPU para mayor velocidad
- **Optimización automática**: Calcula chunk sizes y threads óptimos según tu sistema
- **Configuración adaptativa**: dbt ajusta threads automáticamente según el sistema
- **Sistema de logging robusto**: Trazabilidad completa de la ejecución del pipeline
- **Tests automatizados**: Validación de calidad de datos y procesos
- **Transformaciones con dbt**: Modelos analíticos y tests de calidad

## ⚙️ Optimización Automática del Sistema

El proyecto incluye utilidades para optimizar automáticamente la configuración según las especificaciones de tu sistema:

### Ver especificaciones del sistema

```bash
python utils/system_optimizer.py
```

Esto mostrará:
- CPU cores disponibles
- Memoria RAM total y disponible
- Threads recomendados para dbt
- Chunk sizes óptimos para diferentes tamaños de dataset

### Generar profiles.yml optimizado

```bash
python utils/generate_dbt_profile.py
```

Esto creará automáticamente `datajob_etl/profiles.yml` con:
- Número óptimo de threads según tus CPU cores
- Configuración de conexión desde variables de entorno (.env)
- Configuraciones separadas para dev y prod

### Cómo funciona la optimización

El sistema calcula automáticamente:

**Threads para dbt:**
- 2-4 cores: 2-4 threads
- 4-8 cores: 4-8 threads  
- 8+ cores: hasta 12 threads (límite para evitar overhead)

**Chunk size para extracción:**
- Datasets pequeños (<100k): 10k-25k por chunk
- Datasets medianos (100k-1M): 25k-50k por chunk
- Datasets grandes (>1M): 50k-100k por chunk
- Ajustado según memoria RAM disponible

## Sistema de Logging

El proyecto implementa un sistema de logging completo que permite rastrear cada paso del pipeline:

- **Logs multinivel**: DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Archivos separados**: `pipeline.log`, `extraction.log`, `test_extraction.log`
- **Formato estructurado**: Timestamp, módulo, nivel y mensaje
- **Trazabilidad completa**: Seguimiento detallado de cada operación

Para más información, consulta la [documentación de logging](docs/LOGGING.md).

### Ejemplo de uso

```bash
# Ejecutar el pipeline (genera logs automáticamente)
python main.py

# Ver logs en tiempo real
Get-Content logs/pipeline.log -Wait

# Buscar errores
Select-String -Path logs/*.log -Pattern "ERROR"
```

## Estructura del Proyecto

```
datajob_etl/
├── extraction/          # Módulo de extracción de datos
│   ├── extraction.py   # Lógica de extracción con logging
│   └── tests.py        # Tests del módulo
├── db/                 # Configuración de base de datos
├── datajob_etl/        # Proyecto dbt para transformaciones
├── tests/              # Tests del pipeline
│   └── test_extraction.py  # Tests con logging
├── logs/               # Archivos de log
│   ├── pipeline.log
│   ├── extraction.log
│   └── test_extraction.log
├── utils/              # Utilidades
│   └── logging_config.py  # Configuración centralizada de logging
├── docs/               # Documentación
│   └── LOGGING.md     # Documentación del sistema de logging
└── main.py            # Punto de entrada del pipeline
```

## Ejecución

### Pipeline Automatizado (Recomendado)

Ejecuta todo el pipeline ETL con un solo comando:

```powershell
# Windows PowerShell
.\run_pipeline.ps1

# Linux/Mac/Git Bash
./run_pipeline.sh
```

El script ejecuta automáticamente:
1. Extracción de datos (main.py)
2. Tests de extracción (pytest)
3. Transformaciones dbt (dbt run)
4. Tests de transformaciones (dbt test)
5. Tests de calidad de datos

### Ejecución Manual por Pasos

```bash
# 1. Extracción
python main.py

# 2. Tests de extracción
pytest tests/test_extraction.py -v

# 3. Transformaciones dbt
cd datajob_etl
dbt run

# 4. Tests dbt
dbt test

# 5. Ver ejemplo de logging
python examples/logging_example.py
```

Para más detalles sobre la automatización, consulta [docs/PIPELINE_AUTOMATION.md](docs/PIPELINE_AUTOMATION.md).
