# datajob_ETL

Pipeline ETL completo para análisis de datos de empleos con optimización automática y tests de calidad.

## 🚀 Inicio Rápido

### Para Evaluadores (Entrevista Técnica)

Ejecuta el pipeline completo con un solo comando:

```powershell
.\evaluate.ps1
```

Este script ejecuta automáticamente:
1. Detección de especificaciones del sistema
2. Inicio de PostgreSQL en Docker
3. Extracción de datos con chunk sizes óptimos
4. Tests de extracción (pytest)
5. Generación de profiles.yml optimizado
6. Transformaciones dbt
7. Tests de transformaciones y calidad de datos

**Prerequisito**: Asegúrate de que `data_jobs.csv` esté en el directorio raíz.

## ✨ Características

- **Pipeline ETL completo**: Extracción, transformación y carga de datos de empleos
- **Optimización automática**: Calcula chunk sizes y threads óptimos según tu sistema
- **Procesamiento paralelizado**: Aprovecha múltiples cores de CPU
- **Sistema de logging robusto**: Trazabilidad completa sin duplicación
- **Tests automatizados**: 84 tests de calidad de datos
- **Transformaciones con dbt**: Modelos analíticos en 3NF

## ⚙️ Optimización Automática del Sistema

### Ver especificaciones del sistema

```bash
python utils/system_optimizer.py
```

Muestra:
- CPU cores disponibles
- Memoria RAM total y disponible
- Threads recomendados para dbt
- Chunk sizes óptimos para diferentes tamaños de dataset

### Generar profiles.yml optimizado

```bash
python utils/generate_dbt_profile.py
```

Crea automáticamente `datajob_etl/profiles.yml` con:
- Número óptimo de threads según tus CPU cores
- Configuración de conexión desde variables de entorno (.env)
- Configuraciones separadas para dev y prod

### Cómo funciona la optimización

**Threads para dbt:**
- 2-4 cores: 2-4 threads
- 4-8 cores: 4-8 threads  
- 8+ cores: hasta 12 threads (límite para evitar overhead)

**Chunk size para extracción:**
- Datasets pequeños (<100k): 10k-25k por chunk
- Datasets medianos (100k-1M): 25k-50k por chunk
- Datasets grandes (>1M): 50k-100k por chunk
- Ajustado según memoria RAM disponible

## 📋 Comandos de Ejecución

### Pipeline Completo (Recomendado)

```powershell
.\evaluate.ps1
```

### Ejecución Individual de Componentes

#### 1. Test de Conexión a Base de Datos

```bash
# Verificar conexión a PostgreSQL
python test_db_connection.py
```

#### 2. Optimización del Sistema

```bash
# Ver especificaciones y configuraciones recomendadas
python utils/system_optimizer.py

# Generar profiles.yml optimizado
python utils/generate_dbt_profile.py
```

#### 3. Extracción de Datos

```bash
# Ejecutar extracción completa
python main.py

# Extracción con tracking detallado de pérdida de datos
python main_temporal.py

# Ver logs de extracción
Get-Content logs/extraction.log -Wait
```

#### 4. Tests de Extracción (pytest)

```bash
# Ejecutar todos los tests
poetry run pytest tests/test_extraction.py -v

# Ejecutar un test específico
poetry run pytest tests/test_extraction.py::test_nombre -v

# Ver cobertura
poetry run pytest tests/ --cov=extraction --cov-report=html
```

#### 5. Transformaciones dbt

```bash
cd datajob_etl

# Ejecutar todas las transformaciones
poetry run dbt run

# Ejecutar un modelo específico
poetry run dbt run --select stg_job_postings

# Ejecutar modelos de staging
poetry run dbt run --select staging.*

# Ejecutar modelos de marts
poetry run dbt run --select marts.*

# Full refresh
poetry run dbt run --full-refresh
```

#### 6. Tests de dbt

```bash
cd datajob_etl

# Ejecutar todos los tests
poetry run dbt test

# Tests genéricos (not_null, unique, etc.)
poetry run dbt test --select test_type:generic

# Tests personalizados (archivos .sql en tests/)
poetry run dbt test --select test_type:singular

# Test de un modelo específico
poetry run dbt test --select stg_job_postings
```

#### 7. Análisis de Datos

```bash
# Análisis completo del dataset
python utils/analysis.py

# Calcular registros esperados por tabla
python utils/analyze_dataset.py

# Comparar resultados esperados vs reales
python utils/comparar_resultados.py
```

#### 8. Comandos Útiles de dbt

```bash
cd datajob_etl

# Generar y servir documentación
poetry run dbt docs generate
poetry run dbt docs serve

# Compilar sin ejecutar
poetry run dbt compile

# Ver SQL compilado
poetry run dbt show --select modelo_nombre

# Limpiar archivos temporales
poetry run dbt clean
```

### Logs y Debugging

```bash
# Ver logs en tiempo real
Get-Content logs/pipeline.log -Wait
Get-Content logs/extraction.log -Wait

# Buscar errores
Select-String -Path logs/*.log -Pattern "ERROR"

# Ver últimas 50 líneas
Get-Content logs/pipeline.log -Tail 50

# Logs de dbt
Get-Content datajob_etl/logs/dbt.log -Tail 100
```

## 📁 Estructura del Proyecto

```
datajob_etl/
├── extraction/              # Módulo de extracción
│   ├── extraction.py       # Extracción optimizada
│   ├── extraction_temporal.py  # Con tracking de pérdida
│   └── transformacion.py   # Transformaciones en memoria
├── db/                     # Configuración de BD
│   ├── config/db.py       # SQLAlchemy config
│   └── data_models/       # Modelos ORM
├── datajob_etl/           # Proyecto dbt
│   ├── models/
│   │   ├── staging/       # Modelos staging
│   │   └── marts/         # Dimensiones y hechos
│   ├── tests/             # Tests de calidad
│   └── macros/            # Macros dbt
├── tests/                 # Tests pytest
├── utils/                 # Utilidades
│   ├── system_optimizer.py
│   ├── generate_dbt_profile.py
│   ├── analysis.py
│   └── logging_config.py
├── logs/                  # Archivos de log
├── main.py               # Punto de entrada
├── test_db_connection.py # Test de conexión
├── evaluate.ps1          # Script de evaluación
└── CHANGELOG.md          # Registro de cambios
```

## 🧪 Tests y Calidad de Datos

### Cobertura de Tests

- ✅ **84 tests pasando** (0 errores)
- ⚠️ **5 warnings** (datos faltantes esperados)
- 📊 **Cobertura completa**: staging, dimensiones, hechos, relaciones

### Tipos de Tests

**Tests de Extracción (pytest):**
- Validación de carga de datos
- Verificación de parseo de skills
- Tests de limpieza de datos

**Tests de Transformación (dbt):**
- Integridad referencial (foreign keys)
- Unicidad de primary keys
- Validación de rangos de datos
- Normalización de campos
- Validación de duplicados

**Tests de Calidad:**
- Campos requeridos no nulos
- Consistencia de salarios
- Validación de fechas
- Limpieza de datos

## 📊 Métricas de Performance

- **Velocidad de carga**: ~12,000 registros/segundo
- **Tiempo de extracción**: ~65 segundos para 785k registros
- **Tiempo de transformación**: ~3 minutos (dbt run)
- **Tiempo de tests**: ~50 segundos (89 tests)
- **Optimización automática**: Chunk sizes y threads según sistema

## 🛠️ Tecnologías Utilizadas

- **Python 3.12** - Lenguaje principal
- **PostgreSQL** - Base de datos
- **dbt** - Transformaciones de datos
- **SQLAlchemy** - ORM
- **pandas** - Procesamiento de datos
- **pytest** - Testing
- **Poetry** - Gestión de dependencias
- **Docker** - Contenedorización

## 📚 Documentación Adicional

- [CHANGELOG.md](CHANGELOG.md) - Registro detallado de cambios
- Logs del sistema en `logs/`

## 🔧 Configuración

### Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto:

```env
DB_HOST=localhost
DB_PORT=5433
DB_NAME=job_posting
DB_USER=postgres
DB_PASSWORD=postgres
```

### Instalación de Dependencias

```bash
# Instalar dependencias con Poetry
poetry install

# Activar entorno virtual
poetry shell
```

### Iniciar PostgreSQL

```bash
# Con Docker Compose
docker compose up -d db

# Verificar que esté corriendo
docker compose ps
```

## 📝 Notas

- El archivo `profiles.yml` se genera automáticamente y está en `.gitignore`
- Los logs se rotan automáticamente para evitar archivos grandes
- La optimización automática detecta las specs de tu sistema en cada ejecución
- Todos los tests deben pasar antes de considerar el pipeline completo

## 🤝 Contribución

Este es un proyecto de entrevista técnica. Para más información, consulta el [CHANGELOG.md](CHANGELOG.md).
