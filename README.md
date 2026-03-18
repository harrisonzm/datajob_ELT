# datajob_ETL

Pipeline ETL completo para análisis de datos de empleos con optimización automática y tests de calidad.

![CI Pipeline](https://github.com/[tu-usuario]/datajob_ETL/workflows/CI%20Pipeline/badge.svg)
![Code Quality](https://github.com/[tu-usuario]/datajob_ETL/workflows/Code%20Quality/badge.svg)

## 🚀 Inicio Rápido

### Para Evaluadores (Entrevista Técnica)

**Ejecución local:**
```powershell
.\evaluate.ps1
```

**Ejecución en Docker:**
```powershell
.\run-docker-pipeline.ps1
```

Ambos scripts ejecutan automáticamente:
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

## 📊 Métricas de Performance

- **Velocidad de carga**: ~12,000 registros/segundo
- **Tiempo de extracción**: ~65 segundos para 785k registros
- **Tiempo de transformación**: ~3 minutos (dbt run)
- **Tiempo de tests**: ~50 segundos (89 tests)
- **Optimización automática**: Chunk sizes y threads según sistema

## 🔄 Orquestación con Apache Airflow

### Inicio Rápido con Airflow

```powershell
# Iniciar Airflow y el pipeline completo
.\start_airflow.ps1
```

Accede a la UI en http://localhost:8080 (usuario: `airflow`, password: `airflow`)

**Servicios incluidos:**
- PostgreSQL (datos): puerto 5433
- PostgreSQL (Airflow metadata): puerto 5434  
- Airflow Webserver: puerto 8080
- Airflow Scheduler: background

### Características del DAG

- ✅ Ejecución automática diaria a las 2 AM
- ✅ Validaciones previas (CSV, conexión DB)
- ✅ Extracción optimizada con verificación
- ✅ Transformaciones dbt automatizadas
- ✅ Tests de calidad integrados
- ✅ Generación automática de documentación
- ✅ Reintentos automáticos en caso de fallo
- ✅ Alertas por email configurables

Ver documentación completa en [airflow_setup.md](airflow_setup.md)

## 🔄 CI/CD Pipeline

### GitHub Actions Workflows

El proyecto incluye pipelines automatizados de CI/CD:

**CI Pipeline** (`.github/workflows/ci.yml`):
- Linting con flake8 y black
- Tests unitarios con pytest
- Tests de integración con dbt
- Cobertura de código con Codecov
- Ejecuta en PostgreSQL real

**Code Quality** (`.github/workflows/code-quality.yml`):
- Análisis de seguridad con bandit
- Verificación de dependencias con safety
- Type checking con mypy
- Análisis de complejidad con radon

### Ejecutar localmente

```bash
# Linting
poetry run flake8 extraction/ db/ utils/
poetry run black --check extraction/ db/ utils/

# Tests
poetry run pytest tests/ -v --cov

# Security scan
poetry run bandit -r extraction/ db/ utils/

# Type checking
poetry run mypy extraction/ db/ utils/
```

# Data Engineering Technical Assessment

A data pipeline to transform `data_jobs.csv` into a normalized **3NF** relational model, using an **ELT** architecture, automated tests, logging, and a database ready for downstream analytics.

---

## 1. Objective

The goal of this technical assessment is to build a reproducible pipeline that:

- ingests the `data_jobs.csv` dataset,
- loads the raw data into PostgreSQL,
- transforms the raw data into a normalized **3NF** relational model,
- correctly handles semi-structured columns such as `job_skills` and `job_type_skills`,
- implements tests to validate extraction, transformation, and data quality,
- and leaves a solid foundation for future analytical layers.

---

## 2. Solution Overview

The solution was designed using an **ELT** architecture:

1. **Extract**: read and lightly preprocess the CSV with Python.
2. **Load**: load the raw data into PostgreSQL in a raw table.
3. **Transform**: normalize and clean the data inside the database using dbt.

General flow:

```text
CSV -> Python ingestion -> raw.job_posting -> staging -> dimensions -> relationships -> facts
```

---

## 3. Architecture

### Main components

- **Python**: initial ingestion, parsing helpers, and utilities.
- **PostgreSQL**: transactional storage and transformation layer.
- **dbt**: SQL transformations, modeling, and data quality tests.
- **Pytest**: unit tests and basic integration checks.
- **Docker Compose**: reproducible local environment for database and services.
- **Logging**: execution traceability and failure diagnosis.

### High-level flow

```text
data_jobs.csv
   |
   v
Python ingestion
   |
   v
raw.job_posting
   |
   v
dbt staging models
   |
   v
dbt dimension models
   |
   v
dbt relationship tables
   |
   v
fact_job_posts
```

---

## 4. Design Decisions

This is the most important section of the project because it explains the engineering reasoning behind the solution.

### 4.1 Why ELT instead of ETL

I chose **ELT** instead of traditional ETL because it:

- allows the raw data to be loaded quickly first,
- leverages the database engine for transformations,
- improves traceability and auditability by keeping the source data intact,
- and scales better if the solution is later migrated to a cloud platform or a data warehouse.

Separating ingestion from transformation also makes it easier to identify data issues without losing the original state of the dataset.

### 4.2 Why PostgreSQL

I chose **PostgreSQL** because it:

- handles large loads reliably,
- supports efficient operations such as `COPY`,
- works very well for SQL transformations and relational modeling,
- and is a robust, standard choice for this type of technical assessment.

### 4.3 Why Docker Compose

I used **Docker Compose** to make the environment reproducible:

- it avoids machine-specific differences,
- makes it easy to start the database quickly,
- simplifies technical evaluation,
- and reduces setup friction for running the full pipeline end to end.

### 4.4 Why keep a raw table

The `job_posting` table was kept as a **raw** layer in order to:

- preserve traceability to the original source,
- allow reprocessing without manually rereading the CSV,
- isolate ingestion from business logic,
- and make debugging easier when a transformation fails.

### 4.5 Why a 3NF model

I designed the model in **Third Normal Form (3NF)** to:

- reduce redundancy,
- avoid inconsistencies,
- separate entities with their own business meaning,
- and correctly represent one-to-many and many-to-many relationships.

The main separated entities were:

- `companies`
- `countries`
- `locations`
- `vias`
- `schedule_types`
- `short_titles`
- `skills`
- `types`
- `skill_types`
- `job_skills`
- `fact_job_posts`

### 4.6 Why `short_title` is a separate entity

Even though `job_title_short` may appear to be derived from `job_title`, I modeled it as its own entity because it has real analytical value.

`job_title` is highly variable and depends on how each company publishes a vacancy. By contrast, `short_title` makes it possible to group semantically equivalent roles under a consistent label, which improves query quality and aggregate analysis.

### 4.7 Why resolve skills with bridge tables

`job_skills` and `job_type_skills` contain semi-structured information, so it would not have been correct to leave them as plain text in the final model.

I resolved them as follows:

- `skills`: skill catalog
- `types`: skill categories
- `skill_types`: relationship between a skill and a category
- `job_skills`: bridge table between job postings and skill types

This decision makes it possible to:

- correctly represent many-to-many relationships,
- preserve the classification of skills by type,
- and avoid losing analytical context.

### 4.8 Why some foreign keys remain nullable

I preferred using `NULL` for some optional foreign keys instead of creating artificial values such as `"Unknown"` because:

- it is semantically more correct,
- it avoids contaminating dimensions with invented data,
- and it preserves analytical flexibility when deciding how to handle missing values later.

### 4.9 Why `location` is required

Even though some remote jobs could suggest a missing location, I decided to enforce a defined location using a value such as `anywhere`, because location is a key analytical dimension and this avoids inconsistencies in joins and geographic filters.

### 4.10 Why salary values were not imputed

I did not impute `salary_rate`, `salary_year_avg`, or `salary_hour_avg` because they contain a high proportion of nulls and there is not enough evidence to estimate them reliably without introducing bias.

The decision was to preserve integrity rather than artificially fill them.

### 4.11 Why dbt for transformations

I chose **dbt** for the transformation layer because it:

- organizes the pipeline into clear layers (`staging`, `dimensions`, `relationships`, `facts`),
- makes SQL transformations versionable,
- allows data quality tests to be embedded into the workflow,
- and improves maintainability compared with loose SQL scripts or transformation logic mixed into Python.

### 4.12 Why pytest for tests

I used **pytest** to:

- test critical extraction and parsing functions,
- validate connectivity, completeness, and schema expectations,
- and ensure changes do not break the pipeline.

### 4.13 Why structured logging

I implemented logging because a data pipeline should be observable. Logging makes it possible to:

- measure execution time,
- record processed volume,
- detect parsing errors,
- audit executions,
- and simplify debugging.

---

## 5. 3NF Data Model

The final relational model includes:

### Core tables
- `fact_job_posts`

### Dimensions
- `dim_companies`
- `dim_countries`
- `dim_locations`
- `dim_schedule_types`
- `dim_short_titles`
- `dim_skills`
- `dim_types`
- `dim_vias`

### Relationship tables
- `skill_types`
- `job_skills`

### Raw / Staging
- `job_posting`
- `stg_job_postings`
- `stg_job_skills`

### Main business rules

- Every primary key must be defined.
- All boolean fields must be non-null.
- `location` must always be defined.
- `salary_year_avg` and `salary_hour_avg` are mutually exclusive.
- Prefixes such as `via` and `melalui` must be removed from `job_via`.
- Schedule types must be normalized.

---

## 6. Project Structure

```text
.
├── data/
│   └── data_jobs.csv
├── extraction/
│   └── extraction.py
├── datajob_etl/
│   └── models/
│       ├── staging/
│       │   ├── stg_job_postings.sql
│       │   ├── stg_job_skills.sql
│       │   ├── sources.yml
│       │   └── schema.yml
│       └── marts/
│           ├── dimensions/
│           ├── relationships/
│           └── facts/
├── tests/
│   ├── test_extraction.py
│   ├── test_transformation.py
│   └── test_business_logic.py
├── logs/
├── docker-compose.yml
├── pyproject.toml
├── .env.example
└── README.md
```

---

## 7. Execution Instructions

### 7.1 Prerequisites

Make sure you have the following installed:

- Docker and Docker Compose
- Python 3.11+ or the version used in the project
- Poetry
- dbt
- Git

### 7.2 Clone the repository

```bash
git clone <YOUR_REPOSITORY_URL>
cd <YOUR_REPOSITORY_NAME>
```

### 7.3 Configure environment variables

Create your `.env` file from the example:

```bash
cp .env.example .env
```

Example variables:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=datajobs
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/datajobs
```

### 7.4 Start the database

```bash
docker compose up -d
```

### 7.5 Install Python dependencies

```bash
poetry install
```

Activate the environment:

```bash
poetry env activate
```

### 7.6 Run the ingestion pipeline

```bash
python main.py
```

Or, if your project uses a specific script:

```bash
python extraction/extraction.py
```

### 7.7 Run dbt transformations

```bash
cd datajob_etl
poetry run dbt deps
poetry run dbt run
```

### 7.8 Run dbt tests

```bash
cd datajob_etl
poetry run dbt test
```

---

## 8. Testing Guide

The project implements tests at three levels:

### 8.1 Python unit / integration tests

These validate:

- access to the source file,
- database connectivity,
- table existence,
- schema consistency,
- `job_skills` parsing,
- `job_type_skills` parsing,
- cleaning functions,
- and basic data quality checks.

Run all tests:

```bash
pytest -v
```

Or run by module:

```bash
pytest tests/test_extraction.py -v
pytest tests/test_transformation.py -v
pytest tests/test_business_logic.py -v
```

### 8.2 dbt data quality tests

These validate:

- referential integrity,
- completeness of critical fields,
- temporal consistency,
- `job_via` cleaning,
- schedule type normalization,
- mutual exclusivity of salary fields,
- bridge table integrity,
- and non-null primary keys.

Run them with:

```bash
cd datajob_etl
dbt test
```

### 8.3 Expected outcome

A successful execution should:

- load the CSV into the raw table,
- build the staging, dimensions, relationships, and fact models,
- and pass the tests defined in both pytest and dbt.

---

## 9. Logging and Observability

The project implements structured logging to support monitoring and debugging.

### Log files
- `logs/pipeline.log`
- `logs/extraction.log`
- `logs/test_extraction.log`

### Logged information
- execution start and end,
- total duration,
- processed records,
- removed duplicates,
- parsing errors,
- executed validations,
- and final pipeline status.

---

## 10. Data Quality Strategy

The data quality strategy focused on:

- **completeness**: verify that critical data exists,
- **referential integrity**: ensure foreign keys point to valid dimensions,
- **temporal consistency**: validate dates within expected ranges,
- **normalization**: remove prefixes and standardize categories,
- **salary consistency**: avoid invalid combinations,
- **uniqueness**: prevent duplicates in dimensions and bridge tables.

---

## 11. Bonus: Conceptual Star Schema for BI

Starting from the 3NF model, I would design a star schema for analytics.

### Fact table
**`fact_job_postings_analytics`**

**Grain:** one row per job posting.

### Main dimensions
- `dim_company`
- `dim_date`
- `dim_location`
- `dim_country`
- `dim_short_title`
- `dim_schedule_type`
- `dim_via`

### Optional analytical dimensions
- `dim_skill`
- `bridge_job_skill`

### Measures
- number of job postings,
- average annual salary,
- average hourly salary,
- count of remote postings,
- count of postings with health insurance,
- count of postings with no degree requirement.

### Design challenges
- **job_skills**: this would be solved with a bridge table between the fact table and `dim_skill`.
- **boolean flags**: these could remain as direct columns in the fact table or, if they grew in number, be evaluated as a junk dimension.

---

## 12. Future Improvements

- Data quality monitoring with alerts.
- Incremental optimization of dbt models.
- Exposure of metrics for an analytical dashboard.

---

## 13. Final Notes

The solution prioritizes reproducibility, traceability, data quality, and design clarity. More than simply loading data, the goal was to build a maintainable and extensible foundation for future analytical and operational needs.

