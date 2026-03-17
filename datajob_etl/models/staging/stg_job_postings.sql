{{ config(materialized='view') }}


WITH source_data AS (
    SELECT * FROM {{ source('raw', 'job_posting') }}
),

cleaned_data AS (
    SELECT
        id,

-- Limpiar job_title_short
CASE
    WHEN TRIM(job_title_short) = '' THEN NULL
    ELSE TRIM(job_title_short)
END AS job_title_short_clean,

-- Limpiar job_title
CASE
    WHEN TRIM(job_title) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            job_title,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_title_clean,

-- Limpiar job_location
CASE
    WHEN TRIM(job_location) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            job_location,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_location_clean,

-- Limpiar job_via (remover prefijos)
CASE
    WHEN TRIM(job_via) = '' THEN NULL
    WHEN LOWER(TRIM(job_via)) LIKE 'via %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 5
        )
    )
    WHEN LOWER(TRIM(job_via)) LIKE 'melalui %' THEN TRIM(
        SUBSTRING(
            job_via
            FROM 9
        )
    )
    ELSE TRIM(
        REGEXP_REPLACE(
            job_via,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS job_via_clean,

-- Estandarizar job_schedule_type
CASE
    WHEN TRIM(job_schedule_type) = '' THEN NULL
    ELSE TRIM(job_schedule_type)
END AS job_schedule_type,

-- Campos booleanos (ya procesados en extracción)
job_work_from_home,

-- Limpiar search_location
CASE
    WHEN TRIM(search_location) = '' THEN NULL
    ELSE TRIM(search_location)
END AS search_location_clean,

-- Fecha (ya procesada en extracción)
job_posted_date,

-- Campos booleanos
job_no_degree_mention, job_health_insurance,

-- Limpiar job_country
CASE
    WHEN TRIM(job_country) = '' THEN NULL
    ELSE TRIM(job_country)
END AS job_country_clean,

-- Limpiar salary_rate
CASE
    WHEN TRIM(salary_rate) = '' THEN NULL
    ELSE TRIM(salary_rate)
END AS salary_rate_clean,

-- Campos numéricos (ya procesados en extracción)
salary_year_avg, salary_hour_avg,

-- Limpiar company_name
CASE
    WHEN TRIM(company_name) = '' THEN NULL
    ELSE TRIM(
        REGEXP_REPLACE(
            company_name,
            '[^\w\s+().-]',
            '',
            'g'
        )
    )
END AS company_name_clean,

-- Arrays y JSON (ya procesados en extracción)
job_skills, job_type_skills FROM source_data )

SELECT * FROM cleaned_data