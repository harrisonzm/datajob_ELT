{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_job_postings') }}
),

-- Descomponer job_skills array en filas individuales
job_skills_unnested AS (
    SELECT
        id AS job_posting_id,
        TRIM(skill) AS skill_name,
        'direct' AS skill_source -- Para identificar que viene del array directo
    FROM source_data
        CROSS JOIN UNNEST (job_skills) AS skill
    WHERE
        job_skills IS NOT NULL
        AND skill IS NOT NULL
        AND TRIM(skill) != ''
),

-- Descomponer job_type_skills JSON en filas individuales
job_type_skills_unnested AS (
    SELECT
        id AS job_posting_id,
        key AS type_name,
        TRIM(skill_value) AS skill_name,
        'typed' AS skill_source -- Para identificar que viene del JSON con tipo
    FROM
        source_data
        CROSS JOIN JSON_EACH (job_type_skills) AS type_entry (key, value)
        CROSS JOIN JSON_ARRAY_ELEMENTS_TEXT (type_entry.value) AS skill_value
    WHERE
        job_type_skills IS NOT NULL
        AND type_entry.value IS NOT NULL
        AND skill_value IS NOT NULL
        AND TRIM(skill_value) != ''
),

-- Combinar ambas fuentes de skills
all_skills AS (
    SELECT
        job_posting_id,
        skill_name,
        skill_source,
        NULL AS type_name
    FROM job_skills_unnested
    UNION ALL
    SELECT
        job_posting_id,
        skill_name,
        skill_source,
        type_name
    FROM job_type_skills_unnested
),

-- Limpiar y estandarizar nombres de skills
cleaned_skills AS (
    SELECT
        job_posting_id,
        LOWER(TRIM(skill_name)) AS skill_name_clean,
        skill_source,
        CASE
            WHEN type_name IS NOT NULL THEN LOWER(TRIM(type_name))
            ELSE NULL
        END AS type_name_clean
    FROM all_skills
    WHERE
        skill_name IS NOT NULL
        AND TRIM(skill_name) != ''
)

SELECT
    job_posting_id,
    skill_name_clean AS skill_name,
    type_name_clean AS type_name,
    skill_source
FROM cleaned_skills
ORDER BY job_posting_id, skill_name