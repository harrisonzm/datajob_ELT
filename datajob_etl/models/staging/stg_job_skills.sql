{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_job_postings') }}
),

-- Descomponer job_skills array en filas individuales
job_skills_unnested AS (
    SELECT
        id AS job_posting_id,
        TRIM(skill) AS skill_name,
        'direct' AS skill_source,
        NULL AS type_name
    FROM source_data, UNNEST (job_skills) AS skill
    WHERE
        job_skills IS NOT NULL
        AND skill IS NOT NULL
        AND TRIM(skill) != ''
)

-- Por ahora solo procesamos job_skills (arrays simples)
-- TODO: Agregar job_type_skills (JSON) en una segunda iteración
SELECT
    job_posting_id,
    LOWER(TRIM(skill_name)) AS skill_name,
    type_name,
    skill_source
FROM job_skills_unnested
WHERE
    skill_name IS NOT NULL
    AND TRIM(skill_name) != ''
ORDER BY job_posting_id, skill_name