{{ config(
    materialized='incremental',
    unique_key='job_posting_id',
    on_schema_change='fail'
) }}


WITH source_data AS (
    SELECT 
        id,
        job_type_skills
    FROM {{ ref('stg_job_postings') }}
    WHERE job_type_skills IS NOT NULL
    
    {% if is_incremental() %}
    -- Solo procesar registros nuevos en ejecuciones incrementales
    AND id > (SELECT MAX(job_posting_id) FROM {{ this }})
    {% endif %}
)

-- Descomponer job_type_skills JSON en una sola pasada
SELECT
    id AS job_posting_id,
    LOWER(TRIM(skill_value)) AS skill_name,
    LOWER(TRIM(type_key)) AS type_name
FROM source_data,
LATERAL jsonb_each(job_type_skills::jsonb) AS types(type_key, skills_array),
LATERAL jsonb_array_elements_text(skills_array) AS skill_value
WHERE 
    skill_value IS NOT NULL
    AND TRIM(skill_value) != ''