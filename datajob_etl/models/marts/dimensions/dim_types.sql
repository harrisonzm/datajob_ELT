{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['name'], 'unique': True}
    ]
) }}

-- Extract unique types from job_type_skills JSON
WITH unique_types AS (
    SELECT DISTINCT 
        type_name AS name
    FROM {{ ref('stg_job_skills') }}
    WHERE type_name IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_types