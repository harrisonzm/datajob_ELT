{{ config(
    materialized='table',
    indexes=[
        {'columns': ['name'], 'unique': True}
    ]
) }}

WITH unique_skills AS (
    SELECT DISTINCT skill_name
    FROM {{ ref('stg_job_skills') }}
)

SELECT ROW_NUMBER() OVER (
        ORDER BY skill_name
    ) AS id, skill_name AS name
FROM unique_skills