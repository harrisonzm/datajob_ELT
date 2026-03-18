{{ config(
    materialized='table',
    indexes=[
        {'columns': ['skill_id', 'type_id'], 'unique': True}
    ]
) }}

WITH skill_type_relationships AS (
    SELECT DISTINCT 
        skill_name,
        type_name
    FROM {{ ref('stg_job_skills') }}
    WHERE type_name IS NOT NULL
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY ds.id, dt.id) AS id,
    ds.id AS skill_id,
    dt.id AS type_id
FROM skill_type_relationships str
INNER JOIN {{ ref('dim_skills') }} ds 
    ON str.skill_name = ds.name
INNER JOIN {{ ref('dim_types') }} dt 
    ON str.type_name = dt.name