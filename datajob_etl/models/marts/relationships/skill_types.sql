{{ config(materialized='table') }}


WITH skill_type_relationships AS (
    SELECT DISTINCT 
        skill_name,
        type_name
    FROM {{ ref('stg_job_skills') }}
    WHERE skill_name IS NOT NULL 
      AND type_name IS NOT NULL
),

skill_types_with_ids AS (
    SELECT 
        ds.id AS skill_id,
        dt.id AS type_id,
        str.skill_name,
        str.type_name
    FROM skill_type_relationships str
    INNER JOIN {{ ref('dim_skills') }} ds 
        ON str.skill_name = ds.name
    INNER JOIN {{ ref('dim_types') }} dt 
        ON str.type_name = dt.name
)

SELECT ROW_NUMBER() OVER (
        ORDER BY skill_id, type_id
    ) AS id, skill_id, type_id
FROM skill_types_with_ids
ORDER BY skill_id, type_id