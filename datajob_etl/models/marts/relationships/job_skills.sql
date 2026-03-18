{{ config(
    materialized='table',
    indexes=[
        {'columns': ['job_id', 'skill_types_id'], 'unique': True}
    ]
) }}

-- Simplemente hacer lookup de IDs desde stg_job_skills
-- No recalculamos, solo mapeamos a las dimensiones
SELECT DISTINCT
    sjs.job_posting_id AS job_id,
    st.id AS skill_types_id
FROM {{ ref('stg_job_skills') }} sjs
INNER JOIN {{ ref('dim_skills') }} ds 
    ON sjs.skill_name = ds.name
INNER JOIN {{ ref('dim_types') }} dt 
    ON sjs.type_name = dt.name
INNER JOIN {{ ref('skill_types') }} st
    ON st.skill_id = ds.id AND st.type_id = dt.id
-- Solo incluir jobs que existen en fact_job_posts
INNER JOIN {{ ref('fact_job_posts') }} fjp
    ON sjs.job_posting_id = fjp.id