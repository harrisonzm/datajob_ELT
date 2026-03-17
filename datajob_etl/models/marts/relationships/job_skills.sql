{{ config(materialized='table') }}


WITH job_skill_types_relationships AS (
    SELECT DISTINCT
        sjs.job_posting_id,
        sjs.skill_name,
        sjs.type_name
    FROM {{ ref('stg_job_skills') }} sjs
    WHERE sjs.type_name IS NOT NULL  -- Solo skills que tienen tipo definido
),

job_skills_with_lookups AS (
    SELECT 
        jstr.job_posting_id,
        jstr.skill_name,
        jstr.type_name,

-- Get foreign keys
fjp.id AS job_id, -- From fact_job_posts
st.id AS skill_types_id -- From skill_types
FROM
    job_skill_types_relationships jstr

-- Join with fact_job_posts to get the job_id
INNER JOIN {{ ref('fact_job_posts') }} fjp 
        ON jstr.job_posting_id = fjp.id

-- Join with skill_types through skills and types
INNER JOIN {{ ref('dim_skills') }} ds 
        ON jstr.skill_name = ds.name
    INNER JOIN {{ ref('dim_types') }} dt 
        ON jstr.type_name = dt.name
    INNER JOIN {{ ref('skill_types') }} st
        ON st.skill_id = ds.id AND st.type_id = dt.id
)

SELECT DISTINCT
    job_id,
    skill_types_id
FROM job_skills_with_lookups
ORDER BY job_id, skill_types_id