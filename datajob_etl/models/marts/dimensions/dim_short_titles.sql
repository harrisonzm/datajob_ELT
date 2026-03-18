{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['name'], 'unique': True}
    ]
) }}

WITH unique_short_titles AS (
    SELECT DISTINCT 
        job_title_short_clean AS name
    FROM {{ ref('stg_job_postings') }}
    WHERE job_title_short_clean IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_short_titles