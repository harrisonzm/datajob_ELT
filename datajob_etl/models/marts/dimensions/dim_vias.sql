{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['name'], 'unique': True}
    ]
) }}

WITH unique_vias AS (
    SELECT DISTINCT 
        job_via_clean AS name
    FROM {{ ref('stg_job_postings') }}
    WHERE job_via_clean IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_vias