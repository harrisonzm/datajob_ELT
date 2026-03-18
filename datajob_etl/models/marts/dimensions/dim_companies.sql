{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['name'], 'unique': True}
    ]
) }}

WITH unique_companies AS (
    SELECT DISTINCT 
        company_name_clean AS name
    FROM {{ ref('stg_job_postings') }}
    WHERE company_name_clean IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_companies