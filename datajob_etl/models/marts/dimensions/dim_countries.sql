{{ config(materialized='table') }}

WITH unique_countries AS (
    SELECT DISTINCT 
        job_country_clean AS name
    FROM {{ ref('stg_job_postings') }}
    WHERE job_country_clean IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM unique_countries
ORDER BY name