{{ config(
    materialized='table',
    indexes=[
        {'columns': ['id'], 'unique': True},
        {'columns': ['location']},
        {'columns': ['country_id']}
    ]
) }}


WITH unique_locations AS (
    SELECT DISTINCT 
        job_location_clean AS location,
        job_country_clean AS country_name
    FROM {{ ref('stg_job_postings') }}
    WHERE job_location_clean IS NOT NULL
),

locations_with_country_id AS (
    SELECT DISTINCT
        ul.location,
        dc.id AS country_id
    FROM unique_locations ul
    LEFT JOIN {{ ref('dim_countries') }} dc 
        ON ul.country_name = dc.name
)

SELECT ROW_NUMBER() OVER (
        ORDER BY location, country_id
    ) AS id, location, country_id
FROM locations_with_country_id