{{ config(
    materialized='table',
    indexes=[
        {'columns': ['name'], 'unique': True}
    ]
) }}

WITH unique_types AS (
    SELECT DISTINCT type_name
    FROM {{ ref('stg_job_skills') }}
    WHERE type_name IS NOT NULL
)

SELECT ROW_NUMBER() OVER (
        ORDER BY type_name
    ) AS id, type_name AS name
FROM unique_types