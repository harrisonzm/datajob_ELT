{{ config(materialized='view') }}


WITH source_data AS (
    SELECT * FROM {{ source('raw', 'job_posting') }}
),

cleaned_data AS (
    SELECT
        id,
        NULLIF(TRIM(job_title_short), '') AS job_title_short_clean,
        NULLIF(TRIM(job_title), '') AS job_title_clean,
        NULLIF(TRIM(job_location), '') AS job_location_clean,

-- Limpiar job_via (remover prefijos)
CASE
            WHEN job_via LIKE 'via %' THEN TRIM(SUBSTRING(job_via, 5))
            WHEN job_via LIKE 'melalui %' THEN TRIM(SUBSTRING(job_via, 9))
            ELSE NULLIF(TRIM(job_via), '')
        END AS job_via_clean,
        
        NULLIF(TRIM(job_schedule_type), '') AS job_schedule_type,
        job_work_from_home,
        NULLIF(TRIM(search_location), '') AS search_location_clean,
        job_posted_date,
        job_no_degree_mention,
        job_health_insurance,
        NULLIF(TRIM(job_country), '') AS job_country_clean,
        NULLIF(TRIM(salary_rate), '') AS salary_rate_clean,
        salary_year_avg,
        salary_hour_avg,
        NULLIF(TRIM(company_name), '') AS company_name_clean,
        job_skills,
        job_type_skills
    FROM source_data
)

SELECT * FROM cleaned_data