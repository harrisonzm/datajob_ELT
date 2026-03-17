{{ config(materialized='table') }}

WITH job_posts_with_lookups AS (
    SELECT 
        -- Primary key from source
        sp.id,

-- Foreign keys (lookups to dimensions)
dc.id AS company_id,
dco.id AS country_id,
dl.id AS location_id,
dv.id AS via_id,
dst.id AS schedule_type_id,
dsh.id AS short_title_id,

-- Transactional data
sp.job_title_clean AS job_title,
sp.search_location_clean AS search_location,
sp.job_posted_date,

-- Boolean flags
sp.job_work_from_home,
sp.job_no_degree_mention,
sp.job_health_insurance,

-- Salary metrics

sp.salary_rate_clean AS salary_rate,
        sp.salary_year_avg,
        sp.salary_hour_avg
        
    FROM {{ ref('stg_job_postings') }} sp

-- LEFT JOINs to handle NULLs in dimensions

LEFT JOIN {{ ref('dim_companies') }} dc 
        ON sp.company_name_clean = dc.name
    
    LEFT JOIN {{ ref('dim_countries') }} dco 
        ON sp.job_country_clean = dco.name
    
    LEFT JOIN {{ ref('dim_locations') }} dl 
        ON sp.job_location_clean = dl.location
    
    LEFT JOIN {{ ref('dim_vias') }} dv 
        ON sp.job_via_clean = dv.name
    
    LEFT JOIN {{ ref('dim_schedule_types') }} dst 
        ON sp.job_schedule_type = dst.name
    
    LEFT JOIN {{ ref('dim_short_titles') }} dsh 
        ON sp.job_title_short_clean = dsh.name
)

SELECT
    id,
    company_id,
    country_id,
    location_id,
    via_id,
    schedule_type_id,
    short_title_id,
    job_title,
    search_location,
    job_posted_date,
    job_work_from_home,
    job_no_degree_mention,
    job_health_insurance,
    salary_rate,
    salary_year_avg,
    salary_hour_avg
FROM job_posts_with_lookups
ORDER BY id