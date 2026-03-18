-- Validation queries to check for duplicates in all models
-- Returns rows only where duplicates are found


WITH duplicate_checks AS (
    -- 1. Check stg_job_postings for duplicate IDs
    SELECT 
        'stg_job_postings' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('stg_job_postings') }}
    
    UNION ALL

-- 2. Check dim_companies for duplicates
SELECT 
        'dim_companies' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_companies') }}
    
    UNION ALL
    
    SELECT 
        'dim_companies' AS model_name,
        'duplicate_names' AS issue_type,
        COUNT(*) - COUNT(DISTINCT name) AS issue_count
    FROM {{ ref('dim_companies') }}
    
    UNION ALL

-- 3. Check dim_countries for duplicates
SELECT 
        'dim_countries' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_countries') }}
    
    UNION ALL

-- 4. Check dim_locations for duplicates
SELECT 
        'dim_locations' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_locations') }}
    
    UNION ALL

-- 5. Check dim_skills for duplicates
SELECT 
        'dim_skills' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_skills') }}
    
    UNION ALL

-- 6. Check dim_types for duplicates
SELECT 
        'dim_types' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_types') }}
    
    UNION ALL

-- 7. Check dim_vias for duplicates
SELECT 
        'dim_vias' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('dim_vias') }}
    
    UNION ALL

-- 8. Check skill_types for duplicates
SELECT 
        'skill_types' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('skill_types') }}
    
    UNION ALL

-- 9. Check fact_job_posts for duplicates
SELECT 
        'fact_job_posts' AS model_name,
        'duplicate_ids' AS issue_type,
        COUNT(*) - COUNT(DISTINCT id) AS issue_count
    FROM {{ ref('fact_job_posts') }}
    
    UNION ALL

-- 10. Check job_skills for duplicates
SELECT 
        'job_skills' AS model_name,
        'duplicate_combinations' AS issue_type,
        COUNT(*) - COUNT(DISTINCT (job_id, skill_types_id)) AS issue_count
    FROM {{ ref('job_skills') }}
)

-- Return only rows where duplicates were found
SELECT * FROM duplicate_checks WHERE issue_count > 0