-- Test: Validar consistencia entre salary_rate y salary_avg
-- Según análisis:
-- 1. Si salary_rate = 'hour', debe tener salary_hour_avg (no salary_year_avg)
-- 2. Si salary_rate = 'year', debe tener salary_year_avg (no salary_hour_avg)
-- 3. salary_rate puede existir sin ningún salary_avg (month, week, day no tienen avg)


WITH salary_inconsistencies AS (
    -- Caso 1: salary_rate = 'hour' pero tiene year_avg en lugar de hour_avg
    SELECT 
        id,
        job_title,
        salary_rate,
        salary_year_avg,
        salary_hour_avg,
        'hour_rate_with_year_avg' as issue_type
    FROM {{ ref('fact_job_posts') }}
    WHERE salary_rate = 'hour'
      AND salary_year_avg IS NOT NULL
      AND salary_hour_avg IS NULL
    
    UNION ALL

-- Caso 2: salary_rate = 'year' pero tiene hour_avg en lugar de year_avg
SELECT 
        id,
        job_title,
        salary_rate,
        salary_year_avg,
        salary_hour_avg,
        'year_rate_with_hour_avg' as issue_type
    FROM {{ ref('fact_job_posts') }}
    WHERE salary_rate = 'year'
      AND salary_hour_avg IS NOT NULL
      AND salary_year_avg IS NULL
    
    UNION ALL

-- Caso 3: Tiene salary_avg pero no tiene salary_rate definido
SELECT 
        id,
        job_title,
        salary_rate,
        salary_year_avg,
        salary_hour_avg,
        'has_avg_without_rate' as issue_type
    FROM {{ ref('fact_job_posts') }}
    WHERE salary_rate IS NULL
      AND (salary_year_avg IS NOT NULL OR salary_hour_avg IS NOT NULL)
)

SELECT * FROM salary_inconsistencies