-- Test: Validar rangos de salarios cuando están definidos
-- Según modelo: salary_year_avg y salary_hour_avg pueden ser NULL
-- Solo valida rangos cuando los valores existen


WITH salary_validations AS (
    -- Validar salary_year_avg
    SELECT 
        id,
        salary_year_avg as salary_value,
        'salary_year_avg' as salary_type,
        CASE 
            WHEN salary_year_avg > 1000000 THEN 'too_high'
            WHEN salary_year_avg < 10000 THEN 'too_low'
        END as issue_type
    FROM {{ ref('fact_job_posts') }}
    WHERE salary_year_avg IS NOT NULL
      AND (salary_year_avg < 10000 OR salary_year_avg > 1000000)
    
    UNION ALL

-- Validar salary_hour_avg
SELECT 
        id,
        salary_hour_avg as salary_value,
        'salary_hour_avg' as salary_type,
        CASE 
            WHEN salary_hour_avg > 500 THEN 'too_high'
            WHEN salary_hour_avg < 5 THEN 'too_low'
        END as issue_type
    FROM {{ ref('fact_job_posts') }}
    WHERE salary_hour_avg IS NOT NULL
      AND (salary_hour_avg < 5 OR salary_hour_avg > 500)
)

SELECT * FROM salary_validations