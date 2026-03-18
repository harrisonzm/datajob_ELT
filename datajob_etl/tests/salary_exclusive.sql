-- Test: Validar que un job_post NO tenga ambos salary_year_avg Y salary_hour_avg
-- Según análisis: "Ninguna fila tiene tanto salary_year como salary_hour" (hasBothSalary == 0)
-- Los salarios son mutuamente excluyentes

SELECT 
    id,
    job_title,
    salary_year_avg,
    salary_hour_avg,
    salary_rate
FROM {{ ref('fact_job_posts') }}
WHERE 
    salary_year_avg IS NOT NULL 
    AND salary_hour_avg IS NOT NULL