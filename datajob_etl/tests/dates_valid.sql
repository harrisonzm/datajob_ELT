-- Test personalizado: Validar consistencia temporal
-- Verifica que las fechas NO sean anteriores a 2023

SELECT 
    id,
    job_posted_date,
    CASE 
        WHEN job_posted_date > CURRENT_DATE THEN 'future_date'
        WHEN job_posted_date < '2023-01-01' THEN 'before_2023'
        WHEN EXTRACT(YEAR FROM job_posted_date) = 1900 THEN 'default_date'
    END as issue_type
FROM {{ ref('fact_job_posts') }}
WHERE 
    job_posted_date > CURRENT_DATE 
    OR job_posted_date < '2023-01-01'
    OR EXTRACT(YEAR FROM job_posted_date) = 1900