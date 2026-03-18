-- Test: Validar que location_id es obligatorio
-- Según modelo: "se decidió almacenar 'anywhere' como valor y obligarlo a estar definido"

SELECT 
    id,
    job_title,
    location_id
FROM {{ ref('fact_job_posts') }}
WHERE location_id IS NULL