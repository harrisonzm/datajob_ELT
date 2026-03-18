-- Test: Validar que los prefijos 'via' y 'melalui' fueron removidos de dim_vias
-- Según análisis: "job_via tiene de prefijo via o melalui que necesita limpieza"

SELECT 
    id,
    name
FROM {{ ref('dim_vias') }}
WHERE 
    LOWER(name) LIKE 'via %'
    OR LOWER(name) LIKE 'melalui %'
    OR LOWER(name) = 'via'
    OR LOWER(name) = 'melalui'