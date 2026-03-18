-- Test: Validar que schedule_types fueron normalizados
-- Según análisis: "Per diem" (per day) y "Pekerjaan tetap" (permanent) deberían estar unificados

SELECT 
    id,
    name
FROM {{ ref('dim_schedule_types') }}
WHERE 
    LOWER(name) IN ('pekerjaan tetap', 'per diem')
    OR name LIKE '%pekerjaan%'