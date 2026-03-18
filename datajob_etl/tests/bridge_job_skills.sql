-- Test: Validar integridad de la tabla puente job_skills
-- Verifica que las relaciones muchos-a-muchos sean válidas


WITH bridge_integrity AS (
    -- Verificar que job_id existe en fact_job_posts
    SELECT 
        'orphan_job_id' as issue_type,
        COUNT(*) as issue_count
    FROM {{ ref('job_skills') }} js
    LEFT JOIN {{ ref('fact_job_posts') }} f ON js.job_id = f.id
    WHERE f.id IS NULL
    
    UNION ALL

-- Verificar que skill_type_id existe en skill_types
SELECT 
        'orphan_skill_type_id' as issue_type,
        COUNT(*) as issue_count
    FROM {{ ref('job_skills') }} js
    LEFT JOIN {{ ref('skill_types') }} st ON js.skill_type_id = st.id
    WHERE st.id IS NULL
    
    UNION ALL

-- Verificar que no hay duplicados en la combinación job_id + skill_type_id
SELECT 
        'duplicate_combinations' as issue_type,
        COUNT(*) - COUNT(DISTINCT CONCAT(job_id, '-', skill_type_id)) as issue_count
    FROM {{ ref('job_skills') }}
)

SELECT * FROM bridge_integrity WHERE issue_count > 0