-- Test: Validar integridad de skill_types (relación skill <-> type)
-- Verifica que la tabla intermedia mantiene relaciones válidas


WITH skill_type_checks AS (
    -- Verificar que skill_id existe en dim_skills
    SELECT 
        'orphan_skill_id' as issue_type,
        COUNT(*) as issue_count
    FROM {{ ref('skill_types') }} st
    LEFT JOIN {{ ref('dim_skills') }} s ON st.skill_id = s.id
    WHERE s.id IS NULL
    
    UNION ALL

-- Verificar que type_id existe en dim_types
SELECT 
        'orphan_type_id' as issue_type,
        COUNT(*) as issue_count
    FROM {{ ref('skill_types') }} st
    LEFT JOIN {{ ref('dim_types') }} t ON st.type_id = t.id
    WHERE t.id IS NULL
    
    UNION ALL

-- Verificar que no hay duplicados en la combinación skill_id + type_id
SELECT 
        'duplicate_skill_type_pairs' as issue_type,
        COUNT(*) - COUNT(DISTINCT CONCAT(skill_id, '-', type_id)) as issue_count
    FROM {{ ref('skill_types') }}
)

SELECT * FROM skill_type_checks WHERE issue_count > 0