-- Test: Validar integridad referencial entre fact_job_posts y dimensiones
-- SOLO valida FKs que NO son nulas - si están definidas, deben existir en dimensión
-- Según modelo: via_id, location_id, schedule_type_id, country_id pueden ser NULL


WITH integrity_checks AS (
    -- company_id: OBLIGATORIO - debe existir siempre
    SELECT 
        'company_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_companies') }} d ON f.company_id = d.id
    WHERE d.id IS NULL
    
    UNION ALL

-- short_title_id: OBLIGATORIO - debe existir siempre
SELECT 
        'short_title_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_short_titles') }} d ON f.short_title_id = d.id
    WHERE d.id IS NULL
    
    UNION ALL

-- location_id: OBLIGATORIO (con "anywhere" como default)
SELECT 
        'location_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_locations') }} d ON f.location_id = d.id
    WHERE d.id IS NULL
    
    UNION ALL

-- country_id: OPCIONAL - solo valida si NO es NULL
SELECT 
        'country_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_countries') }} d ON f.country_id = d.id
    WHERE f.country_id IS NOT NULL AND d.id IS NULL
    
    UNION ALL

-- via_id: OPCIONAL - solo valida si NO es NULL
SELECT 
        'via_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_vias') }} d ON f.via_id = d.id
    WHERE f.via_id IS NOT NULL AND d.id IS NULL
    
    UNION ALL

-- schedule_type_id: OPCIONAL - solo valida si NO es NULL
SELECT 
        'schedule_type_id' as fk_name,
        COUNT(*) as orphan_count
    FROM {{ ref('fact_job_posts') }} f
    LEFT JOIN {{ ref('dim_schedule_types') }} d ON f.schedule_type_id = d.id
    WHERE f.schedule_type_id IS NOT NULL AND d.id IS NULL
)

SELECT * FROM integrity_checks WHERE orphan_count > 0