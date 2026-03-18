-- Test: Validar que todas las PKs están definidas (NOT NULL)
-- Según restricciones del modelo: "Todo atributo que sirva como PK debe estar definido"


WITH pk_checks AS (
    SELECT 'fact_job_posts' as table_name, COUNT(*) as null_pk_count
    FROM {{ ref('fact_job_posts') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_companies', COUNT(*)
    FROM {{ ref('dim_companies') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_countries', COUNT(*)
    FROM {{ ref('dim_countries') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_locations', COUNT(*)
    FROM {{ ref('dim_locations') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_short_titles', COUNT(*)
    FROM {{ ref('dim_short_titles') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_vias', COUNT(*)
    FROM {{ ref('dim_vias') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_schedule_types', COUNT(*)
    FROM {{ ref('dim_schedule_types') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_skills', COUNT(*)
    FROM {{ ref('dim_skills') }}
    WHERE id IS NULL
    
    UNION ALL
    
    SELECT 'dim_types', COUNT(*)
    FROM {{ ref('dim_types') }}
    WHERE id IS NULL
)

SELECT * FROM pk_checks WHERE null_pk_count > 0