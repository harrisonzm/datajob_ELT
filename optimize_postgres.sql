-- Script de optimización manual para PostgreSQL
-- Ejecutar después de cargar datos para máximo rendimiento

-- ============================================
-- 1. VACUUM y ANALYZE para actualizar estadísticas
-- ============================================

VACUUM ANALYZE job_posting.job_posting;

VACUUM ANALYZE job_posting.stg_job_postings;

VACUUM ANALYZE job_posting.fact_job_posts;

VACUUM ANALYZE job_posting.dim_companies;

VACUUM ANALYZE job_posting.dim_countries;

VACUUM ANALYZE job_posting.dim_locations;

VACUUM ANALYZE job_posting.dim_vias;

VACUUM ANALYZE job_posting.dim_schedule_types;

VACUUM ANALYZE job_posting.dim_short_titles;

VACUUM ANALYZE job_posting.dim_skills;

VACUUM ANALYZE job_posting.dim_types;

-- ============================================
-- 2. Crear índices adicionales si no existen
-- ============================================

-- Índice en tabla raw para staging
CREATE INDEX IF NOT EXISTS idx_job_posting_company ON job_posting.job_posting (company_name);

CREATE INDEX IF NOT EXISTS idx_job_posting_country ON job_posting.job_posting (job_country);

CREATE INDEX IF NOT EXISTS idx_job_posting_location ON job_posting.job_posting (job_location);

-- Índices compuestos para queries comunes
CREATE INDEX IF NOT EXISTS idx_fact_company_date ON job_posting.fact_job_posts (company_id, job_posted_date);

CREATE INDEX IF NOT EXISTS idx_fact_location_date ON job_posting.fact_job_posts (location_id, job_posted_date);

CREATE INDEX IF NOT EXISTS idx_fact_title_date ON job_posting.fact_job_posts (
    short_title_id,
    job_posted_date
);

-- ============================================
-- 3. Crear índices para tablas de relaciones
-- ============================================

CREATE INDEX IF NOT EXISTS idx_job_skills_job ON job_posting.job_skills (job_id);

CREATE INDEX IF NOT EXISTS idx_job_skills_skill_type ON job_posting.job_skills (skill_type_id);

CREATE INDEX IF NOT EXISTS idx_skill_types_skill ON job_posting.skill_types (skill_id);

CREATE INDEX IF NOT EXISTS idx_skill_types_type ON job_posting.skill_types (type_id);

-- ============================================
-- 4. Verificar índices creados
-- ============================================

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE
    schemaname = 'job_posting'
ORDER BY tablename, indexname;

-- ============================================
-- 5. Ver tamaño de tablas e índices
-- ============================================

SELECT
    schemaname,
    tablename,
    pg_size_pretty (
        pg_total_relation_size (
            schemaname || '.' || tablename
        )
    ) AS total_size,
    pg_size_pretty (
        pg_relation_size (
            schemaname || '.' || tablename
        )
    ) AS table_size,
    pg_size_pretty (
        pg_total_relation_size (
            schemaname || '.' || tablename
        ) - pg_relation_size (
            schemaname || '.' || tablename
        )
    ) AS indexes_size
FROM pg_tables
WHERE
    schemaname = 'job_posting'
ORDER BY pg_total_relation_size (
        schemaname || '.' || tablename
    ) DESC;

-- ============================================
-- 6. Estadísticas de uso de índices
-- ============================================

SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE
    schemaname = 'job_posting'
ORDER BY idx_scan DESC;

-- ============================================
-- 7. Identificar tablas que necesitan VACUUM
-- ============================================

SELECT
    schemaname,
    tablename,
    n_dead_tup as dead_tuples,
    n_live_tup as live_tuples,
    ROUND(
        n_dead_tup * 100.0 / NULLIF(n_live_tup + n_dead_tup, 0),
        2
    ) as dead_percentage
FROM pg_stat_user_tables
WHERE
    schemaname = 'job_posting'
    AND n_dead_tup > 0
ORDER BY dead_percentage DESC;

-- ============================================
-- 8. Queries lentas (si pg_stat_statements está habilitado)
-- ============================================

-- Habilitar extensión si no está activa:
-- CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Ver queries más lentas:
SELECT 
    query,
    calls,
    ROUND(total_exec_time::numeric, 2) as total_time_ms,
    ROUND(mean_exec_time::numeric, 2) as avg_time_ms,
    ROUND((100 * total_exec_time / SUM(total_exec_time) OVER ())::numeric, 2) as percentage
FROM pg_stat_statements
WHERE query LIKE '%job_posting%'
ORDER BY total_exec_time DESC
LIMIT 10;

-- ============================================
-- 9. Configuración actual de PostgreSQL
-- ============================================

SELECT name, setting, unit, short_desc
FROM pg_settings
WHERE
    name IN (
        'shared_buffers',
        'work_mem',
        'maintenance_work_mem',
        'effective_cache_size',
        'max_connections',
        'max_worker_processes',
        'max_parallel_workers_per_gather',
        'max_parallel_workers',
        'random_page_cost',
        'checkpoint_timeout',
        'max_wal_size'
    )
ORDER BY name;

-- ============================================
-- 10. Recomendaciones de configuración
-- ============================================

-- Para aplicar estas configuraciones, editar postgresql.conf
-- y reiniciar PostgreSQL

/*
CONFIGURACIÓN RECOMENDADA PARA SISTEMA CON 8GB RAM:

# Memoria
shared_buffers = 2GB
work_mem = 256MB
maintenance_work_mem = 1GB
effective_cache_size = 6GB

# Paralelización
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8

# Optimización para SSD
random_page_cost = 1.1

# Checkpoints
checkpoint_timeout = 30min
max_wal_size = 4GB
wal_buffers = 16MB

# Logging (opcional, para debugging)
log_min_duration_statement = 1000  # Log queries > 1s
log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h '
*/