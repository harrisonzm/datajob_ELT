-- Test usando macro personalizado para validar completitud de datos críticos
-- Solo valida campos OBLIGATORIOS: company_id, short_title_id, location_id, job_title
{{ test_critical_data_completeness(ref('fact_job_posts'), ['company_id', 'short_title_id', 'location_id', 'job_title']) }}