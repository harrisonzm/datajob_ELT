-- Test: Validar que todos los campos booleanos son NOT NULL
-- Según restricciones del modelo: "Todo atributo booleano en el modelo es no nulo"


WITH boolean_nulls AS (
    SELECT 
        'job_work_from_home' as column_name,
        COUNT(*) as null_count
    FROM {{ ref('fact_job_posts') }}
    WHERE job_work_from_home IS NULL

    UNION ALL

    SELECT 
        'job_no_degree_mention' as column_name,
        COUNT(*) as null_count
    FROM {{ ref('fact_job_posts') }}
    WHERE job_no_degree_mention IS NULL

    UNION ALL

    SELECT 
        'job_health_insurance' as column_name,
        COUNT(*) as null_count
    FROM {{ ref('fact_job_posts') }}
    WHERE job_health_insurance IS NULL
)

SELECT * FROM boolean_nulls WHERE null_count > 0