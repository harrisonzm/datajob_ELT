{{ config(materialized='table') }}

-- Temporal: Crear tipos básicos hasta que implementemos JSON completo
WITH
    basic_types AS (
        SELECT 'programming' AS name
        UNION ALL
        SELECT 'database'
        UNION ALL
        SELECT 'cloud'
        UNION ALL
        SELECT 'framework'
        UNION ALL
        SELECT 'tool'
        UNION ALL
        SELECT 'other'
    )

SELECT ROW_NUMBER() OVER (
        ORDER BY name
    ) AS id, name
FROM basic_types
ORDER BY name