-- Macro para validar rangos de salarios
{% macro test_salary_range(model, column_name, min_value, max_value) %}


SELECT 
        {{ column_name }},
        COUNT(*) as invalid_count
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
      AND ({{ column_name }} < {{ min_value }} OR {{ column_name }} > {{ max_value }})
    GROUP BY {{ column_name }}
    HAVING COUNT(*) > 0

{% endmacro %}

-- Macro para detectar duplicados con variaciones menores
{% macro test_normalized_duplicates(model, column_name, threshold=1) %}


WITH normalized_values AS (
        SELECT 
            {{ column_name }} as original_value,
            LOWER(TRIM({{ column_name }})) as normalized_value,
            COUNT(*) as occurrence_count
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
        GROUP BY {{ column_name }}, LOWER(TRIM({{ column_name }}))
    ),
    
    duplicate_groups AS (
        SELECT 
            normalized_value,
            COUNT(*) as variation_count,
            STRING_AGG(original_value, ', ') as all_variations
        FROM normalized_values
        GROUP BY normalized_value
        HAVING COUNT(*) > {{ threshold }}
    )


SELECT 
        normalized_value,
        variation_count,
        all_variations
    FROM duplicate_groups

{% endmacro %}

-- Macro para validar completitud de datos críticos
{% macro test_critical_data_completeness(model, critical_columns) %}

WITH completeness_check AS (
        SELECT 
            COUNT(*) as total_records,
            {% for column in critical_columns %}
            COUNT({{ column }}) as {{ column }}_count,
            ROUND(COUNT({{ column }}) * 100.0 / COUNT(*), 2) as {{ column }}_completeness
            {%- if not loop.last -%},{%- endif -%}
            {% endfor %}
        FROM {{ model }}
    )


SELECT 
        total_records,
        {% for column in critical_columns %}
        '{{ column }}' as column_name,
        {{ column }}_count as non_null_count,
        {{ column }}_completeness as completeness_percentage
        FROM completeness_check
        WHERE {{ column }}_completeness < 80.0  -- Menos del 80% de completitud
        {%- if not loop.last %}
        UNION ALL
        SELECT 
            total_records,
        {%- endif -%}
        {% endfor %}

{% endmacro %}

-- Macro para validar consistencia temporal
{% macro test_temporal_consistency(model, date_column) %}


SELECT 
        {{ date_column }},
        COUNT(*) as record_count,
        CASE 
            WHEN {{ date_column }} > CURRENT_DATE THEN 'future_date'
            WHEN {{ date_column }} < '2020-01-01' THEN 'too_old'
            WHEN EXTRACT(YEAR FROM {{ date_column }}) = 1900 THEN 'default_date'
            ELSE 'valid'
        END as date_category
    FROM {{ model }}
    WHERE {{ date_column }} IS NOT NULL
      AND (
          {{ date_column }} > CURRENT_DATE 
          OR {{ date_column }} < '2023-01-01'
          OR EXTRACT(YEAR FROM {{ date_column }}) = 1900
      )
    GROUP BY {{ date_column }}
    HAVING COUNT(*) > 0

{% endmacro %}