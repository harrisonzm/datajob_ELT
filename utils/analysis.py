def salary_by_job_category_analysis():
    """
    Analiza por cada categoría de job_title_short cuántas filas tienen 
    valor en salary_year_avg o salary_hour_avg
    """
    import pandas as pd

    df = pd.read_csv('./data_jobs.csv')
    
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f'[*] Duplicados eliminados: {initial_count - len(df):,}')
    print(f'[*] Registros únicos: {len(df):,}')
    print()
    
    print('ANÁLISIS DE SALARIOS POR CATEGORÍA DE TRABAJO')
    print('=' * 60)
    
    # Crear máscaras para identificar filas con salarios
    has_year_salary = df['salary_year_avg'].notna()
    has_hour_salary = df['salary_hour_avg'].notna()
    has_any_salary = has_year_salary | has_hour_salary
    has_both_salaries = has_year_salary & has_hour_salary
    
    # Agrupar por job_title_short y calcular estadísticas
    salary_stats = df.groupby('job_title_short').agg({
        'salary_year_avg': ['count', 'mean', 'median'],
        'salary_hour_avg': ['count', 'mean', 'median']
    }).round(2)
    
    # Calcular estadísticas adicionales por categoría
    category_analysis = []
    
    for category in df['job_title_short'].unique():
        if pd.isna(category):
            continue
            
        category_df = df[df['job_title_short'] == category]
        total_jobs = len(category_df)
        
        # Contar diferentes tipos de salarios
        year_count = category_df['salary_year_avg'].notna().sum()
        hour_count = category_df['salary_hour_avg'].notna().sum()
        any_salary_count = (category_df['salary_year_avg'].notna() | 
                           category_df['salary_hour_avg'].notna()).sum()
        both_salary_count = (category_df['salary_year_avg'].notna() & 
                            category_df['salary_hour_avg'].notna()).sum()
        no_salary_count = total_jobs - any_salary_count
        
        # Calcular porcentajes
        year_pct = (year_count / total_jobs * 100) if total_jobs > 0 else 0
        hour_pct = (hour_count / total_jobs * 100) if total_jobs > 0 else 0
        any_salary_pct = (any_salary_count / total_jobs * 100) if total_jobs > 0 else 0
        
        category_analysis.append({
            'job_category': category,
            'total_jobs': total_jobs,
            'with_year_salary': year_count,
            'with_hour_salary': hour_count,
            'with_any_salary': any_salary_count,
            'with_both_salaries': both_salary_count,
            'no_salary': no_salary_count,
            'year_salary_pct': round(year_pct, 2),
            'hour_salary_pct': round(hour_pct, 2),
            'any_salary_pct': round(any_salary_pct, 2)
        })
    
    # Convertir a DataFrame para mejor visualización
    analysis_df = pd.DataFrame(category_analysis)
    analysis_df = analysis_df.sort_values('total_jobs', ascending=False)
    
    # Mostrar resumen general
    print(f"Total de categorías de trabajo: {len(analysis_df)}")
    print(f"Total de trabajos analizados: {df.shape[0]}")
    print(f"Trabajos con salary_year_avg: {df['salary_year_avg'].notna().sum()}")
    print(f"Trabajos con salary_hour_avg: {df['salary_hour_avg'].notna().sum()}")
    print(f"Trabajos con cualquier salario: {has_any_salary.sum()}")
    print(f"Trabajos con ambos salarios: {has_both_salaries.sum()}")
    print()
    
    # Mostrar análisis detallado por categoría
    print("ANÁLISIS DETALLADO POR CATEGORÍA:")
    print("-" * 100)
    print(f"{'Categoría':<25} {'Total':<8} {'Year':<6} {'Hour':<6} {'Any':<6} {'Both':<6} {'None':<6} {'%Year':<7} {'%Hour':<7} {'%Any':<7}")
    print("-" * 100)
    
    for _, row in analysis_df.iterrows():
        print(f"{row['job_category']:<25} "
              f"{row['total_jobs']:<8} "
              f"{row['with_year_salary']:<6} "
              f"{row['with_hour_salary']:<6} "
              f"{row['with_any_salary']:<6} "
              f"{row['with_both_salaries']:<6} "
              f"{row['no_salary']:<6} "
              f"{row['year_salary_pct']:<7}% "
              f"{row['hour_salary_pct']:<7}% "
              f"{row['any_salary_pct']:<7}%")
    
    print("-" * 100)
    
    # Top 5 categorías con más trabajos
    print("\nTOP 5 CATEGORÍAS CON MÁS TRABAJOS:")
    top_5_jobs = analysis_df.head(5)
    for _, row in top_5_jobs.iterrows():
        print(f"• {row['job_category']}: {row['total_jobs']} trabajos "
              f"({row['any_salary_pct']}% con salario)")
    
    # Top 5 categorías con mejor porcentaje de salarios
    print("\nTOP 5 CATEGORÍAS CON MEJOR % DE SALARIOS:")
    top_5_salary = analysis_df.sort_values('any_salary_pct', ascending=False).head(5)
    for _, row in top_5_salary.iterrows():
        print(f"• {row['job_category']}: {row['any_salary_pct']}% con salario "
              f"({row['with_any_salary']}/{row['total_jobs']} trabajos)")
    
    # Categorías con peor porcentaje de salarios
    print("\nTOP 5 CATEGORÍAS CON MENOR % DE SALARIOS:")
    bottom_5_salary = analysis_df.sort_values('any_salary_pct', ascending=True).head(5)
    for _, row in bottom_5_salary.iterrows():
        print(f"• {row['job_category']}: {row['any_salary_pct']}% con salario "
              f"({row['with_any_salary']}/{row['total_jobs']} trabajos)")
    
    print('=' * 60)
    
    return analysis_df

def payment_analysis():
    import pandas as pd

    df = pd.read_csv('./data/data_jobs.csv')
    
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f'[*] Duplicados eliminados: {initial_count - len(df):,}')
    print(f'[*] Registros únicos: {len(df):,}')
    print()

    # Contar todas las ocurrencias
    job_rate = df['salary_rate'].str.isalnum().value_counts()
    job_rate = df['salary_rate'].value_counts().reset_index()
    job_year_pay = df['salary_year_avg'].count()
    job_hourly_pay = df['salary_hour_avg'].count()

    # Contar un job específico
    print('salary_rate')
    print('size: ', job_rate['count'].sum())
    print(job_rate)

    # Contar filas que tienen salary_rate y también tienen salary_year o salary_hour
    has_salary_rate = df['salary_rate'].notna()
    has_year_or_hour = df['salary_year_avg'].notna() | df['salary_hour_avg'].notna()
    salary_rate_with_pay = (has_salary_rate & has_year_or_hour)

    # Para hacer value_counts, necesitas filtrar el DataFrame primero
    df_with_rate_and_pay = df[salary_rate_with_pay]
    print('salary_rate (con pay):')
    print('size:', salary_rate_with_pay.sum())
    print(df_with_rate_and_pay['salary_rate'].value_counts())

    # Validar filas que tienen ambos salarios (year y hour) no nulos
    has_both_salary = df['salary_year_avg'].notna() & df['salary_hour_avg'].notna()
    hasBothSalary = has_both_salary.sum()


    # DataFrame que tiene uno u otro pero no ambos
    has_year_only = df['salary_year_avg'].notna() & df['salary_hour_avg'].isna()
    has_hour_only = df['salary_hour_avg'].notna() & df['salary_year_avg'].isna()

    # Contar cada uno
    year_only_count = has_year_only.sum()
    hour_only_count = has_hour_only.sum()

    # Crear DataFrame con solo uno u otro (no ambos)
    df_one_salary = df[has_year_only | has_hour_only].copy()
    has_salary_rate = df['salary_rate'].notna()
    no_year_avg = df['salary_year_avg'].isna()
    no_hour_avg = df['salary_hour_avg'].isna()

    # Filtrar: tiene salary_rate pero no tiene ninguno de los otros dos
    df_rate_only = df[has_salary_rate & no_year_avg & no_hour_avg]

    print('salary_rate sin year/hour avg:')
    print('size:', len(df_rate_only))

    # Value counts sobre salary_rate en este dataset filtrado
    rate_counts = df_rate_only['salary_rate'].value_counts()
    print(rate_counts)

    print('hasYearOnly:', year_only_count)
    print('hasHourOnly:', hour_only_count)
    print('hasBothSalary:', hasBothSalary)
    print('sumExceptHourWith: ',job_rate[job_rate['salary_rate'] != 'hour']['count'].sum())
    print('hasPay: ', (df['salary_hour_avg'].notna() | df['salary_year_avg'].notna()).sum())
    print('yearPayCount: ',job_year_pay)
    print('hourlyPayCount: ', job_hourly_pay)

def aggregation_analysis():
    import pandas as pd

    df = pd.read_csv('./data/data_jobs.csv')
    
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    
    # Imprimir el tamaño del dataset original como primer dato
    print(f'Dataset original size: {initial_count} filas, {df.shape[1]} columnas')
    print(f'[*] Duplicados eliminados: {duplicates_removed:,}')
    print(f'[*] Registros únicos: {len(df):,} filas')
    print('=' * 50)
    print(len(df['job_title_short'].str.strip().value_counts()))
    # Identificar columnas que no son de salary
    salary_columns = ['salary_rate', 'salary_year_avg', 'salary_hour_avg']
    non_salary_columns = [col for col in df.columns if col not in salary_columns]
    
    # Para cada columna que no es de salary, imprimir value_counts y tamaño
    for column in non_salary_columns:
        print(f'\nColumna: {column}')
        
        # Verificaciones adicionales para entender los datos
        unique_values = df[column].unique()
        nunique = df[column].nunique()
        value_counts = df[column].value_counts()
        
        print(f'df[column].nunique(): {nunique}')
        print(f'len(df[column].unique()): {len(unique_values)}')
        print(f'len(value_counts): {len(value_counts)}')
        print(f'Total registros no nulos: {df[column].notna().sum()}')
        
        # Si hay pocos valores únicos, mostrar todos
        if nunique <= 50:
            print('Value counts (todos los valores):')
            print(value_counts)
        else:
            print('Value counts (primeros 20):')
            print(value_counts.head(20))
            print(f'... y {nunique - 20} valores más')
        
        print('-' * 50)
    
    # Análisis específico para job_via
    print('\n' + '=' * 60)
    print('ANÁLISIS ESPECÍFICO DE JOB_VIA')
    print('=' * 60)
    
    # Contar total de registros en job_via sin filtros
    total_job_via = df['job_via'].count()
    print(f'Total registros en job_via (sin filtros): {total_job_via}')
    
    # Filtrar filas donde job_via contenga la palabra "via"
    contains_via = df['job_via'].str.contains('via', case=False, na=False)
    filtered_job_via = df[contains_via]
    
    print(f'Registros que contienen "via": {len(filtered_job_via)}')
    print(f'Porcentaje que contiene "via": {len(filtered_job_via)/total_job_via*100:.2f}%')
    
    # Filtrar filas que NO contienen "via"
    not_contains_via = ~contains_via & df['job_via'].notna()
    filtered_no_via = df[not_contains_via]
    
    print(f'Registros que NO contienen "via": {len(filtered_no_via)}')
    print(f'Porcentaje que NO contiene "via": {len(filtered_no_via)/total_job_via*100:.2f}%')
    
    # Value counts de los registros filtrados
    print('\nValue counts de job_via que contienen "via":')
    via_value_counts = filtered_job_via['job_via'].value_counts()
    print(via_value_counts)
    
    print('\nValue counts de job_via que NO contienen "via":')
    no_via_value_counts = filtered_no_via['job_via'].value_counts()
    print(no_via_value_counts)
    print('=' * 60)

def encoding_errors():
    import pandas as pd
    import re

    df = pd.read_csv('./data/data_jobs.csv')
    
    print('ANÁLISIS DE ERRORES DE ENCODING')
    print('=' * 50)
    
    # Definir patrón para caracteres permitidos: letras, números, espacios, +, paréntesis, puntos
    allowed_pattern = r'^[a-zA-Z0-9\s+().]*$'
    
    # Analizar job_title
    print('\nColumna: job_title')
    job_title_clean = df['job_title'].notna()
    job_title_with_errors = df[job_title_clean]['job_title'].apply(
        lambda x: not bool(re.match(allowed_pattern, str(x)))
    )
    
    total_job_title = df['job_title'].notna().sum()
    errors_job_title = job_title_with_errors.sum()
    
    print(f'Total filas con job_title: {total_job_title}')
    print(f'Filas con caracteres especiales: {errors_job_title}')
    print(f'Porcentaje con errores: {errors_job_title/total_job_title*100:.2f}%')
    
    # Analizar job_via
    print('\nColumna: job_via')
    job_via_clean = df['job_via'].notna()
    job_via_with_errors = df[job_via_clean]['job_via'].apply(
        lambda x: not bool(re.match(allowed_pattern, str(x)))
    )
    
    total_job_via = df['job_via'].notna().sum()
    errors_job_via = job_via_with_errors.sum()
    
    print(f'Total filas con job_via: {total_job_via}')
    print(f'Filas con caracteres especiales: {errors_job_via}')
    print(f'Porcentaje con errores: {errors_job_via/total_job_via*100:.2f}%')
    
    # Mostrar ejemplos de errores
    if errors_job_title > 0:
        print('\nEjemplos de job_title con caracteres especiales:')
        error_examples_title = df[job_title_clean][job_title_with_errors]['job_title'].head(10)
        for i, example in enumerate(error_examples_title, 1):
            print(f'{i}. {example}')
    
    if errors_job_via > 0:
        print('\nEjemplos de job_via con caracteres especiales:')
        error_examples_via = df[job_via_clean][job_via_with_errors]['job_via'].head(10)
        for i, example in enumerate(error_examples_via, 1):
            print(f'{i}. {example}')
    
    print('=' * 50)

def skills_analysis():
    import pandas as pd
    df = pd.read_csv('data_jobs.csv')
    
    # Eliminar duplicados
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f'[*] Duplicados eliminados: {initial_count - len(df):,}')
    print(f'[*] Registros únicos: {len(df):,}')
    print()
    
    print(df['job_skills'])
    print(df.iloc[len(df)-1]['job_type_skills'])  # Usar el último índice válido

skills_analysis()