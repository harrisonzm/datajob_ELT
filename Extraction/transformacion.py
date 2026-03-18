"""Transformación de datos en memoria para acelerar el proceso ETL."""
import pandas as pd
import ast
import json
from typing import Dict, Tuple
from io import StringIO
from db.config.db import get_db
from sqlalchemy import text
import tempfile
import os


def transformar_datos(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Transforma el DataFrame limpio en todas las entidades dimensionales.
    Genera IDs en memoria sin interactuar con la base de datos.
    
    Args:
        df: DataFrame limpio con datos de job_postings
        
    Returns:
        Diccionario con todos los DataFrames transformados
    """
    print("\n" + "=" * 70)
    print("INICIANDO TRANSFORMACIÓN DE DATOS")
    print("=" * 70)
    
    # ========== 1. EXTRAER SKILLS Y TIPOS ==========
    print("\n[1/8] Extrayendo skills y tipos del JSON...")
    relaciones_skills = []
    
    # Verificar que existe la columna id
    if 'id' not in df.columns:
        print("   ✗ ERROR: DataFrame no tiene columna 'id'")
        print(f"   Columnas disponibles: {df.columns.tolist()}")
        raise ValueError("DataFrame debe tener columna 'id'")
    
    for idx, row in df.iterrows():
        if pd.notna(row['job_type_skills']):
            try:
                # job_type_skills ya viene como dict después del parsing
                tipos_dict = row['job_type_skills']
                
                # Si viene como string, parsearlo
                if isinstance(tipos_dict, str):
                    tipos_dict = ast.literal_eval(tipos_dict)
                
                if isinstance(tipos_dict, dict):
                    for tipo, skills_list in tipos_dict.items():
                        if isinstance(skills_list, list):
                            for skill in skills_list:
                                skill_normalizada = str(skill).strip().lower()
                                relaciones_skills.append({
                                    'job_posting_id': row['id'],
                                    'skill_name': skill_normalizada,
                                    'type_name': tipo.lower()
                                })
            except Exception as e:
                # Debug: mostrar el error
                if len(relaciones_skills) < 5:  # Solo mostrar los primeros errores
                    print(f"   Error procesando fila {idx}: {e}")
                pass
    
    df_stg_job_skills = pd.DataFrame(relaciones_skills)
    
    if len(df_stg_job_skills) == 0:
        print("   ✗ ERROR: No se extrajeron relaciones skill-tipo")
        print("   Verificando datos de entrada...")
        print(f"   Total filas en df: {len(df)}")
        print(f"   Filas con job_type_skills no nulo: {df['job_type_skills'].notna().sum()}")
        if df['job_type_skills'].notna().sum() > 0:
            print(f"   Ejemplo de job_type_skills: {df[df['job_type_skills'].notna()]['job_type_skills'].iloc[0]}")
            print(f"   Tipo: {type(df[df['job_type_skills'].notna()]['job_type_skills'].iloc[0])}")
        raise ValueError("No se pudieron extraer skills del DataFrame")
    
    print(f"   ✓ Extraídas {len(df_stg_job_skills):,} relaciones skill-tipo")
    
    # ========== 2. CREAR DIMENSIONES ==========
    print("\n[2/8] Creando dim_skills...")
    df_dim_skills = pd.DataFrame({
        'name': df_stg_job_skills['skill_name'].unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_skills['id'] = df_dim_skills.index + 1
    df_dim_skills = df_dim_skills[['id', 'name']]
    print(f"   ✓ {len(df_dim_skills):,} skills únicas")
    
    print("\n[3/8] Creando dim_types...")
    df_dim_types = pd.DataFrame({
        'name': df_stg_job_skills['type_name'].unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_types['id'] = df_dim_types.index + 1
    df_dim_types = df_dim_types[['id', 'name']]
    print(f"   ✓ {len(df_dim_types):,} tipos únicos")
    
    print("\n[4/8] Creando dim_companies...")
    df_dim_companies = pd.DataFrame({
        'name': df['company_name'].dropna().unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_companies['id'] = df_dim_companies.index + 1
    df_dim_companies = df_dim_companies[['id', 'name']]
    print(f"   ✓ {len(df_dim_companies):,} empresas únicas")
    
    print("\n[5/8] Creando dim_locations...")
    df_dim_locations = pd.DataFrame({
        'location': df['job_location'].dropna().unique()
    }).sort_values('location').reset_index(drop=True)
    df_dim_locations['id'] = df_dim_locations.index + 1
    df_dim_locations = df_dim_locations[['id', 'location']]
    print(f"   ✓ {len(df_dim_locations):,} ubicaciones únicas")
    
    print("\n[6/8] Creando dim_countries...")
    df_dim_countries = pd.DataFrame({
        'name': df['job_country'].dropna().unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_countries['id'] = df_dim_countries.index + 1
    df_dim_countries = df_dim_countries[['id', 'name']]
    print(f"   ✓ {len(df_dim_countries):,} países únicos")
    
    print("\n[7/8] Creando dim_vias...")
    df_dim_vias = pd.DataFrame({
        'name': df['job_via'].dropna().unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_vias['id'] = df_dim_vias.index + 1
    df_dim_vias = df_dim_vias[['id', 'name']]
    print(f"   ✓ {len(df_dim_vias):,} vías únicas")
    
    print("\n[8/8] Creando dim_schedule_types...")
    df_dim_schedule_types = pd.DataFrame({
        'name': df['job_schedule_type'].dropna().unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_schedule_types['id'] = df_dim_schedule_types.index + 1
    df_dim_schedule_types = df_dim_schedule_types[['id', 'name']]
    print(f"   ✓ {len(df_dim_schedule_types):,} tipos de horario únicos")
    
    print("\nCreando dim_short_titles...")
    df_dim_short_titles = pd.DataFrame({
        'name': df['job_title_short'].dropna().unique()
    }).sort_values('name').reset_index(drop=True)
    df_dim_short_titles['id'] = df_dim_short_titles.index + 1
    df_dim_short_titles = df_dim_short_titles[['id', 'name']]
    print(f"   ✓ {len(df_dim_short_titles):,} títulos cortos únicos")
    
    # ========== 3. CREAR SKILL_TYPES (relación skill-tipo) ==========
    print("\nCreando skill_types (relación skill-tipo)...")
    df_skill_types_temp = df_stg_job_skills[['skill_name', 'type_name']].drop_duplicates()
    
    # Hacer merge con las dimensiones para obtener IDs
    df_skill_types = df_skill_types_temp.merge(
        df_dim_skills, left_on='skill_name', right_on='name', how='inner'
    ).rename(columns={'id': 'skill_id'})
    
    df_skill_types = df_skill_types.merge(
        df_dim_types, left_on='type_name', right_on='name', how='inner'
    ).rename(columns={'id': 'type_id'})
    
    df_skill_types = df_skill_types[['skill_id', 'type_id']].drop_duplicates().reset_index(drop=True)
    df_skill_types['id'] = df_skill_types.index + 1
    df_skill_types = df_skill_types[['id', 'skill_id', 'type_id']]
    print(f"   ✓ {len(df_skill_types):,} combinaciones skill-tipo únicas")
    
    # ========== 4. CREAR FACT_JOB_POSTS ==========
    print("\nCreando fact_job_posts...")
    df_fact = df[['id', 'job_title', 'search_location', 'job_posted_date',
                  'job_work_from_home', 'job_no_degree_mention', 'job_health_insurance',
                  'salary_rate', 'salary_year_avg', 'salary_hour_avg',
                  'company_name', 'job_country', 'job_location', 'job_via',
                  'job_schedule_type', 'job_title_short']].copy()
    
    # Preservar el id original antes de los merges
    original_ids = df_fact['id'].copy()
    
    # Hacer lookups para obtener foreign keys
    df_fact = df_fact.merge(df_dim_companies, left_on='company_name', right_on='name', how='left', suffixes=('', '_dim'))
    df_fact['company_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'name'], inplace=True, errors='ignore')
    
    df_fact = df_fact.merge(df_dim_countries, left_on='job_country', right_on='name', how='left', suffixes=('', '_dim'))
    df_fact['country_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'name'], inplace=True, errors='ignore')
    
    df_fact = df_fact.merge(df_dim_locations, left_on='job_location', right_on='location', how='left', suffixes=('', '_dim'))
    df_fact['location_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'location'], inplace=True, errors='ignore')
    
    df_fact = df_fact.merge(df_dim_vias, left_on='job_via', right_on='name', how='left', suffixes=('', '_dim'))
    df_fact['via_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'name'], inplace=True, errors='ignore')
    
    df_fact = df_fact.merge(df_dim_schedule_types, left_on='job_schedule_type', right_on='name', how='left', suffixes=('', '_dim'))
    df_fact['schedule_type_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'name'], inplace=True, errors='ignore')
    
    df_fact = df_fact.merge(df_dim_short_titles, left_on='job_title_short', right_on='name', how='left', suffixes=('', '_dim'))
    df_fact['short_title_id'] = df_fact['id_dim']
    df_fact.drop(columns=['id_dim', 'name'], inplace=True, errors='ignore')
    
    # Restaurar el id original
    df_fact['id'] = original_ids.values
    
    # Seleccionar solo las columnas finales
    df_fact_final = df_fact[[
        'id', 'company_id', 'country_id', 'location_id', 'via_id',
        'schedule_type_id', 'short_title_id', 'job_title', 'search_location',
        'job_posted_date', 'job_work_from_home', 'job_no_degree_mention',
        'job_health_insurance', 'salary_rate', 'salary_year_avg', 'salary_hour_avg'
    ]].copy()
    
    print(f"   ✓ {len(df_fact_final):,} registros en fact_job_posts")
    
    # ========== 5. CREAR JOB_SKILLS (relación job-skill-tipo) ==========
    print("\nCreando job_skills...")
    
    # Agregar skill_id y type_id a stg_job_skills
    df_job_skills = df_stg_job_skills.merge(
        df_dim_skills, left_on='skill_name', right_on='name', how='inner'
    ).rename(columns={'id': 'skill_id'})
    
    df_job_skills = df_job_skills.merge(
        df_dim_types, left_on='type_name', right_on='name', how='inner'
    ).rename(columns={'id': 'type_id'})
    
    # Obtener skill_types_id
    df_job_skills = df_job_skills.merge(
        df_skill_types[['id', 'skill_id', 'type_id']],
        on=['skill_id', 'type_id'],
        how='inner'
    ).rename(columns={'id': 'skill_types_id'})
    
    # Seleccionar solo job_id y skill_types_id, eliminar duplicados
    df_job_skills_final = df_job_skills[['job_posting_id', 'skill_types_id']].drop_duplicates()
    df_job_skills_final.columns = ['job_id', 'skill_types_id']
    
    print(f"   ✓ {len(df_job_skills_final):,} relaciones job-skill")
    
    print("\n" + "=" * 70)
    print("TRANSFORMACIÓN COMPLETADA")
    print("=" * 70)
    
    return {
        'dim_skills': df_dim_skills,
        'dim_types': df_dim_types,
        'dim_companies': df_dim_companies,
        'dim_locations': df_dim_locations,
        'dim_countries': df_dim_countries,
        'dim_vias': df_dim_vias,
        'dim_schedule_types': df_dim_schedule_types,
        'dim_short_titles': df_dim_short_titles,
        'skill_types': df_skill_types,
        'fact_job_posts': df_fact_final,
        'job_skills': df_job_skills_final
    }


def cargar_con_copy(tablas: Dict[str, pd.DataFrame]) -> bool:
    """
    Carga todas las tablas usando PostgreSQL COPY.
    
    Args:
        tablas: Diccionario con nombre_tabla -> DataFrame
        
    Returns:
        True si la carga fue exitosa
    """
    print("\n" + "=" * 70)
    print("INICIANDO CARGA CON COPY")
    print("=" * 70)
    
    # Orden de carga (respetando foreign keys)
    orden_carga = [
        'dim_skills',
        'dim_types',
        'dim_companies',
        'dim_locations',
        'dim_countries',
        'dim_vias',
        'dim_schedule_types',
        'dim_short_titles',
        'skill_types',
        'fact_job_posts',
        'job_skills'
    ]
    
    db = next(get_db())
    
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        for tabla_nombre in orden_carga:
            if tabla_nombre not in tablas:
                print(f"\n[SKIP] {tabla_nombre} - no encontrada")
                continue
                
            df = tablas[tabla_nombre]
            print(f"\n[{orden_carga.index(tabla_nombre) + 1}/{len(orden_carga)}] Cargando {tabla_nombre}...")
            print(f"   Registros: {len(df):,}")
            
            # Convertir columnas numéricas a int donde sea apropiado
            for col in df.columns:
                if df[col].dtype == 'float64':
                    # Si todos los valores no nulos son enteros, convertir a int
                    if df[col].notna().any():
                        if (df[col].dropna() % 1 == 0).all():
                            df[col] = df[col].fillna(-1).astype('int64').replace(-1, pd.NA)
            
            # Crear buffer en memoria
            buffer = StringIO()
            
            # Escribir datos al buffer
            for _, row in df.iterrows():
                line_parts = []
                for val in row:
                    if pd.isna(val) or val is None:
                        line_parts.append('\\N')
                    elif isinstance(val, bool):
                        line_parts.append('t' if val else 'f')
                    elif isinstance(val, (int, pd.Int64Dtype)):
                        line_parts.append(str(int(val)))
                    elif isinstance(val, float):
                        # Para floats, verificar si es entero
                        if val == int(val):
                            line_parts.append(str(int(val)))
                        else:
                            line_parts.append(str(val))
                    elif isinstance(val, str):
                        escaped = val.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                        line_parts.append(escaped)
                    else:
                        line_parts.append(str(val))
                buffer.write('\t'.join(line_parts) + '\n')
            
            buffer.seek(0)
            
            # Obtener nombres de columnas
            columnas = ', '.join(df.columns)
            
            # Ejecutar COPY
            copy_sql = f"""
                COPY {tabla_nombre} ({columnas})
                FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')
            """
            
            cursor.copy_expert(copy_sql, buffer)
            buffer.close()
            
            print(f"   ✓ Cargado exitosamente")
        
        raw_conn.commit()
        cursor.close()
        
        print("\n" + "=" * 70)
        print("CARGA COMPLETADA EXITOSAMENTE")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error en carga: {e}")
        import traceback
        traceback.print_exc()
        raw_conn.rollback()
        return False
        
    finally:
        db.close()
