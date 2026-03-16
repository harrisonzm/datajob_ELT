import pandas as pd
import numpy as np
import time
from db.config.db import engine, create_tables, drop_tables, get_db
from sqlalchemy import String, Boolean, DateTime, Float, text
from sqlalchemy.dialects.postgresql import ARRAY, JSON
import json
import ast
from typing import Dict, List, Optional

def clean_df(df):
    print(" Eliminando duplicados exactos...")
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    print(f" Eliminados {duplicates_removed} duplicados exactos")
    print(f" Registros únicos: {len(df):,}")
    
    # 4. Limpieza rápida de datos
    print(" Limpieza rápida de datos...")
    
    # Reemplazar valores nulos de forma eficiente
    df = df.replace(['null', 'NULL', 'Null', '', ' ', 'nan'], np.nan)
    
    # Convertir booleanos
    boolean_cols = ['job_work_from_home', 'job_no_degree_mention', 'job_health_insurance']
    for col in boolean_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lower().map({
                'true': True, 't': True, '1': True, 'yes': True,
                'false': False, 'f': False, '0': False, 'no': False,
                'nan': None
            })
    
    # Convertir fechas
    if 'job_posted_date' in df.columns:
        df['job_posted_date'] = pd.to_datetime(df['job_posted_date'], errors='coerce')
    
    # Convertir numéricos
    numeric_cols = ['salary_year_avg', 'salary_hour_avg']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def parse_skills_string(skills_str: str) -> Optional[List[str]]:
    """
    Convierte string de skills a lista de strings
    Maneja formatos: "['skill1', 'skill2']" o "skill1, skill2"
    """
    if pd.isna(skills_str) or skills_str == '' or skills_str == 'nan':
        return None
    
    try:
        # Intentar parsear como lista literal de Python
        if skills_str.startswith('[') and skills_str.endswith(']'):
            return ast.literal_eval(skills_str)
        else:
            # Si no es una lista, dividir por comas
            return [skill.strip() for skill in skills_str.split(',') if skill.strip()]
    except:
        # Si falla todo, retornar como lista de un elemento
        return [skills_str.strip()] if skills_str.strip() else None

def parse_type_skills_string(type_skills_str: str) -> Optional[Dict[str, List[str]]]:
    """
    Convierte string de type_skills a diccionario
    Maneja formato: "{'programming': ['python', 'sql'], 'cloud': ['aws']}"
    """
    if pd.isna(type_skills_str) or type_skills_str == '' or type_skills_str == 'nan':
        return None
    
    try:
        # Intentar parsear como diccionario literal de Python
        if type_skills_str.startswith('{') and type_skills_str.endswith('}'):
            return ast.literal_eval(type_skills_str)
        else:
            # Si no es un diccionario válido, retornar None
            return None
    except:
        return None

def skills_parser(df):
    print(" Procesando skills...")
    if 'job_skills' in df.columns:
        df['job_skills'] = df['job_skills'].apply(parse_skills_string)
        skills_count = df['job_skills'].notna().sum()
        print(f"   {skills_count:,} job_skills procesados")
    
    if 'job_type_skills' in df.columns:
        df['job_type_skills'] = df['job_type_skills'].apply(parse_type_skills_string)
        type_skills_count = df['job_type_skills'].notna().sum()
        print(f"   {type_skills_count:,} job_type_skills procesados")
    return df

def load_optimized_fast(path: str) -> bool:
    """
    Carga optimizada ultra-rápida: elimina solo duplicados exactos y usa toda la RAM disponible
    """
    print(" Iniciando carga optimizada ultra-rápida...")
    
    try:
        # 1. Limpiar y recrear tabla
        print("  Limpiando tabla...")
        drop_tables()
        create_tables()
        
        # 2. Cargar CSV completo en memoria (tienes ~14GB libres)
        print(" Cargando CSV completo en memoria...")
        df = pd.read_csv(path)
        print(f" Cargados {len(df):,} registros en memoria")
        
        # 3. Eliminar SOLO duplicados exactos (101 registros)
        df = clean_df(df)
        print(" Datos limpiados")
        
        df = skills_parser(df)
        print("skills parseadas a array y json")
        df['id'] = None
        print("agregamos fila id")
        # 5. Inserción ultra-rápida usando to_sql con chunks grandes
        print(" Insertando datos con chunks optimizados...")
        
        df.to_sql(
            'job_posting',
            engine,
            if_exists='append',
            index=False,
            method='multi',  # Usar inserción múltiple
            chunksize=50000,  # Chunks grandes para aprovechar la RAM
            dtype={
                'job_title_short': String,
                'job_title': String,
                'job_location': String,
                'job_via': String,
                'job_schedule_type': String,
                'job_work_from_home': Boolean,
                'search_location': String,
                'job_posted_date': DateTime,
                'job_no_degree_mention': Boolean,
                'job_health_insurance': Boolean,
                'job_country': String,
                'salary_rate': String,
                'salary_year_avg': Float,
                'salary_hour_avg': Float,
                'company_name': String,
                'job_skills': ARRAY(String),
                'job_type_skills': JSON  
            }
        )
        
        print(f" Insertados {len(df):,} registros exitosamente")
        
        # 6. Verificación final
        db = next(get_db())
        try:
            total_count = db.execute(text("SELECT COUNT(*) FROM job_posting")).scalar()
            print(f" Total final en base de datos: {total_count:,}")
            
            # Mostrar algunos registros
            result = db.execute(text("SELECT id, job_title, company_name FROM job_posting ORDER BY id LIMIT 5"))
            print(" Primeros 5 registros:")
            for row in result:
                print(f"  - ID: {row[0]}, Título: {row[1]}, Empresa: {row[2]}")
                
        finally:
            db.close()
        
        return True
        
    except Exception as e:
        print(f" Error en carga optimizada: {e}")
        import traceback
        traceback.print_exc()
        return False

def execute_extraction(csv_path):
        
        print(" CARGA OPTIMIZADA ")
        print("=" * 50)
        print("Configuración:")
        print("- Engine sin echo (sin logging)")
        print("- Pool de conexiones aumentado")
        print("- Chunks de 50,000 registros")
        print("- Solo elimina duplicados exactos (101)")
        print("- Usa toda la RAM disponible (~14GB)")
        print("=" * 50)
        
        start_time = time.time()
        
        try:
            success = load_optimized_fast(csv_path)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if success:
                print(f"\n CARGA COMPLETADA EXITOSAMENTE")
                print(f"  Tiempo total: {duration:.2f} segundos")
                print(f" Velocidad aproximada: {785640/duration:.0f} registros/segundo")
            else:
                print(f"\n CARGA FALLÓ")
                print(f"  Tiempo transcurrido: {duration:.2f} segundos")
                
        except Exception as e:
            print(f" Error crítico: {e}")
            import traceback
            traceback.print_exc()