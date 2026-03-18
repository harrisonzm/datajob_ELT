import pandas as pd
import numpy as np
import time
from db.config.db import engine, create_tables, drop_tables, get_db
from sqlalchemy import String, Boolean, DateTime, Float, text
from sqlalchemy.dialects.postgresql import ARRAY, JSON
import json
import ast
from typing import Dict, List, Optional
from extraction.transformacion import transformar_datos, cargar_con_copy

# Importar todos los modelos para que se registren en Base.metadata
import db.data_models  # noqa: F401

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
    Carga optimizada ultra-rápida usando PostgreSQL COPY
    """
    print(" Iniciando carga optimizada con COPY...")
    
    try:
        # 1. Limpiar y recrear tabla
        print("  Limpiando tabla...")
        drop_tables()
        create_tables()
        
        # 2. Cargar CSV completo en memoria
        print(" Cargando CSV completo en memoria...")
        df = pd.read_csv(path)
        print(f" Cargados {len(df):,} registros en memoria")
        
        # 3. Eliminar SOLO duplicados exactos
        df = clean_df(df)
        print(" Datos limpiados")
        
        df = skills_parser(df)
        print("skills parseadas a array y json")
        
        # 4. Ordenar columnas según el modelo de la base de datos
        print(" Ordenando columnas según modelo de BD...")
        column_order = [
            'job_title_short',
            'job_title',
            'job_location',
            'job_via',
            'job_schedule_type',
            'job_work_from_home',
            'search_location',
            'job_posted_date',
            'job_no_degree_mention',
            'job_health_insurance',
            'job_country',
            'salary_rate',
            'salary_year_avg',
            'salary_hour_avg',
            'company_name',
            'job_skills',
            'job_type_skills'
        ]
        df = df[column_order]
        
        # 5. Convertir arrays y JSON a formato PostgreSQL
        print(" Convirtiendo arrays y JSON a formato PostgreSQL...")
        
        # Convertir job_skills a formato array de PostgreSQL
        def format_pg_array(skills_list):
            if skills_list is None or (isinstance(skills_list, float) and pd.isna(skills_list)):
                return None
            # Escapar comillas y crear formato {val1,val2,val3}
            escaped = [str(s).replace('"', '\\"').replace("'", "''") for s in skills_list]
            return '{' + ','.join(f'"{s}"' for s in escaped) + '}'
        
        df['job_skills'] = df['job_skills'].apply(format_pg_array)
        
        # Convertir job_type_skills a JSON string
        def format_pg_json(json_dict):
            if json_dict is None or (isinstance(json_dict, float) and pd.isna(json_dict)):
                return None
            return json.dumps(json_dict)
        
        df['job_type_skills'] = df['job_type_skills'].apply(format_pg_json)
        
        # 6. Usar StringIO para COPY directo desde memoria
        from io import StringIO
        
        print(" Preparando datos para COPY...")
        
        # Crear buffer en memoria
        buffer = StringIO()
        
        # Escribir datos al buffer con formato correcto para COPY
        for _, row in df.iterrows():
            line_parts = []
            for val in row:
                if pd.isna(val) or val is None or val == '':
                    line_parts.append('\\N')  # NULL en formato COPY
                elif isinstance(val, bool):
                    line_parts.append('t' if val else 'f')
                elif isinstance(val, str):
                    # Escapar caracteres especiales
                    escaped = val.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                    line_parts.append(escaped)
                else:
                    line_parts.append(str(val))
            buffer.write('\t'.join(line_parts) + '\n')
        
        buffer.seek(0)  # Volver al inicio del buffer
        
        # 7. Usar COPY para inserción ultra-rápida
        print(" Insertando datos con COPY...")
        
        db = next(get_db())
        try:
            # Usar raw connection para COPY
            raw_conn = db.connection().connection
            cursor = raw_conn.cursor()
            
            copy_sql = """
                COPY job_posting (
                    job_title_short, job_title, job_location, job_via,
                    job_schedule_type, job_work_from_home, search_location,
                    job_posted_date, job_no_degree_mention, job_health_insurance,
                    job_country, salary_rate, salary_year_avg, salary_hour_avg,
                    company_name, job_skills, job_type_skills
                )
                FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')
            """
            
            cursor.copy_expert(copy_sql, buffer)
            raw_conn.commit()
            cursor.close()
                
            print(f" Insertados {len(df):,} registros exitosamente con COPY")
            
        finally:
            db.close()
            buffer.close()
        
        # 8. Verificación final
        db = next(get_db())
        try:
            # Usar MAX(id) en lugar de COUNT(*) - mucho más rápido
            max_id = db.execute(text("SELECT MAX(id) FROM job_posting")).scalar()
            print(f" ID máximo en base de datos: {max_id:,} (aprox. {max_id:,} registros)")
            
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
        
        print(" CARGA OPTIMIZADA CON COPY ")
        print("=" * 50)
        print("Configuración:")
        print("- PostgreSQL COPY (método más rápido)")
        print("- Sin chunks, carga directa")
        print("- Solo elimina duplicados exactos")
        print("- Formato optimizado para PostgreSQL")
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
                
                # Mostrar conteo de la tabla job_posting
                db = next(get_db())
                try:
                    count = db.execute(text("SELECT COUNT(id) FROM job_posting")).scalar()
                    print(f"\njob_posting: {count:,} registros")
                finally:
                    db.close()
            else:
                print(f"\n CARGA FALLÓ")
                print(f"  Tiempo transcurrido: {duration:.2f} segundos")
                
        except Exception as e:
            print(f" Error crítico: {e}")
            import traceback
            traceback.print_exc()

def load_with_transformations(path: str) -> bool:
    """
    Carga optimizada con transformaciones en memoria.
    Hace todas las transformaciones de dbt en Python y luego carga con COPY.
    """
    print(" Iniciando carga con transformaciones en memoria...")
    
    try:
        # 1. Limpiar y recrear tablas
        print("  Limpiando tablas...")
        drop_tables()
        create_tables()
        
        # 2. Cargar CSV
        print(" Cargando CSV completo en memoria...")
        df = pd.read_csv(path)
        print(f" Cargados {len(df):,} registros en memoria")
        
        # 3. Limpiar datos
        df = clean_df(df)
        print(" Datos limpiados")
        
        # 4. Parsear skills
        df = skills_parser(df)
        print("Skills parseadas a array y json")
        
        # 5. Agregar columna id (auto-incremental)
        df = df.reset_index(drop=True)
        df['id'] = df.index + 1
        print(f"Agregada columna id (1 a {len(df):,})")
        
        # 6. Transformar datos (crear todas las dimensiones y hechos)
        tablas = transformar_datos(df)
        
        # 7. Cargar todas las tablas con COPY
        success = cargar_con_copy(tablas)
        
        if success:
            # 8. Verificación final
            db = next(get_db())
            try:
                print("\n" + "=" * 70)
                print("VERIFICACIÓN FINAL")
                print("=" * 70)
                
                for tabla_nombre in tablas.keys():
                    # Saltar verificación de tablas grandes (muy lento)
                    if tabla_nombre in ['job_skills', 'fact_job_posts']:
                        print(f"{tabla_nombre}: cargado (verificación omitida)")
                        continue
                    
                    # Usar MAX(id) para dimensiones pequeñas
                    try:
                        max_id = db.execute(text(f"SELECT MAX(id) FROM {tabla_nombre}")).scalar()
                        if max_id:
                            print(f"{tabla_nombre}: ~{max_id:,} registros (ID máximo)")
                        else:
                            print(f"{tabla_nombre}: cargado")
                    except:
                        # Si falla MAX(id), simplemente confirmar que se cargó
                        print(f"{tabla_nombre}: cargado")
                    
            finally:
                db.close()
        
        return success
        
    except Exception as e:
        print(f" Error en carga con transformaciones: {e}")
        import traceback
        traceback.print_exc()
        return False


def mostrar_conteo_tablas():
    """Muestra el conteo de registros de todas las tablas."""
    db = next(get_db())
    try:
        print("\n" + "=" * 70)
        print("CONTEO FINAL DE REGISTROS POR TABLA")
        print("=" * 70)
        
        tablas = [
            ('job_posting', 'id'),
            ('dim_companies', 'id'),
            ('dim_locations', 'id'),
            ('dim_skills', 'id'),
            ('dim_types', 'id'),
            ('job_skills', 'job_id'),
            ('fact_job_posts', 'id')
        ]
        
        for tabla, columna in tablas:
            try:
                count = db.execute(text(f"SELECT COUNT({columna}) FROM {tabla}")).scalar()
                print(f"{tabla:20} : {count:,} registros")
            except Exception as e:
                print(f"{tabla:20} : Error al contar - {str(e)}")
        
        print("=" * 70)
    finally:
        db.close()


def execute_extraction_with_transforms(csv_path):
    """Ejecuta la extracción con transformaciones en memoria."""
    
    print(" CARGA OPTIMIZADA CON TRANSFORMACIONES ")
    print("=" * 70)
    print("Configuración:")
    print("- Transformaciones en memoria (Python)")
    print("- PostgreSQL COPY para carga")
    print("- Sin dbt, todo en un solo paso")
    print("- Genera todas las dimensiones y hechos")
    print("=" * 70)
    
    start_time = time.time()
    
    try:
        success = load_with_transformations(csv_path)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            print(f"\n CARGA COMPLETADA EXITOSAMENTE")
            print(f"  Tiempo total: {duration:.2f} segundos")
            
            # Mostrar conteo de todas las tablas
            mostrar_conteo_tablas()
        else:
            print(f"\n CARGA FALLÓ")
            print(f"  Tiempo transcurrido: {duration:.2f} segundos")
            
    except Exception as e:
        print(f" Error crítico: {e}")
        import traceback
        traceback.print_exc()
