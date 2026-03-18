import pandas as pd
import numpy as np
import time
import logging
import multiprocessing as mp
from datetime import datetime
from db.config.db import engine, create_tables, drop_tables, get_db
from sqlalchemy import String, Boolean, DateTime, Float, text
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
import ast
from utils.system_optimizer import calculate_optimal_chunk_size, get_system_specs

# Configurar logging
logger = logging.getLogger(__name__)

def setup_logging(log_level=logging.INFO):
    """Configura el sistema de logging para el módulo de extracción"""
    # Evitar duplicación de handlers
    if logger.handlers:
        return
    
    logger.setLevel(log_level)
    
    # Handler para consola
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    
    # Handler para archivo
    file_handler = logging.FileHandler('logs/extraction.log', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # Formato detallado
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    # Evitar propagación al logger raíz
    logger.propagate = False
    
    logger.info("Sistema de logging inicializado")

# Inicializar logging al importar el módulo
setup_logging()

def clean_df(df):
    logger.info("Iniciando limpieza de DataFrame")
    logger.debug(f"Dimensiones iniciales: {df.shape}")
    
    # Eliminar duplicados
    logger.info("Eliminando duplicados exactos...")
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    logger.info(f"Eliminados {duplicates_removed} duplicados exactos")
    logger.info(f"Registros únicos: {len(df):,}")
    
    # Limpieza de datos
    logger.info("Iniciando limpieza de valores nulos y conversión de tipos")
    
    # Reemplazar valores nulos
    null_values = ['null', 'NULL', 'Null', '', ' ', 'nan']
    df = df.replace(null_values, np.nan)
    logger.debug(f"Valores nulos reemplazados: {null_values}")
    
    # Convertir booleanos
    boolean_cols = ['job_work_from_home', 'job_no_degree_mention', 'job_health_insurance']
    for col in boolean_cols:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = df[col].astype(str).str.lower().map({
                'true': True, 't': True, '1': True, 'yes': True,
                'false': False, 'f': False, '0': False, 'no': False,
                'nan': None
            })
            after_nulls = df[col].isna().sum()
            logger.debug(f"Columna '{col}' convertida a booleano. Nulos: {before_nulls} -> {after_nulls}")
    
    # Convertir fechas
    if 'job_posted_date' in df.columns:
        before_nulls = df['job_posted_date'].isna().sum()
        df['job_posted_date'] = pd.to_datetime(df['job_posted_date'], errors='coerce')
        after_nulls = df['job_posted_date'].isna().sum()
        invalid_dates = after_nulls - before_nulls
        logger.info(f"Columna 'job_posted_date' convertida. Fechas inválidas: {invalid_dates}")
    
    # Convertir numéricos
    numeric_cols = ['salary_year_avg', 'salary_hour_avg']
    for col in numeric_cols:
        if col in df.columns:
            before_nulls = df[col].isna().sum()
            df[col] = pd.to_numeric(df[col], errors='coerce')
            after_nulls = df[col].isna().sum()
            invalid_nums = after_nulls - before_nulls
            logger.debug(f"Columna '{col}' convertida a numérico. Valores inválidos: {invalid_nums}")
    
    logger.info("Limpieza de DataFrame completada exitosamente")
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
    except Exception as e:
        logger.warning(f"Error parseando skills: {skills_str[:50]}... - {str(e)}")
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
    except Exception as e:
        logger.warning(f"Error parseando type_skills: {type_skills_str[:50]}... - {str(e)}")
        return None

def skills_parser(df):
    """Procesa skills usando paralelización para mejor rendimiento"""
    logger.info("Iniciando procesamiento de skills con paralelización")
    
    # Determinar número óptimo de workers
    n_cores = mp.cpu_count()
    n_workers = max(1, n_cores - 1)  # Dejar un core libre
    logger.info(f"Usando {n_workers} workers para procesamiento paralelo (de {n_cores} cores disponibles)")
    
    if 'job_skills' in df.columns:
        logger.debug("Procesando columna 'job_skills'")
        before_count = df['job_skills'].notna().sum()
        
        # Procesar en paralelo si hay suficientes datos
        if len(df) > 10000 and n_workers > 1:
            logger.debug(f"Procesamiento paralelo activado para job_skills")
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                df['job_skills'] = list(executor.map(parse_skills_string, df['job_skills']))
        else:
            df['job_skills'] = df['job_skills'].apply(parse_skills_string)
        
        skills_count = df['job_skills'].notna().sum()
        logger.info(f"job_skills procesados: {skills_count:,} registros válidos (antes: {before_count:,})")
    else:
        logger.warning("Columna 'job_skills' no encontrada en DataFrame")
    
    if 'job_type_skills' in df.columns:
        logger.debug("Procesando columna 'job_type_skills'")
        before_count = df['job_type_skills'].notna().sum()
        
        # Procesar en paralelo si hay suficientes datos
        if len(df) > 10000 and n_workers > 1:
            logger.debug(f"Procesamiento paralelo activado para job_type_skills")
            with ThreadPoolExecutor(max_workers=n_workers) as executor:
                df['job_type_skills'] = list(executor.map(parse_type_skills_string, df['job_type_skills']))
        else:
            df['job_type_skills'] = df['job_type_skills'].apply(parse_type_skills_string)
        
        type_skills_count = df['job_type_skills'].notna().sum()
        logger.info(f"job_type_skills procesados: {type_skills_count:,} registros válidos (antes: {before_count:,})")
    else:
        logger.warning("Columna 'job_type_skills' no encontrada en DataFrame")
    
    logger.info("Procesamiento de skills completado")
    return df

def load_optimized_fast(path: str) -> bool:
    """
    Carga optimizada con configuración óptima de chunks
    """
    logger.info("="*60)
    logger.info("INICIANDO CARGA OPTIMIZADA DE DATOS")
    logger.info("="*60)
    
    # Detectar recursos del sistema
    n_cores = mp.cpu_count()
    logger.info(f"Sistema detectado: {n_cores} cores de CPU disponibles")
    
    try:
        # 1. Limpiar y recrear tabla
        logger.info("PASO 1: Preparando base de datos")
        drop_tables()
        create_tables()
        logger.info("Base de datos preparada correctamente")
        
        # 2. Cargar CSV con optimizaciones
        logger.info(f"PASO 2: Cargando archivo CSV desde '{path}'")
        load_start = time.time()
        
        df = pd.read_csv(path, engine='c', low_memory=False)
        
        load_time = time.time() - load_start
        logger.info(f"CSV cargado en memoria: {len(df):,} registros en {load_time:.2f}s")
        
        # 3. Limpieza de datos
        logger.info("PASO 3: Limpiando y transformando datos")
        clean_start = time.time()
        df = clean_df(df)
        clean_time = time.time() - clean_start
        logger.info(f"Limpieza completada en {clean_time:.2f}s")
        
        # 4. Parseo de skills con paralelización
        logger.info("PASO 4: Parseando skills (paralelizado)")
        skills_start = time.time()
        df = skills_parser(df)
        skills_time = time.time() - skills_start
        logger.info(f"Skills parseados en {skills_time:.2f}s")
        
        # 5. Preparar columna ID
        df['id'] = None
        
        # 6. Inserción optimizada con chunk size calculado automáticamente
        logger.info("PASO 5: Insertando datos en PostgreSQL")
        
        # Calcular chunk size óptimo según especificaciones del sistema
        specs = get_system_specs()
        optimal_chunksize = calculate_optimal_chunk_size(
            total_rows=len(df),
            available_memory_gb=specs['available_memory_gb'],
            cpu_cores=specs['cpu_cores']
        )
        logger.info(f"Sistema: {specs['cpu_cores']} cores, {specs['available_memory_gb']:.1f}GB RAM disponible")
        logger.info(f"Chunk size óptimo calculado: {optimal_chunksize:,} registros")
        
        insert_start = time.time()
        
        df.to_sql(
            'job_posting',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=optimal_chunksize,
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
        
        insert_time = time.time() - insert_start
        records_per_sec = len(df) / insert_time if insert_time > 0 else 0
        logger.info(f"Inserción completada: {len(df):,} registros en {insert_time:.2f}s ({records_per_sec:.0f} rec/s)")
        
        # 7. Verificación final
        logger.info("PASO 6: Verificando datos insertados")
        db = next(get_db())
        try:
            total_count = db.execute(text("SELECT COUNT(*) FROM job_posting")).scalar()
            logger.info(f"Total de registros en base de datos: {total_count:,}")
            
            if total_count != len(df):
                logger.warning(f"Discrepancia: esperados {len(df):,}, encontrados {total_count:,}")
                
        finally:
            db.close()
        
        logger.info("="*60)
        logger.info("CARGA COMPLETADA EXITOSAMENTE")
        logger.info("="*60)
        return True
        
    except Exception as e:
        logger.error("="*60)
        logger.error("ERROR EN CARGA DE DATOS")
        logger.error("="*60)
        logger.error(f"Tipo de error: {type(e).__name__}")
        logger.error(f"Mensaje: {str(e)}")
        logger.exception("Traceback completo:")
        return False

def load_with_copy(path: str) -> bool:
    """
    Método alternativo usando PostgreSQL COPY - el más rápido para bulk inserts
    """
    logger.info("="*60)
    logger.info("CARGA ULTRA-RÁPIDA CON POSTGRESQL COPY")
    logger.info("="*60)
    
    n_cores = mp.cpu_count()
    logger.info(f"Sistema detectado: {n_cores} cores de CPU disponibles")
    
    try:
        # 1. Preparar base de datos
        logger.info("PASO 1: Preparando base de datos")
        drop_tables()
        create_tables()
        logger.info("Base de datos preparada")
        
        # 2. Cargar y procesar CSV
        logger.info(f"PASO 2: Cargando y procesando '{path}'")
        load_start = time.time()
        df = pd.read_csv(path, engine='c', low_memory=False)
        logger.info(f"CSV cargado: {len(df):,} registros en {time.time() - load_start:.2f}s")
        
        # 3. Limpieza y transformación
        logger.info("PASO 3: Limpiando datos")
        df = clean_df(df)
        
        logger.info("PASO 4: Parseando skills")
        df = skills_parser(df)
        
        # 4. Preparar datos para COPY
        logger.info("PASO 5: Preparando datos para COPY bulk insert")
        
        # Reordenar columnas (SIN incluir id, ya que es auto-incremental)
        column_order = [
            'job_title_short', 'job_title', 'job_location', 'job_via',
            'job_schedule_type', 'job_work_from_home', 'search_location',
            'job_posted_date', 'job_no_degree_mention', 'job_health_insurance',
            'job_country', 'salary_rate', 'salary_year_avg', 'salary_hour_avg',
            'company_name', 'job_skills', 'job_type_skills'
        ]
        df = df[column_order]
        
        # Convertir arrays y JSON a formato PostgreSQL
        if 'job_skills' in df.columns:
            df['job_skills'] = df['job_skills'].apply(
                lambda x: '{' + ','.join(f'"{s}"' for s in x) + '}' if isinstance(x, list) else None
            )
        
        if 'job_type_skills' in df.columns:
            df['job_type_skills'] = df['job_type_skills'].apply(
                lambda x: str(x).replace("'", '"') if x is not None else None
            )
        
        # 5. Usar COPY para inserción ultra-rápida
        logger.info("PASO 6: Insertando con COPY (método más rápido)")
        insert_start = time.time()
        
        # Crear buffer en memoria
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
        buffer.seek(0)
        
        # Usar COPY desde el buffer
        with engine.raw_connection() as conn:
            with conn.cursor() as cursor:
                cursor.copy_expert(
                    f"""
                    COPY job_posting (
                        job_title_short, job_title, job_location, job_via,
                        job_schedule_type, job_work_from_home, search_location,
                        job_posted_date, job_no_degree_mention, job_health_insurance,
                        job_country, salary_rate, salary_year_avg, salary_hour_avg,
                        company_name, job_skills, job_type_skills
                    )
                    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
                    """,
                    buffer
                )
            conn.commit()
        
        insert_time = time.time() - insert_start
        records_per_sec = len(df) / insert_time if insert_time > 0 else 0
        logger.info(f"COPY completado: {len(df):,} registros en {insert_time:.2f}s ({records_per_sec:.0f} rec/s)")
        
        # 6. Verificación
        logger.info("PASO 7: Verificando datos")
        db = next(get_db())
        try:
            total_count = db.execute(text("SELECT COUNT(*) FROM job_posting")).scalar()
            logger.info(f"Total en base de datos: {total_count:,}")
        finally:
            db.close()
        
        logger.info("="*60)
        logger.info("CARGA COMPLETADA EXITOSAMENTE")
        logger.info("="*60)
        return True
        
    except Exception as e:
        logger.error(f"Error en carga con COPY: {str(e)}")
        logger.exception("Traceback completo:")
        return False

def execute_extraction(csv_path, use_copy=False):
        """
        Ejecuta el pipeline de extracción
        
        Args:
            csv_path: Ruta al archivo CSV
            use_copy: Si True, usa PostgreSQL COPY (más rápido pero menos robusto)
                     Si False, usa inserción paralela con chunks (más robusto)
        """
        n_cores = mp.cpu_count()
        
        logger.info("╔" + "═"*58 + "╗")
        logger.info("║" + " "*15 + "PIPELINE DE EXTRACCIÓN" + " "*21 + "║")
        logger.info("╚" + "═"*58 + "╝")
        
        logger.info("Configuración del pipeline:")
        logger.info("  • Archivo fuente: %s", csv_path)
        logger.info("  • Cores de CPU disponibles: %d", n_cores)
        logger.info("  • Método: %s", "PostgreSQL COPY" if use_copy else "Inserción paralela")
        logger.info("  • Engine: PostgreSQL optimizado")
        logger.info("  • Pool de conexiones: optimizado")
        logger.info("  • Procesamiento: paralelizado")
        logger.info("")
        
        start_time = time.time()
        logger.info("Inicio de ejecución: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        try:
            # Elegir método de carga
            if use_copy:
                success = load_with_copy(csv_path)
            else:
                success = load_optimized_fast(csv_path)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if success:
                logger.info("")
                logger.info("╔" + "═"*58 + "╗")
                logger.info("║" + " "*10 + "✓ PIPELINE COMPLETADO EXITOSAMENTE" + " "*13 + "║")
                logger.info("╚" + "═"*58 + "╝")
                logger.info("Tiempo total de ejecución: %.2f segundos", duration)
                logger.info("Velocidad promedio: %.0f registros/segundo", 785640/duration if duration > 0 else 0)
                logger.info("Fin de ejecución: %s", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            else:
                logger.error("")
                logger.error("╔" + "═"*58 + "╗")
                logger.error("║" + " "*15 + "✗ PIPELINE FALLÓ" + " "*26 + "║")
                logger.error("╚" + "═"*58 + "╝")
                logger.error("Tiempo transcurrido: %.2f segundos", duration)
                logger.error("Revise los logs anteriores para más detalles")
                
        except Exception as e:
            logger.critical("Error crítico en execute_extraction: %s", str(e))
            logger.exception("Traceback completo:")
            raise