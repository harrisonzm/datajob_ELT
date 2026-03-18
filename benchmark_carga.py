#!/usr/bin/env python3
"""Benchmark de diferentes métodos de carga a PostgreSQL."""

import pandas as pd
import time
from io import StringIO
from sqlalchemy import create_engine, text
from db.config.db import DATABASE_URL, get_db
import psycopg2
from typing import Dict, Callable
import numpy as np
import logging

# Desactivar logging de SQLAlchemy
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)


def crear_tabla_test(nombre_tabla: str, drop_if_exists: bool = True):
    """Crea una tabla de prueba."""
    db = next(get_db())
    try:
        if drop_if_exists:
            db.execute(text(f"DROP TABLE IF EXISTS {nombre_tabla}"))
            db.commit()
        
        if nombre_tabla == 'test_job_posts':
            db.execute(text(f"""
                CREATE TABLE {nombre_tabla} (
                    id BIGINT PRIMARY KEY,
                    company_id BIGINT,
                    country_id BIGINT,
                    location_id BIGINT,
                    via_id BIGINT,
                    schedule_type_id BIGINT,
                    short_title_id BIGINT,
                    job_title TEXT,
                    search_location TEXT,
                    job_posted_date TIMESTAMP,
                    job_work_from_home BOOLEAN,
                    job_no_degree_mention BOOLEAN,
                    job_health_insurance BOOLEAN,
                    salary_rate TEXT,
                    salary_year_avg FLOAT,
                    salary_hour_avg FLOAT
                )
            """))
        elif nombre_tabla == 'test_job_skills':
            db.execute(text(f"""
                CREATE TABLE {nombre_tabla} (
                    job_id BIGINT,
                    skill_types_id BIGINT,
                    PRIMARY KEY (job_id, skill_types_id)
                )
            """))
        
        db.commit()
        print(f"✓ Tabla {nombre_tabla} creada")
    finally:
        db.close()


def metodo_copy_iterrows(df: pd.DataFrame, tabla: str) -> float:
    """Método 1: COPY con iterrows (método actual)."""
    print(f"\n[1] COPY con iterrows - {tabla}")
    start = time.time()
    
    db = next(get_db())
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        buffer = StringIO()
        
        for _, row in df.iterrows():
            line_parts = []
            for val in row:
                if pd.isna(val) or val is None:
                    line_parts.append('\\N')
                elif isinstance(val, bool):
                    line_parts.append('t' if val else 'f')
                elif isinstance(val, (int, np.integer)):
                    line_parts.append(str(int(val)))
                elif isinstance(val, (float, np.floating)):
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
        columnas = ', '.join(df.columns)
        copy_sql = f"COPY {tabla} ({columnas}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"
        
        cursor.copy_expert(copy_sql, buffer)
        raw_conn.commit()
        cursor.close()
        buffer.close()
        
    finally:
        db.close()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def metodo_copy_to_csv(df: pd.DataFrame, tabla: str) -> float:
    """Método 2: COPY usando to_csv directo."""
    print(f"\n[2] COPY con to_csv - {tabla}")
    start = time.time()
    
    db = next(get_db())
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        buffer = StringIO()
        
        # Preparar DataFrame
        df_copy = df.copy()
        
        # Convertir booleanos a t/f
        for col in df_copy.columns:
            if df_copy[col].dtype == 'bool':
                df_copy[col] = df_copy[col].map({True: 't', False: 'f', pd.NA: None})
        
        # Escribir a CSV
        df_copy.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
        buffer.seek(0)
        
        columnas = ', '.join(df.columns)
        copy_sql = f"COPY {tabla} ({columnas}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"
        
        cursor.copy_expert(copy_sql, buffer)
        raw_conn.commit()
        cursor.close()
        buffer.close()
        
    finally:
        db.close()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def metodo_copy_binary(df: pd.DataFrame, tabla: str) -> float:
    """Método 3: COPY con formato binario."""
    print(f"\n[3] COPY con formato binario - {tabla}")
    start = time.time()
    
    db = next(get_db())
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        buffer = StringIO()
        df.to_csv(buffer, sep='\t', header=False, index=False, na_rep='\\N')
        buffer.seek(0)
        
        columnas = ', '.join(df.columns)
        copy_sql = f"COPY {tabla} ({columnas}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"
        
        cursor.copy_expert(copy_sql, buffer)
        raw_conn.commit()
        cursor.close()
        buffer.close()
        
    finally:
        db.close()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def metodo_to_sql_multi(df: pd.DataFrame, tabla: str) -> float:
    """Método 4: to_sql con method='multi'."""
    print(f"\n[4] to_sql con method='multi' - {tabla}")
    start = time.time()
    
    engine = create_engine(DATABASE_URL, echo=False)
    df.to_sql(tabla, engine, if_exists='append', index=False, method='multi', chunksize=10000)
    engine.dispose()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def metodo_executemany(df: pd.DataFrame, tabla: str) -> float:
    """Método 5: executemany con psycopg2."""
    print(f"\n[5] executemany con psycopg2 - {tabla}")
    start = time.time()
    
    db = next(get_db())
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        columnas = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_sql = f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})"
        
        # Convertir DataFrame a lista de tuplas
        data = [tuple(row) for row in df.values]
        
        cursor.executemany(insert_sql, data)
        raw_conn.commit()
        cursor.close()
        
    finally:
        db.close()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def metodo_execute_values(df: pd.DataFrame, tabla: str) -> float:
    """Método 6: execute_values (más rápido que executemany)."""
    print(f"\n[6] execute_values con psycopg2.extras - {tabla}")
    start = time.time()
    
    from psycopg2.extras import execute_values
    
    db = next(get_db())
    try:
        raw_conn = db.connection().connection
        cursor = raw_conn.cursor()
        
        columnas = ', '.join(df.columns)
        data = [tuple(row) for row in df.values]
        
        execute_values(
            cursor,
            f"INSERT INTO {tabla} ({columnas}) VALUES %s",
            data,
            page_size=10000
        )
        
        raw_conn.commit()
        cursor.close()
        
    finally:
        db.close()
    
    elapsed = time.time() - start
    print(f"   Tiempo: {elapsed:.2f}s | Velocidad: {len(df)/elapsed:,.0f} filas/s")
    return elapsed


def benchmark_tabla(df: pd.DataFrame, nombre_tabla: str, metodos: list):
    """Ejecuta benchmark para una tabla específica."""
    print("\n" + "=" * 80)
    print(f"BENCHMARK: {nombre_tabla}")
    print(f"Registros: {len(df):,}")
    print("=" * 80)
    
    resultados = {}
    
    for metodo_nombre, metodo_func in metodos:
        try:
            # Recrear tabla
            crear_tabla_test(nombre_tabla)
            
            # Ejecutar método
            tiempo = metodo_func(df, nombre_tabla)
            resultados[metodo_nombre] = tiempo
            
            # Verificar conteo
            db = next(get_db())
            try:
                count = db.execute(text(f"SELECT COUNT(*) FROM {nombre_tabla}")).scalar()
                if count != len(df):
                    print(f"   ⚠ ADVERTENCIA: Se insertaron {count:,} filas, esperadas {len(df):,}")
            finally:
                db.close()
                
        except Exception as e:
            print(f"   ✗ ERROR: {e}")
            resultados[metodo_nombre] = None
    
    return resultados


def main():
    print("\n" + "=" * 80)
    print("BENCHMARK DE MÉTODOS DE CARGA A POSTGRESQL")
    print("=" * 80)
    
    # Cargar datos reales de las transformaciones
    print("\nCargando datos transformados...")
    
    # Simular fact_job_posts (780k filas)
    print("Cargando fact_job_posts...")
    db = next(get_db())
    try:
        df_job_posts = pd.read_sql("SELECT * FROM fact_job_posts LIMIT 100000", db.connection())
        print(f"✓ Cargadas {len(df_job_posts):,} filas de fact_job_posts")
    finally:
        db.close()
    
    # Simular job_skills (3M filas)
    print("Cargando job_skills...")
    db = next(get_db())
    try:
        df_job_skills = pd.read_sql("SELECT * FROM job_skills LIMIT 500000", db.connection())
        print(f"✓ Cargadas {len(df_job_skills):,} filas de job_skills")
    finally:
        db.close()
    
    # Definir métodos a probar
    metodos = [
        ("COPY iterrows", metodo_copy_iterrows),
        ("COPY to_csv", metodo_copy_to_csv),
        ("COPY binary", metodo_copy_binary),
        ("to_sql multi", metodo_to_sql_multi),
        ("executemany", metodo_executemany),
        ("execute_values", metodo_execute_values),
    ]
    
    # Benchmark para fact_job_posts
    resultados_posts = benchmark_tabla(df_job_posts, "test_job_posts", metodos)
    
    # Benchmark para job_skills
    resultados_skills = benchmark_tabla(df_job_skills, "test_job_skills", metodos)
    
    # Resumen final
    print("\n" + "=" * 80)
    print("RESUMEN DE RESULTADOS")
    print("=" * 80)
    
    print(f"\nFACT_JOB_POSTS ({len(df_job_posts):,} filas):")
    print("-" * 80)
    for metodo, tiempo in sorted(resultados_posts.items(), key=lambda x: x[1] if x[1] else float('inf')):
        if tiempo:
            velocidad = len(df_job_posts) / tiempo
            print(f"{metodo:<25} {tiempo:>8.2f}s  |  {velocidad:>12,.0f} filas/s")
        else:
            print(f"{metodo:<25} {'ERROR':>8}")
    
    print(f"\nJOB_SKILLS ({len(df_job_skills):,} filas):")
    print("-" * 80)
    for metodo, tiempo in sorted(resultados_skills.items(), key=lambda x: x[1] if x[1] else float('inf')):
        if tiempo:
            velocidad = len(df_job_skills) / tiempo
            print(f"{metodo:<25} {tiempo:>8.2f}s  |  {velocidad:>12,.0f} filas/s")
        else:
            print(f"{metodo:<25} {'ERROR':>8}")
    
    # Limpiar tablas de prueba
    print("\nLimpiando tablas de prueba...")
    db = next(get_db())
    try:
        db.execute(text("DROP TABLE IF EXISTS test_job_posts"))
        db.execute(text("DROP TABLE IF EXISTS test_job_skills"))
        db.commit()
        print("✓ Tablas de prueba eliminadas")
    finally:
        db.close()


if __name__ == "__main__":
    main()
