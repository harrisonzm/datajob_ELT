#!/usr/bin/env python3
"""
Módulo de análisis de datos para calcular registros esperados en cada tabla
sin necesidad de cargar en base de datos.
"""

import pandas as pd
import numpy as np
import ast
from typing import Dict, List, Optional
import json


def parse_skills_string(skills_str: str) -> Optional[List[str]]:
    """Convierte string de skills a lista de strings"""
    if pd.isna(skills_str) or skills_str == '' or skills_str == 'nan':
        return None
    
    try:
        if skills_str.startswith('[') and skills_str.endswith(']'):
            result = ast.literal_eval(skills_str)
            if isinstance(result, list):
                result = [s.strip() for s in result if s and s.strip()]
                return result if result else None
            return result
        else:
            result = [skill.strip() for skill in skills_str.split(',') if skill.strip()]
            return result if result else None
    except:
        return [skills_str.strip()] if skills_str.strip() else None


def parse_type_skills_string(type_skills_str: str) -> Optional[Dict[str, List[str]]]:
    """Convierte string de type_skills a diccionario"""
    if pd.isna(type_skills_str) or type_skills_str == '' or type_skills_str == 'nan':
        return None
    
    try:
        if type_skills_str.startswith('{') and type_skills_str.endswith('}'):
            result = ast.literal_eval(type_skills_str)
            if isinstance(result, dict):
                cleaned = {}
                for key, values in result.items():
                    if isinstance(values, list):
                        cleaned_values = [v.strip() for v in values if v and v.strip()]
                        if cleaned_values:
                            cleaned[key] = cleaned_values
                return cleaned if cleaned else None
            return result
        else:
            return None
    except:
        return None


def analizar_dataset(csv_path: str):
    """
    Analiza el dataset CSV y calcula cuántos registros únicos debe tener cada tabla.
    
    Args:
        csv_path: Ruta al archivo CSV
    """
    print("=" * 80)
    print(" " * 25 + "ANÁLISIS DE DATASET")
    print("=" * 80)
    print()
    
    # 1. Cargar CSV
    print(f"[*] Cargando CSV desde: {csv_path}")
    df = pd.read_csv(csv_path)
    print(f"[OK] CSV cargado: {len(df):,} registros")
    print()
    
    # 2. Eliminar duplicados
    print("[*] Eliminando duplicados exactos...")
    initial_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = initial_count - len(df)
    print(f"[-] Duplicados eliminados: {duplicates_removed:,}")
    print(f"[OK] Registros únicos: {len(df):,}")
    print()
    
    # 3. Parsear skills
    print("[*] Parseando skills...")
    df['job_skills_parsed'] = df['job_skills'].apply(parse_skills_string)
    df['job_type_skills_parsed'] = df['job_type_skills'].apply(parse_type_skills_string)
    print("[OK] Skills parseados")
    print()
    
    # Diccionario para almacenar resultados
    resultados = {}
    
    # ========== TABLA STAGING ==========
    print("=" * 80)
    print(" " * 30 + "TABLAS STAGING")
    print("=" * 80)
    
    # stg_job_postings (igual a job_posting sin duplicados)
    resultados['stg_job_postings'] = len(df)
    print(f"stg_job_postings                : {resultados['stg_job_postings']:,} registros")
    
    # stg_job_skills (descomponer arrays y JSON)
    # Contar skills desde job_skills (sin tipo)
    skills_from_array = []
    for idx, row in df.iterrows():
        if isinstance(row['job_skills_parsed'], list):
            for skill in row['job_skills_parsed']:
                if skill and skill.strip():
                    skills_from_array.append({
                        'job_posting_id': idx,
                        'skill_name': skill.lower().strip(),
                        'type_name': None
                    })
    
    # Contar skills desde job_type_skills (con tipo)
    skills_from_json = []
    for idx, row in df.iterrows():
        if isinstance(row['job_type_skills_parsed'], dict):
            for type_name, skills_list in row['job_type_skills_parsed'].items():
                if isinstance(skills_list, list):
                    for skill in skills_list:
                        if skill and skill.strip():
                            skills_from_json.append({
                                'job_posting_id': idx,
                                'skill_name': skill.lower().strip(),
                                'type_name': type_name.lower().strip()
                            })
    
    # Combinar ambas fuentes
    all_skills = skills_from_array + skills_from_json
    df_skills = pd.DataFrame(all_skills)
    
    # Eliminar duplicados exactos (mismo job_posting_id, skill_name, type_name)
    # Esto simula el comportamiento real de la tabla staging
    if not df_skills.empty:
        # Primero eliminar duplicados exactos
        df_skills_unique = df_skills.drop_duplicates()
        resultados['stg_job_skills'] = len(df_skills_unique)
    else:
        resultados['stg_job_skills'] = 0
    
    print(f"stg_job_skills                  : {resultados['stg_job_skills']:,} registros")
    print()
    
    # ========== DIMENSIONES ==========
    print("=" * 80)
    print(" " * 30 + "TABLAS DIMENSIONES")
    print("=" * 80)
    
    # dim_companies (aplicar limpieza de texto como en stg_job_postings)
    df_companies = df[df['company_name'].notna()].copy()
    # Simular limpieza: TRIM y REGEXP_REPLACE para remover caracteres especiales
    df_companies['company_name_clean'] = df_companies['company_name'].str.strip()
    df_companies['company_name_clean'] = df_companies['company_name_clean'].str.replace(r'[^\w\s+().-]', '', regex=True)
    # Eliminar duplicados después de limpieza
    df_companies_unique = df_companies[['company_name_clean']].drop_duplicates()
    unique_companies = len(df_companies_unique)
    resultados['dim_companies'] = unique_companies
    print(f"dim_companies                   : {unique_companies:,} registros únicos")
    
    # dim_countries (aplicar limpieza)
    df_countries = df[df['job_country'].notna()].copy()
    df_countries['job_country_clean'] = df_countries['job_country'].str.strip()
    # Eliminar duplicados después de limpieza
    df_countries_unique = df_countries[['job_country_clean']].drop_duplicates()
    unique_countries = len(df_countries_unique)
    resultados['dim_countries'] = unique_countries
    print(f"dim_countries                   : {unique_countries:,} registros únicos")
    
    # dim_locations (aplicar limpieza y combinación única de location + country)
    df_locations = df[df['job_location'].notna()].copy()
    df_locations['job_location_clean'] = df_locations['job_location'].str.strip()
    df_locations['job_location_clean'] = df_locations['job_location_clean'].str.replace(r'[^\w\s+().-]', '', regex=True)
    df_locations['job_country_clean'] = df_locations['job_country'].str.strip()
    # Eliminar duplicados de la combinación location + country
    unique_locations = df_locations[['job_location_clean', 'job_country_clean']].drop_duplicates()
    resultados['dim_locations'] = len(unique_locations)
    print(f"dim_locations                   : {len(unique_locations):,} registros únicos")
    
    # dim_vias (aplicar limpieza como en stg_job_postings)
    df_vias = df[df['job_via'].notna()].copy()
    # Simular limpieza de "via " y "melalui " exactamente como en SQL
    def clean_via(x):
        x_str = str(x).strip()
        if x_str == '':
            return None
        
        x_lower = x_str.lower()
        
        # Remover prefijos "via " o "melalui "
        if x_lower.startswith('via '):
            result = x_str[4:].strip()
        elif x_lower.startswith('melalui '):
            result = x_str[8:].strip()
        else:
            # Si no tiene prefijo, aplicar REGEXP_REPLACE
            result = pd.Series([x_str]).str.replace(r'[^\w\s+().-]', '', regex=True).iloc[0]
            result = result.strip()
        
        return result if result else None
    
    df_vias['job_via_clean'] = df_vias['job_via'].apply(clean_via)
    # Filtrar NULLs y eliminar duplicados después de limpieza
    df_vias_clean = df_vias[df_vias['job_via_clean'].notna()][['job_via_clean']].drop_duplicates()
    unique_vias = len(df_vias_clean)
    resultados['dim_vias'] = unique_vias
    print(f"dim_vias                        : {unique_vias:,} registros únicos")
    
    # dim_schedule_types (aplicar limpieza)
    df_schedule = df[df['job_schedule_type'].notna()].copy()
    df_schedule['job_schedule_type_clean'] = df_schedule['job_schedule_type'].str.strip()
    # Eliminar duplicados después de limpieza
    df_schedule_unique = df_schedule[['job_schedule_type_clean']].drop_duplicates()
    unique_schedule_types = len(df_schedule_unique)
    resultados['dim_schedule_types'] = unique_schedule_types
    print(f"dim_schedule_types              : {unique_schedule_types:,} registros únicos")
    
    # dim_short_titles (aplicar limpieza)
    df_short_titles = df[df['job_title_short'].notna()].copy()
    df_short_titles['job_title_short_clean'] = df_short_titles['job_title_short'].str.strip()
    # Eliminar duplicados después de limpieza
    df_short_titles_unique = df_short_titles[['job_title_short_clean']].drop_duplicates()
    unique_short_titles = len(df_short_titles_unique)
    resultados['dim_short_titles'] = unique_short_titles
    print(f"dim_short_titles                : {unique_short_titles:,} registros únicos")
    
    # dim_skills (solo de job_type_skills que tienen tipo)
    if not df_skills.empty:
        # Eliminar duplicados de skill_name
        skills_con_tipo = df_skills[df_skills['type_name'].notna()][['skill_name']].drop_duplicates()
        resultados['dim_skills'] = len(skills_con_tipo)
    else:
        resultados['dim_skills'] = 0
    print(f"dim_skills                      : {resultados['dim_skills']:,} registros únicos")
    
    # dim_types
    if not df_skills.empty:
        # Eliminar duplicados de type_name
        types_unicos = df_skills[df_skills['type_name'].notna()][['type_name']].drop_duplicates()
        resultados['dim_types'] = len(types_unicos)
    else:
        resultados['dim_types'] = 0
    print(f"dim_types                       : {resultados['dim_types']:,} registros únicos")
    print()
    
    # ========== TABLAS DE HECHOS Y RELACIONES ==========
    print("=" * 80)
    print(" " * 25 + "TABLAS DE HECHOS Y RELACIONES")
    print("=" * 80)
    
    # fact_job_posts (uno por cada job posting)
    resultados['fact_job_posts'] = len(df)
    print(f"fact_job_posts                  : {resultados['fact_job_posts']:,} registros")
    
    # skill_types (relación entre skills y types)
    if not df_skills.empty:
        skill_types_unique = df_skills[df_skills['type_name'].notna()][['skill_name', 'type_name']].drop_duplicates()
        resultados['skill_types'] = len(skill_types_unique)
    else:
        resultados['skill_types'] = 0
    print(f"skill_types                     : {resultados['skill_types']:,} registros únicos")
    
    # job_skills (relación entre jobs y skill_types)
    # Solo incluye skills que tienen tipo definido
    if not df_skills.empty:
        job_skills_con_tipo = df_skills[df_skills['type_name'].notna()].copy()
        # Necesitamos agrupar por job_posting_id y la combinación skill_name+type_name
        job_skills_unique = job_skills_con_tipo[['job_posting_id', 'skill_name', 'type_name']].drop_duplicates()
        resultados['job_skills'] = len(job_skills_unique)
    else:
        resultados['job_skills'] = 0
    print(f"job_skills                      : {resultados['job_skills']:,} registros únicos")
    print()
    
    # ========== ANÁLISIS ADICIONAL ==========
    print("=" * 80)
    print(" " * 28 + "ANÁLISIS ADICIONAL")
    print("=" * 80)
    
    # Contar registros con skills
    registros_con_job_skills = df['job_skills_parsed'].notna().sum()
    registros_con_type_skills = df['job_type_skills_parsed'].notna().sum()
    
    print(f"Registros con job_skills        : {registros_con_job_skills:,} ({registros_con_job_skills/len(df)*100:.2f}%)")
    print(f"Registros con job_type_skills   : {registros_con_type_skills:,} ({registros_con_type_skills/len(df)*100:.2f}%)")
    
    # Contar elementos totales en arrays
    total_skills_en_arrays = sum(len(skills) for skills in df['job_skills_parsed'] if isinstance(skills, list))
    total_skills_en_json = sum(
        sum(len(skills_list) for skills_list in type_skills.values() if isinstance(skills_list, list))
        for type_skills in df['job_type_skills_parsed'] if isinstance(type_skills, dict)
    )
    
    print(f"Total elementos en job_skills   : {total_skills_en_arrays:,} elementos")
    print(f"Total elementos en job_type_skills: {total_skills_en_json:,} elementos")
    print()
    
    # ========== RESUMEN FINAL ==========
    print("=" * 80)
    print(" " * 30 + "RESUMEN FINAL")
    print("=" * 80)
    print()
    print("Registros esperados por tabla:")
    print("-" * 80)
    
    # Ordenar por nombre de tabla
    for tabla in sorted(resultados.keys()):
        print(f"{tabla:30} : {resultados[tabla]:>10,} registros")
    
    print("=" * 80)
    
    return resultados


if __name__ == "__main__":
    import sys
    
    csv_path = "data_jobs.csv"
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    
    try:
        resultados = analizar_dataset(csv_path)
    except Exception as e:
        print(f"[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
