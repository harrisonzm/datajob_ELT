"""Análisis de habilidades y tipos de habilidades."""
import pandas as pd
import ast
import os
from typing import Dict, Any


def analizar_skills_y_tipos(csv_path: str = 'data_jobs.csv') -> Dict[str, Any]:
    """
    Analiza las habilidades y tipos de habilidades del CSV.
    
    Args:
        csv_path: Ruta al archivo CSV
        
    Returns:
        Diccionario con:
        - df_skills_tipos: DataFrame con columnas 'tipo', 'skill'
        - num_tipos_distintos: Cantidad de tipos únicos
        - num_skills_distintas: Cantidad de skills únicas
        - num_relaciones: Número total de filas (relaciones skill-tipo)
    """
    # Si la ruta es relativa, buscar desde el directorio raíz del proyecto
    if not os.path.isabs(csv_path) and not os.path.exists(csv_path):
        # Intentar desde el directorio padre (raíz del proyecto)
        csv_path = os.path.join('..', csv_path)
    
    # Leer el CSV
    df = pd.read_csv(csv_path)
    
    # Conjunto para almacenar todas las skills únicas
    todas_skills = set()
    
    # Lista para almacenar las relaciones tipo-skill
    relaciones = []
    
    # Procesar cada fila
    for idx, row in df.iterrows():
        # Procesar job_skills (array de strings)
        if pd.notna(row['job_skills']):
            try:
                skills_list = ast.literal_eval(row['job_skills'])
                if isinstance(skills_list, list):
                    # Normalizar skills: trim y minúsculas
                    skills_normalizadas = [skill.strip().lower() for skill in skills_list]
                    todas_skills.update(skills_normalizadas)
            except:
                pass
        
        # Procesar job_type_skills (JSON con tipos y arrays de skills)
        if pd.notna(row['job_type_skills']):
            try:
                tipos_dict = ast.literal_eval(row['job_type_skills'])
                if isinstance(tipos_dict, dict):
                    # Iterar sobre cada tipo (key) y sus skills (items)
                    for tipo, skills_list in tipos_dict.items():
                        if isinstance(skills_list, list):
                            for skill in skills_list:
                                # Normalizar: trim y minúsculas
                                skill_normalizada = skill.strip().lower()
                                relaciones.append({
                                    'tipo': tipo,
                                    'skill': skill_normalizada
                                })
            except:
                pass
    
    # Crear DataFrame con las relaciones tipo-skill
    df_skills_tipos = pd.DataFrame(relaciones)
    
    # Calcular estadísticas
    num_tipos_distintos = df_skills_tipos['tipo'].nunique() if len(df_skills_tipos) > 0 else 0
    num_skills_distintas = len(todas_skills)
    num_relaciones = len(df_skills_tipos)
    
    # Crear resumen
    resultado = {
        'df_skills_tipos': df_skills_tipos,
        'num_tipos_distintos': num_tipos_distintos,
        'num_skills_distintas': num_skills_distintas,
        'num_relaciones': num_relaciones,
        'todas_skills': todas_skills
    }
    
    return resultado


def mostrar_resumen(resultado: Dict[str, Any]) -> None:
    """
    Muestra un resumen del análisis de skills y tipos.
    
    Args:
        resultado: Diccionario retornado por analizar_skills_y_tipos
    """
    print("=" * 70)
    print("RESUMEN DE ANÁLISIS DE SKILLS Y TIPOS")
    print("=" * 70)
    print(f"\nNúmero de tipos distintos: {resultado['num_tipos_distintos']}")
    print(f"Número de skills distintas: {resultado['num_skills_distintas']}")
    print(f"Número total de relaciones job-skill: {resultado['num_relaciones']}")
    
    # Calcular combinaciones únicas de skill-tipo
    if len(resultado['df_skills_tipos']) > 0:
        df_sin_duplicados = resultado['df_skills_tipos'].drop_duplicates(subset=['tipo', 'skill'])
        num_skill_tipos_unicos = len(df_sin_duplicados)
        print(f"Número total de skill-tipe únicos: {num_skill_tipos_unicos}")
        
        print("\n" + "=" * 70)
        print("PRIMERAS 20 FILAS DE LA TABLA TIPO-SKILL")
        print("=" * 70)
        print(resultado['df_skills_tipos'].head(20).to_string(index=False))
        
        print("\n" + "=" * 70)
        print("DISTRIBUCIÓN DE TIPOS")
        print("=" * 70)
        print(resultado['df_skills_tipos']['tipo'].value_counts())
        
        print("\n" + "=" * 70)
        print("TOP 20 SKILLS MÁS FRECUENTES")
        print("=" * 70)
        print(resultado['df_skills_tipos']['skill'].value_counts().head(20))
    else:
        print("\nNo se encontraron relaciones skill-tipo en el dataset.")


def analizar_columnas_categoricas(csv_path: str = 'data_jobs.csv') -> Dict[str, Any]:
    """
    Analiza valores distintos en columnas categóricas del CSV.
    Excluye columnas de salary y booleanas.
    
    Args:
        csv_path: Ruta al archivo CSV
        
    Returns:
        Diccionario con el conteo de valores distintos por columna
    """
    # Si la ruta es relativa, buscar desde el directorio raíz del proyecto
    if not os.path.isabs(csv_path) and not os.path.exists(csv_path):
        csv_path = os.path.join('..', csv_path)
    
    # Leer el CSV
    df = pd.read_csv(csv_path)
    
    # Columnas a excluir (salary y booleanas)
    columnas_excluir = [
        'salary_rate',
        'salary_year_avg', 
        'salary_hour_avg',
        'job_work_from_home',
        'job_no_degree_mention',
        'job_health_insurance',
        'job_posted_date',
        'job_title',
        'search_location',
        'job_skills',  # Ya analizada en otra función
        'job_type_skills'  # Ya analizada en otra función
    ]
    
    # Obtener columnas categóricas
    columnas_analizar = [col for col in df.columns if col not in columnas_excluir]
    
    # Contar valores distintos por columna
    conteos = {}
    for col in columnas_analizar:
        # Contar valores no nulos y distintos
        valores_distintos = df[col].nunique()
        valores_no_nulos = df[col].notna().sum()
        valores_nulos = df[col].isna().sum()
        
        conteos[col] = {
            'valores_distintos': valores_distintos,
            'valores_no_nulos': valores_no_nulos,
            'valores_nulos': valores_nulos,
            'porcentaje_nulos': (valores_nulos / len(df)) * 100
        }
    
    return conteos


def mostrar_analisis_columnas(conteos: Dict[str, Any]) -> None:
    """
    Muestra el análisis de columnas categóricas.
    
    Args:
        conteos: Diccionario retornado por analizar_columnas_categoricas
    """
    print("\n" + "=" * 80)
    print("ANÁLISIS DE COLUMNAS CATEGÓRICAS")
    print("=" * 80)
    print(f"\n{'Columna':<25} {'Distintos':<12} {'No Nulos':<12} {'Nulos':<12} {'% Nulos':<10}")
    print("-" * 80)
    
    # Ordenar por número de valores distintos
    for col, stats in sorted(conteos.items(), key=lambda x: x[1]['valores_distintos'], reverse=True):
        print(f"{col:<25} {stats['valores_distintos']:<12,} {stats['valores_no_nulos']:<12,} "
              f"{stats['valores_nulos']:<12,} {stats['porcentaje_nulos']:<10.2f}%")
    
    print("=" * 80)


if __name__ == '__main__':
    # Ejecutar análisis de skills
    resultado = analizar_skills_y_tipos()
    mostrar_resumen(resultado)
    
    # Ejecutar análisis de columnas categóricas
    print("\n")
    conteos = analizar_columnas_categoricas()
    mostrar_analisis_columnas(conteos)
