"""Extracción paralela de skills usando multiprocessing."""
import pandas as pd
import ast
from multiprocessing import Pool, cpu_count
from typing import List, Dict, Tuple
import numpy as np


def process_chunk(chunk_data: Tuple[pd.DataFrame, int]) -> List[Dict]:
    """
    Procesa un chunk del DataFrame en paralelo.
    
    Args:
        chunk_data: Tupla con (DataFrame chunk, chunk_id)
        
    Returns:
        Lista de diccionarios con relaciones skill-tipo
    """
    df_chunk, chunk_id = chunk_data
    relaciones = []
    
    print(f"  Procesando chunk {chunk_id}...")
    
    for idx, row in df_chunk.iterrows():
        if pd.notna(row['job_type_skills']):
            try:
                tipos_dict = ast.literal_eval(row['job_type_skills'])
                if isinstance(tipos_dict, dict):
                    for tipo, skills_list in tipos_dict.items():
                        if isinstance(skills_list, list):
                            for skill in skills_list:
                                skill_normalizada = skill.strip().lower()
                                relaciones.append({
                                    'job_posting_id': row['id'],
                                    'skill_name': skill_normalizada,
                                    'type_name': tipo.lower()
                                })
            except:
                pass
    
    print(f"  Chunk {chunk_id} completado: {len(relaciones)} relaciones")
    return relaciones


def extraer_skills_paralelo(csv_path: str = 'data_jobs.csv', num_workers: int = None) -> pd.DataFrame:
    """
    Extrae skills del CSV usando procesamiento paralelo.
    
    Args:
        csv_path: Ruta al archivo CSV
        num_workers: Número de workers (default: CPU count - 1)
        
    Returns:
        DataFrame con columnas job_posting_id, skill_name, type_name
    """
    if num_workers is None:
        num_workers = max(1, cpu_count() - 1)
    
    print(f"Iniciando extracción paralela con {num_workers} workers...")
    
    # Leer CSV
    print("Cargando CSV...")
    df = pd.read_csv(csv_path)
    print(f"Cargados {len(df):,} registros")
    
    # Dividir en chunks
    chunk_size = len(df) // num_workers
    chunks = []
    
    for i in range(num_workers):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size if i < num_workers - 1 else len(df)
        chunk = df.iloc[start_idx:end_idx]
        chunks.append((chunk, i + 1))
    
    print(f"Dividido en {len(chunks)} chunks de ~{chunk_size:,} registros cada uno")
    
    # Procesar en paralelo
    print("Procesando chunks en paralelo...")
    with Pool(processes=num_workers) as pool:
        results = pool.map(process_chunk, chunks)
    
    # Combinar resultados
    print("Combinando resultados...")
    all_relaciones = []
    for chunk_result in results:
        all_relaciones.extend(chunk_result)
    
    # Crear DataFrame final
    df_skills = pd.DataFrame(all_relaciones)
    print(f"Extracción completada: {len(df_skills):,} relaciones skill-tipo")
    
    return df_skills


if __name__ == '__main__':
    import time
    
    start = time.time()
    df_result = extraer_skills_paralelo()
    end = time.time()
    
    print(f"\nTiempo total: {end - start:.2f} segundos")
    print(f"\nPrimeras 10 filas:")
    print(df_result.head(10))
    
    # Guardar resultado
    output_path = 'skills_extracted.csv'
    df_result.to_csv(output_path, index=False)
    print(f"\nResultado guardado en: {output_path}")
