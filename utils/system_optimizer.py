#!/usr/bin/env python3
"""
Módulo para calcular configuraciones óptimas según las especificaciones del sistema
"""
import psutil
import multiprocessing as mp
from typing import Dict, Any


def get_system_specs() -> Dict[str, Any]:
    """Obtiene las especificaciones del sistema"""
    cpu_count = mp.cpu_count()
    memory = psutil.virtual_memory()
    
    return {
        'cpu_cores': cpu_count,
        'total_memory_gb': round(memory.total / (1024**3), 2),
        'available_memory_gb': round(memory.available / (1024**3), 2),
        'memory_percent': memory.percent
    }


def calculate_optimal_threads(cpu_cores: int = None) -> int:
    """
    Calcula el número óptimo de threads según los cores disponibles.
    
    Regla: Usar CPU cores - 1 para dejar un core libre para el sistema
    Mínimo: 1 thread
    Máximo: 8 threads (para evitar sobrecarga)
    """
    if cpu_cores is None:
        cpu_cores = mp.cpu_count()
    
    # Dejar al menos 1 core libre
    optimal = max(1, cpu_cores - 1)
    
    # Limitar a 8 threads máximo para evitar overhead
    optimal = min(optimal, 8)
    
    return optimal


def calculate_optimal_chunk_size(
    total_rows: int,
    available_memory_gb: float = None,
    cpu_cores: int = None
) -> int:
    """
    Calcula el tamaño óptimo de chunk según memoria disponible y CPU.
    
    Args:
        total_rows: Número total de filas a procesar
        available_memory_gb: Memoria disponible en GB
        cpu_cores: Número de cores de CPU
    
    Returns:
        Tamaño óptimo de chunk
    
    Reglas:
    - Para datasets pequeños (<100k): 10k-25k por chunk
    - Para datasets medianos (100k-1M): 25k-50k por chunk
    - Para datasets grandes (>1M): 50k-100k por chunk
    - Ajustar según memoria disponible
    """
    if available_memory_gb is None:
        memory = psutil.virtual_memory()
        available_memory_gb = memory.available / (1024**3)
    
    if cpu_cores is None:
        cpu_cores = mp.cpu_count()
    
    # Calcular chunk size base según tamaño del dataset
    if total_rows < 100_000:
        base_chunk = 10_000
    elif total_rows < 1_000_000:
        base_chunk = 25_000
    else:
        base_chunk = 50_000
    
    # Ajustar según memoria disponible
    # Regla: ~1GB de RAM por cada 50k filas (estimación conservadora)
    memory_based_chunk = int(available_memory_gb * 50_000 / 2)  # Usar solo 50% de memoria disponible
    
    # Ajustar según CPU cores (más cores = chunks más grandes)
    cpu_multiplier = min(cpu_cores / 4, 2.0)  # Máximo 2x el base
    cpu_adjusted_chunk = int(base_chunk * cpu_multiplier)
    
    # Tomar el mínimo entre los tres cálculos
    optimal_chunk = min(base_chunk, memory_based_chunk, cpu_adjusted_chunk)
    
    # Asegurar límites razonables
    optimal_chunk = max(5_000, optimal_chunk)  # Mínimo 5k
    optimal_chunk = min(100_000, optimal_chunk)  # Máximo 100k
    
    # Redondear a múltiplos de 5000 para consistencia
    optimal_chunk = round(optimal_chunk / 5_000) * 5_000
    
    return optimal_chunk


def calculate_dbt_threads() -> int:
    """
    Calcula el número óptimo de threads para dbt.
    
    dbt puede beneficiarse de más threads que el procesamiento de datos
    porque ejecuta queries SQL en paralelo.
    """
    cpu_cores = mp.cpu_count()
    
    # Para dbt, usar más threads es beneficioso
    if cpu_cores <= 2:
        return 2
    elif cpu_cores <= 4:
        return 4
    elif cpu_cores <= 8:
        return cpu_cores
    else:
        # Para sistemas con muchos cores, limitar a 12 threads
        return min(cpu_cores, 12)


def get_optimal_config(total_rows: int = None) -> Dict[str, Any]:
    """
    Obtiene la configuración óptima completa para el sistema.
    
    Args:
        total_rows: Número total de filas a procesar (opcional)
    
    Returns:
        Diccionario con configuración óptima
    """
    specs = get_system_specs()
    
    config = {
        'system_specs': specs,
        'dbt_threads': calculate_dbt_threads(),
        'extraction_threads': calculate_optimal_threads(specs['cpu_cores']),
    }
    
    if total_rows:
        config['chunk_size'] = calculate_optimal_chunk_size(
            total_rows,
            specs['available_memory_gb'],
            specs['cpu_cores']
        )
    
    return config


def print_system_info():
    """Imprime información del sistema y configuraciones recomendadas"""
    specs = get_system_specs()
    
    print("=" * 70)
    print(" " * 20 + "ESPECIFICACIONES DEL SISTEMA")
    print("=" * 70)
    print(f"CPU Cores:              {specs['cpu_cores']}")
    print(f"Memoria Total:          {specs['total_memory_gb']} GB")
    print(f"Memoria Disponible:     {specs['available_memory_gb']} GB")
    print(f"Uso de Memoria:         {specs['memory_percent']}%")
    print()
    
    print("=" * 70)
    print(" " * 20 + "CONFIGURACIONES RECOMENDADAS")
    print("=" * 70)
    print(f"dbt threads:            {calculate_dbt_threads()}")
    print(f"Extraction threads:     {calculate_optimal_threads()}")
    print()
    
    # Ejemplos de chunk sizes para diferentes datasets
    print("Chunk sizes recomendados por tamaño de dataset:")
    for rows in [50_000, 100_000, 500_000, 1_000_000]:
        chunk = calculate_optimal_chunk_size(rows, specs['available_memory_gb'], specs['cpu_cores'])
        print(f"  {rows:>10,} filas -> chunk size: {chunk:>7,}")
    
    print("=" * 70)


if __name__ == "__main__":
    print_system_info()
