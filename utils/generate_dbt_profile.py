#!/usr/bin/env python3
"""
Script para generar profiles.yml de dbt con configuración óptima según el sistema
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from system_optimizer import calculate_dbt_threads, get_system_specs

# Cargar variables de entorno
load_dotenv()

def generate_dbt_profile():
    """Genera el archivo profiles.yml con configuración óptima"""
    
    # Obtener configuración de base de datos desde .env
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "job_posting")
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD", "postgres")
    
    # Calcular threads óptimos
    specs = get_system_specs()
    optimal_threads = calculate_dbt_threads()
    
    # Contenido del profiles.yml
    profile_content = f"""# dbt profiles.yml
# Generado automáticamente según especificaciones del sistema
# Sistema: {specs['cpu_cores']} cores, {specs['total_memory_gb']}GB RAM

datajob_etl:
  target: dev
  outputs:
    dev:
      type: postgres
      host: {db_host}
      port: {db_port}
      user: {db_user}
      password: {db_password}
      dbname: {db_name}
      schema: public
      threads: {optimal_threads}
      keepalives_idle: 0
      connect_timeout: 10
      
    prod:
      type: postgres
      host: {db_host}
      port: {db_port}
      user: {db_user}
      password: {db_password}
      dbname: {db_name}
      schema: public
      threads: {optimal_threads}
      keepalives_idle: 0
      connect_timeout: 10
"""
    
    # Ruta del archivo
    profile_path = Path("datajob_etl/profiles.yml")
    
    # Crear el archivo
    with open(profile_path, 'w') as f:
        f.write(profile_content)
    
    print("=" * 70)
    print(" " * 20 + "DBT PROFILE GENERADO")
    print("=" * 70)
    print(f"Archivo: {profile_path}")
    print(f"Threads óptimos: {optimal_threads}")
    print(f"Sistema: {specs['cpu_cores']} cores, {specs['total_memory_gb']}GB RAM")
    print()
    print("Configuración:")
    print(f"  Host: {db_host}:{db_port}")
    print(f"  Database: {db_name}")
    print(f"  User: {db_user}")
    print(f"  Threads: {optimal_threads}")
    print("=" * 70)
    print()
    print("⚠️  NOTA: Este archivo contiene credenciales y está en .gitignore")
    print("   No lo subas al repositorio.")
    print()

if __name__ == "__main__":
    generate_dbt_profile()
