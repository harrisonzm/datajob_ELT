"""
Script para generar automáticamente el archivo profiles.yml de dbt
desde las variables de entorno del archivo .env con optimización de threads
"""
import os
import multiprocessing as mp
from pathlib import Path
from dotenv import load_dotenv

def get_optimal_threads():
    """Calcula el número óptimo de threads para dbt basado en el sistema"""
    n_cores = mp.cpu_count()
    
    # Estrategia: usar 75% de los cores disponibles, mínimo 2, máximo 16
    optimal = max(2, min(16, int(n_cores * 0.75)))
    
    return optimal, n_cores

def create_dbt_profile():
    """Crea el archivo profiles.yml para dbt usando variables de entorno"""
    
    # Cargar variables de entorno
    load_dotenv()
    
    # Obtener credenciales
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'datajob_db')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', '')
    
    # Calcular threads óptimos
    optimal_threads, total_cores = get_optimal_threads()
    
    # Validar que tenemos las credenciales necesarias
    if not db_password:
        print("⚠️  Advertencia: DB_PASSWORD no está configurado en .env")
    
    # Contenido del profiles.yml con threads optimizados
    profile_content = f"""datajob_etl:
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
"""
    
    # Crear directorio datajob_etl si no existe
    profile_dir = Path('datajob_etl')
    profile_dir.mkdir(exist_ok=True)
    
    # Escribir archivo profiles.yml
    profile_path = profile_dir / 'profiles.yml'
    
    try:
        with open(profile_path, 'w', encoding='utf-8') as f:
            f.write(profile_content)
        
        print(f"✓ Archivo profiles.yml creado exitosamente en: {profile_path}")
        print(f"  • Host: {db_host}")
        print(f"  • Port: {db_port}")
        print(f"  • Database: {db_name}")
        print(f"  • User: {db_user}")
        print(f"  • Threads: {optimal_threads} (de {total_cores} cores disponibles)")
        return True
        
    except Exception as e:
        print(f"✗ Error creando profiles.yml: {e}")
        return False

if __name__ == "__main__":
    create_dbt_profile()
