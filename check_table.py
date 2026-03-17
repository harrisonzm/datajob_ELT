from sqlalchemy import text
from db.config.db import engine

# Verificar qué tablas existen
with engine.connect() as conn:
    # Verificar todas las tablas
    result = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_name = 'job_posting';"))
    tables = result.fetchall()
    
    print("Tablas job_posting encontradas:")
    for table in tables:
        print(f"  Schema: {table[0]}, Tabla: {table[1]}")
    
    if not tables:
        print("No se encontró la tabla job_posting")
        
        # Verificar todas las tablas disponibles
        result = conn.execute(text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' ORDER BY table_schema, table_name;"))
        all_tables = result.fetchall()
        
        print("\nTodas las tablas disponibles:")
        for table in all_tables:
            print(f"  {table[0]}.{table[1]}")