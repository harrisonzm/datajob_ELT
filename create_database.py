#!/usr/bin/env python3
"""
Script para crear la base de datos job_posting
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys

def create_job_posting_database():
    """Crea la base de datos job_posting"""
    try:
        # Conectar a postgres con autocommit para poder crear bases de datos
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Verificar si la base de datos ya existe
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'job_posting';")
        exists = cursor.fetchone()
        
        if exists:
            print("📋 Database 'job_posting' already exists")
        else:
            # Crear la base de datos
            cursor.execute("CREATE DATABASE job_posting;")
            print("✅ Database 'job_posting' created successfully!")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

def test_job_posting_connection():
    """Prueba la conexión a la base de datos job_posting"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="job_posting",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT current_database();")
        db_name = cursor.fetchone()
        print(f"✅ Successfully connected to database: {db_name[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to job_posting: {e}")
        return False

if __name__ == "__main__":
    print("🔧 Creating job_posting database...")
    print("=" * 50)
    
    # Create database
    created = create_job_posting_database()
    print()
    
    # Test connection
    if created:
        connected = test_job_posting_connection()
        print()
        
        print("=" * 50)
        if connected:
            print("🎉 Database creation and connection successful!")
            sys.exit(0)
        else:
            print("💥 Database created but connection failed!")
            sys.exit(1)
    else:
        print("=" * 50)
        print("💥 Database creation failed!")
        sys.exit(1)