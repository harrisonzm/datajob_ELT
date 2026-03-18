#!/usr/bin/env python3
"""
Script para probar la conexión a PostgreSQL usando variables de entorno
"""
import psycopg2
from sqlalchemy import create_engine, text
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener configuración de base de datos
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "job_posting")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

def test_psycopg2_connection():
    """Prueba conexión usando psycopg2 directamente"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ psycopg2 connection successful!")
        print(f"   Host: {DB_HOST}:{DB_PORT}")
        print(f"   Database: {DB_NAME}")
        print(f"   PostgreSQL version: {version[0][:50]}...")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ psycopg2 connection failed: {e}")
        return False

def test_sqlalchemy_connection():
    """Prueba conexión usando SQLAlchemy"""
    try:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()
            print(f"✅ SQLAlchemy connection successful!")
            print(f"   Connection string: postgresql://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}")
            print(f"   PostgreSQL version: {version[0][:50]}...")
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        return False

def test_database_exists():
    """Verifica que la base de datos existe"""
    try:
        # Conectar a postgres por defecto para listar bases de datos
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database="postgres",
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datname = %s;", (DB_NAME,))
        result = cursor.fetchone()
        
        if result:
            print(f"✅ Database '{DB_NAME}' exists")
        else:
            print(f"❌ Database '{DB_NAME}' does not exist")
            
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"❌ Error checking database existence: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing PostgreSQL connection...")
    print("=" * 50)
    print(f"Configuration from environment:")
    print(f"  DB_HOST: {DB_HOST}")
    print(f"  DB_PORT: {DB_PORT}")
    print(f"  DB_NAME: {DB_NAME}")
    print(f"  DB_USER: {DB_USER}")
    print("=" * 50)
    print()
    
    # Test database existence
    db_exists = test_database_exists()
    print()
    
    # Test connections
    psycopg2_ok = test_psycopg2_connection()
    print()
    
    sqlalchemy_ok = test_sqlalchemy_connection()
    print()
    
    print("=" * 50)
    if db_exists and psycopg2_ok and sqlalchemy_ok:
        print("🎉 All connection tests passed!")
        sys.exit(0)
    else:
        print("💥 Some connection tests failed!")
        sys.exit(1)