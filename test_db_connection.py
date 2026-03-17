#!/usr/bin/env python3
"""
Script para probar la conexión a PostgreSQL
"""
import psycopg2
from sqlalchemy import create_engine, text
import sys

def test_psycopg2_connection():
    """Prueba conexión usando psycopg2 directamente"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="job_posting",
            user="postgres",
            password="dbpassword"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ psycopg2 connection successful!")
        print(f"PostgreSQL version: {version[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ psycopg2 connection failed: {e}")
        return False

def test_sqlalchemy_connection():
    """Prueba conexión usando SQLAlchemy"""
    try:
        engine = create_engine("postgresql://postgres:postgres@localhost:5432/job_posting")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()
            print(f"✅ SQLAlchemy connection successful!")
            print(f"PostgreSQL version: {version[0]}")
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy connection failed: {e}")
        return False

def test_database_exists():
    """Verifica que la base de datos existe"""
    try:
        # Conectar a postgres por defecto para listar bases de datos
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT datname FROM pg_database WHERE datname = 'job_posting';")
        result = cursor.fetchone()
        
        if result:
            print(f"✅ Database 'job_posting' exists")
        else:
            print(f"❌ Database 'job_posting' does not exist")
            
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"❌ Error checking database existence: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing PostgreSQL connection...")
    print("=" * 50)
    
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