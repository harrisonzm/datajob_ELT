#!/usr/bin/env python3
"""
Script para probar la conexión básica a PostgreSQL
"""
import psycopg2
import sys

def test_postgres_connection():
    """Prueba conexión a la base de datos postgres por defecto"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="postgres",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅ Connection to postgres database successful!")
        print(f"PostgreSQL version: {version[0]}")
        
        # Listar todas las bases de datos
        cursor.execute("SELECT datname FROM pg_database;")
        databases = cursor.fetchall()
        print(f"\n📋 Available databases:")
        for db in databases:
            print(f"  - {db[0]}")
            
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Testing basic PostgreSQL connection...")
    print("=" * 50)
    
    success = test_postgres_connection()
    
    print("=" * 50)
    if success:
        print("🎉 Basic connection test passed!")
        sys.exit(0)
    else:
        print("💥 Basic connection test failed!")
        sys.exit(1)