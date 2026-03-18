from typing import Generator, Any
from sqlalchemy import Engine
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configuración de la base de datos
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "job_posting")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dbpassword")

# URL de conexión
DATABASE_URL: str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Crear engine optimizado para carga rápida con paralelización
engine: Engine = create_engine(
    DATABASE_URL, 
    echo=False,  # Desactivar logging para mayor velocidad
    pool_size=10,  # Pool base de conexiones
    max_overflow=20,  # Permitir hasta 30 conexiones totales (10 + 20)
    pool_pre_ping=True,  # Verificar conexiones antes de usar
    pool_recycle=3600,  # Reciclar conexiones cada hora
    pool_timeout=30,  # Timeout para obtener conexión del pool
    connect_args={
        "options": "-c statement_timeout=300000"  # 5 minutos timeout por statement
    }
)

# Crear sessionmaker
SessionLocal: sessionmaker[Session] = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base declarativa
Base = declarative_base()

def get_db() -> Generator[Session, Any, None]:
    """Función para obtener una sesión de base de datos"""
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():
    """Crear todas las tablas definidas en Base"""
    Base.metadata.create_all(bind=engine)

def drop_tables():
    """Eliminar todas las tablas definidas en Base"""
    from sqlalchemy import text
    
    # Primero intentar eliminar con metadata
    Base.metadata.drop_all(bind=engine)
    
    # Asegurar que job_posting se elimine con CASCADE
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS job_posting CASCADE"))
        conn.commit()