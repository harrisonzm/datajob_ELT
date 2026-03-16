from typing import Generator, Any
from sqlalchemy import Engine
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
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

# Crear engine optimizado para carga rápida
engine: Engine = create_engine(
    DATABASE_URL, 
    echo=False,  # Desactivar logging para mayor velocidad
    pool_size=20,  # Aumentar pool de conexiones
    max_overflow=30,  # Permitir más conexiones overflow
    pool_pre_ping=True,  # Verificar conexiones antes de usar
    pool_recycle=3600,  # Reciclar conexiones cada hora
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
    Base.metadata.drop_all(bind=engine)