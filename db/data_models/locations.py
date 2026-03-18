"""Modelo para la tabla dim_locations."""
from sqlalchemy import String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class Location(Base):
    __tablename__ = 'dim_locations'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    location: Mapped[str] = mapped_column(String, nullable=False, unique=True)
