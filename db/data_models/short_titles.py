"""Modelo para la tabla dim_short_titles."""
from sqlalchemy import String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class ShortTitle(Base):
    __tablename__ = 'dim_short_titles'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
