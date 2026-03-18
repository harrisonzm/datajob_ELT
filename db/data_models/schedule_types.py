"""Modelo para la tabla dim_schedule_types."""
from sqlalchemy import String, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class ScheduleType(Base):
    __tablename__ = 'dim_schedule_types'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False, unique=True)
