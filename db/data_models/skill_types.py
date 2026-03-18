"""Modelo para la tabla skill_types."""
from sqlalchemy import BigInteger, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class SkillType(Base):
    __tablename__ = 'skill_types'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    skill_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_skills.id'), nullable=False)
    type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_types.id'), nullable=False)
