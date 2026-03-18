"""Modelo para la tabla job_skills."""
from sqlalchemy import BigInteger, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class JobPostSkill(Base):
    __tablename__ = 'job_skills'
    
    job_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('fact_job_posts.id'), primary_key=True)
    skill_types_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('skill_types.id'), primary_key=True)
