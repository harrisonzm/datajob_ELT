from typing import Optional, Dict, List
from datetime import datetime
from sqlalchemy import String, Boolean, Float, BigInteger, DateTime
from sqlalchemy.dialects.postgresql import JSON, ARRAY
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base

class JobPosting(Base):
    __tablename__ = 'job_posting'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_title_short: Mapped[str] = mapped_column(String, nullable=True)
    job_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    job_location: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    job_via: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    job_schedule_type: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    job_work_from_home: Mapped[bool] = mapped_column(Boolean, nullable=True)
    search_location: Mapped[str] = mapped_column(String, nullable=True)
    job_posted_date: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    job_no_degree_mention: Mapped[bool] = mapped_column(Boolean, nullable=True)
    job_health_insurance: Mapped[bool] = mapped_column(Boolean, nullable=True)
    job_country: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    salary_rate: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    salary_year_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    salary_hour_avg: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    company_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    
    # Campos especiales para arrays y JSON
    job_skills: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String), nullable=True)
    job_type_skills: Mapped[Optional[Dict[str, List[str]]]] = mapped_column(JSON, nullable=True)
    
    def __repr__(self) -> str:
        return f"<JobPosting(id={self.id}, job_title='{self.job_title}', company='{self.company_name}')>"