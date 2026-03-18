"""Modelo para la tabla fact_job_posts."""
from sqlalchemy import String, Boolean, Float, BigInteger, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from db.config.db import Base


class JobPost(Base):
    __tablename__ = 'fact_job_posts'
    
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    company_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_companies.id'), nullable=True)
    country_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_countries.id'), nullable=True)
    location_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_locations.id'), nullable=True)
    via_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_vias.id'), nullable=True)
    schedule_type_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_schedule_types.id'), nullable=True)
    short_title_id: Mapped[int] = mapped_column(BigInteger, ForeignKey('dim_short_titles.id'), nullable=True)
    job_title: Mapped[str] = mapped_column(String, nullable=True)
    search_location: Mapped[str] = mapped_column(String, nullable=True)
    job_posted_date: Mapped[DateTime] = mapped_column(DateTime, nullable=True)
    job_work_from_home: Mapped[bool] = mapped_column(Boolean, nullable=True)
    job_no_degree_mention: Mapped[bool] = mapped_column(Boolean, nullable=True)
    job_health_insurance: Mapped[bool] = mapped_column(Boolean, nullable=True)
    salary_rate: Mapped[str] = mapped_column(String, nullable=True)
    salary_year_avg: Mapped[float] = mapped_column(Float, nullable=True)
    salary_hour_avg: Mapped[float] = mapped_column(Float, nullable=True)
