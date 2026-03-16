from sqlalchemy import Table, Column, String, Boolean, DateTime, Float, BigInteger
from db.config.db import metadata

job_posting_table = Table(
    'job_posting',
    metadata,
    Column('id', BigInteger, primary_key=True, autoincrement=True),
    Column('job_title_short', String),
    Column('job_title', String),
    Column('job_location', String),
    Column('job_via', String),
    Column('job_schedule_type', String),
    Column('job_work_from_home', Boolean),
    Column('search_location', String),
    Column('job_posted_date', DateTime),
    Column('job_no_degree_mention', Boolean),
    Column('job_health_insurance', Boolean),
    Column('job_country', String),
    Column('salary_rate', String, nullable=True),
    Column('salary_year_avg', Float, nullable=True),
    Column('salary_hour_avg', Float, nullable=True),
    Column('company_name', String),
    Column('job_skills', String),
    Column('job_type_skills', String)
)