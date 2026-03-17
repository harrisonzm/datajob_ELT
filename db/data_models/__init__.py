"""
Modelos de datos para el esquema 3NF de job postings
"""

from db.data_models.types import Type
from db.data_models.companies import Company
from db.data_models.countries import Country
from db.data_models.locations import Location
from db.data_models.vias import Via
from db.data_models.schedule_types import ScheduleType
from db.data_models.short_titles import ShortTitle
from db.data_models.skill_types import SkillType
from db.data_models.skills import Skill
from db.data_models.job_posts import JobPost
from db.data_models.job_skills import JobPostSkill
from db.data_models.job_posting import JobPosting

__all__ = [
    "Type",
    "Company",
    "Country",
    "Location",
    "Via",
    "ScheduleType",
    "ShortTitle",
    "SkillType",
    "Skill",
    "JobPost",
    "JobPostSkill",
    "JobPosting",
]
