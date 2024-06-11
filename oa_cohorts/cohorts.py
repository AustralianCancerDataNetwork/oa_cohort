from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence
from sqlalchemy import Enum
import enum


# refresh_cohort(id)

# delete all 'person_cohort' records for that cohort
# delete all 'person_measure' records for that cohort
# delete all 'person_query' recogrs for that cohort
# for each measure in cohort:
#   for each query in measure:
#       select people who belong to that cohort and insert into person_query

class Cohort_Refresh(Base):
    __tablename__ = 'cohort_refresh'
    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_def.id'))
    refresh_date: so.Mapped[date] = so.mapped_column(sa.DateTime)


class Cohort_Person(Base):
    __tablename__ = 'cohort_person'
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'), primary_key=True)
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_def.id'), primary_key=True)


class Measure_Person(Base):
    __tablename__ = 'measure_person' 
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'), primary_key=True)
    measure_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure_def.id'), primary_key=True)
    measure_date: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    measure_value: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)

    person: so.Mapped["Person"] = so.relationship(foreign_keys=[person_id])

class Subquery_Person(Base):
    __tablename__ = 'subquery_person'
    person_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('person.person_id'), primary_key=True)
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)
    subquery_date: so.Mapped[Optional[date]] = so.mapped_column(sa.DateTime, nullable=True)
    subquery_value: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)

    person: so.Mapped["Person"] = so.relationship(foreign_keys=[person_id])
