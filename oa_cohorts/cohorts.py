from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from sqlalchemy import Enum
import enum

class RuleTarget(enum.Enum):
    dx_any = 1
    dx_primary = 2
    dx_first_primary = 3
    dx_mets = 4
    dx_stage = 5
    tx_any = 6
    tx_first_line = 7
    tx_current_episode = 8
    tx_chemotherapy = 9
    tx_radiotherapy = 10
    tx_surgical = 11
    demog_gender = 12
    demog_death = 13
    obs_value = 14
    obs_concept = 15
    proc_concept = 16

class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3
    rule_simple = 4

class RuleType(enum.Enum):
    dx_rule = 1
    tx_rule = 2
    obs_rule = 3
    person_rule = 4
    proc_rule = 5

class RuleMatcher(enum.Enum):
    substring = 1
    exact = 2
    hierarchy = 3

class RuleTemporality(enum.Enum):
    dt_current_start = 1
    dt_death = 2
    dt_treatment_start = 3
    dt_obs = 4
    dt_proc_start = 5


class Report_Cohort_Map = sa.Table(
    'report_cohort_map', 
    Base.metadata,
    sa.Column('report_id', sa.ForeignKey('report.report_id')),
    sa.Column('dash_cohort_id', sa.ForeignKey('dash_cohort.dash_cohort_id')),
    sa.Column('primary_cohort', sa.Boolean),
)

class Dash_Cohort_Measure_Map = sa.Table(
    'dash_cohort_measure_map', 
    Base.metadata,
    sa.Column('dash_cohort_def_id', sa.ForeignKey('dash_cohort_def.dash_cohort_def_id')),
    sa.Column('measure_id', sa.ForeignKey('measure.measure_id'))
)

class Query_Rule_Map = sa.Table(
    'query_rule_map', 
    Base.metadata,
    sa.Column('subquery_id', sa.ForeignKey('subquery.subquery_id')),
    sa.Column('query_rule_id', sa.ForeignKey('query_rule.query_rule_id'))
)

class Report(Base):
    __tablename__ = 'report'
    report_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    report_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_description: so.Mapped[str] = so.mapped_column(sa.String(1000))
    report_create_date: so.Mapped[date] = so.mapped_column(sa.Date)
    report_update_date: so.Mapped[date] = so.mapped_column(sa.Date)
    report_author: so.Mapped[str] = so.mapped_column(sa.String(250))

    cohort: so.Mapped[List['DashCohort']] = so.relationship(secondary=Report_Cohort_Map, 
                                                            back_populates='in_reports')

class Report_Version(Base):
    __tablename__ = 'report_version'
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'), primary_key=True)
    report_version_major: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_minor: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_label: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_version_date: so.Mapped[date] = so.mapped_column(sa.Date)


class Dash_Cohort(Base):
    __tablename__ = 'dash_cohort'
    dash_cohort_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    dash_cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))
    
    # relationships
    definitions: so.Mapped[List['Dash_Cohort_Def']] = so.relationship(back_populates="dash_cohort_object", 
                                                                      lazy="selectin")


class Dash_Cohort_Def(Base):
    __tablename__ = 'dash_cohort_def'
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    dash_cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_def_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    dash_cohort_measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))
    
    # fks
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'))

    # relationships
    dash_cohort_object: so.Mapped['Dash_Cohort'] = so.relationship(foreign_keys=[dash_cohort_id])
    dash_cohort_measures: so.Mapped[List['Dash_Cohort_Dx_Rule']] = so.relationship(back_populates="dash_def_object", 
                                                                                   lazy="selectin")

class Measure(Base):
    __tablename__ = 'measure'
    measure_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    measure_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # OR, AND, EXCEPT, SIMPLE
    

class Measure_Def(Base):
    __tablename__ = 'measure_def'
    measure_def_id: so.Mapped[int] = so.mapped_column(primary_key=True)

    # fks
    measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    measure_query_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'))
    child_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    
    # relationships    
    subqueries: so.Mapped[List['Subquery']] = so.relationship(foreign_keys=[child_measure_id])
    child_measures: so.Mapped[List['Measure']] = so.relationship(foreign_keys=[child_measure_id])


class Subquery(Base):
    __tablename__ = 'subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    subquery_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    subquery_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    subquery_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType))
    subquery_target: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTarget))
    subquery_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))

class Query_Rule(Base):
    __tablename__ = 'query_rule'
    query_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    query_matcher: so.Mapped[int] = so.mapped_column(sa.Enum(RuleMatcher))
    query_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    
    # relationships
    rule_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[dx_concept_id])

