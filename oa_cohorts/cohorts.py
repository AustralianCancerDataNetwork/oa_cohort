from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from sqlalchemy import Enum
import enum

class DxType(enum.Enum):
    dx_any = 1
    dx_first = 2
    dx_mets = 3

class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3

class RuleType(enum.Enum):
    dx_rule = 1
    tx_rule = 2
    event_rule = 3
    demog_rule = 4

class Dash_Cohort(Base):
    __tablename__ = 'dash_cohort'
    dash_cohort_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))

class Dash_Cohort_Definition(Base):
    __tablename__ = 'dash_cohort_def'
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'))

class Dash_Cohort_Rule(Base):
    __tablename__ = 'dash_cohort_rule'
    dash_cohort_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_def.dash_cohort_def_id'))
    cohort_rule_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # OR, AND, EXCEPT
    rule_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType))
    __mapper_args__ = {
        "polymorphic_identity": "cohort_rule",
        "polymorphic_on": "rule_type",
    }

class Dash_Cohort_Dx_Rule(Dash_Cohort_Rule):
    __tablename__ = 'dash_cohort_dx_rule'
    dash_cohort_rule_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_rule.dash_cohort_rule_id'), primary_key=True,)
    cohort_dx_type: so.Mapped[int] = so.mapped_column(sa.Enum(DxType)) # Enum for ANY | FIRST | METS
    __mapper_args__ = {
        "polymorphic_identity": RuleType.dx_rule.value,
    }

class Dash_Cohort_Tx_Rule(Dash_Cohort_Rule):
    __tablename__ = 'dash_cohort_tx_rule'
    dash_cohort_rule_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_rule.dash_cohort_rule_id'), primary_key=True,)
    cohort_tx_type: so.Mapped[int] = so.mapped_column(sa.Enum(DxType)) # Enum for ANY | FIRST LINE | CURRENT EPISODE
    __mapper_args__ = {
        "polymorphic_identity": RuleType.tx_rule.value,
    }

class Dash_Cohort_Dx(Base):
    __tablename__ = 'dash_cohort_dx'
    dash_cohort_dx: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_dx_rule_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_dx_rule.dash_cohort_rule_id'))
    dx_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
