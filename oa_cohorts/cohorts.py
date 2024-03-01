from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from sqlalchemy import Enum
import enum

class DxType(enum.Enum):
    dx_any = 1
    dx_primary = 2
    dx_first_primary = 3
    dx_mets = 4

class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3

class RuleType(enum.Enum):
    dx_rule = 1
    tx_rule = 2
    event_rule = 3
    demog_rule = 4

class TxType(enum.Enum):
    any = 1
    first_line = 2
    current_episode = 3
    chemotherapy = 4
    radiotherapy = 5
    surgical = 4


class Dash_Cohort(Base):
    __tablename__ = 'dash_cohort'
    dash_cohort_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    # relationships
    definitions: so.Mapped[List['Dash_Cohort_Definition']] = so.relationship(back_populates="dash_cohort_object", lazy="selectin")

    def __repr__(self):
        return f'Cohort: ID = {self.dash_cohort_id} > NAME = {self.cohort_name}'

class Dash_Cohort_Definition(Base):
    __tablename__ = 'dash_cohort_def'
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    # fks
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'))
    # relationships
    dash_cohort_object: so.Mapped['Dash_Cohort'] = so.relationship(foreign_keys=[dash_cohort_id])
    dx_rules: so.Mapped[List['Dash_Cohort_Dx_Rule']] = so.relationship(back_populates="dash_def_object", lazy="selectin")
    tx_rules: so.Mapped[List['Dash_Cohort_Tx_Rule']] = so.relationship(back_populates="dash_def_object", lazy="selectin")

    def __repr__(self):
        return f'Cohort Definition: ID = {self.dash_cohort_def_id} > NAME = {self.cohort_def_name}'


class Dash_Cohort_Rule(Base):
    __tablename__ = 'dash_cohort_rule'
    dash_cohort_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_rule_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # OR, AND, EXCEPT
    rule_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType))
    # fks
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_def.dash_cohort_def_id'))
    # relationships
    dash_def_object: so.Mapped['Dash_Cohort_Definition'] = so.relationship(foreign_keys=[dash_cohort_def_id])

    def __repr__(self):
        return f'Cohort Rule: ID = {self.dash_cohort_rule_id} > COMBINATION = {self.cohort_rule_combination} > TYPE = {self.rule_type}'
    
    # dx_rules: so.Mapped[List['Dash_Cohort_Dx_Rule']] = so.relationship(
    #     backref="dx_rule_obj", lazy="selectin", viewonly=True
    # )

    # tx_rules: so.Mapped[List['Dash_Cohort_Tx_Rule']] = so.relationship(
    #     backref="tx_rule_obj", lazy="selectin", viewonly=True
    # )
    
    __mapper_args__ = {
            "polymorphic_on":sa.case(
                (rule_type == RuleType.dx_rule, "dx_rule"),
                 else_="tx_rule"),
            "polymorphic_identity":"dash_cohort_rule"
        }

    # __mapper_args__ = {
    #     "polymorphic_identity": "cohort_rule",
    #     "polymorphic_on": "rule_type",
    # }

class Dash_Cohort_Dx_Rule(Dash_Cohort_Rule):
    __tablename__ = 'dash_cohort_dx_rule'
    dash_cohort_dx_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_dx_type: so.Mapped[int] = so.mapped_column(sa.Enum(DxType)) # Enum for ANY | PRIMARY | FIRST_PRIMARY | METS
    # relationships
    diagnoses: so.Mapped[List['Dash_Cohort_Dx']] = so.relationship(back_populates="dx_rule_object", lazy="selectin")

    def __repr__(self):
        return f'DX Rule: ID = {self.dash_cohort_rule_id} > DX_TYPE = {self.cohort_dx_type} > TYPE = {self.rule_type} > COMBINATION = {self.cohort_rule_combination}'
    
    __mapper_args__ = {
        "polymorphic_identity": "dx_rule",
        'inherit_condition': (dash_cohort_dx_rule_id == Dash_Cohort_Rule.dash_cohort_rule_id)
    }

class Dash_Cohort_Tx_Rule(Dash_Cohort_Rule):
    __tablename__ = 'dash_cohort_tx_rule'
    dash_cohort_tx_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    cohort_tx_type: so.Mapped[int] = so.mapped_column(sa.Enum(TxType)) # Enum for ANY | FIRST LINE | CURRENT EPISODE

    __mapper_args__ = {
        "polymorphic_identity": "tx_rule",
        'inherit_condition': (dash_cohort_tx_rule_id == Dash_Cohort_Rule.dash_cohort_rule_id)
    }

class Dash_Cohort_Dx(Base):
    __tablename__ = 'dash_cohort_dx'
    dash_cohort_dx: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    # fks
    dash_cohort_rule_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort_rule.dash_cohort_rule_id'))
    dx_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'))
    # relationships
    diagnosis_object: so.Mapped['Concept'] = so.relationship(foreign_keys=[dx_concept_id])
    dx_rule_object: so.Mapped['Dash_Cohort_Rule'] = so.relationship(foreign_keys=[dash_cohort_rule_id])

    def __repr__(self):
        return f'DX Rule: ID = {self.dash_cohort_dx}'
