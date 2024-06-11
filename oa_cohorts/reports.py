from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence
from sqlalchemy import Enum
import enum

# valid subquery combinations:
# subquery type    | subquery target                | implementation target
# dx_rule          |     dx_any = 1                 | condition_occurrence.condition_concept_id
# dx_rule          |     dx_primary = 2             | episode overarching -> condition_occurrence
# dx_rule          |     dx_first_primary = 3       | episode overarching -> condition_occurrence, sorted for date
# dx_rule          |     dx_mets = 4                | episode dx extent -> condition modifier
# dx_rule          |     dx_stage = 5               | episode overarching -> condition_occurrence
# tx_rule          |     tx_any = 6
# tx_rule          |     tx_first_line = 7
# tx_rule          |     tx_current_episode = 8
# tx_rule          |     tx_chemotherapy = 9
# tx_rule          |     tx_radiotherapy = 10
# tx_rule          |     tx_surgical = 11

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

    def target_table(self):
        return {1: Condition_Occurrence, 
                2: Condition_Occurrence, 
                3: Condition_Occurrence, 
                4: Condition_Occurrence, 
                5: Condition_Occurrence,
                12: Person,
                13: Person,
                14: Observation,
                15: Observation,
                16: Procedure_Occurrence}[self.value]

    def target_options(self):
        return {1: Condition_Occurrence.condition_concept_id, 
                2: Condition_Occurrence.condition_concept_id, 
                3: Condition_Occurrence.condition_concept_id, 
                4: Condition_Occurrence.condition_concept_id, 
                5: Condition_Occurrence.condition_concept_id,
                12: Person.gender_concept_id,
                13: Person.death_datetime,
                14: Observation.value_as_concept_id,
                15: Observation.observation_concept_id,
                16: Procedure_Occurrence.procedure_concept_id}

    def string_target_options(self):
        return {1: Condition_Occurrence.condition_code, 
                2: Condition_Occurrence.condition_code, 
                3: Condition_Occurrence.condition_code, 
                4: Condition_Occurrence.condition_code, 
                5: Condition_Occurrence.condition_code,
                14: Observation.value_as_concept_id,
                15: Observation.observation_concept_id,
                16: Procedure_Occurrence.procedure_concept_id}

    def target(self, str_match=False):
        if str_match:
            return self.string_target_options()[self.value]
        return self.target_options()[self.value]

class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3
    rule_simple = 4
    rule_special = 5

    def combiner_options(self):
        return {1: sa.or_, 2: sa.and_, 3: None, 4: sa.or_, 5: None}

    def combiner(self):
        return self.combiner_options()[self.value]

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
    absence = 4
    presence = 5

class RuleTemporality(enum.Enum):
    dt_current_start = 1
    dt_death = 2
    dt_treatment_start = 3
    dt_obs = 4
    dt_proc_start = 5
    dt_numerator = 6
    dt_denominator = 7
    dt_any = 8


dash_cohort_measure_map = sa.Table(
    'dash_cohort_measure_map', 
    Base.metadata,
    sa.Column('dash_cohort_def_id', sa.ForeignKey('dash_cohort_def.id')),
    sa.Column('measure_id', sa.ForeignKey('measure.id'))
)

query_rule_map =  sa.Table(
    'query_rule_map', 
    Base.metadata,
    sa.Column('subquery_id', sa.ForeignKey('subquery.id')),
    sa.Column('query_rule_id', sa.ForeignKey('query_rule.id'))
)

report_indicator_map =  sa.Table(
    'report_indicator_map', 
    Base.metadata,
    sa.Column('report_id', sa.ForeignKey('report.id')),
    sa.Column('indicator', sa.ForeignKey('indicator.id'))
)

class Report_Cohort_Map(Base):
    __tablename__ = 'report_cohort_map' 
    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.id'), primary_key=True)
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.id'), primary_key=True)
    primary_cohort: so.Mapped[bool] = so.mapped_column(sa.Boolean)
    cohort: so.Mapped['Dash_Cohort'] = so.relationship(back_populates='in_reports')
    report: so.Mapped['Report'] = so.relationship(back_populates='cohorts')

class Report(Base):
    __tablename__ = 'report'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    report_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_description: so.Mapped[str] = so.mapped_column(sa.String(1000))
    report_create_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report_edit_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report_author: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_owner: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)

    cohorts: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='report')
    indicators: so.Mapped[List['Indicator']] = so.relationship(secondary=report_indicator_map, back_populates='reports')
    report_version: so.Mapped["Report_Version"] = so.relationship("Report_Version")

    @property
    def version_string(self):
        if self.report_version:
            return f'{self.report_version.report_version_major}.{self.report_version.report_version_minor} ({self.report_version.report_version_label})'


class Report_Version(Base):
    __tablename__ = 'report_version'
    id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.id'), primary_key=True)
    report_version_major: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_minor: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_label: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_version_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report: so.Mapped["Report"] = so.relationship(foreign_keys=[report_id], back_populates='report_version')


class Dash_Cohort(Base):
    __tablename__ = 'dash_cohort'
    id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    dash_cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))
    
    in_reports: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='cohort')
    # relationships
    definitions: so.Mapped[List['Dash_Cohort_Def']] = so.relationship(back_populates="dash_cohort_object", 
                                                                      lazy="selectin")

    
    def full_cohort_def(self):
        return self.dash_cohort_combination.combiner()(*[m.cohort_definition() for m in self.definitions])                                                                    


class Dash_Cohort_Def(Base):
    __tablename__ = 'dash_cohort_def'
    id: so.Mapped[int] = so.mapped_column(primary_key=True, autoincrement=True)
    dash_cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_def_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    dash_cohort_measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))
    
    # fks
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.id'))

    # relationships
    dash_cohort_object: so.Mapped['Dash_Cohort'] = so.relationship(foreign_keys=[dash_cohort_id])
    
    dash_cohort_measures: so.Mapped[List['Measure']] = so.relationship(secondary=dash_cohort_measure_map, 
                                                                       back_populates="in_dash_cohort")

    def cohort_definition(self):
        return self.dash_cohort_measure_combination.combiner()(*[m.measure_definition() for m in self.dash_cohort_measures])

class Measure(Base):
    __tablename__ = 'measure'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    measure_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # OR, AND, EXCEPT, SIMPLE

    in_dash_cohort: so.Mapped[List['Dash_Cohort_Def']] = so.relationship(secondary=dash_cohort_measure_map, 
                                                                         back_populates="dash_cohort_measures")

    measure_defs: so.Mapped[List["Measure_Def"]] = so.relationship("Measure_Def", foreign_keys='Measure_Def.measure_id')

    def measure_definition(self):
        return self.measure_combination.combiner()(*[m.get_filter() for m in self.measure_defs])

class Measure_Def(Base):
    __tablename__ = 'measure_def'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)

    # fks
    measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.id'))
    measure_query_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('subquery.id'), nullable=True)
    child_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.id'), nullable=True)
    
    # relationships    
    measure: so.Mapped['Measure'] = so.relationship(foreign_keys=[measure_id], back_populates="measure_defs")
    child_measures: so.Mapped['Measure'] = so.relationship(foreign_keys=[child_measure_id])
    subqueries: so.Mapped['Subquery'] = so.relationship(foreign_keys=[measure_query_id])


    
    def get_measure_def(self, db):
        if self.subqueries:
            return self.subqueries.get_subquery(db)
        elif self.child_measures:
            cm = [md.execute_measure_def(db) for md in self.child_measures.measure_defs(db)]
            query = cm[0]
            for q in cm[1:]:
                if self.subquery_combination in [RuleCombination.rule_or, RuleCombination.rule_simple]:
                    query = query.union(q)
                elif self.subquery_combination in [RuleCombination.rule_and]:
                    query = query.intersection(q)
            return query

    def execute_measure_def(self, db):
        return self.get_measure_def(db).all()

    def combiner(self):
        return self.measure.measure_combination.combiner()

    def get_filter(self):
        if self.child_measures:
            return self.child_filters() 
        if self.subqueries:
            return self.my_filter()  

    def my_filter(self):
        return self.combiner()(*[self.subqueries.get_filter()])

    def child_filters(self):
        return self.combiner()(*[sq.get_filter() for sq in self.child_measures.measure_defs])

class Subquery(Base):
    __tablename__ = 'subquery'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    is_core: so.Mapped[bool] = so.mapped_column(sa.Boolean)
    subquery_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    subquery_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    subquery_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType))
    subquery_target: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTarget))
    subquery_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination))
    query_rules: so.Mapped[List['Query_Rule']] = so.relationship(secondary=query_rule_map, 
                                                                 back_populates="subqueries")

    @property
    def filter_field(self):
        if len(self.query_rules) == 0:
            return None
        return self.subquery_target.target(self.query_rules[0].query_matcher == RuleMatcher.substring)

    @property
    def filter_table(self):
        if len(self.query_rules) == 0:
            return None
        return self.subquery_target.target_table

    __mapper_args__ = {
        "polymorphic_on":sa.case(
            (subquery_type == RuleType.dx_rule, "diagnostic"),
            (subquery_type == RuleType.tx_rule, "treatment"),
            (subquery_type == RuleType.obs_rule, "observation"),
            (subquery_type == RuleType.proc_rule, "procedure"),
            else_="person"),
        "polymorphic_identity":"subquery_type"
    }

    def get_subquery(self, db):
        if len(self.query_rules) == 0:
            return None
        qr = [db.query(self.filter_table()).filter(sq.get_filter_details(self.filter_field)) for sq in self.query_rules]
        query = qr[0]
        for q in qr[1:]:
            if self.subquery_combination in [RuleCombination.rule_or, RuleCombination.rule_simple]:
                query = query.union(q)
            elif self.subquery_combination in [RuleCombination.rule_and]:
                query = query.intersection(q)
        return query

    def execute_subquery(self, db):
        return self.get_subquery(db).all()

    def get_filter(self, str_match=False):
        # given the properties of subquery type (dx, tx, person, observation or procedure)
        # this function will combine the associated query rules to produce the required filter
        # for selecting the target cohort
        raise NotImplemented()


class Dx_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in diagnostic episodes
    __tablename__ = 'dx_subquery'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)

    def get_filter(self):
        # todo: check allowed combinations of target and type
        # todo: move field from condition_occurrence concept to primary dx ep concept
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "diagnostic",
        'inherit_condition': (id == Subquery.id)
    }


class Tx_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in treatment episodes
    __tablename__ = 'tx_subquery'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)
    
    def get_filter(self):
        raise NotImplemented()

    __mapper_args__ = {
        "polymorphic_identity": "treatment",
        'inherit_condition': (id == Subquery.id)
    }

class Obs_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in the observation domain
    __tablename__ = 'obs_subquery'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)
    
    def get_filter(self):
        raise NotImplemented()
        # field = self.subquery_target.target(self.query_rules[0].query_matcher == RuleMatcher.substring)
        # return self.subquery_combination.combiner()(*[sq.get_filter_details() for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "observation",
        'inherit_condition': (id == Subquery.id)
    }

class Proc_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in the procedure domain
    __tablename__ = 'proc_subquery'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)

    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "procedure",
        'inherit_condition': (id == Subquery.id)
    }

class Person_Subquery(Subquery):
    # filters cohort based on demographic criteria
    __tablename__ = 'person_subquery'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.id'), primary_key=True)

    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])
    
    __mapper_args__ = {
        "polymorphic_identity": "person",
        'inherit_condition': (id == Subquery.id)
    }

class Query_Rule(Base):
    __tablename__ = 'query_rule'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    query_matcher: so.Mapped[int] = so.mapped_column(sa.Enum(RuleMatcher))
    query_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), default=0)
    query_temporal_association: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTemporality), default=RuleTemporality.dt_any)
    query_notes: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)        
    
    # relationships
    rule_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[query_concept_id])
    subqueries: so.Mapped[List['Subquery']] = so.relationship(secondary=query_rule_map, back_populates="query_rules")

    __mapper_args__ = {
        "polymorphic_on":sa.case(
            (query_matcher == RuleMatcher.exact, "exact"),
            (query_matcher == RuleMatcher.hierarchy, "hierarchy"),
            (query_matcher == RuleMatcher.substring, "substring"),
            (query_matcher == RuleMatcher.absence, "absence"),
            else_="presence"),
        "polymorphic_identity":"query_match_type"
    }

    @property
    def comparator(self):
        raise NotImplementedError('Looking for comparator on base class is invalid')

    def get_filter_details(self, field):
        # given the properties of query matcher (exact, hierarchical, substring, absence or presence)
        # and temporal association details, this function will produce the required filter
        # for selecting the target cohort
        raise NotImplementedError('Filter details undefined on base class')

class Exact_Query_Rule(Query_Rule):
    
    __tablename__ = 'exact_query_rule'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('query_rule.id'), primary_key=True)
        
    @property
    def comparator(self):
        # for exact match we are comparing against the actual concept_id
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return self.query_concept_id

    def get_filter_details(self, field):
        return field.__eq__(self.comparator)

    __mapper_args__ = {
        "polymorphic_identity": "exact",
        'inherit_condition': (id == Query_Rule.id)
    }

class Hierarchy_Query_Rule(Query_Rule):
    
    __tablename__ = 'hierarchy_query_rule'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('query_rule.id'), primary_key=True)
        
    @property
    def comparator(self):
        # for hierarchical match we are comparing against the actual concept_id, plus the set of child concept_ids
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return [c.concept_id for c in self.children]

    def get_filter_details(self, field):
        return field.in_(self.comparator)

    __mapper_args__ = {
        "polymorphic_identity": "hierarchy",
        'inherit_condition': (id == Query_Rule.id)
    }


@sa.event.listens_for(Hierarchy_Query_Rule, 'load')
def get_standard_hierarchy(target, context):
    if not target.rule_concept:
        raise RuntimeError(f'Rule concept {target.query_concept_id} not found - unable to load query hierarchy')
    children = context.session.query(Concept_Ancestor
                                    ).options(so.joinedload(Concept_Ancestor.descendant)
                                    ).filter(Concept_Ancestor.ancestor_concept_id == target.query_concept_id).distinct().all()
    target.children = [c.descendant for c in children]

class Substring_Query_Rule(Query_Rule):
    __tablename__ = 'substring_query_rule'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('query_rule.id'), primary_key=True)
    
    @property
    def comparator(self):
        # for substring match we are comparing against the concept code
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return self.rule_concept.concept_code

    def get_filter_details(self, field):
        return field.ilike(f'%{self.comparator}%')

    __mapper_args__ = {
        "polymorphic_identity": "substring",
        'inherit_condition': (id == Query_Rule.id)
    }

class Absence_Query_Rule(Query_Rule):
    __tablename__ = 'absence_query_rule'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('query_rule.id'), primary_key=True)
    
    __mapper_args__ = {
        "polymorphic_identity": "absence",
        'inherit_condition': (id == Query_Rule.id)
    }

    def get_filter_details(self, field):
        return field.is_(None)

class Presence_Query_Rule(Query_Rule):
    __tablename__ = 'presence_query_rule'
    id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('query_rule.id'), primary_key=True)
    
    __mapper_args__ = {
        "polymorphic_identity": "presence",
        'inherit_condition': (id == Query_Rule.id)
    }

    def get_filter_details(self, field):
        return field.is_not(None)

class Indicator(Base):
    __tablename__ = 'indicator'
    id: so.Mapped[int] = so.mapped_column(primary_key=True)
    indicator_description: so.Mapped[str] = so.mapped_column(sa.String(250))
    indicator_reference: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    numerator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.id'))
    numerator_label: so.Mapped[str] = so.mapped_column(sa.String[50])
    denominator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.id'))
    denominator_label: so.Mapped[str] = so.mapped_column(sa.String[50])
    temporal_early: so.Mapped[Optional[int]] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)
    temporal_late: so.Mapped[Optional[int]] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)
    temporal_min: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
    temporal_min_units: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)
    temporal_max: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
    temporal_max_units: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)
    benchmark: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
    benchmark_unit: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)

    numerator_measure: so.Mapped['Measure'] = so.relationship(foreign_keys=[numerator_measure_id])
    denominator_measure: so.Mapped['Measure'] = so.relationship(foreign_keys=[denominator_measure_id])
    reports: so.Mapped[List['Report']] = so.relationship(secondary=report_indicator_map,back_populates="indicators")
