from datetime import datetime, date
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence
from sqlalchemy import Enum
import enum, uuid

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

    def combiner_options(self):
        return {1: sa.or_, 2: sa.and_, 3: sa.not_}

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

class ReportStatus(enum.Enum):
    st_current = 1
    st_draft = 2
    st_historical = 3



# report_indicator_map =  sa.Table(
#     'report_indicator_map', 
#     Base.metadata,
#     sa.Column('report_id', sa.ForeignKey('report.id')),
#     sa.Column('indicator', sa.ForeignKey('indicator.id'))
# )

query_rule_map =  sa.Table(
    'query_rule_map', 
    Base.metadata,
    sa.Column('subquery_id', sa.ForeignKey('subquery.subquery_id')),
    sa.Column('query_rule_id', sa.ForeignKey('query_rule.query_rule_id'))
)


"""Association table for n-m mapping between dash_cohort and dash_cohort_def"""
dash_cohort_def_map = sa.Table(
    'dash_cohort_def_map', 
    Base.metadata,
    sa.Column('dash_cohort_def_id', sa.ForeignKey('dash_cohort_def.dash_cohort_def_id')),
    sa.Column('dash_cohort_id', sa.ForeignKey('dash_cohort.dash_cohort_id'))
)


"""Association table for n-m mapping between dash_cohort_def and included measures"""
dash_cohort_measure_map = sa.Table(
    'dash_cohort_measure_map', 
    Base.metadata,
    sa.Column('dash_cohort_def_id', sa.ForeignKey('dash_cohort_def.dash_cohort_def_id')),
    sa.Column('measure_id', sa.ForeignKey('measure.measure_id'))
)

class Report(Base):
    """Primary report class that is used to hold the full report definition. 
    
    Report definition is made up of a combination of:

        1. one or more report cohort(s)
        2. included report indicators
        3. included report metrics
    """
    __tablename__ = 'report'
    report_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    report_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_description: so.Mapped[str] = so.mapped_column(sa.String(1000))
    report_create_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_edit_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_author: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_owner: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)

    cohorts: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='report')
#    indicators: so.Mapped[List['Indicator']] = so.relationship(secondary=report_indicator_map, back_populates='reports')
    report_versions: so.Mapped[List["Report_Version"]] = so.relationship("Report_Version")

    # def __init__(self, 
    #              *args, 
    #              **kwargs):
    #     super().__init__(*args, **kwargs)

    @property
    def version_string(self):
        if self.report_version:
            return f'{self.report_version.report_version_major}.{self.report_version.report_version_minor} ({self.report_version.report_version_label})'

class Report_Version(Base):
    """Report versioning table. 

    There should be only one 'current' report version for each report.
    """
    __tablename__ = 'report_version'
    report_version_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'), primary_key=True)
    report_version_major: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_minor: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_version_label: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_version_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report_status: so.Mapped[int] = so.mapped_column(sa.Enum(ReportStatus)) # st_current, st_draft, st_historical

    report: so.Mapped["Report"] = so.relationship(foreign_keys=[report_id], back_populates='report_versions')

class Report_Cohort_Map(Base):
    """Class that is used to map between cohorts and reports. 
    
    A 'primary' cohort will be used as the default landing filter. 
    
    There may be more than one cohort that is considered 'primary' for a given report. 
    
    All non-primary cohorts will be offered as filter / select options.

    For example, lung MDT report will have patients with primary lung cancer as the primary cohort, and mesothelioma patients as a non-primary option.
    """
    __tablename__ = 'report_cohort_map' 
    
    report_cohort_map_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'), primary_key=True)
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'), primary_key=True)

    primary_cohort: so.Mapped[bool] = so.mapped_column(sa.Boolean)
    cohort: so.Mapped['Dash_Cohort'] = so.relationship(back_populates='in_reports')
    report: so.Mapped['Report'] = so.relationship(back_populates='cohorts')

class Dash_Cohort(Base):
    """Top-level class for dash cohorts. 
    
    These should be conceptually congruant so that they are useful for end-user filtering actions.

    e.g. 'Primary head and neck cancer' as a dash cohort could be made up of dash cohort definitions specific to 
    oropharynx, nasopharynx etc.

    It is theoretically possible to define primary head and neck cancer as a single dash cohort definition, but 
    then it is less user friendly for filtering, and it should also be possible to re-use cohort definitions in 
    different cohorts as well, e.g. 
    Cohort=Colorectal -> Cohort Def = Colon, Rectum, etc.
    Cohort=GI Cancer -> Cohort Def = Stomach, Colon, etc...

    For this reason, the Dash_Cohort class does not add any additional functionality beyond what is already 
    configured in the Dash_Cohort_Def class
    """
    __tablename__ = 'dash_cohort'
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    dash_cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    
    in_reports: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='cohort')
    definitions: so.Mapped[List['Measure']] = so.relationship(secondary=dash_cohort_measure_map, 
                                                              back_populates="dash_cohort_objects")
    # def full_cohort_def(self):
    #     return self.dash_cohort_combination.combiner()(*[m.cohort_definition() for m in self.definitions])                                                                    

class Dash_Cohort_Def(Base):
    """Conceptually-useful filtering units for end users.

    Combines measures into cohorts available for report configuration.
    """
    __tablename__ = 'dash_cohort_def'
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    dash_cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_def_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    dash_cohort_def_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # rule_and, rule_or, rule_except
    
    dash_cohort_objects: so.Mapped[List['Dash_Cohort']] = so.relationship(secondary=dash_cohort_def_map, 
                                                                          back_populates="in_dash_cohort")
    dash_cohort_measures: so.Mapped[List['Measure']] = so.relationship(secondary=dash_cohort_measure_map, 
                                                                       back_populates="definitions")

    def cohort_definition(self):
        return self.dash_cohort_measure_combination.combiner()(*[m.measure_definition() for m in self.dash_cohort_measures])

class Measure(Base):
    """Measure class can combine child measures using boolean logic to an arbitrary depth in order to build complex definitions.

    A measure that contains a subquery should be the root measure definition and therefore not contain any child measures of its own.

    An example measure may have the sub-queries _Lung Cancer_ **and** _Stage IV_, to select all patients with Stage IV lung cancer, 
    or it could be broken down further with sub-query _Lung Cancer_ **and** a child measure representing the combination (_Stage I_
    **or** _Stage 2_).

    Note that there may end up being some duplicate measures, as each measure must have only a single parent - enforce a 1-n mapping
    for simplicity.
    """
    __tablename__ = 'measure'
    measure_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    measure_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # rule_and, rule_or, rule_except
    parent_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.measure_id'), nullable=True) 
    subquery_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), nullable=True)

    in_dash_cohort: so.Mapped[List['Dash_Cohort_Def']] = so.relationship(secondary=dash_cohort_measure_map, 
                                                                         back_populates="dash_cohort_measures")

    parent_measure: so.Mapped["Measure"] = so.relationship("Measure", 
                                                           remote_side=measure_id, 
                                                           foreign_keys=parent_measure_id,
                                                           back_populates="child_measures")

    child_measures: so.Mapped[List["Measure"]] = so.relationship("Measure", back_populates="parent_measure")
    subquery: so.Mapped["Subquery"] = so.relationship("Subquery", foreign_keys=[subquery_id], back_populates='measures')

    # def measure_definition(self):
    #     return self.measure_combination.combiner()(*[m.get_filter() for m in self.measure_defs])



class Subquery(Base):
    """ Subqueries correspond to specific OHDSI fields within the CDM in which to look for presence or absence of  target concepts. 
        
        Subqueries are polymorphic on their domain (diagnosis, treatment, procedure, measurement, person or observation), which defines
        the data model fields on which queries will be run, as well as which fields meet the definition for temporal inclusion.
    """
    __tablename__ = 'subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    subquery_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    subquery_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    subquery_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType)) # dx_rule, tx_rule, obs_rule, proc_rule
    subquery_temporality: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTemporality),       # dt_current_start, dt_death, dt_treatment_start, dt_any
                                                            default=RuleTemporality.dt_any) # dt_obs, dt_proc_start, dt_numerator, dt_denominator                                                                                                                        
    subquery_target: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTarget)) # dx_primary, dx_any, dx_stage, tx_current_ep, tx_any, tx_chemo, tx_radio, tx_surgery,
                                                                            # demog_gender, demog_death, obs_value, obs_concept, proc_concept, meas_concept, meas_value

    measures: so.Mapped[List['Measure']] = so.relationship("Measure", back_populates='subquery')
    query_rules: so.Mapped[List['Query_Rule']] = so.relationship(secondary=query_rule_map, back_populates="subqueries")

    @property
    def filter_field(self):
        return self.subquery_target.target(self.subquery_matcher == RuleMatcher.substring)

    @property
    def filter_table(self):
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
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def get_filter(self):
        # todo: check allowed combinations of target and type
        # todo: move field from condition_occurrence concept to primary dx ep concept
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "diagnostic",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Tx_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in treatment episodes
    __tablename__ = 'tx_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)
    
    def get_filter(self):
        raise NotImplemented()

    __mapper_args__ = {
        "polymorphic_identity": "treatment",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Obs_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in the observation domain
    __tablename__ = 'obs_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)
    
    def get_filter(self):
        raise NotImplemented()
        # field = self.subquery_target.target(self.query_rules[0].query_matcher == RuleMatcher.substring)
        # return self.subquery_combination.combiner()(*[sq.get_filter_details() for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "observation",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Proc_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in the procedure domain
    __tablename__ = 'proc_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "procedure",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Person_Subquery(Subquery):
    # filters cohort based on demographic criteria
    __tablename__ = 'person_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])
    
    __mapper_args__ = {
        "polymorphic_identity": "person",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Query_Rule(Base):
    """ Query_Rules correspond to specific OHDSI concepts that are the target of particular definitions.

        Query_Rules can require exact match to the concept id, a substring match to concept code (typically 
        diagnostic categories such as C34.*), or match all concept ids in a specific hierarchy.
    """
    __tablename__ = 'query_rule'
    query_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    query_matcher: so.Mapped[int] = so.mapped_column(sa.Enum(RuleMatcher)) # substring, exact, hierarchy, presence, absence
    query_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), default=0)
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
        "polymorphic_identity":"query_rule"
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
        
    @property
    def comparator(self):
        # for exact match we are comparing against the actual concept_id
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return self.query_concept_id

    def get_filter_details(self, field):
        return field.__eq__(self.comparator)

    __mapper_args__ = { "polymorphic_identity": "exact" }

class Hierarchy_Query_Rule(Query_Rule):

    @property
    def comparator(self):
        # for hierarchical match we are comparing against the actual concept_id, plus the set of child concept_ids
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return [c.concept_id for c in self.children]

    def get_filter_details(self, field):
        return field.in_(self.comparator)

    __mapper_args__ = { "polymorphic_identity": "hierarchy" }


@sa.event.listens_for(Hierarchy_Query_Rule, 'load')
def get_standard_hierarchy(target, context):
    if not target.rule_concept:
        raise RuntimeError(f'Rule concept {target.query_concept_id} not found - unable to load query hierarchy')
    children = context.session.query(Concept_Ancestor
                                    ).options(so.joinedload(Concept_Ancestor.descendant)
                                    ).filter(Concept_Ancestor.ancestor_concept_id == target.query_concept_id).distinct().all()
    target.children = [c.descendant for c in children]
class Substring_Query_Rule(Query_Rule):
    
    @property
    def comparator(self):
        # for substring match we are comparing against the concept code
        if not self.rule_concept:
            raise RuntimeError(f'Rule concept {self.query_concept_id} not found')
        return self.rule_concept.concept_code

    def get_filter_details(self, field):
        return field.ilike(f'%{self.comparator}%')

    __mapper_args__ = { "polymorphic_identity": "substring" }

class Absence_Query_Rule(Query_Rule):

    __mapper_args__ = { "polymorphic_identity": "absence" }

    def get_filter_details(self, field):
        return field.is_(None)

class Presence_Query_Rule(Query_Rule):
    
    __mapper_args__ = { "polymorphic_identity": "presence" }

    def get_filter_details(self, field):
        return field.is_not(None)

# class Indicator(Base):
#     __tablename__ = 'indicator'
#     id: so.Mapped[int] = so.mapped_column(primary_key=True)
#     indicator_description: so.Mapped[str] = so.mapped_column(sa.String(250))
#     indicator_reference: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
#     numerator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.id'))
#     numerator_label: so.Mapped[str] = so.mapped_column(sa.String[50])
#     denominator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.id'))
#     denominator_label: so.Mapped[str] = so.mapped_column(sa.String[50])
#     temporal_early: so.Mapped[Optional[int]] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)
#     temporal_late: so.Mapped[Optional[int]] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)
#     temporal_min: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
#     temporal_min_units: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)
#     temporal_max: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
#     temporal_max_units: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)
#     benchmark: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer(), nullable=True)
#     benchmark_unit: so.Mapped[Optional[int]] = so.mapped_column(sa.String(20), nullable=True)

#     numerator_measure: so.Mapped['Measure'] = so.relationship(foreign_keys=[numerator_measure_id])
#     denominator_measure: so.Mapped['Measure'] = so.relationship(foreign_keys=[denominator_measure_id])
#     reports: so.Mapped[List['Report']] = so.relationship(secondary=report_indicator_map,back_populates="indicators")
