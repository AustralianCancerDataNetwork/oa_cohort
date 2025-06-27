from datetime import datetime, date, timedelta
from typing import Optional, List
import sqlalchemy as sa
import sqlalchemy.orm as so
from omop_alchemy.db import Base
from omop_alchemy.model.vocabulary import Concept, Concept_Ancestor
from omop_alchemy.model.clinical import Condition_Occurrence, Person, Observation, Procedure_Occurrence, Measurement
from omop_alchemy.conventions.constructs import Condition_Episode, Historical_Surgical_Procedure, Dated_Surgical_Procedure, Dx_Treat_Start, Dx_RT_Start, Dx_SACT_Start, Dx_Surg, Treatment_Window, Dx_Concurrent_Start, Treatment_Consult_Window
from sqlalchemy import Enum
import enum, uuid
from itertools import chain

from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
import logging, shutil

logger = logging.getLogger(__name__)


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
    # todo add a rule target for surgical history vs. surgical procedure
    # i.e. - observation.value_as_concept_id = 4301352 for surgical history on lung / procedure_concept_id = [specific procedure code]
    demog_gender = 12
    demog_death = 13
    obs_value = 14
    obs_concept = 15
    proc_concept = 16
    meas_concept = 17
    # todo consider splitting windows and other scalars into their own class?
    tx_to_death_window = 18
    dx_to_tx_window = 19
    referral_to_tx_window = 20
    referral_to_specialist_window = 21
    tx_concurrent = 22

    def table_selectables(self):
        # using explicit labels even where not strictly necessary for convenient handling 
        return {1: (Condition_Episode.person_id.label('person_id'), Condition_Episode.episode_id.label('episode_id'), Condition_Episode.episode_id.label('measure_resolver')), 
                2: (Condition_Episode.person_id.label('person_id'), Condition_Episode.episode_id.label('episode_id'), Condition_Episode.episode_id.label('measure_resolver')), 
                3: (Condition_Episode.person_id.label('person_id'), Condition_Episode.episode_id.label('episode_id'), Condition_Episode.episode_id.label('measure_resolver')), 
                4: (Condition_Episode.person_id.label('person_id'), Condition_Episode.episode_id.label('episode_id'), Condition_Episode.episode_id.label('measure_resolver')), 
                5: (Condition_Episode.person_id.label('person_id'), Condition_Episode.episode_id.label('episode_id'), Condition_Episode.episode_id.label('measure_resolver')),
                8: (Dx_Treat_Start.person_id.label('person_id'), Dx_Treat_Start.dx_id.label('episode_id'), Dx_Treat_Start.dx_id.label('measure_resolver')),
                9: (Dx_SACT_Start.person_id.label('person_id'), Dx_SACT_Start.dx_id.label('episode_id'), Dx_SACT_Start.dx_id.label('measure_resolver')),
                10: (Dx_RT_Start.person_id.label('person_id'), Dx_RT_Start.dx_id.label('episode_id'), Dx_RT_Start.dx_id.label('measure_resolver')),
                11: (Dx_Surg.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Dx_Surg.person_id.label('measure_resolver')),
                #11: (Dated_Surgical_Procedure.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Dated_Surgical_Procedure.person_id.label('measure_resolver')),
                12: (Person.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Person.person_id.label('measure_resolver')),
                13: (Person.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Person.person_id.label('measure_resolver')),
                14: (Observation.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Observation.person_id.label('measure_resolver')),
                15: (Observation.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Observation.person_id.label('measure_resolver')),
                16: (Procedure_Occurrence.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Procedure_Occurrence.person_id.label('measure_resolver')),
                17: (Measurement.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Measurement.person_id.label('measure_resolver')),
                18: (Treatment_Window.person_id.label('person_id'), Treatment_Window.episode_id.label('episode_id'), Treatment_Window.person_id.label('measure_resolver')),
                21: (Treatment_Consult_Window.person_id.label('person_id'), sa.sql.expression.literal_column('0').label('episode_id'), Treatment_Consult_Window.person_id.label('measure_resolver')),
                22: (Dx_Concurrent_Start.person_id.label('person_id'), Dx_Concurrent_Start.dx_id.label('episode_id'), Dx_Concurrent_Start.dx_id.label('measure_resolver'))}[self.value]

    def target_table(self, ep_override=False):
        target_cols = self.table_selectables()
        if ep_override:
            # for targets that should be resolved by episode but have not been inserted within the requisite hierarchy, override the target and 
            # default to person_id as the measure resolver
            return (target_cols[0], sa.sql.expression.literal_column('0').label('episode_id'), target_cols[0].label('measure_resolver')) 
        return target_cols


    def target_options(self):
        return {1: Condition_Episode.condition_concept_id, 
                2: Condition_Episode.condition_concept_id, 
                3: Condition_Episode.condition_concept_id, 
                4: Condition_Episode.condition_concept_id, 
                5: Condition_Episode.modifier_concepts,
                8: Dx_Treat_Start.dx_id, #Systemic_Therapy_Episode.episode_id,
                9: Dx_SACT_Start.dx_id, #Systemic_Therapy_Episode.episode_id, #Chemo_Episode.episode_id,
                10: Dx_RT_Start.dx_id, #Radiation_Therapy_Episode.episode_id, #Chemo_Episode.episode_id,
                11: Dx_Surg.surgery_concept_id,
                #11: Procedure_Occurrence.procedure_concept_id,
                12: Person.gender_concept_id,
                13: Person.death_datetime,
                14: Observation.value_as_concept_id,
                15: Observation.observation_concept_id,
                16: Procedure_Occurrence.procedure_concept_id,
                17: Measurement.measurement_concept_id,
                18: Treatment_Window.treatment_days_before_death,
                21: Treatment_Consult_Window.initial_gp_referral,
                22: Dx_Concurrent_Start.dx_id}

    def string_target_options(self):
        return {1: Condition_Occurrence.condition_code, 
                2: Condition_Occurrence.condition_code, 
                3: Condition_Occurrence.condition_code, 
                4: Condition_Occurrence.condition_code, 
                5: Condition_Occurrence.condition_code,
                14: Observation.value_as_concept_id,
                15: Observation.observation_concept_id,
                16: Procedure_Occurrence.procedure_concept_id}
               # 17: Measurement.measurement_concept_code}

    def target(self, str_match=False):
        if str_match:
            return self.string_target_options()[self.value]
        return self.target_options()[self.value]

class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3

    @property
    def label(self):
        return {
            1: 'OR',
            2: 'AND',
            3: 'EXCEPT'
        }[self.value]

    def combiner_options(self):
        return {1: sa.union_all, 2: sa.intersect_all, 3: sa.except_all}

    def combiner(self):
        return self.combiner_options()[self.value]

class RuleType(enum.Enum):
    dx_rule = 1
    tx_rule = 2
    obs_rule = 3
    person_rule = 4
    proc_rule = 5
    meas_rule = 6

class ThresholdDirection(enum.Enum):
    gt = 1
    lt = 2
    
class RuleMatcher(enum.Enum):
    substring = 1
    exact = 2
    hierarchy = 3
    absence = 4
    presence = 5
    hierarchyexclusion = 6
    scalar = 7

class RuleTemporality(enum.Enum):
    dt_current_start = 1
    dt_death = 2
    dt_treatment_start = 3
    dt_obs = 4
    dt_proc_start = 5
    dt_numerator = 6 # change this 
    dt_denominator = 7 # change this 
    dt_any = 8
    dt_meas = 9
    dt_rad = 10
    dt_surg = 11
    dt_treat = 12 # this needs to be renamed for sact
    dt_treatment_end = 13
    dt_concurrent = 14
    dt_consult = 15
    
    def target_date_field(self):
        return {1: Condition_Occurrence.condition_start_date,
                2: Person.death_datetime,
                3: Dx_Treat_Start.treatment_start, 
                4: Observation.observation_date,
                5: Procedure_Occurrence.procedure_date,
                6: Dx_RT_Start.rt_start, 
                7: Dated_Surgical_Procedure.procedure_datetime,
                # this needs to be extended to allow for historical treatments
                8: Historical_Surgical_Procedure.history_datettime,
                9: Measurement.measurement_date,
                # coalesce function required here for dates of dx to propagate where query_matcher = absence
                10: sa.func.coalesce(Dx_RT_Start.rt_start, Dx_RT_Start.dx_date),
                11: sa.func.coalesce(Dx_Surg.surg_date, Dx_Surg.dx_date), 
                12: sa.func.coalesce(Dx_SACT_Start.sact_start, Dx_SACT_Start.dx_date),
                13: Treatment_Window.latest_treatment,
                14: sa.func.coalesce(Dx_Concurrent_Start.treatment_start, Dx_Concurrent_Start.dx_date),
                15: Treatment_Consult_Window.initial_gp_referral}[self.value]

class ReportStatus(enum.Enum):
    st_current = 1
    st_draft = 2
    st_historical = 3


report_indicator_map =  sa.Table(
    'report_indicator_map', 
    Base.metadata,
    sa.Column('report_id', sa.ForeignKey('report.report_id')),
    sa.Column('indicator_id', sa.ForeignKey('indicator.indicator_id'))
)

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


class Report(Base):
    """Primary report class that is used to hold the full report definition. 
    
    Report definition is made up of a combination of:

        1. one or more report cohort(s)
        2. included report indicators
        3. included report metrics
    """
    __tablename__ = 'report'
    report_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('report_id')

    report_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_description: so.Mapped[str] = so.mapped_column(sa.String(1000))
    report_create_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_edit_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_author: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_owner: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)

    cohorts: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='report')
    indicators: so.Mapped[List['Indicator']] = so.relationship(secondary=report_indicator_map, 
                                                               back_populates='in_reports')
    report_versions: so.Mapped[List["Report_Version"]] = so.relationship("Report_Version")

    denominator_measures: AssociationProxy[List["Measure"]] = association_proxy("indicators", "denominator_measure")
    numerator_measures: AssociationProxy[List["Measure"]] = association_proxy("indicators", "numerator_measure")

    @property
    def report_cohorts(self): 
        return [c.cohort for c in self.cohorts]

    @property
    def indicator_measures(self):
        return list(set(self.numerator_measures + self.denominator_measures))

    @property
    def cohort_measures(self):
        return list(set(chain.from_iterable([c.measures for c in self.report_cohorts])))

    @property
    def report_measures(self):
        return sorted(set(self.numerator_measures + self.denominator_measures + self.cohort_measures), key=lambda x: x.id)

    @property
    def members(self):
        return list(set(chain.from_iterable([c.members for c in self.report_cohorts])))

    # use hybrid properties judiciously as they force eager loads when working with related objects like this, 
    # but they are required for any calculated fields that you want to use
    # directly in the fastapi serialisation steps - if these fields do not require joins then you should use
    # pydantic calculated fields directly. 
    @sa.ext.hybrid.hybrid_property
    def version_string(self):
        if self.report_versions:
            return ';'.join([f'{rv.report_version_major}.{rv.report_version_minor} ({rv.report_version_label})' for rv in self.report_versions])



class Report_Version(Base):
    """Report versioning table. 

    There should be only one 'current' report version for each report.
    """
    __tablename__ = 'report_version'
    report_version_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    id = so.synonym('report_version_id')
    
    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'))
    report_version_major: so.Mapped[int] = so.mapped_column(sa.Integer)
    report_version_minor: so.Mapped[int] = so.mapped_column(sa.Integer)
    report_version_label: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_version_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report_status: so.Mapped[int] = so.mapped_column(sa.Enum(ReportStatus)) # st_current, st_draft, st_historical

    report: so.Mapped["Report"] = so.relationship(foreign_keys=[report_id], back_populates='report_versions')


class Indicator(Base):
    __tablename__ = 'indicator'
    indicator_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('indicator_id')

    indicator_description: so.Mapped[str] = so.mapped_column(sa.String(250))
    indicator_reference: so.Mapped[Optional[str]] = so.mapped_column(sa.String(50), nullable=True)
    numerator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    numerator_label: so.Mapped[str] = so.mapped_column(sa.String[50])
    denominator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
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
    in_reports: so.Mapped[List['Report']] = so.relationship(secondary=report_indicator_map,
                                                            back_populates="indicators")


class Report_Cohort_Map(Base):
    """Class that is used to map between cohorts and reports. 
    
    A 'primary' cohort will be used as the default landing filter. 
    
    There may be more than one cohort that is considered 'primary' for a given report. 
    
    All non-primary cohorts will be offered as filter / select options.

    For example, lung MDT report will have patients with primary lung cancer as the primary cohort, and mesothelioma patients as a non-primary option.
    """
    __tablename__ = 'report_cohort_map' 
    
    report_cohort_map_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    id = so.synonym('report_cohort_map_id')

    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'), primary_key=True)
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'), primary_key=True)
    primary_cohort: so.Mapped[bool] = so.mapped_column(sa.Boolean)

    cohort: so.Mapped['Dash_Cohort'] = so.relationship(back_populates='in_reports')
    report: so.Mapped['Report'] = so.relationship(back_populates='cohorts')

    measures: AssociationProxy[List["Measure"]] = association_proxy("cohort", "measures")
    definition_count: AssociationProxy[int] = association_proxy("cohort", "definition_count")


    # @property
    # def measures(self):
    #     if self.cohort:
    #         return [d.dash_cohort_measure for d in self.cohort.definitions]

    # @property
    # def definition_count(self):
    #     if self.cohort:
    #         return self.cohort.definition_count
    #     return 0

    @property
    def measure_count(self):
        if self.cohort:
            return sum([d.measure_count for d in self.cohort.definitions])
        return 0

class Dash_Cohort(Base):
    """Top-level class for dash cohorts. 
    
    These should be conceptually congruant so that they are useful for end-user filtering actions.

    e.g. 'Primary head and neck cancer' as a dash cohort could be made up of dash cohort definitions specific to 
    oropharynx, nasopharynx etc.

    It is theoretically possible to define primary head and neck cancer as a single dash cohort definition, but 
    then it is less user friendly for filtering, and it should also be possible to re-use cohort definitions in 
    different cohorts as well, e.g. 
    * Cohort=Colorectal -> Cohort Def = Colon, Rectum, etc.
    * Cohort=GI Cancer -> Cohort Def = Stomach, Colon, etc...

    For this reason, the Dash_Cohort class does not add any additional functionality beyond what is already 
    configured in the Dash_Cohort_Def class
    """
    __tablename__ = 'dash_cohort'
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    id = so.synonym('dash_cohort_id')

    dash_cohort_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    
    in_reports: so.Mapped[List['Report_Cohort_Map']] = so.relationship(back_populates='cohort')
    definitions: so.Mapped[List['Dash_Cohort_Def']] = so.relationship(secondary=dash_cohort_def_map, 
                                                                      back_populates="dash_cohort_objects")

    measures: AssociationProxy[List["Measure"]] = association_proxy("definitions", "dash_cohort_measure")
    measure_ids: AssociationProxy[List[int]] = association_proxy("definitions", "measure_id")
                                                                    
    @property
    def cohort_def_labels(self):
        return [(self.dash_cohort_name, d.dash_cohort_def_name, d.measure_id) for d in self.definitions]

    @property
    def members(self):
        return list(set(chain.from_iterable([d.members for d in self.definitions])))

    @property
    def definition_count(self):
        return len(self.definitions)

    @property
    def measure_count(self):
        return len(self.measures)
        
class Dash_Cohort_Def(Base):
    """Conceptually-useful filtering units for end users.

    Maps single measure into cohorts available for report configuration.
    """
    __tablename__ = 'dash_cohort_def'
    dash_cohort_def_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('dash_cohort_def_id')

    dash_cohort_def_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    dash_cohort_def_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    
    dash_cohort_objects: so.Mapped[List['Dash_Cohort']] = so.relationship(secondary=dash_cohort_def_map, 
                                                                          back_populates="definitions")

    dash_cohort_measure: so.Mapped['Measure'] = so.relationship("Measure", foreign_keys=[measure_id], back_populates='in_dash_cohort')


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_on_load()

    @property
    def members(self):
        return self._members

    @so.reconstructor
    def init_on_load(self):
        self._members = []

    def execute_cohort(self, db):
        self._members = self.dash_cohort_measure.execute_measure(db)
        # query = self.get_cohort()
        # self._members = db.execute(sa.Select(query.c).distinct()).all()

    def get_cohort(self):
        return self.dash_cohort_measure.get_measure()

    def cohort_definition(self):
        return self.dash_cohort_measure.measure_definition()

    @property
    def measure_count(self):
        if self.dash_cohort_measure:
            return self.dash_cohort_measure.measure_count
        return 0

        

class Measure(Base):
    """Measure class can combine child measures using boolean logic to an arbitrary depth in order to build complex definitions.

    A measure that contains a subquery should be the root measure definition and therefore not contain any child measures of its own.

    An example measure may have the sub-queries _Lung Cancer_ **and** _Stage IV_, to select all patients with Stage IV lung cancer, 
    or it could be broken down further with sub-query _Lung Cancer_ **and** a child measure representing the combination (_Stage I_
    **or** _Stage 2_).
    """
    __tablename__ = 'measure'
    measure_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('measure_id')

    measure_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    measure_combination: so.Mapped[int] = so.mapped_column(sa.Enum(RuleCombination)) # rule_and, rule_or, rule_except
    subquery_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), nullable=True)
    person_ep_override: so.Mapped[bool] = so.mapped_column(sa.Boolean)
    
    subquery: so.Mapped["Subquery"] = so.relationship("Subquery", foreign_keys=[subquery_id], back_populates='measures')
    in_dash_cohort: so.Mapped[List['Dash_Cohort_Def']] = so.relationship("Dash_Cohort_Def", back_populates="dash_cohort_measure")

    child_measures: so.Mapped[List["Measure_Relationship"]] = so.relationship("Measure_Relationship", 
                                                                              foreign_keys="Measure_Relationship.parent_measure_id", 
                                                                              viewonly=True)
    parent_measures: so.Mapped[List["Measure_Relationship"]] = so.relationship("Measure_Relationship", 
                                                                               foreign_keys="Measure_Relationship.child_measure_id", 
                                                                               viewonly=True)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_on_load()

    @so.reconstructor
    def init_on_load(self):
        self._members = []

    def get_measure(self, ep_override=False):
        ep_override = self.person_ep_override or ep_override
        if self.subquery:
            return self.subquery.get_subquery_any(ep_override)
        elif self.measure_combination==RuleCombination.rule_or:
            return sa.union_all(*[m.get_measure(ep_override) for m in self.children])
        else:
            return self.get_measure_first_qualifying(ep_override)
        #return self.measure_combination.combiner()(*[m.get_measure(ep_override) for m in self.children])

    def get_measure_any(self, ep_override=False):
        ep_override = self.person_ep_override or ep_override
        if self.subquery:
            return self.subquery.get_subquery_any(ep_override)
        else:
            return sa.union(*[m.get_measure_any(ep_override) for m in self.children])

    def get_measure_earliest(self, ep_override=False):
        m = self.get_measure(ep_override).subquery()
        earliest = (
            sa.select(
                m.c.person_id, 
                m.c.episode_id, 
                m.c.measure_resolver, 
                sa.func.min(m.c.measure_date).label('measure_date')
            )
            .group_by(m.c.person_id, m.c.episode_id, m.c.measure_resolver)
        )
        return earliest
        
    def get_measure_first_qualifying(self, ep_override=False):
        # as per subquery_all - this is required to get the first qualifying date for this subquery
        if self.subquery:
            return self.subquery.get_subquery_first(ep_override)      

        #earliest_qualifiers = [c.get_measure_first(ep_override).subquery() for c in self.children]
        earliest_qualifiers = [c.get_measure_earliest(ep_override).subquery() for c in self.children]

        start = earliest_qualifiers[0]
        lhs = earliest_qualifiers[0]        
        date_columns = [lhs.c.measure_date]

        for rhs in earliest_qualifiers[1:]:
            lhs = lhs.join(
                        rhs, start.c.measure_resolver==rhs.c.measure_resolver
                  )
            date_columns.append(rhs.c.measure_date)

        combined = sa.select(
            start.c.person_id, start.c.episode_id, start.c.measure_resolver, sa.func.greatest(*date_columns).label('measure_date')
        ).select_from(lhs)

        return combined

    
    def get_measure_date(self):
        if self.subquery:
            return self.subquery.subquery_date

    def execute_measure(self, db, people=[], force_refresh=False):
        # nb this does not currently return distinct rows - todo?
        if not force_refresh and len(self._members) > 0:
            return self._members
        query = self.get_measure()
        if len(people) > 0:
            query = sa.select(query.subquery()).filter(sa.column('person_id').in_(people))
        self._members = db.execute(query).all()
        return self._members
    
    @property
    def members(self):
        return self._members
    
    @property
    def children(self):
        return [c.child for c in self.child_measures]
     
    @property
    def depth(self):
        return 1 + max([c.depth for c in self.children] + [0])

    @property
    def height(self):
        if not self.children:
            return 1
        return 1 + sum([c.height for c in self.children])

    @property
    def measure_count(self):
        return max(sum([c.measure_count for c in self.children]), 1)

    @property
    def child_defs(self, db):
        return [c.full_measure_expression(db) for c in self.children]

    @property
    def measure_defs(self, db):
        if len(self.children) > 0:
            return self.child_defs(db)
        return self.measure_name

class Measure_Relationship(Base):
    """Association object for n-m mapping between parent and child measures.
    
    This can't be achieved via association table alone, despite lack of additional data, due to the self-referential nature of this relationship.
    """
    __tablename__ = 'measure_relationship'
    parent_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True) 
    child_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True) 

    parent: so.Mapped["Measure"] = so.relationship("Measure", foreign_keys=[parent_measure_id], back_populates='child_measures')
    child: so.Mapped["Measure"] = so.relationship("Measure", foreign_keys=[child_measure_id], back_populates='parent_measures')

class Subquery(Base):
    """ Subqueries correspond to specific OHDSI fields within the CDM in which to look for presence or absence of  target concepts. 
        
        Subqueries are polymorphic on their domain (diagnosis, treatment, procedure, measurement, person or observation), which defines
        the data model fields on which queries will be run, as well as which fields meet the definition for temporal inclusion.
    """
    __tablename__ = 'subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('subquery_id')

    subquery_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    subquery_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    subquery_type: so.Mapped[int] = so.mapped_column(sa.Enum(RuleType)) # dx_rule, tx_rule, obs_rule, proc_rule, meas_rule
    subquery_temporality: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTemporality),       # dt_current_start, dt_death, dt_treatment_start, dt_any
                                                            default=RuleTemporality.dt_any) # dt_obs, dt_proc_start, dt_numerator, dt_denominator                                                                                                                        
    subquery_target: so.Mapped[int] = so.mapped_column(sa.Enum(RuleTarget)) # dx_primary, dx_any, dx_stage, tx_current_ep, tx_any, tx_chemo, tx_radio, tx_surgery,
                                                                            # demog_gender, demog_death, obs_value, obs_concept, proc_concept, meas_concept, meas_value

    measures: so.Mapped[List['Measure']] = so.relationship("Measure", back_populates='subquery')
    query_rules: so.Mapped[List['Query_Rule']] = so.relationship(secondary=query_rule_map, back_populates="subqueries")
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ep_override = False

    @property
    def subquery_matcher(self):
        if len(self.query_rules) == 0:
            raise RuntimeError(f'Unable to generate filter for subquery {self.subquery_id} - no valid query rules assigned')
        return self.query_rules[0].query_matcher

    @property
    def filter_field(self):
        return self.subquery_target.target(self.subquery_matcher == RuleMatcher.substring)

    @property
    def filter_table(self):
        return self.subquery_target.target_table(self.ep_override)
        
    @property
    def filter_table_dated(self):
        return (*self.subquery_target.target_table(self.ep_override), self.subquery_temporality.target_date_field().label('measure_date'))

    __mapper_args__ = {
        "polymorphic_on":sa.case(
            (subquery_type == RuleType.dx_rule, "diagnostic"),
            (subquery_type == RuleType.tx_rule, "treatment"),
            (subquery_type == RuleType.obs_rule, "observation"),
            (subquery_type == RuleType.proc_rule, "procedure"),
            (subquery_type == RuleType.meas_rule, "measurement"),
            else_="person"),
        "polymorphic_identity":"subquery_type"
    }

    def get_subquery_any(self, ep_override):
        # call this if the measure combination is OR - for this one you can allow through ANY of the dates that the 
        # person qualifies for the measure --> we are performing a UNION and therefore do not need to do anything 
        # clever to resolve which of the record dates can be used as the basis.
        self.ep_override = ep_override
        if len(self.query_rules) == 0:
            return None
        qr = [sa.select(*self.filter_table_dated).filter(sq.get_filter_details(self.filter_field)) for sq in self.query_rules]
        query = sa.union_all(*qr)
        return query

    def get_subquery_undated(self, ep_override):
        # call this if the measure combination is AND - for this one you need to return the earliest date that the
        # person qualifies for the measure --> we then take the intersection of the undated columns, join with the 
        # first date that each subquery is met, and then take the max of the min dates (i.e. the first date that 
        # ALL conditions are met...)
        self.ep_override = ep_override
        if len(self.query_rules) == 0:
            return None
        # n.b. ALL subqueries must be combined with an OR - if you need to 'AND' concepts together, they must be put into
        # separate measures and combined thusly
        qr = [sa.select(*self.filter_table).filter(sq.get_filter_details(self.filter_field)) for sq in self.query_rules]
        query = sa.union_all(*qr)
        return query

    def get_subquery_first(self, ep_override):
        # as per subquery_all - this is required to get the first qualifying date for this subquery
        sq = self.get_subquery_any(ep_override).subquery()
        dt = sa.select(
                sq.c.person_id, 
                sq.c.episode_id, 
                sq.c.measure_resolver, 
                sa.func.min(sq.c.measure_date).label('measure_date')
             ).group_by(
                sq.c.person_id, 
                sq.c.episode_id,
                sq.c.measure_resolver
             )
        return dt


class Dx_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in diagnostic episodes

    # todo: why is this concrete inheritence? tbc?
    __tablename__ = 'dx_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_filter(self):
        # todo: check allowed combinations of target and type
        # todo: move field from condition_occurrence concept to primary dx ep concept
        #return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])
        return [sq.get_filter_details(self.filter_field) for sq in self.query_rules]

    __mapper_args__ = {
        "polymorphic_identity": "diagnostic",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Tx_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in treatment episodes
    __tablename__ = 'tx_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])

    __mapper_args__ = {
        "polymorphic_identity": "procedure",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Meas_Subquery(Subquery):
    # filters cohort based on presence or absence of stated criteria in the measurement domain
    __tablename__ = 'meas_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    
    def get_filter(self):
        return self.subquery_combination.combiner()(*[sq.get_filter_details(self.filter_field) for sq in self.query_rules])
    
    __mapper_args__ = {
        "polymorphic_identity": "measurement",
        'inherit_condition': (subquery_id == Subquery.subquery_id)
    }

class Person_Subquery(Subquery):
    # filters cohort based on demographic criteria
    __tablename__ = 'person_subquery'
    subquery_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('subquery.subquery_id'), primary_key=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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
    id = so.synonym('query_rule_id')

    query_matcher: so.Mapped[int] = so.mapped_column(sa.Enum(RuleMatcher)) # substring, exact, hierarchy, presence, absence
    query_concept_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('concept.concept_id'), default=0)
    query_notes: so.Mapped[Optional[str]] = so.mapped_column(sa.String(250), nullable=True)        
    
    # these two columns are only used by Scalar_Threshold_Rule
    scalar_threshold: so.Mapped[Optional[int]] = so.mapped_column(sa.Integer, nullable=True)
    threshold_direction: so.Mapped[Optional[str]] = so.mapped_column(sa.Enum(ThresholdDirection), nullable=True)

    # relationships
    rule_concept: so.Mapped['Concept'] = so.relationship(foreign_keys=[query_concept_id])
    subqueries: so.Mapped[List['Subquery']] = so.relationship(secondary=query_rule_map, back_populates="query_rules")

    __mapper_args__ = {
        "polymorphic_on":sa.case(
            (query_matcher == RuleMatcher.exact, "exact"),
            (query_matcher == RuleMatcher.hierarchy, "hierarchy"),
            (query_matcher == RuleMatcher.substring, "substring"),
            (query_matcher == RuleMatcher.absence, "absence"),
            (query_matcher == RuleMatcher.hierarchyexclusion, "hierarchyexclusion"),
            (query_matcher == RuleMatcher.scalar, "scalar"),
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

class Hierarchy_Exclusion_Rule(Hierarchy_Query_Rule):

    def get_filter_details(self, field):
        return field.not_in(self.comparator)

    __mapper_args__ = { "polymorphic_identity": "hierarchyexclusion" }


@sa.event.listens_for(Hierarchy_Query_Rule, 'load')
@sa.event.listens_for(Hierarchy_Exclusion_Rule, 'load')
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

class Scalar_Threshold_Rule(Query_Rule):
    """Handles scalar threshold comparisons (greater than or less than a stored integer value)."""

    @property
    def comparator(self):
        if self.scalar_threshold is None:
            raise RuntimeError(f'Scalar threshold is not set on rule {self.query_rule_id}')
        return self.scalar_threshold

    def get_filter_details(self, field):
        if self.threshold_direction == ThresholdDirection.gt:
            return field > self.comparator
        elif self.threshold_direction == ThresholdDirection.lt:
            return field < self.comparator
        else:
            raise ValueError(f'Unknown threshold direction: {self.threshold_direction}')

    __mapper_args__ = {"polymorphic_identity": "scalar"}

