from omop_constructs.alchemy.episodes import SurgicalProcedureMV, DxTreatStartMV, ConditionTreatmentEpisode
from orm_loader.helpers import Base, get_logger
from .measurable_base import MeasurableSpec, MeasurableBase, MeasurableDomain

logger = get_logger(__name__)

class SurgicalMeasurable(SurgicalProcedureMV, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Cancer Surgical Procedures",
        person_id_attr="person_id",
        episode_id_attr="condition_episode_id",
        event_date_attr="surgery_datetime",
        value_concept_attr="surgery_concept_id",
        value_string_attr="surgery_name"
    )

class AllCurrentTreatmentMeasurable(DxTreatStartMV, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Current Condition Treatment",
        person_id_attr="person_id",
        episode_id_attr="dx_episode_id",
        event_date_attr="treatment_start",
        value_concept_attr="treatment_regimen_count",
    )

class ChemoTreatmentMeasurable(ConditionTreatmentEpisode, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Chemotherapy Treatment Episodes",
        person_id_attr="person_id",
        episode_id_attr="condition_episode_id",
        event_date_attr="regimen_start_date",
        value_concept_attr="regimen_number",   
        value_string_attr="regimen_concept"
    )

class RTTreatmentMeasurable(ConditionTreatmentEpisode, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Radiotherapy Treatment Episodes",
        person_id_attr="person_id",
        episode_id_attr="condition_episode_id",
        event_date_attr="course_start_date",
        value_concept_attr="course_count",
        value_string_attr="course_concept" 
    )

class IntentChemoMeasurable(ConditionTreatmentEpisode, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Intent of Chemotherapy Treatment Episodes",
        person_id_attr="person_id",
        episode_id_attr="condition_episode_id",
        event_date_attr="regimen_start_date",
        value_concept_attr="sact_intent_concept_id",   
        value_string_attr="sact_intent_concept"
    )

class IntentRTMeasurable(ConditionTreatmentEpisode, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.tx,
        label="Intent of Radiotherapy Treatment Episodes",
        person_id_attr="person_id",
        episode_id_attr="condition_episode_id",
        event_date_attr="course_start_date",
        value_concept_attr="rt_intent_concept_id",   
        value_string_attr="rt_intent_concept"
    )