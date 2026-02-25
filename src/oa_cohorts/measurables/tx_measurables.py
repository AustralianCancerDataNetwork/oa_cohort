from omop_constructs.alchemy.modifiers import ModifiedCondition
from omop_constructs.alchemy.episodes import SurgicalProcedureMV, DxTreatStartMV
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