from omop_constructs.alchemy.modifiers import ModifiedCondition
from omop_constructs.alchemy.events import DxMeasurementMV
from orm_loader.helpers import Base, get_logger
from .measurable_base import MeasurableSpec, MeasurableBase, MeasurableDomain

logger = get_logger(__name__)

class MeasurementMeasurable(DxMeasurementMV, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.meas,
        label="Diagnosis-episode linked measurements",
        person_id_attr="person_id",
        episode_id_attr="episode_id",
        event_date_attr="event_date",
        value_concept_attr="event_concept_id",
    )
