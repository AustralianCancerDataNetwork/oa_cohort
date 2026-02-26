from omop_constructs.alchemy.demography import PersonDemography
from orm_loader.helpers import Base, get_logger
from .measurable_base import MeasurableSpec, MeasurableBase, MeasurableDomain

logger = get_logger(__name__)

class DeathMeasurable(PersonDemography, MeasurableBase, Base):
    __measurable__ = MeasurableSpec(
        domain=MeasurableDomain.person,
        label="Death",
        person_id_attr="person_id",
        episode_id_attr="episode_id",
        event_date_attr="death_datetime",
        value_concept_attr="gender_concept_id",
    )
