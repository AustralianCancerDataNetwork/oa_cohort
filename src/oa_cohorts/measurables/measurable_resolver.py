from typing import Type, TYPE_CHECKING
from ..core import RuleTarget, RuleTemporality
from .measurable_base import MeasurableBase

def get_measurable_registry() -> dict[RuleTarget, Type[MeasurableBase]]:
    from .dx_measurables import (
        AnyConditionMeasurable,
        StagedConditionMeasurable,
        MetsConditionMeasurable,
    )

    return {
        RuleTarget.dx_any: AnyConditionMeasurable,
        RuleTarget.dx_primary: AnyConditionMeasurable,
        RuleTarget.dx_stage: StagedConditionMeasurable,
        RuleTarget.dx_mets: MetsConditionMeasurable,
        # later:
        # RuleTarget.tx_current_episode: TreatmentEpisodeMeasurable,
        # RuleTarget.proc_concept: ProcedureMeasurable,
        # ...
    }
