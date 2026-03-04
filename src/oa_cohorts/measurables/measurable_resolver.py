from typing import Type, TYPE_CHECKING
from ..core import RuleTarget, RuleTemporality
from .measurable_base import MeasurableBase

def get_measurable_registry() -> dict[RuleTarget, Type[MeasurableBase]]:
    from .dx_measurables import (
        AnyConditionMeasurable,
        StagedConditionMeasurable,
        MetsConditionMeasurable,
    )

    from .tx_measurables import (
        SurgicalMeasurable,
        AllCurrentTreatmentMeasurable,
        ChemoTreatmentMeasurable,
        RTTreatmentMeasurable,
        IntentChemoMeasurable,
        IntentRTMeasurable
    )

    from .ev_measureables import (
        MeasurementMeasurable,
        ProcedureMeasurable,
        ObserveMeasurable
    )

    from .pr_measurables import (
        DeathMeasurable
    )

    return {
        RuleTarget.dx_any: AnyConditionMeasurable,
        RuleTarget.dx_primary: AnyConditionMeasurable,
        RuleTarget.dx_stage: StagedConditionMeasurable,
        RuleTarget.dx_mets: MetsConditionMeasurable,
        RuleTarget.tx_surgical: SurgicalMeasurable,
        RuleTarget.tx_current_episode: AllCurrentTreatmentMeasurable,
        RuleTarget.meas_concept: MeasurementMeasurable,
        RuleTarget.proc_concept: ProcedureMeasurable,
        RuleTarget.obs_concept: ObserveMeasurable,
        RuleTarget.tx_chemotherapy: ChemoTreatmentMeasurable,
        RuleTarget.tx_radiotherapy: RTTreatmentMeasurable,
        RuleTarget.intent_sact: IntentChemoMeasurable,
        RuleTarget.intent_rt: IntentRTMeasurable,
        RuleTarget.demog_death: DeathMeasurable
    }
