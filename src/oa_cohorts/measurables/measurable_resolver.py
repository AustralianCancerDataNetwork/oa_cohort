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


#             target             | count 
# -------------------------------+-------
#  referral_to_specialist_window |     1
#  intent_rt                     |     1
#  tx_to_death_window            |     1

#  proc_concept                  |     4
#  meas_concept                  |     9
#  obs_concept                   |     2

#  demog_gender                  |     3
#  demog_death                   |     1

#  tx_concurrent                 |     1
#  tx_chemotherapy               |     1
#  tx_surgical                   |     3
#  tx_current_episode            |     2

#  dx_stage                      |    77
#  dx_mets                       |     2
#  dx_primary                    |    39