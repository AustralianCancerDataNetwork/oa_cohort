from __future__ import annotations

import enum
import sqlalchemy as sa
from typing import Callable, Mapping
import sqlalchemy as sa
from sqlalchemy.sql import Selectable


# Type of SQLAlchemy set combiners like union_all / intersect_all / except_all
SQLSetCombiner = Callable[..., Selectable]

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
    meas_concept = 17

    tx_to_death_window = 18
    dx_to_tx_window = 19
    referral_to_tx_window = 20
    referral_to_specialist_window = 21
    tx_concurrent = 22
    meas_value_num = 23
    meas_value_concept = 24
    intent_rt = 25
    intent_sact = 26



class RuleCombination(enum.Enum):
    rule_or = 1
    rule_and = 2
    rule_except = 3

    @property
    def label(self) -> str:
        labels: Mapping[int, str] = {
            1: "OR",
            2: "AND",
            3: "EXCEPT",
        }
        return labels[self.value]

    def combiner_options(self) -> Mapping[int, SQLSetCombiner]:
        return {
            1: sa.union_all,
            2: sa.intersect_all,
            3: sa.except_all,
        }

    def combiner(self) -> SQLSetCombiner:
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
    eq = 3


class RuleMatcher(enum.Enum):
    substring = 1
    exact = 2
    hierarchy = 3
    absence = 4
    presence = 5
    hierarchyexclusion = 6
    scalar = 7
    phenotype = 8


class RuleTemporality(enum.Enum):
    dt_current_start = 1
    dt_death = 2
    dt_treatment_start = 3
    dt_obs = 4
    dt_proc_start = 5
    dt_numerator = 6
    dt_denominator = 7
    dt_any = 8
    dt_meas = 9
    dt_rad = 10
    dt_surg = 11
    dt_treat = 12
    dt_treatment_end = 13
    dt_concurrent = 14
    dt_consult = 15
    dt_stage = 16
    dt_mets = 17


class ReportStatus(enum.Enum):
    st_current = 1
    st_draft = 2
    st_historical = 3