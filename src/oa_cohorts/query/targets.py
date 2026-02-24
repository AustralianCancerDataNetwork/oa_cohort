# reporting/query/targets.py

from __future__ import annotations
from typing import Mapping, TypeAlias, Any
import sqlalchemy as sa
from sqlalchemy.sql import ColumnElement

from ..core.enums import RuleTarget, RuleTemporality

# ORM imports (these were previously inside enums)
from omop_constructs.alchemy.episodes import (
    OverarchingDiseaseEpisodeMV
)
from omop_constructs.alchemy.modifiers import (
    TStageMV, 
    NStageMV, 
    MStageMV,   
    GroupStageMV,
    GradeModifierMV,
    SizeModifierMV,
    LateralityModifierMV,
)
from omop_constructs.alchemy.modifiers.modifier_mappers import (
    MetastaticDiseaseModifierMV
)

#     OverarchingCondition,
#     MetsModifier,
#     StageModifier,
#     ConditionTreatmentEpisode,
#     SurgicalProcedure,
#     TreatmentEnvelope,
#     ConsultWindow,
#     PersonDemography,
#     DXRelevantObs,
#     DXRelevantProc,
#     DXRelevantMeas,
# )

# from omop_alchemy.conventions.constructs import (
#     Dx_Treat_Start,
#     Dx_RT_Start,
#     Dated_Surgical_Procedure,
# )

# ----
# Types
# ----

SelectableCols: TypeAlias = tuple[
    ColumnElement[Any],
    ColumnElement[Any],
    ColumnElement[Any],
]

TargetCol: TypeAlias = ColumnElement[Any]

# --------------------------------
# RuleTarget → table selectables
# --------------------------------

RULE_TARGET_TABLES: Mapping[RuleTarget, SelectableCols] = {
    RuleTarget.dx_any: (
        OverarchingDiseaseEpisodeMV.person_id.label("person_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("episode_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("measure_resolver"),
    ),
    RuleTarget.dx_primary: (
        OverarchingDiseaseEpisodeMV.person_id.label("person_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("episode_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("measure_resolver"),
    ),
    RuleTarget.dx_first_primary: (
        OverarchingDiseaseEpisodeMV.person_id.label("person_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("episode_id"),
        OverarchingDiseaseEpisodeMV.episode_id.label("measure_resolver"),
    ),
    RuleTarget.dx_mets: (
        MetastaticDiseaseModifierMV.person_id.label("person_id"),
        MetastaticDiseaseModifierMV.condition_episode.label("episode_id"),
        MetastaticDiseaseModifierMV.condition_episode.label("measure_resolver"),
    ),
    RuleTarget.dx_stage: (
        StageModifier.person_id.label("person_id"),
        StageModifier.condition_episode.label("episode_id"),
        StageModifier.condition_episode.label("measure_resolver"),
    ),
    RuleTarget.tx_current_episode: (
        Dx_Treat_Start.person_id.label("person_id"),
        Dx_Treat_Start.dx_id.label("episode_id"),
        Dx_Treat_Start.dx_id.label("measure_resolver"),
    ),
    RuleTarget.tx_chemotherapy: (
        ConditionTreatmentEpisode.person_id.label("person_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("episode_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("measure_resolver"),
    ),
    RuleTarget.tx_radiotherapy: (
        ConditionTreatmentEpisode.person_id.label("person_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("episode_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("measure_resolver"),
    ),
    RuleTarget.tx_surgical: (
        SurgicalProcedure.person_id.label("person_id"),
        SurgicalProcedure.overarching_episode_id.label("episode_id"),
        SurgicalProcedure.overarching_episode_id.label("measure_resolver"),
    ),
    RuleTarget.demog_gender: (
        PersonDemography.person_id.label("person_id"),
        PersonDemography.episode_id.label("episode_id"),
        PersonDemography.episode_id.label("measure_resolver"),
    ),
    RuleTarget.demog_death: (
        PersonDemography.person_id.label("person_id"),
        PersonDemography.episode_id.label("episode_id"),
        PersonDemography.episode_id.label("measure_resolver"),
    ),
    RuleTarget.obs_value: (
        DXRelevantObs.person_id.label("person_id"),
        DXRelevantObs.episode_id.label("episode_id"),
        DXRelevantObs.episode_id.label("measure_resolver"),
    ),
    RuleTarget.obs_concept: (
        DXRelevantObs.person_id.label("person_id"),
        DXRelevantObs.episode_id.label("episode_id"),
        DXRelevantObs.episode_id.label("measure_resolver"),
    ),
    RuleTarget.proc_concept: (
        DXRelevantProc.person_id.label("person_id"),
        DXRelevantProc.episode_id.label("episode_id"),
        DXRelevantProc.episode_id.label("measure_resolver"),
    ),
    RuleTarget.meas_concept: (
        DXRelevantMeas.person_id.label("person_id"),
        DXRelevantMeas.episode_id.label("episode_id"),
        DXRelevantMeas.episode_id.label("measure_resolver"),
    ),
    RuleTarget.tx_to_death_window: (
        TreatmentEnvelope.person_id.label("person_id"),
        TreatmentEnvelope.condition_episode.label("episode_id"),
        TreatmentEnvelope.condition_episode.label("measure_resolver"),
    ),
    RuleTarget.referral_to_specialist_window: (
        ConsultWindow.person_id.label("person_id"),
        ConsultWindow.episode_id.label("episode_id"),
        ConsultWindow.person_id.label("measure_resolver"),
    ),
    RuleTarget.tx_concurrent: (
        TreatmentEnvelope.person_id.label("person_id"),
        TreatmentEnvelope.condition_episode.label("episode_id"),
        TreatmentEnvelope.condition_episode.label("measure_resolver"),
    ),
    RuleTarget.meas_value_num: (
        DXRelevantMeas.person_id.label("person_id"),
        DXRelevantMeas.episode_id.label("episode_id"),
        DXRelevantMeas.episode_id.label("measure_resolver"),
    ),
    RuleTarget.meas_value_concept: (
        DXRelevantMeas.person_id.label("person_id"),
        DXRelevantMeas.episode_id.label("episode_id"),
        DXRelevantMeas.episode_id.label("measure_resolver"),
    ),
    RuleTarget.intent_rt: (
        ConditionTreatmentEpisode.person_id.label("person_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("episode_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("measure_resolver"),
    ),
    RuleTarget.intent_sact: (
        ConditionTreatmentEpisode.person_id.label("person_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("episode_id"),
        ConditionTreatmentEpisode.condition_episode_id.label("measure_resolver"),
    ),
}

# --------------------------------
# RuleTarget → target column
# --------------------------------

RULE_TARGET_COLUMNS: Mapping[RuleTarget, TargetCol] = {
    RuleTarget.dx_any: OverarchingCondition.condition_concept_id,
    RuleTarget.dx_primary: OverarchingCondition.condition_concept_id,
    RuleTarget.dx_first_primary: OverarchingCondition.condition_concept_id,
    RuleTarget.dx_mets: MetsModifier.mets_concept_id,
    RuleTarget.dx_stage: StageModifier.stage_concept_id,
    RuleTarget.tx_current_episode: Dx_Treat_Start.dx_id,
    RuleTarget.tx_chemotherapy: ConditionTreatmentEpisode.regimen_id,
    RuleTarget.tx_radiotherapy: ConditionTreatmentEpisode.condition_episode_id,
    RuleTarget.tx_surgical: SurgicalProcedure.surgery_concept_id,
    RuleTarget.demog_gender: PersonDemography.gender_concept_id,
    RuleTarget.demog_death: PersonDemography.death_datetime,
    RuleTarget.obs_value: DXRelevantObs.value_concept_id,
    RuleTarget.obs_concept: DXRelevantObs.concept_id,
    RuleTarget.proc_concept: DXRelevantProc.concept_id,
    RuleTarget.meas_concept: DXRelevantMeas.measurement_concept_id,
    RuleTarget.tx_to_death_window: TreatmentEnvelope.treatment_days_before_death,
    RuleTarget.referral_to_specialist_window: ConsultWindow.referral_to_specialist,
    RuleTarget.tx_concurrent: TreatmentEnvelope.concurrent_chemort,
    RuleTarget.meas_value_num: DXRelevantMeas.value_as_number,
    RuleTarget.meas_value_concept: DXRelevantMeas.value_as_concept_id,
    RuleTarget.intent_rt: ConditionTreatmentEpisode.rt_intent_concept_id,
    RuleTarget.intent_sact: ConditionTreatmentEpisode.sact_intent_concept_id,
}

# --------------------------------
# RuleTarget → string target column
# --------------------------------

RULE_TARGET_STRING_COLUMNS: Mapping[RuleTarget, TargetCol] = {
    RuleTarget.dx_any: OverarchingCondition.condition_concept,
    RuleTarget.dx_primary: OverarchingCondition.condition_concept,
    RuleTarget.dx_first_primary: OverarchingCondition.condition_concept,
    RuleTarget.dx_mets: OverarchingCondition.condition_concept,  # preserve your previous behaviour
    RuleTarget.obs_value: DXRelevantObs.value_concept_id,
    RuleTarget.obs_concept: DXRelevantObs.concept_id,
    RuleTarget.proc_concept: DXRelevantProc.concept_name,
}

# --------------------------------
# RuleTemporality → date column
# --------------------------------

RULE_TEMPORALITY_DATE_FIELDS: Mapping[RuleTemporality, ColumnElement] = {
    RuleTemporality.dt_current_start: OverarchingCondition.condition_start_date,
    RuleTemporality.dt_death: PersonDemography.death_datetime,
    RuleTemporality.dt_treatment_start: Dx_Treat_Start.treatment_start,
    RuleTemporality.dt_obs: DXRelevantObs.observation_date,
    RuleTemporality.dt_proc_start: DXRelevantProc.procedure_date,
    RuleTemporality.dt_numerator: Dx_RT_Start.rt_start,
    RuleTemporality.dt_denominator: Dated_Surgical_Procedure.procedure_datetime,
    RuleTemporality.dt_any: sa.func.coalesce(
        PersonDemography.death_datetime,
        PersonDemography.episode_start_datetime,
    ),
    RuleTemporality.dt_meas: sa.func.coalesce(
        DXRelevantMeas.measurement_date,
        DXRelevantMeas.episode_start_datetime,
    ),
    RuleTemporality.dt_rad: sa.func.coalesce(
        ConditionTreatmentEpisode.course_start_date,
        ConditionTreatmentEpisode.condition_start_date,
    ),
    RuleTemporality.dt_surg: sa.func.coalesce(
        SurgicalProcedure.surgery_datetime,
        SurgicalProcedure.condition_start_date,
    ),
    RuleTemporality.dt_treat: sa.func.coalesce(
        ConditionTreatmentEpisode.regimen_start_date,
        ConditionTreatmentEpisode.condition_start_date,
    ),
    RuleTemporality.dt_treatment_end: TreatmentEnvelope.latest_treatment,
    RuleTemporality.dt_concurrent: sa.func.coalesce(
        TreatmentEnvelope.earliest_treatment,
        TreatmentEnvelope.condition_start_date,
    ),
    RuleTemporality.dt_consult: ConsultWindow.episode_start_datetime,
    RuleTemporality.dt_stage: StageModifier.stage_date,
    RuleTemporality.dt_mets: sa.func.coalesce(
        MetsModifier.mets_date,
        MetsModifier.condition_start_date,
    ),
}


# -------------------------
# Public helper functions
# -------------------------

def get_target_table(rt: RuleTarget) -> SelectableCols:
    return RULE_TARGET_TABLES[rt]


def get_target_column(rt: RuleTarget, *, string_match: bool = False) -> TargetCol:
    if string_match:
        return RULE_TARGET_STRING_COLUMNS[rt]
    return RULE_TARGET_COLUMNS[rt]


def get_temporal_date_field(rt: RuleTemporality) -> ColumnElement:
    return RULE_TEMPORALITY_DATE_FIELDS[rt]