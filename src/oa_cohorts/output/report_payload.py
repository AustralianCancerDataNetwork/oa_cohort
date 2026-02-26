
from typing import List, Optional
from datetime import datetime, date
from pydantic import BaseModel, Field

class MeasureSummary(BaseModel):
    id: int
    measure_name: str
    measure_combination: str


class IndicatorPayload(BaseModel):
    indicator_id: int
    indicator_description: str
    indicator_reference: Optional[str] = None

    numerator_label: str
    denominator_label: str

    numerator_measure: MeasureSummary
    denominator_measure: MeasureSummary


class DashCohortDefinitionPayload(BaseModel):
    dash_cohort_def_id: int
    dash_cohort_def_name: str
    measure_id: int
    measure_count: int
    members: List[int] = Field(default_factory=list)


class DashCohortPayload(BaseModel):
    dash_cohort_id: int
    dash_cohort_name: str
    definitions: List[DashCohortDefinitionPayload]


class ReportMeasurePayload(BaseModel):
    measure_id: int
    materialised_measure_id: Optional[int] = None
    refresh_date: Optional[datetime] = None


class ReportPayload(BaseModel):
    report_name: str
    report_short_name: str
    report_description: str

    indicators: List[IndicatorPayload]
    report_cohorts: List[DashCohortPayload]
    report_measures: List[ReportMeasurePayload]


class CohortDemographyRow(BaseModel):
    person_id: int
    mrn: Optional[str] = None
    year_of_birth: Optional[int] = None
    death_datetime: Optional[datetime] = None
    gender: Optional[str] = None
    language_spoken: Optional[str] = None
    country_of_birth: Optional[str] = None
    post_code: Optional[int] = None


class PivotCohortRow(BaseModel):
    episode_id: Optional[int] = None
    measure_date: Optional[date] = None
    measure_resolver: int
    person_id: int
    cohort_label: str
    subcohort_label: Optional[str] = None
    measure_id: int


class PivotIndicatorRow(BaseModel):
    person_id: int
    measure_resolver: int

    numerator_date: Optional[date] = None
    denominator_date: Optional[date] = None

    numerator_measure_id: int
    denominator_measure_id: int
    indicator: int

    numerator_value: bool
    denominator_value: bool


class ReportBundle(BaseModel):
    report: ReportPayload
    cohort_demography: List[CohortDemographyRow]
    pivot_cohort: List[PivotCohortRow]
    pivot_indicators: List[PivotIndicatorRow]

    model_config = {"extra": "forbid"}
