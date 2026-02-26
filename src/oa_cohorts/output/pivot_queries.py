from typing import Sequence
from ..query.report import Report
from ..query.typing import Row
from .report_payload import (
    DashCohortDefinitionPayload, 
    DashCohortPayload, 
    IndicatorPayload, 
    MeasureSummary, 
    ReportPayload, 
    CohortDemographyRow,
    PivotIndicatorRow, 
    PivotCohortRow,
    ReportMeasurePayload
)

def build_cohort_demography(rows: Sequence[Row]) -> list[CohortDemographyRow]:
    out: list[CohortDemographyRow] = []

    for r in rows:
        # If your select is just PersonDemography.*, r will already expose fields
        out.append(
            CohortDemographyRow(
                person_id=r.person_id,
                mrn=getattr(r, "mrn", None),
                year_of_birth=getattr(r, "year_of_birth", None),
                death_datetime=getattr(r, "death_datetime", None),
                gender=getattr(r, "sex", None) or getattr(r, "gender", None),
                language_spoken=getattr(r, "language_spoken", None),
                country_of_birth=getattr(r, "country_of_birth", None),
                post_code=getattr(r, "post_code", None),
            )
        )

    return out


def build_pivot_indicators(report: Report) -> list[PivotIndicatorRow]:
    rows: list[PivotIndicatorRow] = []

    for ind in report.indicators:
        num = {mm for mm in ind.numerator_members}
        den = {mm for mm in ind.denominator_members}

        for mm in num & den:
            rows.append(
                PivotIndicatorRow(
                    person_id=mm.person_id,
                    measure_resolver=mm.measure_resolver,
                    numerator_date=mm.measure_date,
                    denominator_date=mm.measure_date,  # or pull from den if you later preserve both
                    numerator_measure_id=ind.numerator_measure.measure_id,
                    denominator_measure_id=ind.denominator_measure.measure_id,
                    indicator=ind.indicator_id,
                    numerator_value=True,
                    denominator_value=True,
                )
            )

    return rows

def build_pivot_cohort(report: Report) -> list[PivotCohortRow]:
    rows: list[PivotCohortRow] = []

    for rc in report.cohorts:
        cohort_label = rc.cohort.dash_cohort_name

        for d in rc.cohort.definitions:
            m = d.dash_cohort_measure
            if not m:
                continue

            for mm in m.members:
                rows.append(
                    PivotCohortRow(
                        episode_id=mm.episode_id,
                        measure_date=mm.measure_date,
                        measure_resolver=mm.measure_resolver,
                        person_id=mm.person_id,
                        cohort_label=cohort_label,
                        subcohort_label=d.dash_cohort_def_name,
                        measure_id=m.measure_id,
                    )
                )

    return rows

def build_cohort_payloads(report: Report) -> list[DashCohortPayload]:
    out = []

    for rc in report.cohorts:
        cohort = rc.cohort
        defs = []

        for d in cohort.definitions:
            m = d.dash_cohort_measure
            member_ids = [mm.person_id for mm in m.members] if m else []

            defs.append(
                DashCohortDefinitionPayload(
                    dash_cohort_def_id=d.dash_cohort_def_id,
                    dash_cohort_def_name=d.dash_cohort_def_name,
                    measure_id=d.measure_id,
                    measure_count=d.measure_count,
                    members=member_ids,
                )
            )

        out.append(
            DashCohortPayload(
                dash_cohort_id=cohort.dash_cohort_id,
                dash_cohort_name=cohort.dash_cohort_name,
                definitions=defs,
            )
        )

    return out


def build_report_payload(report: Report) -> ReportPayload:
    return ReportPayload(
        report_name=report.report_name,
        report_short_name=report.report_short_name,
        report_description=report.report_description,
        indicators=[
            IndicatorPayload(
                indicator_id=ind.indicator_id,
                indicator_description=ind.indicator_description,
                indicator_reference=ind.indicator_reference,
                numerator_label=ind.numerator_label,
                denominator_label=ind.denominator_label,
                numerator_measure=MeasureSummary(
                    id=ind.numerator_measure.measure_id,
                    measure_name=ind.numerator_measure.name,
                    measure_combination=ind.numerator_measure.combination.value,
                ),
                denominator_measure=MeasureSummary(
                    id=ind.denominator_measure.measure_id,
                    measure_name=ind.denominator_measure.name,
                    measure_combination=ind.denominator_measure.combination.value,
                ),
            )
            for ind in report.indicators
        ],
        report_cohorts=build_cohort_payloads(report),
        report_measures=[
            ReportMeasurePayload(
                measure_id=m.measure_id,
                materialised_measure_id=None,
                refresh_date=None,
            )
            for m in report.report_measures
        ],
    )