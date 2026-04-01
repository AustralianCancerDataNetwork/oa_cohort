from __future__ import annotations

from datetime import date, datetime
from typing import Sequence
from typing import TYPE_CHECKING
from ..query.report import Report
from ..query.measure import MeasureExecutor, MeasureMember
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

if TYPE_CHECKING:
    from omop_constructs.alchemy.demography import PersonDemography


def collect_report_cohort_members(report: Report, executor: MeasureExecutor) -> list[MeasureMember]:
    members: list[MeasureMember] = []

    for report_cohort in report.cohorts:
        cohort = report_cohort.cohort
        if not cohort:
            continue

        for definition in cohort.definitions:
            measure = definition.dash_cohort_measure
            if not measure:
                continue
            members.extend(measure.members(executor))

    return members


def _dedupe_members(members: Sequence[MeasureMember]) -> list[MeasureMember]:
    seen: set[MeasureMember] = set()
    out: list[MeasureMember] = []

    for member in members:
        if member in seen:
            continue
        seen.add(member)
        out.append(member)

    return out


def _coerce_date(value: date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    return value


def _pick_earliest_date(current: date | None, candidate: date | None) -> date | None:
    if current is None:
        return candidate
    if candidate is None:
        return current
    return min(current, candidate)


def build_cohort_demography(rows: Sequence[PersonDemography]) -> list[CohortDemographyRow]:
    out: list[CohortDemographyRow] = []
    for r in rows:
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


def build_pivot_indicators(
    report: Report,
    executor: MeasureExecutor,
    strict: bool = True,
    cohort_members: Sequence[MeasureMember] | None = None,
) -> list[PivotIndicatorRow]:
    rows: list[PivotIndicatorRow] = []
    resolved_cohort_members = list(cohort_members) if cohort_members is not None else collect_report_cohort_members(report, executor)
    cohort_resolvers = {member.measure_resolver for member in resolved_cohort_members}

    for ind in report.indicators:
        try:
            if ind.denominator_measure_id == 0:
                denominator_members = _dedupe_members(resolved_cohort_members)
            else:
                denominator_members = _dedupe_members([
                    mm
                    for mm in ind.denominator_measure.members(executor)
                    if mm.measure_resolver in cohort_resolvers and mm.measure_date is not None
                ])

            denominator_keys = {
                (member.person_id, member.measure_resolver)
                for member in denominator_members
            }
            numerator_dates: dict[tuple[int, int], date | None] = {}

            for member in ind.numerator_measure.members(executor):
                key = (member.person_id, member.measure_resolver)
                if key not in denominator_keys:
                    continue
                numerator_dates[key] = _pick_earliest_date(
                    numerator_dates.get(key),
                    _coerce_date(member.measure_date),
                )

            for mm in denominator_members:
                key = (mm.person_id, mm.measure_resolver)
                rows.append(
                    PivotIndicatorRow(
                        person_id=mm.person_id,
                        measure_resolver=mm.measure_resolver,
                        numerator_date=numerator_dates.get(key),
                        denominator_date=_coerce_date(mm.measure_date),
                        numerator_measure_id=ind.numerator_measure.measure_id,
                        denominator_measure_id=ind.denominator_measure.measure_id,
                        indicator=ind.indicator_id,
                        numerator_value=key in numerator_dates,
                        denominator_value=True,
                    )
                )
        except Exception as e:
            if strict:
                raise
            else:
                print(f"Error processing indicator {ind.indicator_id}: {e}")

    return rows

def build_pivot_cohort(report: Report, executor: MeasureExecutor, strict: bool = True) -> list[PivotCohortRow]:
    rows: list[PivotCohortRow] = []

    for rc in report.cohorts:
        cohort_label = rc.cohort.dash_cohort_name

        for d in rc.cohort.definitions:
            m = d.dash_cohort_measure
            if not m:
                continue

            try:
                for mm in m.members(executor):
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
            except Exception as e:
                if strict:
                    raise
                else:
                    print(f"Error processing cohort {cohort_label} definition {d.dash_cohort_def_name}: {e}")

    return rows

def build_cohort_payloads(report: Report, executor: MeasureExecutor) -> list[DashCohortPayload]:
    out = []

    for rc in report.cohorts:
        cohort = rc.cohort
        defs = []

        for d in cohort.definitions:
            m = d.dash_cohort_measure
            member_ids = [mm.person_id for mm in m.members(executor)] if m else []

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


def build_report_payload(report: Report, executor: MeasureExecutor) -> ReportPayload:
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
        report_cohorts=build_cohort_payloads(report, executor=executor),
        report_measures=[
            ReportMeasurePayload(
                measure_id=m.measure_id,
                materialised_measure_id=None,
                refresh_date=None,
            )
            for m in report.indicator_measures + report.cohort_measures
        ],
    )
