"""CLI-facing helpers for loading compact indicator summary views from ORM models."""

from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.query.indicator import Indicator
from oa_cohorts.query.measure import Measure, MeasureRelationship
from oa_cohorts.query.report import Report, report_indicator_map
from oa_cohorts.query.subquery import Subquery


@dataclass(frozen=True)
class IndicatorSummary:
    """Compact report-scoped indicator summary used for list and table views."""

    report_id: int
    report_name: str
    report_short_name: str
    indicator_id: int
    description: str
    reference: str | None
    numerator_measure_id: int
    numerator_measure_name: str
    numerator_label: str
    denominator_measure_id: int
    denominator_measure_name: str
    denominator_label: str
    benchmark_summary: str


@dataclass(frozen=True)
class MeasureSummary:
    """Normalized measure summary embedded inside detailed indicator output."""

    measure_id: int
    name: str
    combination: str
    person_ep_override: bool
    subquery_name: str | None
    subquery_short_name: str | None
    subquery_target: str | None
    subquery_temporality: str | None
    child_measure_names: tuple[str, ...]


@dataclass(frozen=True)
class IndicatorDetailSummary:
    """Detailed indicator summary with report memberships and resolved measure metadata."""

    indicator_id: int
    description: str
    reference: str | None
    report_memberships: tuple[str, ...]
    numerator_label: str
    numerator_measure: MeasureSummary
    denominator_label: str
    denominator_measure: MeasureSummary
    benchmark_summary: str


def has_indicator_summary_tables(session: so.Session) -> bool:
    """Return whether the minimum schema required for indicator summary queries exists."""

    bind = session.get_bind()
    inspector = sa.inspect(bind)
    return all(
        inspector.has_table(table_name)
        for table_name in (
            Report.__tablename__,
            Indicator.__tablename__,
            Measure.__tablename__,
            Subquery.__tablename__,
            report_indicator_map.name,
        )
    )


def load_indicator_summaries(session: so.Session, *, report_id: int) -> list[IndicatorSummary]:
    """Load summaries for all indicators attached to a report.

    Returns an empty list when the required tables are unavailable or the report
    does not exist.
    """

    if not has_indicator_summary_tables(session):
        return []

    stmt = (
        sa.select(Report)
        .where(Report.report_id == report_id)
        .options(
            so.selectinload(Report.indicators).selectinload(Indicator.numerator_measure),
            so.selectinload(Report.indicators).selectinload(Indicator.denominator_measure),
        )
    )
    report = session.execute(stmt).scalars().unique().one_or_none()
    if report is None:
        return []

    return [_to_summary(report, indicator) for indicator in sorted(report.indicators)]


def load_report_brief(session: so.Session, *, report_id: int) -> tuple[str, str] | None:
    """Return the report name and short name for a report, if available."""

    if not has_indicator_summary_tables(session):
        return None

    stmt = sa.select(Report.report_name, Report.report_short_name).where(Report.report_id == report_id)
    row = session.execute(stmt).one_or_none()
    if row is None:
        return None
    return row[0], row[1]


def load_indicator_detail_summary(
    session: so.Session,
    *,
    indicator_id: int,
) -> IndicatorDetailSummary | None:
    """Load a detailed summary for a single indicator and its related measures."""

    if not has_indicator_summary_tables(session):
        return None

    stmt = (
        sa.select(Indicator)
        .where(Indicator.indicator_id == indicator_id)
        .options(
            so.selectinload(Indicator.in_reports),
            so.joinedload(Indicator.numerator_measure).joinedload(Measure.subquery),
            so.joinedload(Indicator.denominator_measure).joinedload(Measure.subquery),
            so.joinedload(Indicator.numerator_measure)
            .selectinload(Measure.child_links)
            .joinedload(MeasureRelationship.child),
            so.joinedload(Indicator.denominator_measure)
            .selectinload(Measure.child_links)
            .joinedload(MeasureRelationship.child),
        )
    )
    indicator = session.execute(stmt).scalars().unique().one_or_none()
    if indicator is None:
        return None

    return IndicatorDetailSummary(
        indicator_id=indicator.indicator_id,
        description=indicator.indicator_description,
        reference=indicator.indicator_reference,
        report_memberships=tuple(
            sorted(f"{report.report_name} ({report.report_short_name})" for report in indicator.in_reports)
        ),
        numerator_label=indicator.numerator_label,
        numerator_measure=_to_measure_summary(indicator.numerator_measure),
        denominator_label=indicator.denominator_label,
        denominator_measure=_to_measure_summary(indicator.denominator_measure),
        benchmark_summary=_render_benchmark_summary(indicator),
    )


def _to_summary(report: Report, indicator: Indicator) -> IndicatorSummary:
    """Normalize a report-indicator pair into an immutable summary view."""

    return IndicatorSummary(
        report_id=report.report_id,
        report_name=report.report_name,
        report_short_name=report.report_short_name,
        indicator_id=indicator.indicator_id,
        description=indicator.indicator_description,
        reference=indicator.indicator_reference,
        numerator_measure_id=indicator.numerator_measure_id,
        numerator_measure_name=indicator.numerator_measure.name,
        numerator_label=indicator.numerator_label,
        denominator_measure_id=indicator.denominator_measure_id,
        denominator_measure_name=indicator.denominator_measure.name,
        denominator_label=indicator.denominator_label,
        benchmark_summary=_render_benchmark_summary(indicator),
    )


def _render_benchmark_summary(indicator: Indicator) -> str:
    """Render benchmark metadata as a compact display string."""

    if indicator.benchmark is None:
        return "-"
    unit = indicator.benchmark_unit or ""
    return f"{indicator.benchmark} {unit}".strip()


def _to_measure_summary(measure: Measure) -> MeasureSummary:
    """Normalize a measure ORM object into an immutable summary dataclass."""

    subquery = measure.subquery
    child_measure_names = tuple(sorted(link.child.name for link in measure.child_links if link.child is not None))
    return MeasureSummary(
        measure_id=measure.measure_id,
        name=measure.name,
        combination=measure.combination.value,
        person_ep_override=measure.person_ep_override,
        subquery_name=subquery.name if subquery is not None else None,
        subquery_short_name=subquery.short_name if subquery is not None else None,
        subquery_target=subquery.target.value if subquery is not None else None,
        subquery_temporality=subquery.temporality.value if subquery is not None else None,
        child_measure_names=child_measure_names,
    )
