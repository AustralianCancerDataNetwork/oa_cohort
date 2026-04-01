from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.query.report import Report, ReportCohortMap


@dataclass(frozen=True)
class ReportSummary:
    report_id: int
    report_name: str
    report_short_name: str
    description: str
    author: str
    owner: str | None
    versions: str
    statuses: tuple[str, ...]
    cohort_count: int
    cohort_names: tuple[str, ...]
    primary_cohort_names: tuple[str, ...]
    indicator_count: int


def load_report_summaries(
    session: so.Session,
    *,
    report_id: int | None = None,
    short_name: str | None = None,
) -> list[ReportSummary]:
    if not has_report_summary_tables(session):
        return []

    stmt = (
        sa.select(Report)
        .options(
            so.selectinload(Report.cohorts).selectinload(ReportCohortMap.cohort),
            so.selectinload(Report.indicators),
            so.selectinload(Report.report_versions),
        )
        .order_by(Report.report_id)
    )

    if report_id is not None:
        stmt = stmt.where(Report.report_id == report_id)

    if short_name is not None:
        stmt = stmt.where(sa.func.lower(Report.report_short_name) == short_name.lower())

    reports = session.execute(stmt).scalars().unique().all()
    return [_to_summary(report) for report in reports]


def has_report_summary_tables(session: so.Session) -> bool:
    bind = session.get_bind()
    inspector = sa.inspect(bind)
    return inspector.has_table(Report.__tablename__)


def _to_summary(report: Report) -> ReportSummary:
    cohort_names = tuple(
        sorted(
            rc.cohort.dash_cohort_name
            for rc in report.cohorts
            if rc.cohort is not None
        )
    )
    primary_cohort_names = tuple(
        sorted(
            rc.cohort.dash_cohort_name
            for rc in report.cohorts
            if rc.primary_cohort and rc.cohort is not None
        )
    )
    statuses = tuple(sorted({version.report_status.value for version in report.report_versions}))

    return ReportSummary(
        report_id=report.report_id,
        report_name=report.report_name,
        report_short_name=report.report_short_name,
        description=report.report_description or "",
        author=report.report_author,
        owner=report.report_owner,
        versions=report.version_string or "",
        statuses=statuses,
        cohort_count=len(report.cohorts),
        cohort_names=cohort_names,
        primary_cohort_names=primary_cohort_names,
        indicator_count=len(report.indicators),
    )
