from __future__ import annotations
from datetime import date
from itertools import chain
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.ext.hybrid import hybrid_property
from orm_loader.helpers import Base
from sqlalchemy.ext.associationproxy import association_proxy
from ..core.utils import HTMLRenderable, RawHTML, esc, td, th, exec_badge, table
from ..core import ReportStatus
from ..core.executability import ExecStatus

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .indicator import Indicator
    from .dash_cohort import DashCohort

report_indicator_map = sa.Table(
    'report_indicator_map',
    Base.metadata,
    sa.Column('report_id', sa.ForeignKey('report.report_id'), primary_key=True),
    sa.Column('indicator_id', sa.ForeignKey('indicator.indicator_id'), primary_key=True),
)

class ReportCohortMap(HTMLRenderable, Base):
    """
    Maps cohorts to reports, with primary/non-primary semantics.
    """
    __tablename__ = 'report_cohort_map'

    report_cohort_map_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    id = so.synonym('report_cohort_map_id')

    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'))
    dash_cohort_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('dash_cohort.dash_cohort_id'))
    primary_cohort: so.Mapped[bool] = so.mapped_column(sa.Boolean, default=False)

    cohort: so.Mapped['DashCohort'] = so.relationship(back_populates='in_reports', lazy="joined")
    report: so.Mapped['Report'] = so.relationship(back_populates='cohorts')

    measures = association_proxy("cohort", "measures")
    definition_count = association_proxy("cohort", "definition_count")

    @property
    def measure_count(self) -> int:
        if self.cohort:
            return sum(d.measure_count for d in self.cohort.definitions)
        return 0

    def __repr__(self):
        if self.cohort:
            return f"<ReportCohortMap cohort={self.cohort.dash_cohort_name!r} primary={self.primary_cohort}>"
        return super().__repr__()

    def _html_css_class(self) -> str:
        return "report-cohort"

    def _html_title(self) -> str:
        label = "Primary cohort" if self.primary_cohort else "Cohort"
        return f"{label}: {self.cohort.dash_cohort_name if self.cohort else self.dash_cohort_id}"

    def _html_header(self) -> dict[str, str]:
        hdr = {
            "Primary": "yes" if self.primary_cohort else "no",
        }

        if self.cohort:
            hdr["Cohort"] = self.cohort.dash_cohort_name
            hdr["Definitions"] = str(self.cohort.definition_count)
            hdr["Measures"] = str(self.measure_count)

        return hdr

    def _html_inner(self):
        blocks: list[object] = []

        if not self.cohort:
            blocks.append(RawHTML("<div class='muted'><i>Cohort not loaded</i></div>"))
            return blocks

        # Delegate to Dash_Cohort renderer
        blocks.append(self.cohort)

        return blocks
    
class Report(HTMLRenderable, Base):
    """Primary report class that holds the full report definition."""
    __tablename__ = 'report'

    report_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('report_id')

    report_name: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_short_name: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_description: so.Mapped[str] = so.mapped_column(sa.String(1000))
    report_create_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_edit_date: so.Mapped[date] = so.mapped_column(sa.DateTime, default=date.today)
    report_author: so.Mapped[str] = so.mapped_column(sa.String(250))
    report_owner: so.Mapped[str | None] = so.mapped_column(sa.String(250), nullable=True)

    cohorts: so.Mapped[list['ReportCohortMap']] = so.relationship(back_populates='report')
    indicators: so.Mapped[list['Indicator']] = so.relationship(
        secondary=report_indicator_map,
        back_populates="in_reports",
        lazy="selectin",
    )

    report_versions: so.Mapped[list["ReportVersion"]] = so.relationship(
        "ReportVersion",
        back_populates="report",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    denominator_measures = association_proxy("indicators", "denominator_measure")
    numerator_measures = association_proxy("indicators", "numerator_measure")

    @property
    def report_cohorts(self):
        return [c.cohort for c in self.cohorts]

    @property
    def indicator_measures(self):
        return list(set(self.numerator_measures + self.denominator_measures))

    @property
    def cohort_measures(self):
        return list(set(chain.from_iterable([c.measures for c in self.report_cohorts])))

    @property
    def report_measures(self):
        return sorted(
            set(self.numerator_measures + self.denominator_measures + self.cohort_measures),
            key=lambda x: x.id,
        )
    
    @property
    def members(self):
        return list(set(chain.from_iterable([c.members for c in self.report_cohorts])))

    def execute(self, db: so.Session, *, people: list[int] | None = None):
        """
        Execute all measures required by this report.
        """
        from .measure import MeasureExecutor

        executor = MeasureExecutor(db)
        for m in self.report_measures:
            executor.execute(m, people=people)

    @hybrid_property
    def version_string(self):
        if self.report_versions:
            return '; '.join(
                f'{rv.report_version_major}.{rv.report_version_minor} ({rv.report_version_label})'
                for rv in self.report_versions
            )
        return ""

    def __repr__(self):
        return f"<Report {self.report_short_name!r} ({len(self.indicators)} indicators)>"

    def _html_css_class(self) -> str:
        return "report"

    def _html_title(self) -> str:
        return f"Report: {self.report_name}"

    def _html_header(self) -> dict[str, str]:
        hdr = {
            "Short name": self.report_short_name,
            "Author": self.report_author,
        }

        if self.report_owner:
            hdr["Owner"] = self.report_owner

        if self.version_string:
            hdr["Versions"] = self.version_string

        return hdr

    def _html_inner(self):
        blocks: list[object] = []

        # Description
        if self.report_description:
            blocks.append(RawHTML(f"<div class='muted'>{esc(self.report_description)}</div>"))
        
        blocks.extend(self._html_exec_summary())
        
        # Indicators
        blocks.append(RawHTML("<div class='subquery-section-title'>Indicators</div>"))
        if self.indicators:
            blocks.extend(sorted(self.indicators))
        else:
            blocks.append(RawHTML("<div class='muted'><i>No indicators</i></div>"))

        return blocks
    
    def executable_status(self) -> ExecStatus:
        statuses: list[ExecStatus] = []

        # Indicator-level statuses
        for ind in self.indicators:
            statuses.append(ind.is_executable().status)

        # Dash cohort definition statuses
        for rc in self.cohorts:
            cohort = rc.cohort
            if not cohort:
                continue
            for d in cohort.definitions:
                statuses.append(d.is_executable().status)

        if ExecStatus.FAIL in statuses:
            return ExecStatus.FAIL
        if ExecStatus.WARN in statuses:
            return ExecStatus.WARN
        return ExecStatus.PASS
    

    def _html_exec_summary(self):
        blocks: list[object] = []

        # === Overall header ===
        overall = self.executable_status()
        blocks.append(RawHTML("<div class='subquery-section-title'>Executability Summary</div>"))
        blocks.append(
            RawHTML(
                f"<div style='margin-bottom:8px'>"
                f"<b>Overall report executability:</b> {exec_badge(overall)}</div>"
            )
        )

        # === Section 1: Dash cohorts ===
        headers = [
            "Cohort",
            "Definition",
            "Measure",
            "Status",
        ]

        cohort_rows = []

        for rc in self.cohorts:
            cohort = rc.cohort
            if not cohort:
                cohort_rows.append([
                    td("<i>Missing cohort</i>"), td(""), td(""), td(exec_badge(ExecStatus.FAIL))
                ])
                continue

            for d in cohort.definitions:
                check = d.is_executable()
                cohort_rows.append([
                    td(cohort.dash_cohort_name),
                    td(d.dash_cohort_def_name),
                    td(d.dash_cohort_measure.name if d.dash_cohort_measure else "<i>None</i>"),
                    td(exec_badge(check.status)),
                ])

        if len(cohort_rows) > 1:
            blocks.append(
                RawHTML(
                    table(
                        headers=headers,
                        rows=cohort_rows,
                        cls="concept-table compact"
                    )
                )
            )
        else:
            blocks.append(RawHTML("<div class='muted'><i>No dash cohorts</i></div>"))

        # === Section 2: Indicators ===
        blocks.append(RawHTML("<div class='subquery-section-title'>Indicators</div>"))

        headers = [
            "Indicator",
            "Numerator",
            "Num",
            "Denominator",
            "Den",
            "Indicator Status",
        ]

        indicator_rows = []

        for ind in sorted(self.indicators):
            check = ind.is_executable()

            indicator_rows.append([
                td(ind.indicator_description),
                td(ind.numerator_measure.name),
                td(exec_badge(check.numerator.status)),
                td(ind.denominator_measure.name),
                td(exec_badge(check.denominator.status)),
                td(exec_badge(check.status)),
            ])

        if len(indicator_rows) > 1:
            blocks.append(
                RawHTML(
                    table(
                        headers=headers,
                        rows=indicator_rows,
                        cls="concept-table compact"
                    )
                )
            )
        else:
            blocks.append(RawHTML("<div class='muted'><i>No indicators</i></div>"))

        return blocks
    

    
class ReportVersion(HTMLRenderable, Base):
    """Report versioning table. There should be only one current version per report."""
    __tablename__ = 'report_version'

    report_version_id: so.Mapped[int] = so.mapped_column(sa.Integer, primary_key=True)
    id = so.synonym('report_version_id')

    report_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('report.report_id'))

    report_version_major: so.Mapped[int] = so.mapped_column(sa.Integer)
    report_version_minor: so.Mapped[int] = so.mapped_column(sa.Integer)
    report_version_label: so.Mapped[str] = so.mapped_column(sa.String(50))
    report_version_date: so.Mapped[date] = so.mapped_column(sa.DateTime)
    report_status: so.Mapped['ReportStatus'] = so.mapped_column(sa.Enum(ReportStatus))

    report: so.Mapped["Report"] = so.relationship(back_populates='report_versions')

    def __repr__(self):
        return (
            f"<ReportVersion {self.report_version_major}."
            f"{self.report_version_minor} "
            f"[{self.report_version_label}] {self.report_status.value}>"
        )

    def _html_css_class(self) -> str:
        return "report-version"

    def _html_title(self) -> str:
        return f"Version {self.report_version_major}.{self.report_version_minor}"

    def _html_header(self) -> dict[str, str]:
        return {
            "Label": self.report_version_label,
            "Status": self.report_status.value,
            "Date": str(self.report_version_date),
        }

    def _html_inner(self):
        return []