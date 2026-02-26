from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Sequence
from orm_loader.helpers import Base
from ..core import RuleTemporality
from ..core.html_utils import HTMLRenderable, RawHTML, esc
from .report import Report, report_indicator_map
from .measure import Measure, MeasureMember
from .typing import Row
from ..core.executability import ExecStatus, IndicatorExecCheck

class Indicator(HTMLRenderable, Base):
    __tablename__ = 'indicator'

    indicator_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym('indicator_id')

    indicator_description: so.Mapped[str] = so.mapped_column(sa.String(250))
    indicator_reference: so.Mapped[str | None] = so.mapped_column(sa.String(100), nullable=True)

    numerator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    numerator_label: so.Mapped[str] = so.mapped_column(sa.String(100))
    denominator_measure_id: so.Mapped[int] = so.mapped_column(sa.ForeignKey('measure.measure_id'))
    denominator_label: so.Mapped[str] = so.mapped_column(sa.String(100))

    temporal_early: so.Mapped[RuleTemporality | None] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)
    temporal_late: so.Mapped[RuleTemporality | None] = so.mapped_column(sa.Enum(RuleTemporality), nullable=True)

    temporal_min: so.Mapped[int | None] = so.mapped_column(sa.Integer(), nullable=True)
    temporal_min_units: so.Mapped[str | None] = so.mapped_column(sa.String(20), nullable=True)

    temporal_max: so.Mapped[int | None] = so.mapped_column(sa.Integer(), nullable=True)
    temporal_max_units: so.Mapped[str | None] = so.mapped_column(sa.String(20), nullable=True)

    benchmark: so.Mapped[int | None] = so.mapped_column(sa.Integer(), nullable=True)
    benchmark_unit: so.Mapped[str | None] = so.mapped_column(sa.String(20), nullable=True)

    numerator_measure: so.Mapped['Measure'] = so.relationship(
        foreign_keys=[numerator_measure_id], lazy="joined"
    )
    denominator_measure: so.Mapped['Measure'] = so.relationship(
        foreign_keys=[denominator_measure_id], lazy="joined"
    )

    in_reports: so.Mapped[list['Report']] = so.relationship(
        secondary=report_indicator_map,
        back_populates="indicators"
    )

    def execute(self, db: so.Session, *, people: list[int] | None = None):
        """
        Execute both numerator and denominator measures for this indicator.
        """
        from .measure import MeasureExecutor

        executor = MeasureExecutor(db)

        executor.execute(
            self.denominator_measure,
            people=people,
        )

        executor.execute(
            self.numerator_measure,
            people=people,
        )

    @property
    def numerator_members(self) -> Sequence[MeasureMember]:
        """
        Members of the numerator cohort (delegates to numerator measure).
        Returns only those members who are also in the denominator cohort, as per indicator definition
        (i.e. I do not care about the numerator event for members not in the denominator).
        
        Assumes the numerator measure has been executed.
        """
        num = set(self.numerator_measure.members)
        den = set(self.denominator_measure.members)
        return list(num & den)

    @property
    def denominator_members(self) -> Sequence[MeasureMember]:
        """
        Members of the denominator cohort (delegates to denominator measure).
        Assumes the denominator measure has been executed.
        """
        return self.denominator_measure.members

    def __lt__(self, other):
        if self.indicator_id != other.indicator_id:
            return self.indicator_id < other.indicator_id
        return self.indicator_description < other.indicator_description

    def __repr__(self):
        i = f'({self.id}) {self.indicator_description}'
        if self.indicator_reference:
            i += f' [{self.indicator_reference}]'
        n = f'\tNumerator: {self.numerator_measure}'
        d = f'\tDenominator: {self.denominator_measure}'
        return f'{i}\n{n}\n{d}'

    def _html_css_class(self) -> str:
        return "indicator"

    def _html_title(self) -> str:
        return f"Indicator: {self.indicator_description}"

    def _html_header(self) -> dict[str, str]:
        hdr: dict[str, str] = {
            "ID": str(self.indicator_id),
        }

        if self.indicator_reference:
            hdr["Reference"] = self.indicator_reference

        if self.benchmark is not None:
            unit = self.benchmark_unit or ""
            hdr["Benchmark"] = f"{self.benchmark} {unit}".strip()

        # Temporal constraints summary
        temporal_bits = []
        if self.temporal_min is not None:
            temporal_bits.append(f"≥ {self.temporal_min} {self.temporal_min_units or ''}".strip())
        if self.temporal_max is not None:
            temporal_bits.append(f"≤ {self.temporal_max} {self.temporal_max_units or ''}".strip())
        if self.temporal_early:
            temporal_bits.append(f"Early: {self.temporal_early.value}")
        if self.temporal_late:
            temporal_bits.append(f"Late: {self.temporal_late.value}")

        if temporal_bits:
            hdr["Temporal"] = " / ".join(temporal_bits)

        return hdr

    def _html_inner(self):
        blocks: list[object] = []

        # Numerator
        blocks.append(RawHTML("<div class='subquery-section-title'>Numerator</div>"))
        blocks.append(RawHTML(f"<div class='muted'>{esc(self.numerator_label)}</div>"))
        blocks.append(self.numerator_measure)

        # Denominator
        blocks.append(RawHTML("<div class='subquery-section-title'>Denominator</div>"))
        blocks.append(RawHTML(f"<div class='muted'>{esc(self.denominator_label)}</div>"))
        blocks.append(self.denominator_measure)

        return blocks
    
    def is_executable(self) -> IndicatorExecCheck:
        """
        Indicator is executable iff BOTH numerator and denominator are executable.

        PASS = both PASS
        WARN = at least one WARN, none FAIL
        FAIL = either FAIL
        """
        num_check = self.numerator_measure.is_executable()
        den_check = self.denominator_measure.is_executable()

        statuses = {num_check.status, den_check.status}

        if ExecStatus.FAIL in statuses:
            status = ExecStatus.FAIL
        elif ExecStatus.WARN in statuses:
            status = ExecStatus.WARN
        else:
            status = ExecStatus.PASS

        return IndicatorExecCheck(
            status=status,
            numerator=num_check,
            denominator=den_check,
        )
    