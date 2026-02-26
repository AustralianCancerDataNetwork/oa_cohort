from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Sequence
from ..query.typing import Row
from ..query.report import Report
from .person_demography import DemographyFilter
from .query_plan import QueryPlan, MeasureNode
from .report_payload import ReportBundle, PivotCohortRow, PivotIndicatorRow
from .pivot_queries import (
    build_pivot_indicators, 
    build_pivot_cohort, 
    build_cohort_demography,
    build_report_payload
)

class ReportRunner:
    """
    Orchestrates execution and materialisation of a Report into a transport payload.
    """

    def __init__(self, db: so.Session, report: Report):
        self.db = db
        self.report = report
        self._demography_rows: Sequence[Row] | None = None
        self._cohort_rows: list[PivotCohortRow] | None = None
        self._indicator_rows: list[PivotIndicatorRow] | None = None
        self._plans: dict[int, QueryPlan] = {}

    def execute(self, *, people: list[int] | None = None, strict: bool = True) -> None:
        """
        Executes all measures needed for this report.
        """
        # 1. Preflight compile
        for m in self.report.report_measures:
            plan = QueryPlan(root=MeasureNode(m))
            _ = plan.root.sql_any()

        # 2. Execute actual measures
        self.report.execute(self.db, people=people, strict=strict)

    def build_plans(self) -> dict[int, QueryPlan]:
        if not self._plans:
            for m in self.report.report_measures:
                self._plans[m.measure_id] = QueryPlan(root=MeasureNode(m))
        return self._plans

    def all_plan_measures(self) -> set[int]:
        ids = set()
        for m in self.report.report_measures:
            plan = QueryPlan(root=MeasureNode(m))
            for mm in plan.root.iter_measures():
                ids.add(mm.measure_id)
        return ids

    def collect_demography(self):
        self.report.assert_executed()

        cohort_person_ids = [m.person_id for m in self.report.members]

        demo_filter = DemographyFilter()

        stmt = demo_filter.to_rows_stmt(
            restrict_to_person_ids=cohort_person_ids
        )

        self._demography_rows = self.db.execute(stmt).all()
        return self._demography_rows

    def collect_pivot_cohort(self):
        """
        Build cohort × measure pivot rows from executed Measure.members.
        """
        self._cohort_rows = build_pivot_cohort(self.report)
        return self._cohort_rows

    def collect_pivot_indicators(self):
        """
        Build indicator pivot rows from executed Measure.members.
        """
        self._indicator_rows = build_pivot_indicators(self.report)
        return self._indicator_rows
    
    def build_bundle(self) -> ReportBundle:
        if self._demography_rows is None:
            self.collect_demography()

        if self._cohort_rows is None:
            self.collect_pivot_cohort()

        if self._indicator_rows is None:
            self.collect_pivot_indicators()

        assert self._demography_rows is not None
        assert self._cohort_rows is not None
        assert self._indicator_rows is not None

        return ReportBundle(
            report=build_report_payload(self.report),
            cohort_demography=build_cohort_demography(self._demography_rows),
            pivot_cohort=self._cohort_rows,
            pivot_indicators=self._indicator_rows,
        )

    def to_json(self) -> dict:
        bundle = self.build_bundle()
        return bundle.model_dump(mode="json")