from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.sql import Select, CompoundSelect
from sqlalchemy.engine import Row as SARow
from typing import TypeAlias, Optional, Sequence, Any, cast, Callable

from orm_loader.helpers import Base
from .subquery import Subquery
from ..core.executability import MeasureExecCheck, ExecStatus
from ..core import RuleCombination
from ..core.utils import HTMLRenderable, RawHTML, table, td, esc, HTMLChild, sql_block
Row = SARow[Any]

SQLQuery: TypeAlias = Select | CompoundSelect

COMBINATION_SQL = {
    RuleCombination.rule_or: sa.union_all,
    RuleCombination.rule_and: sa.intersect_all,
    RuleCombination.rule_except: sa.except_all,
}


class Measure(HTMLRenderable, Base):
    __tablename__ = "measure"
    __allow_unmapped__ = True 
    measure_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    id = so.synonym("measure_id")

    name: so.Mapped[str] = so.mapped_column(sa.String(250))
    combination: so.Mapped[RuleCombination] = so.mapped_column(sa.Enum(RuleCombination))

    subquery_id: so.Mapped[int | None] = so.mapped_column(sa.ForeignKey("subquery.subquery_id"), nullable=True)
    person_ep_override: so.Mapped[bool] = so.mapped_column(sa.Boolean, default=False)

    subquery: so.Mapped[Subquery | None] = so.relationship("Subquery", lazy="joined")

    child_links: so.Mapped[list["MeasureRelationship"]] = so.relationship(
        "MeasureRelationship",
        foreign_keys="MeasureRelationship.parent_measure_id",
        lazy="selectin",
    )

    parent_links: so.Mapped[list["MeasureRelationship"]] = so.relationship(
        "MeasureRelationship",
        foreign_keys="MeasureRelationship.child_measure_id",
        lazy="selectin",
    )

    _members: Sequence[Row]

    @so.reconstructor
    def init_on_load(self) -> None:
        self._members = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.init_on_load()

    @property
    def members(self) -> Sequence[Row]:
        return self._members

    @property
    def children(self) -> list["Measure"]:
        return [rel.child for rel in self.child_links]
    
    def __repr__(self) -> str:
        header = f"<Measure {self.name!r} op={self.combination.value}>"

        if self.subquery:
            return f"{header}\n  Subquery: {self.subquery.name}"

        if not self.children:
            return header + "\n  (no children)"

        kids = "\n".join(f"  - {c.name} (#{c.measure_id})" for c in self.children)
        return f"{header}\n{kids}"
    

    def is_executable(self) -> MeasureExecCheck:
        """
        Check whether this measure can successfully generate SQL.

        PASS  = all variants compile
        WARN  = at least one compiles, at least one fails
        FAIL  = none compile
        """
        # Special case: measure_id = 0 (full cohort)
        if self.measure_id == 0:
            return MeasureExecCheck(
                status=ExecStatus.PASS,
                ok_variants=["FULL_COHORT"],
                failed_variants={},
            )

        compiler = MeasureSQLCompiler(self)

        checks: list[tuple[str, Callable]] = [
            ("ANY", compiler.sql_any),
            ("FIRST", compiler.sql_first),
            ("UNDATED", compiler.sql_undated),
        ]

        ok: list[str] = []
        failed: dict[str, str] = {}

        for label, fn in checks:
            try:
                stmt = fn()
                # Force compilation (no execution)
                _ = self._render_sql(stmt)
                ok.append(label)
            except Exception as e:
                failed[label] = str(e)

        if ok and not failed:
            status = ExecStatus.PASS
        elif ok and failed:
            status = ExecStatus.WARN
        else:
            status = ExecStatus.FAIL

        return MeasureExecCheck(
            status=status,
            ok_variants=ok,
            failed_variants=failed,
        )

    def _html_css_class(self) -> str:
        return "measure"

    def _html_title(self) -> str:
        return f"Measure: {self.name}"

    def _html_header(self) -> dict[str, object]:
        if self.measure_id == 0:
            return {
                "ID": self.measure_id,
                "Name": self.name,
                "Combination": RawHTML(
                    f"<span class='badge neutral'>FULL COHORT</span>"
                ),
                "Subquery": RawHTML("<i>Report cohort (no filtering)</i>"),
                "Children": len(self.children),
                "Episode override": "n/a",
            }
        return {
            "ID": self.measure_id,
            "Name": self.name,
            "Combination": RawHTML(
                f"<span class='badge {self.combination.value}'>"
                f"{esc(self.combination.value.upper())}</span>"
            ),
            "Subquery": self.subquery.name if self.subquery else "",
            "Children": len(self.children),
            "Episode override": "yes" if self.person_ep_override else "no",
        }

    def _html_inner(self):
        blocks: list[HTMLChild] = []

        if self.measure_id == 0:
            blocks.append(
                RawHTML(
                    "<div class='muted'>"
                    "<i>This measure represents the full report cohort (no filtering applied).</i>"
                    "</div>"
                )
            )
            return blocks

        # Subquery (leaf)
        if self.subquery:
            blocks.append(RawHTML("<div class='subquery-section-title'>Subquery</div>"))
            blocks.append(self.subquery)

        # Children (composite)
        if self.children:
            blocks.append(RawHTML("<div class='subquery-section-title'>Children</div>"))
            blocks.extend(self.children)
        elif not self.subquery:
            blocks.append(RawHTML("<div class='muted'><i>No children</i></div>"))

        # SQL previews (ANY / FIRST / UNDATED)
        try:
            compiler = MeasureSQLCompiler(self)

            blocks.append(RawHTML("<div class='subquery-section-title'>SQL preview</div>"))

            for label, fn in (
                ("ANY", compiler.sql_any),
                ("FIRST", compiler.sql_first),
                ("UNDATED", compiler.sql_undated),
            ):
                try:
                    blocks.append(
                        RawHTML(
                            f"<div style='margin-top:6px;font-weight:bold'>{esc(label)}</div>"
                        )
                    )
                    blocks.append(sql_block(fn()))
                except Exception as e:
                    blocks.append(
                        RawHTML(
                            f"<div class='sql-error'>"
                            f"{esc(label)} SQL preview failed: {esc(e)}</div>"
                        )
                    )

        except Exception as e:
            blocks.append(
                RawHTML(
                    f"<div class='sql-error'>SQL preview setup failed: {esc(e)}</div>"
                )
            )

        return blocks

    def _render_sql(self, stmt: sa.ClauseElement) -> str:
        try:
            from sqlalchemy.dialects import postgresql
            dialect = postgresql.dialect()
        except Exception:
            dialect = sa.create_engine("sqlite://").dialect

        compiled = stmt.compile(
            dialect=dialect,
            compile_kwargs={"literal_binds": True},
        )
        return str(compiled)

    

class MeasureRelationship(HTMLRenderable, Base):
    """Association object for n-m mapping between parent and child measures.
    
    This can't be achieved via association table alone, despite lack of additional data, due to the self-referential nature of this relationship.
    """
    __tablename__ = 'measure_relationship'
    parent_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True) 
    child_measure_id: so.Mapped[Optional[int]] = so.mapped_column(sa.ForeignKey('measure.measure_id'), primary_key=True) 

    parent: so.Mapped["Measure"] = so.relationship("Measure", foreign_keys=[parent_measure_id], back_populates='child_links')
    child: so.Mapped["Measure"] = so.relationship("Measure", foreign_keys=[child_measure_id], back_populates='parent_links')


    
    def __repr__(self) -> str:
        return f"<MeasureRelationship parent={self.parent_measure_id} child={self.child_measure_id}>"

    def _html_css_class(self) -> str:
        return "measure"

    def _html_title(self) -> str:
        return "Measure Relationship"

    def _html_header(self) -> dict[str, object]:
        return {
            "Parent ID": self.parent_measure_id,
            "Child ID": self.child_measure_id,
            "Parent": self.parent.name if self.parent else "",
            "Child": self.child.name if self.child else "",
        }

    def _html_inner(self):
        rows = []

        if self.parent:
            rows.append(
                ["Parent", self.parent.measure_id, self.parent.name, self.parent.combination.value]
            )

        if self.child:
            rows.append(
                ["Child", self.child.measure_id, self.child.name, self.child.combination.value]
            )

        if not rows:
            return [RawHTML("<div class='muted'><i>Relationship not fully loaded</i></div>")]

        return [
            RawHTML(
                table(
                    headers=["Role", "ID", "Name", "Combination"],
                    rows=[[td(r[0]), td(r[1]), td(r[2]), td(r[3])] for r in rows],
                    cls="concept-table compact",
                )
            )
        ]


class MeasureSQLCompiler:
    def __init__(self, measure: Measure):
        self.measure = measure

    def _combine(self, parts: list[SQLQuery]) -> SQLQuery:
        combiner = COMBINATION_SQL[self.measure.combination]
        return combiner(*parts)

    def sql_any(self, *, ep_override: bool = False) -> SQLQuery:
        if self.measure.subquery is None and not self.measure.children:
            raise ValueError(f"Measure {self.measure.measure_id} has no subquery and no children")
        ep_override = ep_override or self.measure.person_ep_override

        if self.measure.subquery:
            return self.measure.subquery.get_subquery_any(ep_override=ep_override)

        children = [MeasureSQLCompiler(c) for c in self.measure.children]

        if self.measure.combination is RuleCombination.rule_or:
            return self._combine([c.sql_any(ep_override=ep_override) for c in children])
        
        # AND / EXCEPT collapse to FIRST logic
        return self.sql_first(ep_override=ep_override)

    def sql_undated(self, *, ep_override: bool = False) -> SQLQuery:
        if self.measure.subquery is None and not self.measure.children:
            raise ValueError(f"Measure {self.measure.measure_id} has no subquery and no children")
        ep_override = ep_override or self.measure.person_ep_override

        if self.measure.subquery:
            return self.measure.subquery.get_subquery_undated(ep_override=ep_override)

        children = [MeasureSQLCompiler(c) for c in self.measure.children]
        return self._combine([c.sql_undated(ep_override=ep_override) for c in children])

    def sql_first(self, *, ep_override: bool = False) -> SQLQuery:
        if self.measure.subquery is None and not self.measure.children:
            raise ValueError(f"Measure {self.measure.measure_id} has no subquery and no children")
        ep_override = ep_override or self.measure.person_ep_override

        if self.measure.subquery:
            return self.measure.subquery.get_subquery_first(ep_override=ep_override)

        children = [MeasureSQLCompiler(c) for c in self.measure.children]
        earliest = [c.sql_any(ep_override=ep_override).subquery() for c in children]

        lhs = earliest[0]
        date_cols = [lhs.c.measure_date]

        for rhs in earliest[1:]:
            lhs = lhs.join(rhs, lhs.c.measure_resolver == rhs.c.measure_resolver)
            date_cols.append(rhs.c.measure_date)

        return sa.select(
            lhs.c.person_id,
            lhs.c.episode_id,
            lhs.c.measure_resolver,
            sa.func.greatest(*date_cols).label("measure_date"),
        ).select_from(lhs)
    
class MeasureExecutor:
    def __init__(self, db):
        self.db = db
        self._cache: dict[int, Sequence[Row]] = {}

    def execute(
        self,
        measure: Measure,
        *,
        ep_override: bool = False,
        people: list[int] | None = None,
        force_refresh: bool = False,
    ) -> Sequence[Row]:
        if not force_refresh and measure.measure_id in self._cache:
            rows = self._cache[measure.measure_id]
            measure._members = rows
            return rows

        sql = MeasureSQLCompiler(measure).sql_any(ep_override=ep_override)

        if people:
            sq = sql.subquery()
            sql = sa.select(sq).where(sq.c.person_id.in_(people))

        rows = self.db.execute(sql).all()
        rows_typed = cast(Sequence[Row], rows)
        self._cache[measure.measure_id] = rows_typed
        measure._members = rows_typed
        return rows_typed