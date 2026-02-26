from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from typing import Optional, Sequence, cast, Callable
from orm_loader.helpers import Base
from .typing import Row, SQLQuery, COMBINATION_SQL
from .subquery import Subquery
from ..core.executability import MeasureExecCheck, ExecStatus
from ..core import RuleCombination
from ..core.utils import HTMLRenderable, RawHTML, table, td, esc, HTMLChild, sql_block

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

    _members: Sequence[Row] | None = None

    # @so.reconstructor
    # def init_on_load(self) -> None:
    #     self._members = ()

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.init_on_load()

    @property
    def members(self) -> Sequence[Row]:
        if self._members is None:
            raise RuntimeError(
                f"Measure {self.measure_id} ('{self.name}') has not been executed yet. "
                "Call MeasureExecutor.execute(measure) before accessing members."
            )
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

    def _combine(self, parts: Sequence[SQLQuery]) -> SQLQuery:
        combiner = COMBINATION_SQL[self.measure.combination]
        return combiner(*parts)
    
    def _normalise(self, q: SQLQuery) -> sa.Subquery:
        sq = q.subquery()

        required = {"person_id", "episode_id", "measure_resolver", "measure_date"}
        missing = required - set(sq.c.keys())

        if missing:
            raise ValueError(
                f"Normalisation failed: query is missing columns {missing}. "
                f"Available columns: {list(sq.c.keys())}"
            )
        return sa.select(
            sq.c.person_id.label("person_id"),
            sq.c.episode_id.label("episode_id"),
            sq.c.measure_resolver.label("measure_resolver"),
            sq.c.measure_date.label("measure_date")
        ).subquery()

    def sql_any(self, *, ep_override: bool = False) -> SQLQuery:
        if self.measure.subquery is None and not self.measure.children:
            raise ValueError(f"Measure {self.measure.measure_id} has no subquery and no children")
        ep_override = ep_override or self.measure.person_ep_override

        if self.measure.subquery:
            return self.measure.subquery.get_subquery_any(ep_override=ep_override)

        children = [MeasureSQLCompiler(c) for c in self.measure.children]

        if self.measure.combination is RuleCombination.rule_or:
            #return self._combine([c.sql_any(ep_override=ep_override) for c in children])
            parts = [self._normalise(c.sql_any(ep_override=ep_override)).select() for c in children]
            return self._combine(parts)
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
        earliest = [
            self._normalise(c.sql_any(ep_override=ep_override))
            for c in children
        ]        

        # earliest = [c.sql_any(ep_override=ep_override).subquery() for c in children]
        start = earliest[0]
        lhs = start
        date_cols = [start.c.measure_date]

        date_cols = [lhs.c.measure_date]

        for rhs in earliest[1:]:
            lhs = lhs.join(
                rhs,
                start.c.measure_resolver == rhs.c.measure_resolver
            )
            date_cols.append(rhs.c.measure_date)

        return sa.select(
            start.c.person_id,
            start.c.episode_id,
            start.c.measure_resolver,
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
        if measure.measure_id == 0:
            raise RuntimeError(
                "Measure ID = 0 represents FULL COHORT and must be resolved at the Report level. "
                "Call Report.execute() instead."
            )
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