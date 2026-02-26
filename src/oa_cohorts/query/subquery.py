from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from .query_rule import QueryRule
from ..core import RuleTarget, RuleTemporality
from ..measurables import get_measurable_registry, MeasurableBase
from ..core.html_utils import HTMLRenderable, RawHTML, table, td, esc

from sqlalchemy.sql import Select, CompoundSelect
from typing import TypeAlias

SQLQuery: TypeAlias = Select | CompoundSelect

subquery_rule_map = sa.Table(
    "subquery_rule_map",
    Base.metadata,
    sa.Column("subquery_id", sa.ForeignKey("subquery.subquery_id")),
    sa.Column("query_rule_id", sa.ForeignKey("query_rule.query_rule_id")),
)

class Subquery(HTMLRenderable, Base):
    __tablename__ = "subquery"

    subquery_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    target: so.Mapped[RuleTarget] = so.mapped_column(sa.Enum(RuleTarget))
    temporality: so.Mapped[RuleTemporality] = so.mapped_column(sa.Enum(RuleTemporality))
    name: so.Mapped[str] = so.mapped_column(sa.String)
    short_name: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    rules: so.Mapped[list["QueryRule"]] = so.relationship(
        secondary=subquery_rule_map,
        lazy="selectin",
    )

    def sql_any(self, *, ep_override: bool = False) -> SQLQuery:
        return self.get_subquery_any(ep_override=ep_override)

    def sql_first(self, *, ep_override: bool = False) -> SQLQuery:
        return self.get_subquery_first(ep_override=ep_override)

    def sql_undated(self, *, ep_override: bool = False) -> SQLQuery:
        return self.get_subquery_undated(ep_override=ep_override)

    def measurable_cls(self) -> type[MeasurableBase]:
        registry: dict[RuleTarget, type[MeasurableBase]] = get_measurable_registry()
        try:
            return registry[self.target]
        except KeyError:
            raise KeyError(f"No measurable registered for target {self.target}")

    def filter_field(self, measurable: type[MeasurableBase]) -> sa.ColumnElement[bool]:
        """
        Choose concept vs numeric value column depending on rule types.
        """
        use_numeric = any(r.requires_numeric for r in self.rules)
        use_string = any(r.requires_string for r in self.rules) 
        specs = measurable.__bound_measurable__

        if use_numeric:
            col = specs.value_numeric_col
            kind = "numeric"
        elif use_string:
            col = specs.value_string_col
            kind = "string"
        else:
            col = specs.value_concept_col
            kind = "concept"

        if col is None:
            raise ValueError(
                f"{measurable.__name__} does not expose required {kind} value column "
                f"for subquery {self.subquery_id}"
            )

        return col

    def where_clause(self) -> sa.ColumnElement[bool]:
        measurable = self.measurable_cls()
        field = self.filter_field(measurable)

        if not self.rules:
            raise ValueError(f"Subquery {self.subquery_id} has no rules")

        clauses: list[sa.ColumnElement[bool]] = [
            rule.get_filter_details(field)
            for rule in self.rules
        ]
        return sa.and_(*clauses)

    def filter_table(self, *, ep_override: bool = False) -> tuple[sa.ColumnElement, ...]:
        measurable = self.measurable_cls()
        return measurable.filter_table(ep_override=ep_override)

    def filter_table_dated(self, *, ep_override: bool = False) -> tuple[sa.ColumnElement, ...]:
        measurable = self.measurable_cls()
        return measurable.filter_table_dated(self.temporality, ep_override=ep_override)

    def select(self, *, ep_override: bool = False) -> sa.Select:
        return (
            sa.select(*self.base_selectables(ep_override=ep_override))
            .where(self.where_clause())
        )

    def base_selectables(self, *, ep_override: bool = False):
        measurable = self.measurable_cls()
        return measurable.filter_table_dated(
            temporality=self.temporality,
            ep_override=ep_override,
        )
    
    def get_subquery_any(self, *, ep_override: bool = False) -> SQLQuery:
        """
        Return all qualifying rows with dates (UNION ALL over rules).
        Used when measure combination is OR.
        """
        if not self.rules:
            raise ValueError(f"Subquery {self.subquery_id} has no rules")

        measurable = self.measurable_cls()
        field = self.filter_field(measurable)

        selects: list[sa.Select] = [
            sa.select(*self.filter_table_dated(ep_override=ep_override)).where(
                rule.get_filter_details(field)
            )
            for rule in self.rules
        ]

        return sa.union_all(*selects)

    def get_subquery_undated(self, *, ep_override: bool = False) -> SQLQuery:
        """
        Return qualifying rows WITHOUT dates (UNION ALL over rules).
        Used as intermediate for AND logic across subqueries.
        """
        if not self.rules:
            raise ValueError(f"Subquery {self.subquery_id} has no rules")

        measurable = self.measurable_cls()
        field = self.filter_field(measurable)

        selects: list[sa.Select] = [
            sa.select(*self.filter_table(ep_override=ep_override)).where(
                rule.get_filter_details(field)
            )
            for rule in self.rules
        ]

        return sa.union_all(*selects)

    def get_subquery_first(self, *, ep_override: bool = False) -> SQLQuery:
        """
        Return earliest qualifying date per person/episode/measure_resolver.
        Used for AND logic across child measures.
        """
        sq = self.get_subquery_any(ep_override=ep_override).subquery()

        return (
            sa.select(
                sq.c.person_id,
                sq.c.episode_id,
                sq.c.measure_resolver,
                sa.func.min(sq.c.measure_date).label("measure_date"),
            )
            .group_by(
                sq.c.person_id,
                sq.c.episode_id,
                sq.c.measure_resolver,
            )
        )

    def __repr__(self) -> str:
        header = f"<Subquery {self.name!r} target={self.target.value} temporal={self.temporality.value}>"

        if not self.rules:
            return header + "\n  (no rules)"

        rules_repr = "\n".join(f"  - {r!r}" for r in self.rules)
        return f"{header}\n{rules_repr}"

    def _html_css_class(self) -> str:
        return "subquery"

    def _html_title(self) -> str:
        return f"Subquery: {self.name}"

    def _html_header(self) -> dict[str, str]:
        return {
            "ID": str(self.subquery_id),
            "Name": self.name,
            "Short name": self.short_name or "",
            "Target": self.target.value,
            "Temporality": self.temporality.value,
            "Rule count": str(len(self.rules)),
        }

    def _html_inner(self):
        blocks: list[object] = []

        # --- Rules ---
        blocks.append(RawHTML("<div class='subquery-section-title'>Rules</div>"))
        if self.rules:
            blocks.extend(self.rules)
        else:
            blocks.append(RawHTML("<div class='muted'><i>No rules</i></div>"))

        # --- SQL previews ---
        blocks.append(RawHTML("<div class='subquery-section-title'>SQL preview</div>"))

        for label, fn in (
            ("ANY", self.sql_any),
            ("FIRST", self.sql_first),
            ("UNDATED", self.sql_undated),
        ):
            try:
                sql = self._render_sql(fn())
                blocks.append(
                    RawHTML(
                        f"<div style='margin-top:6px;font-weight:bold'>{esc(label)}</div>"
                        f"<pre class='sql-preview'>{esc(sql)}</pre>"
                    )
                )
            except Exception as e:
                blocks.append(
                    RawHTML(
                        f"<div style='margin-top:6px;font-weight:bold'>{esc(label)}</div>"
                        f"<div class='sql-error'>SQL preview failed: {esc(e)}</div>"
                    )
                )

        return blocks

    def _render_sql(self, stmt: sa.ClauseElement) -> str:
        """
        Render SQLAlchemy Select / CompoundSelect / ClauseElement to SQL string.
        """
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