from __future__ import annotations
import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base
from omop_alchemy.cdm.model import Concept
from ..measurables import get_measurable_registry
from ..core import RuleMatcher, ThresholdDirection, RuleTarget
from ..core.html_utils import HTMLRenderable, RawHTML, td, table, render_sql, esc
from .phenotype import Phenotype

class QueryRule(Base, HTMLRenderable):
    __tablename__ = "query_rule"

    query_rule_id: so.Mapped[int] = so.mapped_column(primary_key=True)
    matcher: so.Mapped[RuleMatcher] = so.mapped_column(sa.Enum(RuleMatcher))
    concept_id: so.Mapped[int] = so.mapped_column(
        sa.ForeignKey("concept.concept_id"), nullable=True, index=True
    )
    notes: so.Mapped[str | None] = so.mapped_column(sa.String, nullable=True)

    # scalar support
    scalar_threshold: so.Mapped[int | None] = so.mapped_column(sa.Integer, nullable=True)
    threshold_direction: so.Mapped[ThresholdDirection | None] = so.mapped_column(sa.Enum(ThresholdDirection), nullable=True)    
    threshold_comparator: so.Mapped[RuleTarget | None] = so.mapped_column(sa.Enum(RuleTarget), nullable=True)

    # phenotype support
    phenotype_id: so.Mapped[int | None] = so.mapped_column(
        sa.ForeignKey("phenotype.phenotype_id"), nullable=True, index=True
    )
    __mapper_args__ = {
        "polymorphic_on": matcher,
    }

    # relationships
    concept: so.Mapped[Concept | None] = so.relationship(Concept,lazy="joined")
    phenotype: so.Mapped[Phenotype | None] = so.relationship(Phenotype,lazy="joined")

    @property
    def requires_numeric(self) -> bool:
        return self.matcher == RuleMatcher.scalar

    @property
    def requires_string(self) -> bool:
        return self.matcher == RuleMatcher.substring

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        raise NotImplementedError("get_filter_details must be implemented on subclasses")
    
    def sql_preview(self, field: sa.ColumnElement) -> str:
        """
        Return a SQL WHERE fragment representing this rule.
        """
        expr = self.get_filter_details(field)
        return render_sql(expr)


    def __lt__(self, other) -> bool:
        if self.query_rule_id != other.query_rule_id:
            return self.query_rule_id < other.query_rule_id
        return self.concept_id < other.concept_id

    def __repr__(self) -> str:
        parts = [f"id={self.query_rule_id}", self.matcher.value]

        if self.concept_id:
            parts.append(f"concept={self.concept_id}")

        if self.scalar_threshold is not None:
            if self.threshold_direction is None:
                raise ValueError(f"Rule {self.query_rule_id} has scalar_threshold but no threshold_direction")
            parts.append(f"{self.threshold_direction.value}{self.scalar_threshold}")
            if self.threshold_comparator:
                parts.append(f"on={self.threshold_comparator.value}")

        if self.phenotype_id is not None:
            parts.append(f"phenotype={self.phenotype.phenotype_name if self.phenotype else self.phenotype_id}")

        if self.notes:
            parts.append(f"note={self.notes!r}")

        return f"<{self.__class__.__name__} {' '.join(parts)}>"


    def _html_css_class(self) -> str:
        return "queryrule"
    
    def _html_title(self) -> str:
        return f"Rule #{self.query_rule_id} — {self.matcher.value}"

    def _html_header(self) -> dict[str, str]:
        hdr: dict[str, str] = {
            "Type": self.matcher.value,
        }

        if self.concept and self.concept_id:
            hdr["Concept"] = f"{self.concept.concept_name} ({self.concept_id})"

        if self.phenotype:
            hdr["Phenotype"] = self.phenotype.phenotype_name

        if self.scalar_threshold is not None and self.threshold_direction is not None:
            hdr["Threshold"] = f"{self.threshold_direction.value}{self.scalar_threshold}"

        if self.threshold_comparator:
            hdr["On"] = self.threshold_comparator.value

        if self.notes:
            hdr["Notes"] = self.notes

        return hdr

    def _html_inner(self):
        blocks: list[object] = []

        # Optional SQL preview (WHERE clause only)
        try:
            blocks.append(RawHTML("<div class='subquery-section-title'>SQL filter</div>"))

            # This is a WHERE fragment, not a full query
            # Caller (Subquery) chooses the field
            blocks.append(
                RawHTML(
                    "<div class='muted'>This rule contributes a WHERE clause fragment.</div>"
                )
            )
        except Exception as e:
            blocks.append(
                RawHTML(f"<div class='sql-error'>SQL preview failed: {esc(e)}</div>")
            )

        return blocks


class ExactRule(QueryRule):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.exact,
    }

    @property
    def comparator(self):
        # for exact match we are comparing against the actual concept_id
        if not self.concept:
            raise RuntimeError(f'Rule concept {self.concept_id} not found')
        return self.concept_id

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.__eq__(self.comparator)

class HierarchyBase(QueryRule):

    __mapper_args__ = {
        "polymorphic_abstract": True,
    }

    children: so.Mapped[list[Concept]] = so.relationship(
        Concept,
        secondary="concept_ancestor",
        primaryjoin="QueryRule.concept_id == concept_ancestor.c.ancestor_concept_id",
        secondaryjoin="concept_ancestor.c.descendant_concept_id == Concept.concept_id",
        lazy="selectin",
    )

    @property
    def comparator(self):
        # for hierarchical match we are comparing against the actual concept_id, plus the set of child concept_ids
        if not self.concept:
            raise RuntimeError(f'Rule concept {self.concept_id} not found')
        return [c.concept_id for c in self.children]
    
    def _html_inner(self):
        if not self.children:
            return []

        children = sorted(self.children, key=lambda c: c.concept_id)
        n = len(children)

        preview = children[:5]
        preview_rows = [
            [td(c.concept_id), td(c.concept_name), td(c.vocabulary_id)]
            for c in preview
        ]

        summary = RawHTML(
            table(
                headers=["Concept ID", "Name", "Vocabulary"],
                rows=preview_rows,
                cls="concept-table compact",
            )
        )

        tail = ""
        if n > 5:
            tail = RawHTML(f"<div class='muted'>… and {n - 5} more descendants</div>")

        return [summary, tail]

class HierarchyRule(HierarchyBase):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.hierarchy,
    }

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.in_(self.comparator)

class HierarchyExclusionRule(HierarchyBase):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.hierarchyexclusion,
    }

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.not_in(self.comparator)

class AbsenceRule(QueryRule):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.absence,
    }

    @property
    def comparator(self):
        # this can validly be null or 0
        return self.concept_id

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.is_(None)

class PresenceRule(QueryRule):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.presence,
    }

    @property
    def comparator(self) -> int | None:
        # this can validly be null or 0
        return self.concept_id

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.is_not(None)

class ScalarRule(QueryRule):
    """Handles scalar threshold comparisons (greater than or less than a stored integer value)."""

    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.scalar,
    }

    @property
    def comparator(self) -> tuple[int, int]:
        if self.scalar_threshold is None:
            raise RuntimeError(f'Scalar threshold is not set on rule {self.query_rule_id}')
        if not self.concept:
            raise RuntimeError(f'Rule concept {self.concept_id} not found')
        return self.scalar_threshold, self.concept_id

    @property
    def scalar_field(self):
        if self.threshold_comparator is None:
            raise RuntimeError(f'Threshold comparator is not set on rule {self.query_rule_id}')
        measurable = get_measurable_registry()[self.threshold_comparator]
        col = measurable.__bound_measurable__.value_numeric_col
        if col is None:
            raise RuntimeError(
                f"{measurable.__name__} does not expose a numeric value column"
            )
        return col

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        threshold, concept = self.comparator
        if self.threshold_direction is None:
            raise RuntimeError(f'Threshold direction is not set on scalar rule {self.query_rule_id}')
        if concept == 0:
            concept_clause = sa.true()
        else:
            concept_clause = field == concept
        field_comparator = sa.and_(field.is_not(None), concept_clause)
        if self.threshold_direction == ThresholdDirection.gt:
            return sa.and_(field_comparator, self.scalar_field > threshold)
        elif self.threshold_direction == ThresholdDirection.lt:
            return sa.and_(field_comparator, self.scalar_field < threshold)
        elif self.threshold_direction == ThresholdDirection.eq:
            return sa.and_(field_comparator, self.scalar_field == threshold)
        elif self.threshold_direction == ThresholdDirection.neq:
            return sa.and_(field_comparator, self.scalar_field != threshold)
        else:
            raise ValueError(f'Unknown threshold direction: {self.threshold_direction}')
        
    
    def _html_inner(self):
        bits = []

        bits.append(RawHTML(f"<div><b>Comparator:</b> {self.threshold_direction.value} {self.scalar_threshold}</div>")) # type: ignore

        if self.threshold_comparator:
            bits.append(RawHTML(f"<div><b>Target:</b> {self.threshold_comparator.value}</div>"))

        return bits

class PhenotypeRule(QueryRule):
    """
    Handles phenotype-based rules - this is equivalent to exact match rules but should
    be used when there are a very large number of exact match rules with no logical 
    hierarchy to leverage in order to avoid very large number of subquery combinations
    that could otherwise be an in_ statement.
    """
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.phenotype,
    }

    @property
    def comparator(self):
        if self.phenotype_id is None:
            raise RuntimeError(f'Phenotype is not set on rule {self.query_rule_id}')
        if not self.phenotype:
            raise RuntimeError(f'Rule phenotype {self.phenotype_id} not found')
        return [c.concept_id for c in self.phenotype.phenotype_concepts]

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.in_(self.comparator)
    

    def _html_inner(self):
        if not self.phenotype:
            return []

        concepts = sorted(
            self.phenotype.phenotype_concepts,
            key=lambda c: (c.vocabulary_id or "", c.concept_id),
        )
        n = len(concepts)

        preview = concepts[:5]
        rows = [
            [td(c.concept_id), td(c.concept_name), td(c.vocabulary_id)]
            for c in preview
        ]

        summary = RawHTML(
            table(
                headers=["Concept ID", "Name", "Vocabulary"],
                rows=rows,
                cls="concept-table compact",
            )
        )

        tail = ""
        if n > 5:
            tail = RawHTML(f"<div class='muted'>… and {n - 5} more phenotype concepts</div>")

        return [summary, tail]
        

class SubstringRule(QueryRule):
    __mapper_args__ = {
        "polymorphic_identity": RuleMatcher.substring,
    }
    @property
    def comparator(self):
        # for substring match we are comparing against the concept code
        if not self.concept:
            raise RuntimeError(f'Rule concept {self.concept_id} not found')
        return self.concept.concept_code

    def get_filter_details(self, field: sa.ColumnElement) -> sa.ColumnElement[bool]:
        return field.ilike(f'%{self.comparator}%')
    

    def _html_inner(self):
        if not self.concept:
            return []

        return [
            RawHTML(f"<div><b>Substring:</b> {self.concept.concept_code}</div>")
        ]