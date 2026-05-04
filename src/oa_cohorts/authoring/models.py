from __future__ import annotations

from dataclasses import dataclass, field
import enum
from typing import Any, Mapping, TypeAlias


class EntityKind(str, enum.Enum):
    report = "report"
    report_version = "report_version"
    indicator = "indicator"
    dash_cohort = "dash_cohort"
    dash_cohort_def = "dash_cohort_def"
    measure = "measure"
    subquery = "subquery"
    query_rule = "query_rule"
    phenotype = "phenotype"


class RelationKind(str, enum.Enum):
    report_indicator = "report_indicator"
    report_cohort = "report_cohort"
    dash_cohort_definition = "dash_cohort_definition"
    measure_child = "measure_child"
    subquery_rule = "subquery_rule"
    indicator_numerator = "indicator_numerator"
    indicator_denominator = "indicator_denominator"
    dash_cohort_def_measure = "dash_cohort_def_measure"
    measure_subquery = "measure_subquery"
    report_version = "report_version"


class SQLVariant(str, enum.Enum):
    any = "any"
    first = "first"
    undated = "undated"


class StatusTone(str, enum.Enum):
    pass_ = "pass"
    fail = "fail"
    warn = "warn"
    neutral = "neutral"


EntityPayload: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True)
class ParentRef:
    relation: RelationKind
    parent_id: int
    attrs: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationMessage:
    path: str
    message: str
    level: str = "error"


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    messages: tuple[ValidationMessage, ...] = ()


@dataclass(frozen=True)
class UsageSummary:
    kind: EntityKind
    entity_id: int
    inbound: tuple[str, ...]
    outbound: tuple[str, ...]

    @property
    def inbound_count(self) -> int:
        return len(self.inbound)

    @property
    def outbound_count(self) -> int:
        return len(self.outbound)

    @property
    def shared(self) -> bool:
        return self.inbound_count > 1


@dataclass(frozen=True)
class SQLPreview:
    kind: EntityKind
    entity_id: int
    variant: SQLVariant
    sql: str | None
    executable: bool
    status: str
    errors: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuleStatus:
    code: str
    label: str
    tone: StatusTone
    detail: str | None = None


@dataclass(frozen=True)
class ExecutionIssue:
    label: str
    message: str


@dataclass(frozen=True)
class DetailLink:
    kind: EntityKind
    entity_id: int
    label: str


@dataclass(frozen=True)
class DetailRow:
    label: str
    value: str
    link: DetailLink | None = None


@dataclass(frozen=True)
class DetailSection:
    title: str
    rows: tuple[DetailRow, ...]


@dataclass(frozen=True)
class TailoredDetailView:
    summary_sections: tuple[DetailSection, ...] = ()
    secondary_sections: tuple[DetailSection, ...] = ()
    hide_relationships: bool = False


@dataclass(frozen=True)
class WorkspaceNode:
    kind: EntityKind
    entity_id: int
    label: str
    summary: tuple[tuple[str, str], ...] = ()
    usage_count: int = 0
    shared: bool = False
    editable: bool = True
    valid: bool = True
    executability: str | None = None
    status_label: str | None = None
    status_tone: StatusTone | None = None
    children: tuple["WorkspaceNode", ...] = ()


@dataclass(frozen=True)
class ReportSummary:
    report_id: int
    report_name: str
    report_short_name: str
    author: str
    owner: str | None
    indicator_count: int
    cohort_count: int
    statuses: tuple[str, ...] = ()

@dataclass(frozen=True)
class DashCohortDefSummary:
    dash_cohort_def_id: int
    dash_cohort_def_name: str
    dash_cohort_def_short_name: str
    measure_id: int 

@dataclass(frozen=True)
class DashCohortDefWorkspace:
    dash_cohort_def_id: int
    dash_cohort_def_name: str
    dash_cohort_def_short_name: str
    measure_id: int
    measure: WorkspaceNode | None = None
    subquery: WorkspaceNode | None = None

@dataclass(frozen=True)
class ReportWorkspace:
    report_id: int
    report_name: str
    report_short_name: str
    description: str
    author: str
    owner: str | None
    statuses: tuple[str, ...]
    valid: bool
    executability: str
    primary_cohort_names: tuple[str, ...]
    cohorts: tuple[WorkspaceNode, ...]
    indicators: tuple[WorkspaceNode, ...]


@dataclass(frozen=True)
class EntityDetail:
    kind: EntityKind
    entity_id: int
    title: str
    fields: Mapping[str, Any]
    relationships: Mapping[str, tuple[str, ...]]
    usage: UsageSummary
    validation: ValidationResult
    shared: bool
    editable: bool
    preview_variants: tuple[SQLVariant, ...]
    allowed_actions: Mapping[str, bool]
    executability: str | None = None
    rule_status: RuleStatus | None = None
    execution_issues: tuple[ExecutionIssue, ...] = ()
    detail_view: TailoredDetailView | None = None


@dataclass(frozen=True)
class MutationResult:
    ok: bool
    kind: EntityKind
    entity_id: int | None
    detail: EntityDetail | None = None
    usage: UsageSummary | None = None
    validation: ValidationResult | None = None
    errors: tuple[str, ...] = ()
