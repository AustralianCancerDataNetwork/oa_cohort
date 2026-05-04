from __future__ import annotations

from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.query.dash_cohort import DashCohort, DashCohortDef, dash_cohort_def_map
from oa_cohorts.query.indicator import Indicator
from oa_cohorts.query.measure import Measure, MeasureRelationship
from oa_cohorts.query.phenotype import Phenotype
from oa_cohorts.query.query_rule import QueryRule
from oa_cohorts.query.report import Report, ReportCohortMap, ReportVersion, report_indicator_map
from oa_cohorts.query.subquery import Subquery, subquery_rule_map

from .models import (
    DetailLink,
    DetailRow,
    DetailSection,
    EntityDetail,
    EntityKind,
    ExecutionIssue,
    ReportSummary,
    DashCohortDefSummary,
    ReportWorkspace,
    DashCohortDefWorkspace,
    RuleStatus,
    SQLVariant,
    TailoredDetailView,
    UsageSummary,
    WorkspaceNode,
)
from .preview import preview_measure, preview_subquery
from .status import resolve_rule_status
from .validation import entity_fields, validate_entity_instance

ENTITY_MODEL = {
    EntityKind.report: Report,
    EntityKind.report_version: ReportVersion,
    EntityKind.indicator: Indicator,
    EntityKind.dash_cohort: DashCohort,
    EntityKind.dash_cohort_def: DashCohortDef,
    EntityKind.measure: Measure,
    EntityKind.subquery: Subquery,
    EntityKind.query_rule: QueryRule,
    EntityKind.phenotype: Phenotype,
}

def list_cohorts(session: so.Session) -> list[DashCohortDefSummary]:
    stmt = (
        sa.select(DashCohortDef)
    )
    cohorts = session.execute(stmt).scalars().unique().all()
    return [
        DashCohortDefSummary(
            dash_cohort_def_id=cohort.dash_cohort_def_id,
            dash_cohort_def_name=cohort.dash_cohort_def_name,
            dash_cohort_def_short_name=cohort.dash_cohort_def_short_name,
            measure_id=cohort.measure_id,
        )
        for cohort in cohorts
    ]

def list_reports(session: so.Session) -> list[ReportSummary]:
    stmt = (
        sa.select(Report)
        .options(
            so.selectinload(Report.cohorts).selectinload(ReportCohortMap.cohort),
            so.selectinload(Report.indicators),
            so.selectinload(Report.report_versions),
        )
        .order_by(Report.report_id)
    )
    reports = session.execute(stmt).scalars().unique().all()
    return [
        ReportSummary(
            report_id=report.report_id,
            report_name=report.report_name,
            report_short_name=report.report_short_name,
            author=report.report_author,
            owner=report.report_owner,
            indicator_count=len(report.indicators),
            cohort_count=len(report.cohorts),
            statuses=tuple(sorted({version.report_status.value for version in report.report_versions})),
        )
        for report in reports
    ]


def load_dash_cohort_workspace(session: so.Session, dash_cohort_def_id: int) -> DashCohortDefWorkspace:
    dash_cohort_def = session.execute(
        sa.select(DashCohortDef)
        .where(DashCohortDef.dash_cohort_def_id == dash_cohort_def_id)
    ).scalars().unique().one()
    m = dash_cohort_def.dash_cohort_measure
    measure = _workspace_node_for_measure(session, m)
    subquery = _workspace_node_for_subquery(session, m.subquery) if m.subquery is not None else None
    return DashCohortDefWorkspace(
        dash_cohort_def_id=dash_cohort_def.dash_cohort_def_id,
        dash_cohort_def_name=dash_cohort_def.dash_cohort_def_name,
        dash_cohort_def_short_name=dash_cohort_def.dash_cohort_def_short_name,
        measure_id=dash_cohort_def.measure_id,
        measure=measure,
        subquery=subquery
    )

def load_report_workspace(session: so.Session, report_id: int) -> ReportWorkspace:
    report = session.execute(
        sa.select(Report)
        .where(Report.report_id == report_id)
        .options(
            so.selectinload(Report.cohorts)
            .selectinload(ReportCohortMap.cohort)
            .selectinload(DashCohort.definitions)
            .selectinload(DashCohortDef.dash_cohort_measure)
            .selectinload(Measure.child_links)
            .joinedload(MeasureRelationship.child),
            so.selectinload(Report.cohorts)
            .selectinload(ReportCohortMap.cohort)
            .selectinload(DashCohort.definitions)
            .selectinload(DashCohortDef.dash_cohort_measure)
            .joinedload(Measure.subquery)
            .selectinload(Subquery.rules),
            so.selectinload(Report.indicators).joinedload(Indicator.numerator_measure),
            so.selectinload(Report.indicators).joinedload(Indicator.denominator_measure),
            so.selectinload(Report.report_versions),
        )
    ).scalars().unique().one()

    validation = validate_entity_instance(EntityKind.report, report)
    usage_cache = _build_usage_cache_for_report(session, report)
    cohorts = tuple(
        _workspace_node_for_cohort(session, cohort_map.cohort, usage_cache)
        for cohort_map in report.cohorts
        if cohort_map.cohort is not None
    )
    indicators = tuple(
        _workspace_node_for_indicator(session, indicator, usage_cache)
        for indicator in sorted(report.indicators)
    )
    primary_names = tuple(
        sorted(
            cohort_map.cohort.dash_cohort_name
            for cohort_map in report.cohorts
            if cohort_map.primary_cohort and cohort_map.cohort is not None
        )
    )
    return ReportWorkspace(
        report_id=report.report_id,
        report_name=report.report_name,
        report_short_name=report.report_short_name,
        description=report.report_description or "",
        author=report.report_author,
        owner=report.report_owner,
        statuses=tuple(sorted({version.report_status.value for version in report.report_versions})),
        valid=validation.valid,
        executability=report.executable_status().value,
        primary_cohort_names=primary_names,
        cohorts=cohorts,
        indicators=indicators,
    )

def load_dash_cohort_workspace_by_short_name(session: so.Session, dash_cohort_def_short_name: str) -> DashCohortDefWorkspace:
    dash_cohort_def = session.execute(
        sa.select(DashCohortDef)
        .where(sa.func.lower(DashCohortDef.dash_cohort_def_short_name) == dash_cohort_def_short_name.lower())
    ).scalars().unique().one()
    return load_dash_cohort_workspace(session, dash_cohort_def.dash_cohort_def_id)

def load_report_workspace_by_short_name(session: so.Session, report_short_name: str) -> ReportWorkspace:
    report = session.execute(
        sa.select(Report.report_id).where(sa.func.lower(Report.report_short_name) == report_short_name.lower())
    ).scalar_one()
    return load_report_workspace(session, report)

def is_report_short_name_available(
    session: so.Session,
    report_short_name: str,
    *,
    exclude_report_id: int | None = None,
) -> bool:
    stmt = sa.select(Report.report_id).where(sa.func.lower(Report.report_short_name) == report_short_name.lower())
    ids = session.execute(stmt).scalars().all()
    if exclude_report_id is None:
        return not ids
    return all(report_id == exclude_report_id for report_id in ids)


def get_entity(session: so.Session, kind: EntityKind, entity_id: int) -> Any:
    model = ENTITY_MODEL[kind]
    pk_name = next(iter(model.__table__.primary_key.columns)).name
    stmt = sa.select(model).where(getattr(model, pk_name) == entity_id)
    return session.execute(stmt).scalars().unique().one()


def load_entity_detail(session: so.Session, kind: EntityKind, entity_id: int) -> EntityDetail:
    entity = get_entity(session, kind, entity_id)
    usage = compute_usage(session, kind, entity_id)
    validation = validate_entity_instance(kind, entity)
    preview_variants = ()
    executability: str | None = None
    if kind is EntityKind.measure:
        preview_variants = (SQLVariant.any, SQLVariant.first, SQLVariant.undated)
        executability = entity.is_executable().status.value
    elif kind is EntityKind.subquery:
        preview_variants = (SQLVariant.any, SQLVariant.first, SQLVariant.undated)
        executability = preview_subquery(entity, SQLVariant.any).status
    elif kind is EntityKind.indicator:
        executability = entity.is_executable().status.value
    elif kind is EntityKind.dash_cohort:
        executability = _dash_cohort_status(entity).value
    elif kind is EntityKind.dash_cohort_def:
        executability = entity.is_executable().status.value
    elif kind is EntityKind.report:
        executability = entity.executable_status().value

    fields = entity_fields(kind, entity)
    if kind is EntityKind.measure:
        fields["child_count"] = len(entity.children)
    if kind is EntityKind.subquery:
        fields["rule_count"] = len(entity.rules)
    if kind is EntityKind.report:
        fields["primary_cohort_count"] = sum(1 for item in entity.cohorts if item.primary_cohort)
        fields["indicator_count"] = len(entity.indicators)

    editable = not usage.shared and not (kind is EntityKind.measure and entity_id == 0)
    rule_status: RuleStatus | None = None
    if kind is EntityKind.query_rule:
        rule_status = _rule_status_for_rule(session, entity, validation, usage.shared)
    execution_issues = _execution_issues_for_entity(entity, kind)
    return EntityDetail(
        kind=kind,
        entity_id=entity_id,
        title=_entity_title(kind, entity),
        fields=fields,
        relationships=_relationships(entity, kind),
        usage=usage,
        validation=validation,
        shared=usage.shared,
        editable=editable,
        preview_variants=preview_variants,
        allowed_actions={
            "can_edit": editable,
            "can_clone": usage.shared,
            "can_delete": usage.inbound_count == 0 and not (kind is EntityKind.measure and entity_id == 0),
            "can_add_child": kind in {EntityKind.measure, EntityKind.subquery, EntityKind.report, EntityKind.dash_cohort},
        },
        executability=executability,
        rule_status=rule_status,
        execution_issues=execution_issues,
        detail_view=_tailored_detail_view(entity, kind, usage),
    )


def compute_usage(session: so.Session, kind: EntityKind, entity_id: int) -> UsageSummary:
    inbound: list[str] = []
    outbound: list[str] = []

    if kind is EntityKind.measure:
        parent_measures = session.execute(
            sa.select(Measure.name, Measure.measure_id)
            .join(MeasureRelationship, Measure.measure_id == MeasureRelationship.parent_measure_id)
            .where(MeasureRelationship.child_measure_id == entity_id)
        ).all()
        inbound.extend(f"measure:{item.measure_id}:{item.name}" for item in parent_measures)
        cohort_defs = session.execute(
            sa.select(DashCohortDef.dash_cohort_def_name, DashCohortDef.dash_cohort_def_id)
            .where(DashCohortDef.measure_id == entity_id)
        ).all()
        inbound.extend(f"dash_cohort_def:{item.dash_cohort_def_id}:{item.dash_cohort_def_name}" for item in cohort_defs)
        indicators = session.execute(
            sa.select(Indicator.indicator_id, Indicator.indicator_description)
            .where(
                sa.or_(
                    Indicator.numerator_measure_id == entity_id,
                    Indicator.denominator_measure_id == entity_id,
                )
            )
        ).all()
        inbound.extend(f"indicator:{item.indicator_id}:{item.indicator_description}" for item in indicators)
        children = session.execute(
            sa.select(Measure.measure_id, Measure.name)
            .join(MeasureRelationship, Measure.measure_id == MeasureRelationship.child_measure_id)
            .where(MeasureRelationship.parent_measure_id == entity_id)
        ).all()
        outbound.extend(f"measure:{item.measure_id}:{item.name}" for item in children)
    elif kind is EntityKind.subquery:
        measures = session.execute(
            sa.select(Measure.measure_id, Measure.name).where(Measure.subquery_id == entity_id)
        ).all()
        inbound.extend(f"measure:{item.measure_id}:{item.name}" for item in measures)
        rules = session.execute(
            sa.select(QueryRule.query_rule_id, QueryRule.matcher)
            .join(subquery_rule_map, QueryRule.query_rule_id == subquery_rule_map.c.query_rule_id)
            .where(subquery_rule_map.c.subquery_id == entity_id)
        ).all()
        outbound.extend(f"query_rule:{item.query_rule_id}:{item.matcher.value}" for item in rules)
    elif kind is EntityKind.query_rule:
        subqueries = session.execute(
            sa.select(Subquery.subquery_id, Subquery.name)
            .join(subquery_rule_map, Subquery.subquery_id == subquery_rule_map.c.subquery_id)
            .where(subquery_rule_map.c.query_rule_id == entity_id)
        ).all()
        inbound.extend(f"subquery:{item.subquery_id}:{item.name}" for item in subqueries)
    elif kind is EntityKind.phenotype:
        rules = session.execute(
            sa.select(QueryRule.query_rule_id, QueryRule.matcher)
            .where(QueryRule.phenotype_id == entity_id)
        ).all()
        inbound.extend(f"query_rule:{item.query_rule_id}:{item.matcher.value}" for item in rules)
    elif kind is EntityKind.indicator:
        reports = session.execute(
            sa.select(Report.report_id, Report.report_name)
            .join(report_indicator_map, Report.report_id == report_indicator_map.c.report_id)
            .where(report_indicator_map.c.indicator_id == entity_id)
        ).all()
        inbound.extend(f"report:{item.report_id}:{item.report_name}" for item in reports)
        indicator = get_entity(session, kind, entity_id)
        outbound.extend(
            [
                f"measure:{indicator.numerator_measure_id}:{indicator.numerator_measure.name}",
                f"measure:{indicator.denominator_measure_id}:{indicator.denominator_measure.name}",
            ]
        )
    elif kind is EntityKind.dash_cohort:
        reports = session.execute(
            sa.select(Report.report_id, Report.report_name)
            .join(ReportCohortMap, Report.report_id == ReportCohortMap.report_id)
            .where(ReportCohortMap.dash_cohort_id == entity_id)
        ).all()
        inbound.extend(f"report:{item.report_id}:{item.report_name}" for item in reports)
        defs = session.execute(
            sa.select(DashCohortDef.dash_cohort_def_id, DashCohortDef.dash_cohort_def_name)
            .join(dash_cohort_def_map, DashCohortDef.dash_cohort_def_id == dash_cohort_def_map.c.dash_cohort_def_id)
            .where(dash_cohort_def_map.c.dash_cohort_id == entity_id)
        ).all()
        outbound.extend(f"dash_cohort_def:{item.dash_cohort_def_id}:{item.dash_cohort_def_name}" for item in defs)
    elif kind is EntityKind.dash_cohort_def:
        cohorts = session.execute(
            sa.select(DashCohort.dash_cohort_id, DashCohort.dash_cohort_name)
            .join(dash_cohort_def_map, DashCohort.dash_cohort_id == dash_cohort_def_map.c.dash_cohort_id)
            .where(dash_cohort_def_map.c.dash_cohort_def_id == entity_id)
        ).all()
        inbound.extend(f"dash_cohort:{item.dash_cohort_id}:{item.dash_cohort_name}" for item in cohorts)
        dash_def = get_entity(session, kind, entity_id)
        outbound.append(f"measure:{dash_def.measure_id}:{dash_def.dash_cohort_measure.name}")
    elif kind is EntityKind.report:
        report = get_entity(session, kind, entity_id)
        outbound.extend(f"indicator:{item.indicator_id}:{item.indicator_description}" for item in report.indicators)
        outbound.extend(
            f"dash_cohort:{item.cohort.dash_cohort_id}:{item.cohort.dash_cohort_name}"
            for item in report.cohorts
            if item.cohort is not None
        )
    elif kind is EntityKind.report_version:
        version = get_entity(session, kind, entity_id)
        inbound.append(f"report:{version.report_id}:{version.report.report_name}")

    return UsageSummary(kind=kind, entity_id=entity_id, inbound=tuple(sorted(inbound)), outbound=tuple(sorted(outbound)))

def _workspace_node_for_indicator(
    session: so.Session,
    indicator: Indicator,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.indicator, indicator.indicator_id, usage_cache)
    validation = validate_entity_instance(EntityKind.indicator, indicator)
    children = (
        _workspace_node_for_measure(session, indicator.numerator_measure, usage_cache),
        _workspace_node_for_measure(session, indicator.denominator_measure, usage_cache),
    )
    return WorkspaceNode(
        kind=EntityKind.indicator,
        entity_id=indicator.indicator_id,
        label=indicator.indicator_description,
        summary=(
            ("numerator", indicator.numerator_label),
            ("denominator", indicator.denominator_label),
        ),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared,
        valid=validation.valid,
        executability=indicator.is_executable().status.value,
        children=children,
    )


def _workspace_node_for_cohort(
    session: so.Session,
    cohort: DashCohort,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.dash_cohort, cohort.dash_cohort_id, usage_cache)
    validation = validate_entity_instance(EntityKind.dash_cohort, cohort)
    children = tuple(_workspace_node_for_cohort_def(session, item, usage_cache) for item in cohort.definitions)
    return WorkspaceNode(
        kind=EntityKind.dash_cohort,
        entity_id=cohort.dash_cohort_id,
        label=cohort.dash_cohort_name,
        summary=(("definitions", str(len(cohort.definitions))),),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared,
        valid=validation.valid,
        executability=_dash_cohort_status(cohort).value,
        children=children,
    )


def _workspace_node_for_cohort_def(
    session: so.Session,
    cohort_def: DashCohortDef,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.dash_cohort_def, cohort_def.dash_cohort_def_id, usage_cache)
    validation = validate_entity_instance(EntityKind.dash_cohort_def, cohort_def)
    children = (_workspace_node_for_measure(session, cohort_def.dash_cohort_measure, usage_cache),)
    return WorkspaceNode(
        kind=EntityKind.dash_cohort_def,
        entity_id=cohort_def.dash_cohort_def_id,
        label=cohort_def.dash_cohort_def_name,
        summary=(("short_name", cohort_def.dash_cohort_def_short_name),),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared,
        valid=validation.valid,
        executability=cohort_def.is_executable().status.value,
        children=children,
    )


def _workspace_node_for_measure(
    session: so.Session,
    measure: Measure,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.measure, measure.measure_id, usage_cache)
    validation = validate_entity_instance(EntityKind.measure, measure)
    children: list[WorkspaceNode] = []
    if measure.subquery is not None:
        children.append(_workspace_node_for_subquery(session, measure.subquery, usage_cache))
    children.extend(_workspace_node_for_measure(session, child, usage_cache) for child in measure.children)
    return WorkspaceNode(
        kind=EntityKind.measure,
        entity_id=measure.measure_id,
        label=measure.name,
        summary=(
            ("combination", measure.combination.value),
            ("children", str(len(measure.children))),
        ),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared and measure.measure_id != 0,
        valid=validation.valid,
        executability=measure.is_executable().status.value,
        children=tuple(children),
    )


def _workspace_node_for_subquery(
    session: so.Session,
    subquery: Subquery,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.subquery, subquery.subquery_id, usage_cache)
    validation = validate_entity_instance(EntityKind.subquery, subquery)
    children = tuple(_workspace_node_for_rule(session, rule, usage_cache) for rule in subquery.rules)
    preview = preview_subquery(subquery, SQLVariant.any)
    return WorkspaceNode(
        kind=EntityKind.subquery,
        entity_id=subquery.subquery_id,
        label=subquery.name,
        summary=(
            ("target", subquery.target.value),
            ("temporality", subquery.temporality.value),
        ),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared,
        valid=validation.valid,
        executability=preview.status,
        children=children,
    )


def _workspace_node_for_rule(
    session: so.Session,
    rule: QueryRule,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None = None,
) -> WorkspaceNode:
    usage = _usage_summary(session, EntityKind.query_rule, rule.query_rule_id, usage_cache)
    validation = validate_entity_instance(EntityKind.query_rule, rule)
    rule_status = _rule_status_for_rule(session, rule, validation, usage.shared)
    return WorkspaceNode(
        kind=EntityKind.query_rule,
        entity_id=rule.query_rule_id,
        label=f"{rule.matcher.value} #{rule.query_rule_id}",
        summary=tuple((key, str(value)) for key, value in entity_fields(EntityKind.query_rule, rule).items() if value is not None),
        usage_count=usage.inbound_count,
        shared=usage.shared,
        editable=not usage.shared,
        valid=validation.valid,
        status_label=rule_status.label,
        status_tone=rule_status.tone,
        children=(),
    )


def _rule_status_for_rule(
    session: so.Session,
    rule: QueryRule,
    validation,
    shared: bool,
) -> RuleStatus:
    matcher = rule.matcher.value if rule.matcher is not None else None
    return resolve_rule_status(
        validation=validation,
        matcher=matcher,
        concept_resolves=_concept_resolves(rule),
        phenotype_resolves=_phenotype_resolves(rule),
        execution_blocked=_rule_execution_blocked(session, rule, validation),
        shared=shared,
    )


def _execution_issues_for_entity(entity: Any, kind: EntityKind) -> tuple[ExecutionIssue, ...]:
    if kind is EntityKind.measure:
        return _issues_from_failed_variants(entity.is_executable().failed_variants)
    if kind is EntityKind.dash_cohort_def:
        return _issues_from_failed_variants(entity.is_executable().failed_variants)
    if kind is EntityKind.dash_cohort:
        issues: list[ExecutionIssue] = []
        for definition in entity.definitions:
            issues.extend(
                _issues_from_failed_variants(
                    definition.is_executable().failed_variants,
                    prefix=definition.dash_cohort_def_name,
                )
            )
        return tuple(issues)
    if kind is EntityKind.indicator:
        check = entity.is_executable()
        issues: list[ExecutionIssue] = []
        issues.extend(
            _issues_from_failed_variants(
                check.numerator.failed_variants,
                prefix=f"Numerator {entity.numerator_measure_id}",
            )
        )
        issues.extend(
            _issues_from_failed_variants(
                check.denominator.failed_variants,
                prefix=f"Denominator {entity.denominator_measure_id}",
            )
        )
        return tuple(issues)
    if kind is EntityKind.subquery:
        issues: list[ExecutionIssue] = []
        for variant, fn in (
            ("ANY", entity.sql_any),
            ("FIRST", entity.sql_first),
            ("UNDATED", entity.sql_undated),
        ):
            try:
                _ = fn()
            except Exception as exc:
                issues.append(ExecutionIssue(label=variant, message=str(exc)))
        return tuple(issues)
    if kind is EntityKind.report:
        issues: list[ExecutionIssue] = []
        for cohort_map in entity.cohorts:
            cohort = cohort_map.cohort
            if cohort is None:
                continue
            for definition in cohort.definitions:
                issues.extend(
                    _issues_from_failed_variants(
                        definition.is_executable().failed_variants,
                        prefix=f"Cohort {cohort.dash_cohort_name} / {definition.dash_cohort_def_name}",
                    )
                )
        for indicator in entity.indicators:
            check = indicator.is_executable()
            issues.extend(
                _issues_from_failed_variants(
                    check.numerator.failed_variants,
                    prefix=f"Indicator {indicator.indicator_description} / numerator",
                )
            )
            issues.extend(
                _issues_from_failed_variants(
                    check.denominator.failed_variants,
                    prefix=f"Indicator {indicator.indicator_description} / denominator",
                )
            )
        return tuple(issues)
    return ()


def _issues_from_failed_variants(
    failed_variants: dict[str, str],
    *,
    prefix: str | None = None,
) -> tuple[ExecutionIssue, ...]:
    issues: list[ExecutionIssue] = []
    for variant, message in failed_variants.items():
        label = variant if prefix is None else f"{prefix} / {variant}"
        issues.append(ExecutionIssue(label=label, message=message))
    return tuple(issues)


def _dash_cohort_status(cohort: DashCohort):
    statuses = [definition.is_executable().status for definition in cohort.definitions]
    if not statuses:
        from oa_cohorts.core.executability import ExecStatus

        return ExecStatus.FAIL
    if any(status.value == "fail" for status in statuses):
        from oa_cohorts.core.executability import ExecStatus

        return ExecStatus.FAIL
    if any(status.value == "warn" for status in statuses):
        from oa_cohorts.core.executability import ExecStatus

        return ExecStatus.WARN
    from oa_cohorts.core.executability import ExecStatus

    return ExecStatus.PASS


def _concept_resolves(rule: QueryRule) -> bool | None:
    if rule.concept_id is None:
        return None
    try:
        return rule.concept is not None
    except sa.exc.SQLAlchemyError: # type: ignore
        return False


def _phenotype_resolves(rule: QueryRule) -> bool | None:
    if rule.phenotype_id is None:
        return None
    return rule.phenotype is not None


def _rule_execution_blocked(
    session: so.Session,
    rule: QueryRule,
    validation,
) -> bool:
    if not validation.valid:
        return False
    subqueries = session.execute(
        sa.select(Subquery)
        .join(subquery_rule_map, Subquery.subquery_id == subquery_rule_map.c.subquery_id)
        .where(subquery_rule_map.c.query_rule_id == rule.query_rule_id)
    ).scalars().unique().all()
    for subquery in subqueries:
        preview = preview_subquery(subquery, SQLVariant.any)
        if preview.status.lower() == "fail":
            return True
    return False


def _tailored_detail_view(
    entity: Any,
    kind: EntityKind,
    usage: UsageSummary,
) -> TailoredDetailView | None:
    if kind is EntityKind.report:
        return TailoredDetailView(
            summary_sections=(
                DetailSection(
                    title="Summary",
                    rows=(
                        DetailRow("Name", entity.report_name),
                        DetailRow("Short name", entity.report_short_name),
                        DetailRow("Description", entity.report_description or "-"),
                        DetailRow("Author", entity.report_author or "-"),
                        DetailRow("Owner", entity.report_owner or "-"),
                    ),
                ),
            ),
            hide_relationships=True,
        )
    if kind is EntityKind.indicator:
        return TailoredDetailView(
            summary_sections=(
                DetailSection(
                    title="Summary",
                    rows=(
                        DetailRow("Description", entity.indicator_description),
                        DetailRow("Reference", entity.indicator_reference or "-"),
                        DetailRow("Numerator label", entity.numerator_label or "-"),
                        DetailRow("Denominator label", entity.denominator_label or "-"),
                    ),
                ),
                DetailSection(
                    title="Defining measures",
                    rows=(
                        DetailRow(
                            "Numerator measure",
                            entity.numerator_measure.name,
                            link=_detail_link(EntityKind.measure, entity.numerator_measure_id, entity.numerator_measure.name),
                        ),
                        DetailRow(
                            "Denominator measure",
                            entity.denominator_measure.name,
                            link=_detail_link(EntityKind.measure, entity.denominator_measure_id, entity.denominator_measure.name),
                        ),
                    ),
                ),
            ),
            hide_relationships=True,
        )
    if kind is EntityKind.dash_cohort:
        definition_rows = tuple(
            DetailRow(
                definition.dash_cohort_def_name,
                definition.dash_cohort_measure.name,
                link=_detail_link(
                    EntityKind.measure,
                    definition.measure_id,
                    definition.dash_cohort_measure.name,
                ),
            )
            for definition in entity.definitions
        )
        return TailoredDetailView(
            summary_sections=(
                DetailSection(
                    title="Summary",
                    rows=(
                        DetailRow("Name", entity.dash_cohort_name),
                        DetailRow("Definitions", str(len(entity.definitions))),
                    ),
                ),
            ),
            secondary_sections=(
                DetailSection(title="Definitions", rows=definition_rows),
            ),
            hide_relationships=True,
        )
    if kind is EntityKind.dash_cohort_def:
        return TailoredDetailView(
            summary_sections=(
                DetailSection(
                    title="Summary",
                    rows=(
                        DetailRow("Name", entity.dash_cohort_def_name),
                        DetailRow("Short name", entity.dash_cohort_def_short_name or "-"),
                        DetailRow(
                            "Defining measure",
                            entity.dash_cohort_measure.name,
                            link=_detail_link(EntityKind.measure, entity.measure_id, entity.dash_cohort_measure.name),
                        ),
                    ),
                ),
            ),
            secondary_sections=(
                DetailSection(
                    title="Linked cohorts",
                    rows=tuple(
                        DetailRow(
                            "Cohort",
                            cohort.dash_cohort_name,
                            link=_detail_link(EntityKind.dash_cohort, cohort.dash_cohort_id, cohort.dash_cohort_name),
                        )
                        for cohort in entity.dash_cohort_objects
                    ),
                ),
            ),
            hide_relationships=True,
        )
    if kind is EntityKind.measure:
        rows = [
            DetailRow("Name", entity.name),
            DetailRow("Combination", entity.combination.value),
            DetailRow("Used by", str(usage.inbound_count)),
        ]
        if entity.subquery is not None:
            rows.append(
                DetailRow(
                    "Subquery",
                    entity.subquery.name,
                    link=_detail_link(EntityKind.subquery, entity.subquery_id, entity.subquery.name),
                )
            )
        if entity.children:
            child_rows = tuple(
                DetailRow(
                    child.name,
                    child.combination.value,
                    link=_detail_link(EntityKind.measure, child.measure_id, child.name),
                )
                for child in entity.children
            )
        else:
            child_rows = ()
        secondary_sections = ()
        if child_rows:
            secondary_sections = (DetailSection(title="Child measures", rows=child_rows),)
        return TailoredDetailView(
            summary_sections=(DetailSection(title="Summary", rows=tuple(rows)),),
            secondary_sections=secondary_sections,
            hide_relationships=True,
        )
    return None


def _detail_link(kind: EntityKind, entity_id: int, label: str) -> DetailLink:
    return DetailLink(kind=kind, entity_id=entity_id, label=label)


def _entity_title(kind: EntityKind, entity: Any) -> str:
    if kind is EntityKind.report:
        return entity.report_name
    if kind is EntityKind.report_version:
        return f"{entity.report_version_major}.{entity.report_version_minor} {entity.report_version_label}"
    if kind is EntityKind.indicator:
        return entity.indicator_description
    if kind is EntityKind.dash_cohort:
        return entity.dash_cohort_name
    if kind is EntityKind.dash_cohort_def:
        return entity.dash_cohort_def_name
    if kind is EntityKind.measure:
        return entity.name
    if kind is EntityKind.subquery:
        return entity.name
    if kind is EntityKind.query_rule:
        return f"{entity.matcher.value} #{entity.query_rule_id}"
    return entity.phenotype_name


def _relationships(entity: Any, kind: EntityKind) -> dict[str, tuple[str, ...]]:
    if kind is EntityKind.report:
        return {
            "cohorts": tuple(
                f"{item.cohort.dash_cohort_id}:{item.cohort.dash_cohort_name}"
                for item in entity.cohorts
                if item.cohort is not None
            ),
            "indicators": tuple(f"{item.indicator_id}:{item.indicator_description}" for item in entity.indicators),
            "versions": tuple(f"{item.report_version_id}:{item.report_version_label}" for item in entity.report_versions),
        }
    if kind is EntityKind.indicator:
        return {
            "reports": tuple(f"{item.report_id}:{item.report_name}" for item in entity.in_reports),
            "numerator_measure": (f"{entity.numerator_measure_id}:{entity.numerator_measure.name}",),
            "denominator_measure": (f"{entity.denominator_measure_id}:{entity.denominator_measure.name}",),
        }
    if kind is EntityKind.measure:
        relationships = {
            "children": tuple(f"{item.measure_id}:{item.name}" for item in entity.children),
            "parents": tuple(f"{item.parent.measure_id}:{item.parent.name}" for item in entity.parent_links if item.parent is not None),
        }
        if entity.subquery is not None:
            relationships["subquery"] = (f"{entity.subquery.subquery_id}:{entity.subquery.name}",)
        return relationships
    if kind is EntityKind.subquery:
        return {"rules": tuple(f"{item.query_rule_id}:{item.matcher.value}" for item in entity.rules)}
    if kind is EntityKind.query_rule:
        relationships: dict[str, tuple[str, ...]] = {}
        if entity.phenotype is not None:
            relationships["phenotype"] = (f"{entity.phenotype.phenotype_id}:{entity.phenotype.phenotype_name}",)
        return relationships
    if kind is EntityKind.dash_cohort:
        return {"definitions": tuple(f"{item.dash_cohort_def_id}:{item.dash_cohort_def_name}" for item in entity.definitions)}
    if kind is EntityKind.dash_cohort_def:
        return {
            "cohorts": tuple(f"{item.dash_cohort_id}:{item.dash_cohort_name}" for item in entity.dash_cohort_objects),
            "measure": (f"{entity.measure_id}:{entity.dash_cohort_measure.name}",),
        }
    if kind is EntityKind.phenotype:
        return {"concept_ids": tuple(str(item.query_concept_id) for item in entity.phenotype_definitions)}
    if kind is EntityKind.report_version:
        return {"report": (f"{entity.report_id}:{entity.report.report_name}",)}
    return {}


def _usage_summary(
    session: so.Session,
    kind: EntityKind,
    entity_id: int,
    usage_cache: dict[tuple[EntityKind, int], UsageSummary] | None,
) -> UsageSummary:
    if usage_cache is None:
        return compute_usage(session, kind, entity_id)
    return usage_cache[(kind, entity_id)]


def _build_usage_cache_for_report(
    session: so.Session,
    report: Report,
) -> dict[tuple[EntityKind, int], UsageSummary]:
    measure_ids: set[int] = set()
    subquery_ids: set[int] = set()
    query_rule_ids: set[int] = set()
    cohort_ids = {item.cohort.dash_cohort_id for item in report.cohorts if item.cohort is not None}
    cohort_def_ids = {
        cohort_def.dash_cohort_def_id
        for item in report.cohorts
        if item.cohort is not None
        for cohort_def in item.cohort.definitions
    }
    indicator_ids = {indicator.indicator_id for indicator in report.indicators}

    for item in report.cohorts:
        if item.cohort is None:
            continue
        for cohort_def in item.cohort.definitions:
            _collect_measure_tree(cohort_def.dash_cohort_measure, measure_ids, subquery_ids, query_rule_ids)
    for indicator in report.indicators:
        _collect_measure_tree(indicator.numerator_measure, measure_ids, subquery_ids, query_rule_ids)
        _collect_measure_tree(indicator.denominator_measure, measure_ids, subquery_ids, query_rule_ids)

    usage_map: dict[tuple[EntityKind, int], dict[str, list[str]]] = {}
    all_keys = [(EntityKind.report, report.report_id)]
    all_keys.extend((EntityKind.indicator, indicator_id) for indicator_id in indicator_ids)
    all_keys.extend((EntityKind.dash_cohort, cohort_id) for cohort_id in cohort_ids)
    all_keys.extend((EntityKind.dash_cohort_def, cohort_def_id) for cohort_def_id in cohort_def_ids)
    all_keys.extend((EntityKind.measure, measure_id) for measure_id in measure_ids)
    all_keys.extend((EntityKind.subquery, subquery_id) for subquery_id in subquery_ids)
    all_keys.extend((EntityKind.query_rule, query_rule_id) for query_rule_id in query_rule_ids)
    for key in all_keys:
        usage_map[key] = {"inbound": [], "outbound": []}

    usage_map[(EntityKind.report, report.report_id)]["outbound"].extend(
        f"indicator:{item.indicator_id}:{item.indicator_description}" for item in report.indicators
    )
    usage_map[(EntityKind.report, report.report_id)]["outbound"].extend(
        f"dash_cohort:{item.cohort.dash_cohort_id}:{item.cohort.dash_cohort_name}"
        for item in report.cohorts
        if item.cohort is not None
    )

    if indicator_ids:
        rows = session.execute(
            sa.select(report_indicator_map.c.report_id, report_indicator_map.c.indicator_id, Report.report_name)
            .join(Report, Report.report_id == report_indicator_map.c.report_id)
            .where(report_indicator_map.c.indicator_id.in_(indicator_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.indicator, row.indicator_id)]["inbound"].append(
                f"report:{row.report_id}:{row.report_name}"
            )
    for indicator in report.indicators:
        usage_map[(EntityKind.indicator, indicator.indicator_id)]["outbound"].extend(
            [
                f"measure:{indicator.numerator_measure_id}:{indicator.numerator_measure.name}",
                f"measure:{indicator.denominator_measure_id}:{indicator.denominator_measure.name}",
            ]
        )

    if cohort_ids:
        rows = session.execute(
            sa.select(ReportCohortMap.dash_cohort_id, Report.report_id, Report.report_name)
            .join(Report, Report.report_id == ReportCohortMap.report_id)
            .where(ReportCohortMap.dash_cohort_id.in_(cohort_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.dash_cohort, row.dash_cohort_id)]["inbound"].append(
                f"report:{row.report_id}:{row.report_name}"
            )
    for item in report.cohorts:
        if item.cohort is None:
            continue
        usage_map[(EntityKind.dash_cohort, item.cohort.dash_cohort_id)]["outbound"].extend(
            f"dash_cohort_def:{cohort_def.dash_cohort_def_id}:{cohort_def.dash_cohort_def_name}"
            for cohort_def in item.cohort.definitions
        )

    if cohort_def_ids:
        rows = session.execute(
            sa.select(
                dash_cohort_def_map.c.dash_cohort_def_id,
                DashCohort.dash_cohort_id,
                DashCohort.dash_cohort_name,
            )
            .join(DashCohort, DashCohort.dash_cohort_id == dash_cohort_def_map.c.dash_cohort_id)
            .where(dash_cohort_def_map.c.dash_cohort_def_id.in_(cohort_def_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.dash_cohort_def, row.dash_cohort_def_id)]["inbound"].append(
                f"dash_cohort:{row.dash_cohort_id}:{row.dash_cohort_name}"
            )
        rows = session.execute(
            sa.select(DashCohortDef.dash_cohort_def_id, DashCohortDef.measure_id, Measure.name)
            .join(Measure, Measure.measure_id == DashCohortDef.measure_id)
            .where(DashCohortDef.dash_cohort_def_id.in_(cohort_def_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.dash_cohort_def, row.dash_cohort_def_id)]["outbound"].append(
                f"measure:{row.measure_id}:{row.name}"
            )

    if measure_ids:
        rows = session.execute(
            sa.select(
                MeasureRelationship.child_measure_id,
                Measure.measure_id,
                Measure.name,
            )
            .join(Measure, Measure.measure_id == MeasureRelationship.parent_measure_id)
            .where(MeasureRelationship.child_measure_id.in_(measure_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.measure, row.child_measure_id)]["inbound"].append(
                f"measure:{row.measure_id}:{row.name}"
            )
        rows = session.execute(
            sa.select(
                MeasureRelationship.parent_measure_id,
                Measure.measure_id,
                Measure.name,
            )
            .join(Measure, Measure.measure_id == MeasureRelationship.child_measure_id)
            .where(MeasureRelationship.parent_measure_id.in_(measure_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.measure, row.parent_measure_id)]["outbound"].append(
                f"measure:{row.measure_id}:{row.name}"
            )
        rows = session.execute(
            sa.select(DashCohortDef.measure_id, DashCohortDef.dash_cohort_def_id, DashCohortDef.dash_cohort_def_name)
            .where(DashCohortDef.measure_id.in_(measure_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.measure, row.measure_id)]["inbound"].append(
                f"dash_cohort_def:{row.dash_cohort_def_id}:{row.dash_cohort_def_name}"
            )
        rows = session.execute(
            sa.select(Indicator.numerator_measure_id, Indicator.indicator_id, Indicator.indicator_description)
            .where(Indicator.numerator_measure_id.in_(measure_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.measure, row.numerator_measure_id)]["inbound"].append(
                f"indicator:{row.indicator_id}:{row.indicator_description}"
            )
        rows = session.execute(
            sa.select(Indicator.denominator_measure_id, Indicator.indicator_id, Indicator.indicator_description)
            .where(Indicator.denominator_measure_id.in_(measure_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.measure, row.denominator_measure_id)]["inbound"].append(
                f"indicator:{row.indicator_id}:{row.indicator_description}"
            )

    if subquery_ids:
        rows = session.execute(
            sa.select(Measure.subquery_id, Measure.measure_id, Measure.name)
            .where(Measure.subquery_id.in_(subquery_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.subquery, row.subquery_id)]["inbound"].append(
                f"measure:{row.measure_id}:{row.name}"
            )
        rows = session.execute(
            sa.select(
                subquery_rule_map.c.subquery_id,
                QueryRule.query_rule_id,
                QueryRule.matcher,
            )
            .join(QueryRule, QueryRule.query_rule_id == subquery_rule_map.c.query_rule_id)
            .where(subquery_rule_map.c.subquery_id.in_(subquery_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.subquery, row.subquery_id)]["outbound"].append(
                f"query_rule:{row.query_rule_id}:{row.matcher.value}"
            )

    if query_rule_ids:
        rows = session.execute(
            sa.select(
                subquery_rule_map.c.query_rule_id,
                Subquery.subquery_id,
                Subquery.name,
            )
            .join(Subquery, Subquery.subquery_id == subquery_rule_map.c.subquery_id)
            .where(subquery_rule_map.c.query_rule_id.in_(query_rule_ids))
        ).all()
        for row in rows:
            usage_map[(EntityKind.query_rule, row.query_rule_id)]["inbound"].append(
                f"subquery:{row.subquery_id}:{row.name}"
            )

    return {
        key: UsageSummary(
            kind=key[0],
            entity_id=key[1],
            inbound=tuple(sorted(values["inbound"])),
            outbound=tuple(sorted(values["outbound"])),
        )
        for key, values in usage_map.items()
    }


def _collect_measure_tree(
    measure: Measure,
    measure_ids: set[int],
    subquery_ids: set[int],
    query_rule_ids: set[int],
) -> None:
    if measure.measure_id in measure_ids:
        return
    measure_ids.add(measure.measure_id)
    if measure.subquery is not None:
        subquery_ids.add(measure.subquery.subquery_id)
        query_rule_ids.update(rule.query_rule_id for rule in measure.subquery.rules)
    for child in measure.children:
        _collect_measure_tree(child, measure_ids, subquery_ids, query_rule_ids)
