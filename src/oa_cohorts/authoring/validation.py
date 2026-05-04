from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa

from oa_cohorts.core.coercion import coerce_column_value, parse_int
from oa_cohorts.core import RuleMatcher
from oa_cohorts.query.dash_cohort import DashCohort, DashCohortDef
from oa_cohorts.query.indicator import Indicator
from oa_cohorts.query.measure import Measure
from oa_cohorts.query.phenotype import Phenotype
from oa_cohorts.query.query_rule import QueryRule
from oa_cohorts.query.report import Report, ReportVersion
from oa_cohorts.query.subquery import Subquery

from .models import EntityKind, EntityPayload, ValidationMessage, ValidationResult


MODEL_BY_KIND: dict[EntityKind, type] = {
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


def coerce_payload(kind: EntityKind, payload: EntityPayload) -> dict[str, Any]:
    model = MODEL_BY_KIND[kind]
    available_columns = {column.name: column for column in model.__table__.columns}
    cleaned: dict[str, Any] = {}

    for key, raw_value in payload.items():
        if key not in available_columns:
            continue
        column = available_columns[key]
        if raw_value == "":
            cleaned[key] = None
            continue
        cleaned[key] = _coerce_value(column, raw_value)
    return cleaned


def validate_payload(kind: EntityKind, payload: EntityPayload) -> ValidationResult:
    cleaned = coerce_payload(kind, payload)
    messages: list[ValidationMessage] = []

    if kind is EntityKind.query_rule:
        matcher = cleaned.get("matcher")
        if matcher is None:
            messages.append(ValidationMessage("matcher", "matcher is required"))
        else:
            _validate_query_rule(cleaned, messages)
    elif kind is EntityKind.subquery:
        for field in ("target", "temporality", "name"):
            if not cleaned.get(field):
                messages.append(ValidationMessage(field, f"{field} is required"))
        rule_count = _synthetic_int(payload, "rule_count", cleaned.get("rule_count"))
        if rule_count in (None, 0):
            messages.append(ValidationMessage("rules", "at least one rule is required for a valid subquery"))
    elif kind is EntityKind.measure:
        if not cleaned.get("name"):
            messages.append(ValidationMessage("name", "name is required"))
        if not cleaned.get("combination"):
            messages.append(ValidationMessage("combination", "combination is required"))
        has_subquery = cleaned.get("subquery_id") is not None
        child_count = _synthetic_int(payload, "child_count", cleaned.get("child_count")) or 0
        if not has_subquery and child_count == 0:
            messages.append(
                ValidationMessage("measure", "measure must have either a subquery or child measures")
            )
    elif kind is EntityKind.indicator:
        if cleaned.get("numerator_measure_id") is None:
            messages.append(ValidationMessage("numerator_measure_id", "numerator measure is required"))
        if cleaned.get("denominator_measure_id") is None:
            messages.append(ValidationMessage("denominator_measure_id", "denominator measure is required"))
        for field in (
            "numerator_max_days_prior",
            "numerator_max_days_post",
            "denominator_max_days_prior",
            "denominator_max_days_post",
        ):
            value = cleaned.get(field)
            if value is not None and int(value) < 0:
                messages.append(ValidationMessage(field, f"{field} must be greater than or equal to 0"))
    elif kind is EntityKind.report:
        if not cleaned.get("report_name"):
            messages.append(ValidationMessage("report_name", "report_name is required"))
        if not cleaned.get("report_short_name"):
            messages.append(ValidationMessage("report_short_name", "report_short_name is required"))
        primary_cohort_count = _synthetic_int(payload, "primary_cohort_count", cleaned.get("primary_cohort_count")) or 0
        indicator_count = _synthetic_int(payload, "indicator_count", cleaned.get("indicator_count")) or 0
        if primary_cohort_count == 0:
            messages.append(ValidationMessage("cohorts", "at least one primary cohort is required"))
        if indicator_count == 0:
            messages.append(ValidationMessage("indicators", "at least one indicator is required for execution-ready status"))
    elif kind is EntityKind.dash_cohort_def:
        if not cleaned.get("dash_cohort_def_name"):
            messages.append(ValidationMessage("dash_cohort_def_name", "dash_cohort_def_name is required"))
        if cleaned.get("measure_id") is None:
            messages.append(ValidationMessage("measure_id", "measure_id is required"))
    elif kind is EntityKind.dash_cohort:
        if not cleaned.get("dash_cohort_name"):
            messages.append(ValidationMessage("dash_cohort_name", "dash_cohort_name is required"))
    elif kind is EntityKind.report_version:
        for field in (
            "report_id",
            "report_version_major",
            "report_version_minor",
            "report_version_label",
            "report_version_date",
            "report_status",
        ):
            if cleaned.get(field) is None:
                messages.append(ValidationMessage(field, f"{field} is required"))
    elif kind is EntityKind.phenotype:
        if not cleaned.get("phenotype_name"):
            messages.append(ValidationMessage("phenotype_name", "phenotype_name is required"))

    return ValidationResult(valid=not messages, messages=tuple(messages))


def validate_entity_instance(kind: EntityKind, entity: Any) -> ValidationResult:
    payload = entity_fields(kind, entity)
    if kind is EntityKind.subquery:
        payload["rule_count"] = len(getattr(entity, "rules", []))
    if kind is EntityKind.measure:
        payload["child_count"] = len(getattr(entity, "children", []))
    if kind is EntityKind.report:
        payload["primary_cohort_count"] = sum(1 for item in getattr(entity, "cohorts", []) if item.primary_cohort)
        payload["indicator_count"] = len(getattr(entity, "indicators", []))
    return validate_payload(kind, payload)


def entity_fields(kind: EntityKind, entity: Any) -> dict[str, Any]:
    model = MODEL_BY_KIND[kind]
    out: dict[str, Any] = {}
    for column in model.__table__.columns:
        value = getattr(entity, column.name)
        if isinstance(column.type, sa.Enum) and value is not None:
            out[column.name] = value.value
        else:
            out[column.name] = value
    return out


def _validate_query_rule(cleaned: Mapping[str, Any], messages: list[ValidationMessage]) -> None:
    matcher = cleaned.get("matcher")
    if matcher in {
        RuleMatcher.exact,
        RuleMatcher.hierarchy,
        RuleMatcher.hierarchyexclusion,
        RuleMatcher.substring,
    } and cleaned.get("concept_id") is None:
        messages.append(ValidationMessage("concept_id", "concept_id is required for this matcher"))
    if matcher is RuleMatcher.phenotype and cleaned.get("phenotype_id") is None:
        messages.append(ValidationMessage("phenotype_id", "phenotype_id is required for phenotype rules"))
    if matcher is RuleMatcher.scalar:
        if cleaned.get("scalar_threshold") is None:
            messages.append(ValidationMessage("scalar_threshold", "scalar_threshold is required for scalar rules"))
        if cleaned.get("threshold_direction") is None:
            messages.append(ValidationMessage("threshold_direction", "threshold_direction is required for scalar rules"))
    if matcher in {RuleMatcher.presence, RuleMatcher.absence}:
        for field in ("scalar_threshold", "threshold_direction", "threshold_comparator"):
            if cleaned.get(field) is not None:
                messages.append(ValidationMessage(field, f"{field} is not valid for this matcher"))


def _coerce_value(column: sa.Column[Any], value: Any) -> Any:
    return coerce_column_value(column, value)


def _synthetic_int(payload: EntityPayload, key: str, cleaned_value: Any) -> int | None:
    if cleaned_value is not None:
        return int(cleaned_value)
    raw_value = payload.get(key)
    if raw_value in (None, ""):
        return None
    return parse_int(raw_value)
