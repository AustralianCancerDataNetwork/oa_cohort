from __future__ import annotations

from .models import RuleStatus, StatusTone, ValidationResult


def resolve_rule_status(
    *,
    validation: ValidationResult,
    matcher: str | None,
    concept_resolves: bool | None,
    phenotype_resolves: bool | None,
    execution_blocked: bool,
    shared: bool,
) -> RuleStatus:
    if concept_resolves is False or phenotype_resolves is False:
        return RuleStatus(
            code="broken_reference",
            label="Broken reference",
            tone=StatusTone.fail,
        )

    if execution_blocked:
        return RuleStatus(
            code="execution_blocked",
            label="Execution blocked",
            tone=StatusTone.fail,
        )

    if _has_incompatible_fields(validation):
        return RuleStatus(
            code="incompatible_fields",
            label="Incompatible fields",
            tone=StatusTone.fail,
        )

    has_threshold_missing = _has_message(validation, "scalar_threshold")
    has_direction_missing = _has_message(validation, "threshold_direction")

    if has_threshold_missing and has_direction_missing:
        return RuleStatus(
            code="scalar_setup_incomplete",
            label="Scalar setup incomplete",
            tone=StatusTone.fail,
        )

    if has_threshold_missing:
        return RuleStatus(
            code="threshold_missing",
            label="Threshold missing",
            tone=StatusTone.fail,
        )

    if has_direction_missing:
        return RuleStatus(
            code="direction_missing",
            label="Direction missing",
            tone=StatusTone.fail,
        )

    if _has_message(validation, "phenotype_id"):
        return RuleStatus(
            code="phenotype_missing",
            label="Phenotype missing",
            tone=StatusTone.fail,
        )

    if _has_message(validation, "concept_id"):
        return RuleStatus(
            code="concept_missing",
            label="Concept missing",
            tone=StatusTone.fail,
        )

    if matcher is None or _has_message(validation, "matcher"):
        return RuleStatus(
            code="draft",
            label="Draft",
            tone=StatusTone.warn,
        )

    if shared:
        return RuleStatus(
            code="shared",
            label="Shared",
            tone=StatusTone.warn,
        )

    return RuleStatus(
        code="ready",
        label="Ready",
        tone=StatusTone.pass_,
    )


def _has_message(validation: ValidationResult, path: str) -> bool:
    return any(message.path == path for message in validation.messages)


def _has_incompatible_fields(validation: ValidationResult) -> bool:
    return any("is not valid for this matcher" in message.message for message in validation.messages)
