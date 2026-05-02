from __future__ import annotations

from oa_cohorts.authoring.models import EntityKind
from oa_cohorts.authoring.validation import validate_payload


def test_query_rule_validation_flags_missing_required_fields():
    result = validate_payload(EntityKind.query_rule, {"matcher": "exact"})
    assert result.valid is False
    assert any(message.path == "concept_id" for message in result.messages)


def test_subquery_validation_flags_missing_rules_for_valid_state():
    result = validate_payload(
        EntityKind.subquery,
        {"name": "Eligible", "target": "dx_primary", "temporality": "dt_current_start", "rule_count": 0},
    )
    assert result.valid is False
    assert any(message.path == "rules" for message in result.messages)


def test_measure_validation_requires_subquery_or_children():
    result = validate_payload(
        EntityKind.measure,
        {"name": "Measure", "combination": "or", "child_count": 0},
    )
    assert result.valid is False
    assert any(message.path == "measure" for message in result.messages)


def test_report_validation_requires_primary_cohort_and_indicator():
    result = validate_payload(
        EntityKind.report,
        {
            "report_name": "Test",
            "report_short_name": "test",
            "primary_cohort_count": 0,
            "indicator_count": 0,
        },
    )
    assert result.valid is False
    assert {message.path for message in result.messages} == {"cohorts", "indicators"}
