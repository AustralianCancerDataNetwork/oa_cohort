from __future__ import annotations

from oa_cohorts.authoring.models import StatusTone, ValidationMessage, ValidationResult
from oa_cohorts.authoring.status import resolve_rule_status


def test_rule_status_concept_missing():
    status = resolve_rule_status(
        validation=ValidationResult(
            valid=False,
            messages=(ValidationMessage("concept_id", "concept_id is required for this matcher"),),
        ),
        matcher="exact",
        concept_resolves=None,
        phenotype_resolves=None,
        execution_blocked=False,
        shared=False,
    )
    assert status.code == "concept_missing"
    assert status.label == "Concept missing"
    assert status.tone is StatusTone.fail


def test_rule_status_scalar_setup_incomplete():
    status = resolve_rule_status(
        validation=ValidationResult(
            valid=False,
            messages=(
                ValidationMessage("scalar_threshold", "scalar_threshold is required for scalar rules"),
                ValidationMessage("threshold_direction", "threshold_direction is required for scalar rules"),
            ),
        ),
        matcher="scalar",
        concept_resolves=None,
        phenotype_resolves=None,
        execution_blocked=False,
        shared=False,
    )
    assert status.code == "scalar_setup_incomplete"
    assert status.label == "Scalar setup incomplete"


def test_rule_status_incompatible_fields():
    status = resolve_rule_status(
        validation=ValidationResult(
            valid=False,
            messages=(ValidationMessage("scalar_threshold", "scalar_threshold is not valid for this matcher"),),
        ),
        matcher="presence",
        concept_resolves=None,
        phenotype_resolves=None,
        execution_blocked=False,
        shared=False,
    )
    assert status.code == "incompatible_fields"
    assert status.label == "Incompatible fields"


def test_rule_status_broken_reference_outranks_other_states():
    status = resolve_rule_status(
        validation=ValidationResult(valid=True),
        matcher="phenotype",
        concept_resolves=None,
        phenotype_resolves=False,
        execution_blocked=False,
        shared=True,
    )
    assert status.code == "broken_reference"
    assert status.label == "Broken reference"


def test_rule_status_ready_and_shared():
    shared_status = resolve_rule_status(
        validation=ValidationResult(valid=True),
        matcher="exact",
        concept_resolves=True,
        phenotype_resolves=None,
        execution_blocked=False,
        shared=True,
    )
    assert shared_status.code == "shared"
    assert shared_status.label == "Shared"
    assert shared_status.tone is StatusTone.warn

    ready_status = resolve_rule_status(
        validation=ValidationResult(valid=True),
        matcher="exact",
        concept_resolves=True,
        phenotype_resolves=None,
        execution_blocked=False,
        shared=False,
    )
    assert ready_status.code == "ready"
    assert ready_status.label == "Ready"
    assert ready_status.tone is StatusTone.pass_
