from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as so
from sqlalchemy.exc import IntegrityError

from oa_cohorts.query.dash_cohort import DashCohort, DashCohortDef, dash_cohort_def_map
from oa_cohorts.query.indicator import Indicator
from oa_cohorts.query.measure import Measure, MeasureRelationship
from oa_cohorts.query.phenotype import Phenotype, PhenotypeDefinition
from oa_cohorts.query.query_rule import QueryRule
from oa_cohorts.query.report import Report, ReportCohortMap, ReportVersion, report_indicator_map
from oa_cohorts.query.subquery import Subquery, subquery_rule_map

from .loaders import ENTITY_MODEL, compute_usage, get_entity, load_entity_detail
from .models import EntityKind, EntityPayload, MutationResult, ParentRef, RelationKind
from .validation import coerce_payload, validate_entity_instance, validate_payload
from oa_cohorts.core.coercion import parse_bool


DIRECT_MUTABLE_FIELDS: dict[EntityKind, frozenset[str]] = {
    EntityKind.report: frozenset(
        {
            "report_name",
            "report_short_name",
            "report_description",
            "report_create_date",
            "report_edit_date",
            "report_author",
            "report_owner",
        }
    ),
    EntityKind.report_version: frozenset(
        {
            "report_id",
            "report_version_major",
            "report_version_minor",
            "report_version_label",
            "report_version_date",
            "report_status",
        }
    ),
    EntityKind.indicator: frozenset(
        {
            "indicator_description",
            "indicator_reference",
            "numerator_measure_id",
            "numerator_label",
            "denominator_measure_id",
            "denominator_label",
            "temporal_early",
            "temporal_late",
            "temporal_min",
            "temporal_min_units",
            "temporal_max",
            "temporal_max_units",
            "numerator_max_days_prior",
            "numerator_max_days_post",
            "denominator_max_days_prior",
            "denominator_max_days_post",
            "benchmark",
            "benchmark_unit",
        }
    ),
    EntityKind.dash_cohort: frozenset({"dash_cohort_name"}),
    EntityKind.dash_cohort_def: frozenset({"dash_cohort_def_name", "dash_cohort_def_short_name", "measure_id"}),
    EntityKind.measure: frozenset({"name", "combination", "subquery_id", "person_ep_override"}),
    EntityKind.subquery: frozenset({"target", "temporality", "name", "short_name"}),
    EntityKind.query_rule: frozenset(
        {
            "matcher",
            "concept_id",
            "notes",
            "scalar_threshold",
            "threshold_direction",
            "threshold_comparator",
            "phenotype_id",
        }
    ),
    EntityKind.phenotype: frozenset({"phenotype_name", "description"}),
}


def create_entity(
    session: so.Session,
    kind: EntityKind,
    payload: EntityPayload,
    parent: ParentRef | None = None,
) -> MutationResult:
    validation = validate_payload(kind, payload)
    if not validation.valid:
        return MutationResult(ok=False, kind=kind, entity_id=None, validation=validation, errors=_messages(validation))

    try:
        cleaned = _filtered_payload(kind, payload)
        _validate_unique_report_short_name(session, kind, cleaned, entity_id=None)
        model = ENTITY_MODEL[kind]
        instance = _construct_instance(model, cleaned)
        session.add(instance)
        session.flush()
        if parent is not None:
            _attach_child(session, parent, _entity_id(instance, kind))
        entity_id = _entity_id(instance, kind)
        session.commit()
    except Exception as exc:
        session.rollback()
        return MutationResult(ok=False, kind=kind, entity_id=None, errors=(_friendly_mutation_error(kind, exc),))

    detail = load_entity_detail(session, kind, entity_id)
    return MutationResult(ok=True, kind=kind, entity_id=entity_id, detail=detail, usage=detail.usage, validation=detail.validation)


def update_entity(session: so.Session, kind: EntityKind, entity_id: int, payload: EntityPayload) -> MutationResult:
    entity = get_entity(session, kind, entity_id)
    usage = compute_usage(session, kind, entity_id)
    if usage.shared or (kind is EntityKind.measure and entity_id == 0):
        return MutationResult(
            ok=False,
            kind=kind,
            entity_id=entity_id,
            errors=("entity is shared or read-only; clone before edit",),
        )

    validation_payload: dict[str, Any] = dict(payload)
    if kind is EntityKind.report:
        validation_payload.setdefault(
            "primary_cohort_count",
            sum(1 for item in entity.cohorts if item.primary_cohort),
        )
        validation_payload.setdefault("indicator_count", len(entity.indicators))

    validation = validate_payload(kind, validation_payload)
    if not validation.valid:
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, validation=validation, errors=_messages(validation))

    try:
        cleaned = _filtered_payload(kind, payload)
        _validate_unique_report_short_name(session, kind, cleaned, entity_id=entity_id)
        for key, value in cleaned.items():
            setattr(entity, key, value)
        session.flush()
        refreshed_validation = validate_entity_instance(kind, entity)
        if not refreshed_validation.valid:
            session.rollback()
            return MutationResult(
                ok=False,
                kind=kind,
                entity_id=entity_id,
                validation=refreshed_validation,
                errors=_messages(refreshed_validation),
            )
        session.commit()
    except Exception as exc:
        session.rollback()
        return MutationResult(
            ok=False,
            kind=kind,
            entity_id=entity_id,
            errors=(_friendly_mutation_error(kind, exc),),
        )

    detail = load_entity_detail(session, kind, entity_id)
    return MutationResult(
        ok=True,
        kind=kind,
        entity_id=entity_id,
        detail=detail,
        usage=detail.usage,
        validation=detail.validation,
    )


def link_entities(
    session: so.Session,
    relation: RelationKind,
    left_id: int,
    right_id: int,
    attrs: Mapping[str, Any] | None = None,
) -> MutationResult:
    try:
        kind, entity_id = _link_entities_internal(session, relation, left_id, right_id, attrs or {})
        session.commit()
    except Exception as exc:
        session.rollback()
        kind = _relation_owner_kind(relation)
        entity_id = left_id
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=(str(exc),))

    detail = load_entity_detail(session, kind, entity_id)
    return MutationResult(ok=True, kind=kind, entity_id=entity_id, detail=detail, usage=detail.usage, validation=detail.validation)


def unlink_entities(session: so.Session, relation: RelationKind, left_id: int, right_id: int) -> MutationResult:
    try:
        kind, entity_id = _unlink_entities_internal(session, relation, left_id, right_id)
        session.commit()
    except Exception as exc:
        session.rollback()
        kind = _relation_owner_kind(relation)
        entity_id = left_id
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=(str(exc),))

    detail = load_entity_detail(session, kind, entity_id)
    return MutationResult(ok=True, kind=kind, entity_id=entity_id, detail=detail, usage=detail.usage, validation=detail.validation)


def clone_for_edit(session: so.Session, kind: EntityKind, entity_id: int, parent: ParentRef) -> MutationResult:
    try:
        entity = get_entity(session, kind, entity_id)
        clone_id: int
        if kind is EntityKind.measure:
            clone = _clone_measure_tree(session, entity)
            clone_id = clone.measure_id
        elif kind is EntityKind.subquery:
            clone = _clone_subquery(session, entity)
            clone_id = clone.subquery_id
        elif kind is EntityKind.query_rule:
            clone = _clone_query_rule(session, entity)
            clone_id = clone.query_rule_id
        elif kind is EntityKind.dash_cohort_def:
            clone = DashCohortDef(
                dash_cohort_def_name=entity.dash_cohort_def_name,
                dash_cohort_def_short_name=entity.dash_cohort_def_short_name,
                measure_id=entity.measure_id,
            )
            session.add(clone)
            session.flush()
            clone_id = clone.dash_cohort_def_id
        elif kind is EntityKind.dash_cohort:
            clone = DashCohort(dash_cohort_name=entity.dash_cohort_name)
            session.add(clone)
            session.flush()
            for cohort_def in entity.definitions:
                session.execute(
                    sa.insert(dash_cohort_def_map).values(
                        dash_cohort_id=clone.dash_cohort_id,
                        dash_cohort_def_id=cohort_def.dash_cohort_def_id,
                    )
                )
            clone_id = clone.dash_cohort_id
        elif kind is EntityKind.indicator:
            clone = Indicator(
                indicator_description=entity.indicator_description,
                indicator_reference=entity.indicator_reference,
                numerator_measure_id=entity.numerator_measure_id,
                numerator_label=entity.numerator_label,
                denominator_measure_id=entity.denominator_measure_id,
                denominator_label=entity.denominator_label,
                temporal_early=entity.temporal_early,
                temporal_late=entity.temporal_late,
                temporal_min=entity.temporal_min,
                temporal_min_units=entity.temporal_min_units,
                temporal_max=entity.temporal_max,
                temporal_max_units=entity.temporal_max_units,
                numerator_max_days_prior=entity.numerator_max_days_prior,
                numerator_max_days_post=entity.numerator_max_days_post,
                denominator_max_days_prior=entity.denominator_max_days_prior,
                denominator_max_days_post=entity.denominator_max_days_post,
                benchmark=entity.benchmark,
                benchmark_unit=entity.benchmark_unit,
            )
            session.add(clone)
            session.flush()
            clone_id = clone.indicator_id
        else:
            return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=("clone is not supported for this entity kind",))

        _relink_clone(session, parent, kind, entity_id, clone_id)
        session.commit()
    except Exception as exc:
        session.rollback()
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=(str(exc),))

    detail = load_entity_detail(session, kind, clone_id)
    return MutationResult(ok=True, kind=kind, entity_id=clone_id, detail=detail, usage=detail.usage, validation=detail.validation)


def delete_entity(session: so.Session, kind: EntityKind, entity_id: int) -> MutationResult:
    usage = compute_usage(session, kind, entity_id)
    if usage.inbound_count:
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, usage=usage, errors=("entity has inbound references",))
    if kind is EntityKind.measure and entity_id == 0:
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=("measure_id 0 is read-only",))

    try:
        entity = get_entity(session, kind, entity_id)
        _cleanup_outbound_links(session, kind, entity_id)
        session.delete(entity)
        session.commit()
    except Exception as exc:
        session.rollback()
        return MutationResult(ok=False, kind=kind, entity_id=entity_id, errors=(str(exc),))

    return MutationResult(ok=True, kind=kind, entity_id=entity_id)


def _cleanup_outbound_links(session: so.Session, kind: EntityKind, entity_id: int) -> None:
    if kind is EntityKind.report:
        session.execute(sa.delete(report_indicator_map).where(report_indicator_map.c.report_id == entity_id))
        session.execute(sa.delete(ReportCohortMap).where(ReportCohortMap.report_id == entity_id))
        session.execute(sa.delete(ReportVersion).where(ReportVersion.report_id == entity_id))
    elif kind is EntityKind.dash_cohort:
        session.execute(sa.delete(dash_cohort_def_map).where(dash_cohort_def_map.c.dash_cohort_id == entity_id))
    elif kind is EntityKind.dash_cohort_def:
        session.execute(sa.delete(dash_cohort_def_map).where(dash_cohort_def_map.c.dash_cohort_def_id == entity_id))
    elif kind is EntityKind.measure:
        session.execute(sa.delete(MeasureRelationship).where(MeasureRelationship.parent_measure_id == entity_id))
        session.execute(sa.delete(MeasureRelationship).where(MeasureRelationship.child_measure_id == entity_id))
    elif kind is EntityKind.subquery:
        session.execute(sa.delete(subquery_rule_map).where(subquery_rule_map.c.subquery_id == entity_id))
    elif kind is EntityKind.phenotype:
        session.execute(sa.delete(PhenotypeDefinition).where(PhenotypeDefinition.phenotype_id == entity_id))


def _filtered_payload(kind: EntityKind, payload: EntityPayload) -> dict[str, Any]:
    cleaned = coerce_payload(kind, payload)
    allowed = DIRECT_MUTABLE_FIELDS[kind]
    filtered = {key: value for key, value in cleaned.items() if key in allowed}
    if kind is EntityKind.report and filtered:
        filtered["report_edit_date"] = date.today()
    return filtered


def _construct_instance(model: type, cleaned: dict[str, Any]) -> Any:
    return model(**cleaned)


def _validate_unique_report_short_name(
    session: so.Session,
    kind: EntityKind,
    cleaned: Mapping[str, Any],
    *,
    entity_id: int | None,
) -> None:
    if kind is not EntityKind.report:
        return
    report_short_name = cleaned.get("report_short_name")
    if not report_short_name:
        return
    stmt = sa.select(Report.report_id).where(
        sa.func.lower(Report.report_short_name) == str(report_short_name).lower()
    )
    existing_ids = session.execute(stmt).scalars().all()
    if any(existing_id != entity_id for existing_id in existing_ids):
        raise ValueError("report_short_name must be unique")


def _friendly_mutation_error(kind: EntityKind, exc: Exception) -> str:
    if kind is EntityKind.report and isinstance(exc, IntegrityError):
        detail = str(exc.orig).lower() if exc.orig is not None else str(exc).lower()
        if "report.report_short_name" in detail or "report_short_name" in detail:
            return "report_short_name must be unique"
    return str(exc)


def _entity_id(instance: Any, kind: EntityKind) -> int:
    if kind is EntityKind.report:
        return instance.report_id
    if kind is EntityKind.report_version:
        return instance.report_version_id
    if kind is EntityKind.indicator:
        return instance.indicator_id
    if kind is EntityKind.dash_cohort:
        return instance.dash_cohort_id
    if kind is EntityKind.dash_cohort_def:
        return instance.dash_cohort_def_id
    if kind is EntityKind.measure:
        return instance.measure_id
    if kind is EntityKind.subquery:
        return instance.subquery_id
    if kind is EntityKind.query_rule:
        return instance.query_rule_id
    return instance.phenotype_id


def _attach_child(session: so.Session, parent: ParentRef, child_id: int) -> None:
    _link_entities_internal(session, parent.relation, parent.parent_id, child_id, parent.attrs)


def _link_entities_internal(
    session: so.Session,
    relation: RelationKind,
    left_id: int,
    right_id: int,
    attrs: Mapping[str, Any],
) -> tuple[EntityKind, int]:
    if relation is RelationKind.report_indicator:
        session.execute(sa.insert(report_indicator_map).values(report_id=left_id, indicator_id=right_id))
        return EntityKind.report, left_id
    if relation is RelationKind.report_cohort:
        primary_cohort = parse_bool(attrs.get("primary_cohort", False))
        session.add(
            ReportCohortMap(
                report_id=left_id,
                dash_cohort_id=right_id,
                primary_cohort=bool(primary_cohort),
            )
        )
        return EntityKind.report, left_id
    if relation is RelationKind.dash_cohort_definition:
        session.execute(
            sa.insert(dash_cohort_def_map).values(dash_cohort_id=left_id, dash_cohort_def_id=right_id)
        )
        return EntityKind.dash_cohort, left_id
    if relation is RelationKind.measure_child:
        session.add(MeasureRelationship(parent_measure_id=left_id, child_measure_id=right_id))
        return EntityKind.measure, left_id
    if relation is RelationKind.subquery_rule:
        session.execute(
            sa.insert(subquery_rule_map).values(subquery_id=left_id, query_rule_id=right_id)
        )
        return EntityKind.subquery, left_id
    if relation is RelationKind.indicator_numerator:
        indicator = get_entity(session, EntityKind.indicator, left_id)
        indicator.numerator_measure_id = right_id
        return EntityKind.indicator, left_id
    if relation is RelationKind.indicator_denominator:
        indicator = get_entity(session, EntityKind.indicator, left_id)
        indicator.denominator_measure_id = right_id
        return EntityKind.indicator, left_id
    if relation is RelationKind.dash_cohort_def_measure:
        cohort_def = get_entity(session, EntityKind.dash_cohort_def, left_id)
        cohort_def.measure_id = right_id
        return EntityKind.dash_cohort_def, left_id
    if relation is RelationKind.measure_subquery:
        measure = get_entity(session, EntityKind.measure, left_id)
        measure.subquery_id = right_id
        return EntityKind.measure, left_id
    if relation is RelationKind.report_version:
        version = get_entity(session, EntityKind.report_version, right_id)
        version.report_id = left_id
        return EntityKind.report, left_id
    raise ValueError(f"Unsupported relation {relation}")


def _unlink_entities_internal(
    session: so.Session,
    relation: RelationKind,
    left_id: int,
    right_id: int,
) -> tuple[EntityKind, int]:
    if relation is RelationKind.report_indicator:
        session.execute(
            sa.delete(report_indicator_map).where(
                sa.and_(
                    report_indicator_map.c.report_id == left_id,
                    report_indicator_map.c.indicator_id == right_id,
                )
            )
        )
        return EntityKind.report, left_id
    if relation is RelationKind.report_cohort:
        session.execute(
            sa.delete(ReportCohortMap).where(
                sa.and_(ReportCohortMap.report_id == left_id, ReportCohortMap.dash_cohort_id == right_id)
            )
        )
        return EntityKind.report, left_id
    if relation is RelationKind.dash_cohort_definition:
        session.execute(
            sa.delete(dash_cohort_def_map).where(
                sa.and_(
                    dash_cohort_def_map.c.dash_cohort_id == left_id,
                    dash_cohort_def_map.c.dash_cohort_def_id == right_id,
                )
            )
        )
        return EntityKind.dash_cohort, left_id
    if relation is RelationKind.measure_child:
        session.execute(
            sa.delete(MeasureRelationship).where(
                sa.and_(
                    MeasureRelationship.parent_measure_id == left_id,
                    MeasureRelationship.child_measure_id == right_id,
                )
            )
        )
        return EntityKind.measure, left_id
    if relation is RelationKind.subquery_rule:
        session.execute(
            sa.delete(subquery_rule_map).where(
                sa.and_(
                    subquery_rule_map.c.subquery_id == left_id,
                    subquery_rule_map.c.query_rule_id == right_id,
                )
            )
        )
        return EntityKind.subquery, left_id
    raise ValueError(f"Unsupported relation {relation}")


def _relation_owner_kind(relation: RelationKind) -> EntityKind:
    if relation in {RelationKind.report_indicator, RelationKind.report_cohort, RelationKind.report_version}:
        return EntityKind.report
    if relation is RelationKind.dash_cohort_definition:
        return EntityKind.dash_cohort
    if relation in {RelationKind.measure_child, RelationKind.measure_subquery}:
        return EntityKind.measure
    if relation in {RelationKind.indicator_numerator, RelationKind.indicator_denominator}:
        return EntityKind.indicator
    if relation is RelationKind.subquery_rule:
        return EntityKind.subquery
    if relation is RelationKind.dash_cohort_def_measure:
        return EntityKind.dash_cohort_def
    return EntityKind.report


def _clone_measure_tree(session: so.Session, measure: Measure) -> Measure:
    clone = Measure(
        name=measure.name,
        combination=measure.combination,
        person_ep_override=measure.person_ep_override,
        subquery=_clone_subquery(session, measure.subquery) if measure.subquery is not None else None,
    )
    session.add(clone)
    session.flush()
    for child in measure.children:
        child_clone = _clone_measure_tree(session, child)
        session.add(MeasureRelationship(parent_measure_id=clone.measure_id, child_measure_id=child_clone.measure_id))
    session.flush()
    return clone


def _clone_subquery(session: so.Session, subquery: Subquery | None) -> Subquery:
    if subquery is None:
        raise ValueError("subquery is required")
    clone = Subquery(
        target=subquery.target,
        temporality=subquery.temporality,
        name=subquery.name,
        short_name=subquery.short_name,
    )
    session.add(clone)
    session.flush()
    for rule in subquery.rules:
        rule_clone = _clone_query_rule(session, rule)
        session.execute(
            sa.insert(subquery_rule_map).values(
                subquery_id=clone.subquery_id,
                query_rule_id=rule_clone.query_rule_id,
            )
        )
    return clone


def _clone_query_rule(session: so.Session, rule: QueryRule) -> QueryRule:
    clone = rule.__class__(
        matcher=rule.matcher,
        concept_id=rule.concept_id,
        notes=rule.notes,
        scalar_threshold=rule.scalar_threshold,
        threshold_direction=rule.threshold_direction,
        threshold_comparator=rule.threshold_comparator,
        phenotype_id=rule.phenotype_id,
    )
    session.add(clone)
    session.flush()
    return clone


def _relink_clone(
    session: so.Session,
    parent: ParentRef,
    kind: EntityKind,
    original_id: int,
    clone_id: int,
) -> None:
    if parent.relation is RelationKind.measure_child:
        session.execute(
            sa.delete(MeasureRelationship).where(
                sa.and_(
                    MeasureRelationship.parent_measure_id == parent.parent_id,
                    MeasureRelationship.child_measure_id == original_id,
                )
            )
        )
        session.add(MeasureRelationship(parent_measure_id=parent.parent_id, child_measure_id=clone_id))
    elif parent.relation is RelationKind.subquery_rule:
        session.execute(
            sa.delete(subquery_rule_map).where(
                sa.and_(
                    subquery_rule_map.c.subquery_id == parent.parent_id,
                    subquery_rule_map.c.query_rule_id == original_id,
                )
            )
        )
        session.execute(
            sa.insert(subquery_rule_map).values(subquery_id=parent.parent_id, query_rule_id=clone_id)
        )
    elif parent.relation is RelationKind.measure_subquery:
        measure = get_entity(session, EntityKind.measure, parent.parent_id)
        measure.subquery_id = clone_id
    elif parent.relation is RelationKind.dash_cohort_def_measure:
        cohort_def = get_entity(session, EntityKind.dash_cohort_def, parent.parent_id)
        cohort_def.measure_id = clone_id
    elif parent.relation is RelationKind.indicator_numerator:
        indicator = get_entity(session, EntityKind.indicator, parent.parent_id)
        indicator.numerator_measure_id = clone_id
    elif parent.relation is RelationKind.indicator_denominator:
        indicator = get_entity(session, EntityKind.indicator, parent.parent_id)
        indicator.denominator_measure_id = clone_id
    elif parent.relation is RelationKind.report_indicator:
        session.execute(
            sa.delete(report_indicator_map).where(
                sa.and_(
                    report_indicator_map.c.report_id == parent.parent_id,
                    report_indicator_map.c.indicator_id == original_id,
                )
            )
        )
        session.execute(
            sa.insert(report_indicator_map).values(report_id=parent.parent_id, indicator_id=clone_id)
        )
    elif parent.relation is RelationKind.dash_cohort_definition:
        session.execute(
            sa.delete(dash_cohort_def_map).where(
                sa.and_(
                    dash_cohort_def_map.c.dash_cohort_id == parent.parent_id,
                    dash_cohort_def_map.c.dash_cohort_def_id == original_id,
                )
            )
        )
        session.execute(
            sa.insert(dash_cohort_def_map).values(dash_cohort_id=parent.parent_id, dash_cohort_def_id=clone_id)
        )
    elif parent.relation is RelationKind.report_cohort:
        mapping = session.execute(
            sa.select(ReportCohortMap)
            .where(
                sa.and_(
                    ReportCohortMap.report_id == parent.parent_id,
                    ReportCohortMap.dash_cohort_id == original_id,
                )
            )
        ).scalars().one()
        mapping.dash_cohort_id = clone_id
    else:
        raise ValueError(f"Unsupported parent relation for relink: {parent.relation}")


def _messages(validation) -> tuple[str, ...]:
    return tuple(message.message for message in validation.messages)
