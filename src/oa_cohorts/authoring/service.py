from __future__ import annotations

import sqlalchemy.orm as so

from . import loaders, mutations, preview, validation
from .models import (
    EntityDetail,
    EntityKind,
    EntityPayload,
    MutationResult,
    ParentRef,
    RelationKind,
    ReportSummary,
    DashCohortDefSummary,
    ReportWorkspace,
    SQLPreview,
    SQLVariant,
    UsageSummary,
    ValidationResult,
    DashCohortDefWorkspace
)

class AuthoringService:

    def list_cohorts(self, session: so.Session) -> list[DashCohortDefSummary]:
        return loaders.list_cohorts(session)
    
    def list_reports(self, session: so.Session) -> list[ReportSummary]:
        return loaders.list_reports(session)

    def get_report_workspace(self, session: so.Session, report_id: int) -> ReportWorkspace:
        return loaders.load_report_workspace(session, report_id)

    def get_dash_cohort_workspace_by_short_name(self, session: so.Session, dash_cohort_def_short_name: str) -> DashCohortDefWorkspace:
        return loaders.load_dash_cohort_workspace_by_short_name(session, dash_cohort_def_short_name)

    def get_report_workspace_by_short_name(
        self,
        session: so.Session,
        report_short_name: str,
    ) -> ReportWorkspace:
        return loaders.load_report_workspace_by_short_name(session, report_short_name)

    def is_report_short_name_available(
        self,
        session: so.Session,
        report_short_name: str,
        *,
        exclude_report_id: int | None = None,
    ) -> bool:
        return loaders.is_report_short_name_available(
            session,
            report_short_name,
            exclude_report_id=exclude_report_id,
        )

    def get_entity_detail(self, session: so.Session, kind: EntityKind, entity_id: int) -> EntityDetail:
        return loaders.load_entity_detail(session, kind, entity_id)

    def get_usage(self, session: so.Session, kind: EntityKind, entity_id: int) -> UsageSummary:
        return loaders.compute_usage(session, kind, entity_id)

    def preview_measure_sql(self, session: so.Session, measure_id: int, variant: SQLVariant) -> SQLPreview:
        measure = loaders.get_entity(session, EntityKind.measure, measure_id)
        return preview.preview_measure(measure, variant)

    def preview_subquery_sql(self, session: so.Session, subquery_id: int, variant: SQLVariant) -> SQLPreview:
        subquery = loaders.get_entity(session, EntityKind.subquery, subquery_id)
        return preview.preview_subquery(subquery, variant)

    def validate_entity(
        self,
        session: so.Session,
        kind: EntityKind,
        payload: EntityPayload,
        entity_id: int | None = None,
    ) -> ValidationResult:
        del session, entity_id
        return validation.validate_payload(kind, payload)

    def create_entity(
        self,
        session: so.Session,
        kind: EntityKind,
        payload: EntityPayload,
        parent: ParentRef | None = None,
    ) -> MutationResult:
        return mutations.create_entity(session, kind, payload, parent=parent)

    def update_entity(
        self,
        session: so.Session,
        kind: EntityKind,
        entity_id: int,
        payload: EntityPayload,
    ) -> MutationResult:
        return mutations.update_entity(session, kind, entity_id, payload)

    def link_entities(
        self,
        session: so.Session,
        relation: RelationKind,
        left_id: int,
        right_id: int,
        attrs: dict | None = None,
    ) -> MutationResult:
        return mutations.link_entities(session, relation, left_id, right_id, attrs=attrs)

    def unlink_entities(
        self,
        session: so.Session,
        relation: RelationKind,
        left_id: int,
        right_id: int,
    ) -> MutationResult:
        return mutations.unlink_entities(session, relation, left_id, right_id)

    def clone_for_edit(
        self,
        session: so.Session,
        kind: EntityKind,
        entity_id: int,
        parent: ParentRef,
    ) -> MutationResult:
        return mutations.clone_for_edit(session, kind, entity_id, parent)

    def delete_entity(self, session: so.Session, kind: EntityKind, entity_id: int) -> MutationResult:
        return mutations.delete_entity(session, kind, entity_id)
