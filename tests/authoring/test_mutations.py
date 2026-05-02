from __future__ import annotations

from datetime import date

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.authoring import AuthoringService, EntityKind, ParentRef, RelationKind
from oa_cohorts.cli.config_import import import_config_directory
from oa_cohorts.query.report import Report
from tests.ux.test_config_import import _build_config_dir


def test_create_link_and_delete_rule(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        created = service.create_entity(
            session,
            EntityKind.query_rule,
            {"matcher": "exact", "concept_id": 36561408, "notes": "new rule"},
            parent=ParentRef(relation=RelationKind.subquery_rule, parent_id=1),
        )
        assert created.ok is True
        assert created.entity_id is not None

    with session_factory() as session:
        deleted = service.delete_entity(session, EntityKind.query_rule, created.entity_id)
        assert deleted.ok is False

    with session_factory() as session:
        unlinked = service.unlink_entities(session, RelationKind.subquery_rule, 1, created.entity_id)
        assert unlinked.ok is True
        deleted = service.delete_entity(session, EntityKind.query_rule, created.entity_id)
        assert deleted.ok is True


def test_clone_before_edit_for_shared_measure(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        blocked = service.update_entity(session, EntityKind.measure, 2, {"name": "changed", "combination": "or"})
        assert blocked.ok is False

    with session_factory() as session:
        cloned = service.clone_for_edit(
            session,
            EntityKind.measure,
            2,
            ParentRef(relation=RelationKind.indicator_numerator, parent_id=1),
        )
        assert cloned.ok is True
        assert cloned.entity_id is not None
        updated = service.update_entity(
            session,
            EntityKind.measure,
            cloned.entity_id,
            {"name": "Cloned numerator", "combination": "or", "subquery_id": 2, "person_ep_override": False},
        )
        assert updated.ok is True


def test_create_entity_with_failed_parent_link_rolls_back(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        result = service.create_entity(
            session,
            EntityKind.query_rule,
            {"matcher": "exact", "concept_id": 36561408},
            parent=ParentRef(relation=RelationKind.measure_subquery, parent_id=9999),
        )
        assert result.ok is False

    with session_factory() as session:
        detail = service.get_entity_detail(session, EntityKind.subquery, 1)
        assert len(detail.relationships["rules"]) == 1


def test_report_cohort_link_parses_false_primary_flag(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)
        cohort = service.create_entity(session, EntityKind.dash_cohort, {"dash_cohort_name": "Secondary cohort"})
        assert cohort.ok is True

    with session_factory() as session:
        linked = service.link_entities(
            session,
            RelationKind.report_cohort,
            1,
            cohort.entity_id,
            attrs={"primary_cohort": "false"},
        )
        assert linked.ok is True
        row = session.execute(
            sa.select(sa.literal_column("primary_cohort")).select_from(sa.text("report_cohort_map")).where(
                sa.text(f"dash_cohort_id = {cohort.entity_id}")
            )
        ).one()
        assert row[0] in (False, 0)


def test_report_update_stamps_edit_date(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        updated = service.update_entity(
            session,
            EntityKind.report,
            1,
            {
                "report_name": "Workbench report renamed",
                "report_short_name": "workbench",
                "report_description": "Updated description",
                "report_author": "Author",
                "report_owner": "Owner",
            },
        )
        assert updated.ok is True

    with session_factory() as session:
        report = session.get(Report, 1)
        assert report is not None
        assert report.report_edit_date.date() == date.today()


def test_report_short_name_must_be_unique(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        created = service.create_entity(
            session,
            EntityKind.report,
            {
                "report_name": "Duplicate short name report",
                "report_short_name": "test",
                "report_description": "Duplicate",
                "report_author": "Author",
                "report_owner": "Owner",
                "primary_cohort_count": 1,
                "indicator_count": 1,
            },
        )
        assert created.ok is False
        assert "report_short_name must be unique" in created.errors
