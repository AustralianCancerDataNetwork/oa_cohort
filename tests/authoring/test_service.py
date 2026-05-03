from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.authoring import AuthoringService, EntityKind
from oa_cohorts.cli.config_import import import_config_directory
from tests.ux.test_config_import import _build_config_dir


def test_list_reports_and_workspace(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        reports = service.list_reports(session)
        assert len(reports) == 1
        workspace = service.get_report_workspace(session, 1)
        by_short_name = service.get_report_workspace_by_short_name(session, "test")
        rule_detail = service.get_entity_detail(session, EntityKind.query_rule, 1)
        assert workspace.report_name == "Test report"
        assert by_short_name.report_id == workspace.report_id
        assert workspace.primary_cohort_names == ("Test cohort",)
        assert len(workspace.cohorts) == 1
        assert len(workspace.indicators) == 1
        assert rule_detail.rule_status is not None
        rule_node = workspace.cohorts[0].children[0].children[0].children[0].children[0].children[0]
        assert rule_node.kind is EntityKind.query_rule
        assert rule_node.status_label is not None


def test_entity_detail_reports_shared_measure_usage(tmp_path):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()

    with session_factory() as session:
        import_config_directory(config_dir, session)

    with session_factory() as session:
        detail = service.get_entity_detail(session, EntityKind.measure, 2)
        assert detail.shared is True
        assert detail.allowed_actions["can_clone"] is True
        assert detail.allowed_actions["can_edit"] is False
