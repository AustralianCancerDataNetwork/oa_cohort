from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.orm as so

from oa_cohorts.authoring import AuthoringService, SQLVariant
from oa_cohorts.cli.config_import import import_config_directory
from oa_cohorts.core import RuleCombination, RuleMatcher, RuleTarget, RuleTemporality
from oa_cohorts.query.measure import Measure
from oa_cohorts.query.query_rule import PresenceRule
from oa_cohorts.query.subquery import Subquery, subquery_rule_map
from tests.ux.test_config_import import _build_config_dir


def test_measure_and_subquery_preview_return_sql(tmp_path, patch_measurable_registry, monkeypatch):
    config_dir = _build_config_dir(tmp_path / "config")
    engine = sa.create_engine("sqlite://")
    session_factory = so.sessionmaker(bind=engine, future=True)
    service = AuthoringService()
    registry = dict(patch_measurable_registry)
    registry[RuleTarget.dx_primary] = registry[RuleTarget.dx_any]
    monkeypatch.setattr("oa_cohorts.query.subquery.get_measurable_registry", lambda: registry)
    monkeypatch.setattr("oa_cohorts.query.query_rule.get_measurable_registry", lambda: registry)

    with session_factory() as session:
        import_config_directory(config_dir, session)
        rule = PresenceRule(query_rule_id=99, matcher=RuleMatcher.presence)
        subquery = Subquery(
            subquery_id=99,
            target=RuleTarget.dx_any,
            temporality=RuleTemporality.dt_any,
            name="Presence preview",
            short_name="presence_preview",
        )
        measure = Measure(
            measure_id=99,
            name="Presence measure",
            combination=RuleCombination.rule_or,
            subquery_id=99,
            person_ep_override=False,
        )
        session.add_all([rule, subquery, measure])
        session.flush()
        session.execute(
            sa.insert(subquery_rule_map).values(subquery_id=subquery.subquery_id, query_rule_id=rule.query_rule_id)
        )
        session.commit()

    with session_factory() as session:
        measure_preview = service.preview_measure_sql(session, 99, SQLVariant.any)
        subquery_preview = service.preview_subquery_sql(session, 99, SQLVariant.any)
        assert measure_preview.sql
        assert subquery_preview.sql
        assert measure_preview.variant is SQLVariant.any
        assert subquery_preview.variant is SQLVariant.any
