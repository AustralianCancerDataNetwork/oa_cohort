from __future__ import annotations

import sqlalchemy as sa

from oa_cohorts.cli.runtime import resolve_engine


def test_resolve_engine_uses_explicit_database_url(monkeypatch):
    monkeypatch.delenv("ENGINE", raising=False)

    engine, resolved_url = resolve_engine(database_url="sqlite://")

    assert isinstance(engine, sa.Engine)
    assert resolved_url == "sqlite://"
