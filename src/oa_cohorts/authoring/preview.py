from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from oa_cohorts.core.executability import ExecStatus
from oa_cohorts.query.measure import Measure, MeasureSQLCompiler
from oa_cohorts.query.subquery import Subquery

from .models import EntityKind, SQLPreview, SQLVariant


def preview_measure(measure: Measure, variant: SQLVariant) -> SQLPreview:
    try:
        compiler = MeasureSQLCompiler(measure)
        stmt = _variant_fn(compiler, variant)()
        sql = _render_sql(stmt)
        check = measure.is_executable()
        return SQLPreview(
            kind=EntityKind.measure,
            entity_id=measure.measure_id,
            variant=variant,
            sql=sql,
            executable=check.status is not ExecStatus.FAIL,
            status=check.status.value,
        )
    except Exception as exc:
        return SQLPreview(
            kind=EntityKind.measure,
            entity_id=measure.measure_id,
            variant=variant,
            sql=None,
            executable=False,
            status=ExecStatus.FAIL.value,
            errors=(str(exc),),
        )


def preview_subquery(subquery: Subquery, variant: SQLVariant) -> SQLPreview:
    try:
        stmt = _variant_fn(subquery, variant)()
        sql = _render_sql(stmt)
        statuses: list[str] = []
        for current in SQLVariant:
            try:
                _render_sql(_variant_fn(subquery, current)())
                statuses.append(ExecStatus.PASS.value)
            except Exception:
                statuses.append(ExecStatus.FAIL.value)
        status = ExecStatus.PASS.value if all(item == ExecStatus.PASS.value for item in statuses) else ExecStatus.WARN.value
        return SQLPreview(
            kind=EntityKind.subquery,
            entity_id=subquery.subquery_id,
            variant=variant,
            sql=sql,
            executable=status != ExecStatus.FAIL.value,
            status=status,
        )
    except Exception as exc:
        return SQLPreview(
            kind=EntityKind.subquery,
            entity_id=subquery.subquery_id,
            variant=variant,
            sql=None,
            executable=False,
            status=ExecStatus.FAIL.value,
            errors=(str(exc),),
        )


def _variant_fn(obj: Any, variant: SQLVariant):
    if variant is SQLVariant.any:
        return obj.sql_any
    if variant is SQLVariant.first:
        return obj.sql_first
    return obj.sql_undated


def _render_sql(stmt: sa.ClauseElement) -> str:
    try:
        from sqlalchemy.dialects import postgresql

        dialect = postgresql.dialect()
    except Exception:
        dialect = sa.create_engine("sqlite://").dialect
    compiled = stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    return str(compiled)
