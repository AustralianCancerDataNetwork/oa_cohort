from __future__ import annotations

from dataclasses import dataclass

import sqlalchemy as sa
from orm_loader.helpers import Base


@dataclass(frozen=True)
class SchemaBootstrapResult:
    table_count: int
    created_tables: tuple[str, ...]
    existing_tables: tuple[str, ...]


def bootstrap_query_schema(engine: sa.Engine) -> SchemaBootstrapResult:
    _load_query_metadata()

    inspector = sa.inspect(engine)
    tables = tuple(sorted(Base.metadata.tables))
    existing_before = {table_name for table_name in tables if inspector.has_table(table_name)}

    Base.metadata.create_all(engine)

    created_tables = tuple(table_name for table_name in tables if table_name not in existing_before)
    existing_tables = tuple(table_name for table_name in tables if table_name in existing_before)
    return SchemaBootstrapResult(
        table_count=len(tables),
        created_tables=created_tables,
        existing_tables=existing_tables,
    )


def _load_query_metadata() -> None:
    # Import the config/query ORM modules so their tables are registered on Base.metadata
    import oa_cohorts.query  # noqa: F401
