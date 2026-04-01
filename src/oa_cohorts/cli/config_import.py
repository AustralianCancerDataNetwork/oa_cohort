from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Callable, Iterable

import sqlalchemy as sa
import sqlalchemy.orm as so
from orm_loader.helpers import Base, normalise_null

from ..query import (
    DashCohort,
    DashCohortDef,
    Indicator,
    Measure,
    MeasureRelationship,
    Phenotype,
    PhenotypeDefinition,
    QueryRule,
    Report,
    ReportCohortMap,
    ReportVersion,
    Subquery,
    dash_cohort_def_map,
    report_indicator_map,
    subquery_rule_map,
)


RowTransform = Callable[[Any], Any]
ImportProgressCallback = Callable[["ImportProgressEvent"], None]


@dataclass(frozen=True)
class TableImportSpec:
    table: sa.Table
    filenames: tuple[str, ...]
    legacy_columns: dict[str, str] = field(default_factory=dict)
    value_transforms: dict[str, RowTransform] = field(default_factory=dict)

    @property
    def label(self) -> str:
        return self.table.name

    def resolve_path(self, config_path: Path) -> Path:
        for filename in self.filenames:
            candidate = config_path / filename
            if candidate.exists():
                return candidate
        expected = ", ".join(self.filenames)
        raise FileNotFoundError(f"Missing CSV for {self.table.name}: expected one of {expected}")


@dataclass(frozen=True)
class TableImportResult:
    table_name: str
    file_name: str
    total_rows: int
    dropped_duplicate_rows: int
    skipped_existing_rows: int
    replaced_rows: int
    inserted_rows: int


@dataclass(frozen=True)
class ImportProgressEvent:
    phase: str
    table_name: str | None
    table_index: int
    table_count: int
    detail: str
    dry_run: bool


def _emit_progress(
    progress_callback: ImportProgressCallback | None,
    *,
    phase: str,
    table_name: str | None,
    table_index: int,
    table_count: int,
    detail: str,
    dry_run: bool,
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        ImportProgressEvent(
            phase=phase,
            table_name=table_name,
            table_index=table_index,
            table_count=table_count,
            detail=detail,
            dry_run=dry_run,
        )
    )


def _parse_bool(value: Any) -> bool | None:
    value = normalise_null(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "t", "1", "yes", "y"}:
            return True
        if lowered in {"false", "f", "0", "no", "n"}:
            return False
    raise ValueError(f"Cannot coerce {value!r} to bool")


def _parse_int(value: Any) -> int | None:
    value = normalise_null(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise ValueError(f"Expected integer-compatible float, got {value!r}")
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            numeric = float(text)
            if not numeric.is_integer():
                raise ValueError(f"Expected integer-compatible string, got {value!r}")
            return int(numeric)
    raise ValueError(f"Cannot coerce {value!r} to int")


def _parse_float(value: Any) -> float | None:
    value = normalise_null(value)
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    raise ValueError(f"Cannot coerce {value!r} to float")


def _parse_datetime(value: Any) -> datetime | None:
    value = normalise_null(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, time.min)
    if isinstance(value, str):
        text = value.strip()
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return datetime.combine(date.fromisoformat(text), time.min)
    raise ValueError(f"Cannot coerce {value!r} to datetime")


def _parse_date(value: Any) -> date | None:
    value = normalise_null(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value.strip())
    raise ValueError(f"Cannot coerce {value!r} to date")


def _parse_enum(value: Any, enum_cls: type) -> Any:
    value = normalise_null(value)
    if value is None or isinstance(value, enum_cls):
        return value
    if isinstance(value, str):
        text = value.strip()
        try:
            return enum_cls(text)
        except ValueError:
            for name, member in enum_cls.__members__.items():
                if text == name or text.lower() == name.lower():
                    return member
            if "_" in text:
                try:
                    return enum_cls(text.split("_", 1)[1])
                except ValueError:
                    pass
    return enum_cls(value)


def _coerce_value(column: sa.Column[Any], value: Any) -> Any:
    value = normalise_null(value)
    if value is None:
        return None

    column_type = column.type
    if isinstance(column_type, sa.Enum):
        if column_type.enum_class is None:
            return value
        return _parse_enum(value, column_type.enum_class)
    if isinstance(column_type, sa.Boolean):
        return _parse_bool(value)
    if isinstance(column_type, sa.Integer):
        return _parse_int(value)
    if isinstance(column_type, (sa.Float, sa.Numeric)):
        return _parse_float(value)
    if isinstance(column_type, sa.DateTime):
        return _parse_datetime(value)
    if isinstance(column_type, sa.Date):
        return _parse_date(value)
    return value


def _normalise_header(row: dict[str, str], spec: TableImportSpec) -> dict[str, str]:
    normalised: dict[str, str] = {}
    for key, value in row.items():
        clean_key = key.strip()
        mapped_key = spec.legacy_columns.get(clean_key, clean_key)
        if mapped_key in normalised and normalised[mapped_key] != value:
            raise ValueError(
                f"CSV for {spec.table.name} contains conflicting values for column {mapped_key!r}"
            )
        normalised[mapped_key] = value
    return normalised


def _required_columns(table: sa.Table) -> set[str]:
    return {
        column.name
        for column in table.columns
        if not column.nullable and not column.default and not column.server_default
    }


def _clean_row(raw_row: dict[str, str], spec: TableImportSpec) -> dict[str, Any]:
    row = _normalise_header(raw_row, spec)
    available_columns = {column.name: column for column in spec.table.columns}
    cleaned: dict[str, Any] = {}

    for key, value in row.items():
        if key not in available_columns:
            continue
        transformed = spec.value_transforms.get(key, lambda item: item)(value)
        cleaned[key] = _coerce_value(available_columns[key], transformed)

    missing = _required_columns(spec.table) - cleaned.keys()
    if missing:
        raise ValueError(
            f"Missing required columns for {spec.table.name}: {sorted(missing)}"
        )

    return cleaned


def _row_signature(row: dict[str, Any], column_names: Iterable[str]) -> tuple[Any, ...]:
    return tuple(row.get(column_name) for column_name in column_names)


def _dedupe_incoming_rows(
    rows: list[dict[str, Any]],
    *,
    table: sa.Table,
) -> tuple[list[dict[str, Any]], int]:
    column_names = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]

    deduped: list[dict[str, Any]] = []
    seen_full_rows: set[tuple[Any, ...]] = set()
    seen_pk_rows: dict[tuple[Any, ...], tuple[Any, ...]] = {}
    dropped = 0

    for row in rows:
        full_signature = _row_signature(row, column_names)
        if full_signature in seen_full_rows:
            dropped += 1
            continue
        if primary_keys:
            pk_signature = _row_signature(row, primary_keys)
            previous = seen_pk_rows.get(pk_signature)
            if previous is not None and previous != full_signature:
                raise ValueError(
                    f"Conflicting duplicate primary key {pk_signature!r} found in {table.name}"
                )
            seen_pk_rows[pk_signature] = full_signature

        seen_full_rows.add(full_signature)
        deduped.append(row)

    return deduped, dropped


def _load_rows(path: Path, spec: TableImportSpec) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return []
        return [_clean_row(row, spec) for row in reader]


def _delete_existing_row(
    session: so.Session,
    table: sa.Table,
    pk_names: list[str],
    row: dict[str, Any],
) -> None:
    predicate = sa.and_(*[table.c[name] == row[name] for name in pk_names])
    session.execute(sa.delete(table).where(predicate))


def _table_exists(session: so.Session, table: sa.Table) -> bool:
    bind = session.get_bind()
    inspector = sa.inspect(bind)
    return inspector.has_table(table.name, schema=table.schema)


def _existing_rows(session: so.Session, table: sa.Table) -> list[dict[str, Any]]:
    if not _table_exists(session, table):
        return []
    return [dict(row) for row in session.execute(sa.select(table)).mappings().all()]


def _sync_table(
    session: so.Session,
    spec: TableImportSpec,
    path: Path,
    *,
    table_index: int,
    table_count: int,
    dedupe: bool = True,
    dry_run: bool = False,
    progress_callback: ImportProgressCallback | None = None,
) -> TableImportResult:
    _emit_progress(
        progress_callback,
        phase="load",
        table_name=spec.table.name,
        table_index=table_index,
        table_count=table_count,
        detail=f"Loading {path.name}",
        dry_run=dry_run,
    )
    incoming_rows = _load_rows(path, spec)
    total_rows = len(incoming_rows)

    dropped_duplicate_rows = 0
    if dedupe:
        _emit_progress(
            progress_callback,
            phase="dedupe",
            table_name=spec.table.name,
            table_index=table_index,
            table_count=table_count,
            detail=f"Deduplicating {spec.table.name}",
            dry_run=dry_run,
        )
        incoming_rows, dropped_duplicate_rows = _dedupe_incoming_rows(
            incoming_rows,
            table=spec.table,
        )

    _emit_progress(
        progress_callback,
        phase="compare",
        table_name=spec.table.name,
        table_index=table_index,
        table_count=table_count,
        detail=f"Comparing {spec.table.name} against database",
        dry_run=dry_run,
    )
    existing_rows = _existing_rows(session, spec.table)

    column_names = [column.name for column in spec.table.columns]
    pk_names = [column.name for column in spec.table.primary_key.columns]

    inserted_rows: list[dict[str, Any]] = []
    replaced_rows: list[dict[str, Any]] = []
    skipped_existing_rows = 0

    if pk_names:
        existing_by_pk = {
            _row_signature(row, pk_names): row
            for row in existing_rows
        }
        for row in incoming_rows:
            pk_signature = _row_signature(row, pk_names)
            existing = existing_by_pk.get(pk_signature)
            if existing is None:
                inserted_rows.append(row)
                continue
            if _row_signature(existing, column_names) == _row_signature(row, column_names):
                skipped_existing_rows += 1
                continue
            replaced_rows.append(row)
    else:
        existing_signatures = {
            _row_signature(row, column_names)
            for row in existing_rows
        }
        for row in incoming_rows:
            full_signature = _row_signature(row, column_names)
            if full_signature in existing_signatures:
                skipped_existing_rows += 1
                continue
            inserted_rows.append(row)
            existing_signatures.add(full_signature)

    if not dry_run:
        _emit_progress(
            progress_callback,
            phase="write",
            table_name=spec.table.name,
            table_index=table_index,
            table_count=table_count,
            detail=f"Writing {spec.table.name}",
            dry_run=dry_run,
        )
        for row in replaced_rows:
            _delete_existing_row(session, spec.table, pk_names, row)

        write_rows = replaced_rows + inserted_rows
        if write_rows:
            session.execute(sa.insert(spec.table), write_rows)
        session.commit()

    _emit_progress(
        progress_callback,
        phase="complete",
        table_name=spec.table.name,
        table_index=table_index,
        table_count=table_count,
        detail=f"{'Planned' if dry_run else 'Imported'} {spec.table.name}",
        dry_run=dry_run,
    )

    return TableImportResult(
        table_name=spec.table.name,
        file_name=path.name,
        total_rows=total_rows,
        dropped_duplicate_rows=dropped_duplicate_rows,
        skipped_existing_rows=skipped_existing_rows,
        replaced_rows=len(replaced_rows),
        inserted_rows=len(inserted_rows),
    )


CONFIG_IMPORT_SPECS: tuple[TableImportSpec, ...] = (
    TableImportSpec(
        table=Phenotype.__table__,
        filenames=("phenotype.csv",),
    ),
    TableImportSpec(
        table=PhenotypeDefinition.__table__,
        filenames=("phenotype_definition.csv",),
    ),
    TableImportSpec(
        table=QueryRule.__table__,
        filenames=("query_rule.csv",),
        legacy_columns={
            "query_matcher": "matcher",
            "query_concept_id": "concept_id",
            "query_notes": "notes",
        },
    ),
    TableImportSpec(
        table=Subquery.__table__,
        filenames=("subquery.csv",),
        legacy_columns={
            "subquery_target": "target",
            "subquery_temporality": "temporality",
            "subquery_name": "name",
            "subquery_short_name": "short_name",
        },
    ),
    TableImportSpec(
        table=subquery_rule_map,
        filenames=("subquery_rule_map.csv", "query_rule_map.csv"),
    ),
    TableImportSpec(
        table=Measure.__table__,
        filenames=("measure.csv",),
        legacy_columns={
            "measure_name": "name",
            "measure_combination": "combination",
        },
        value_transforms={
            "combination": lambda value: value.removeprefix("rule_") if isinstance(value, str) else value,
            "person_ep_override": _parse_bool,
        },
    ),
    TableImportSpec(
        table=MeasureRelationship.__table__,
        filenames=("measure_relationship.csv",),
    ),
    TableImportSpec(
        table=DashCohortDef.__table__,
        filenames=("dash_cohort_def.csv",),
    ),
    TableImportSpec(
        table=DashCohort.__table__,
        filenames=("dash_cohort.csv",),
    ),
    TableImportSpec(
        table=dash_cohort_def_map,
        filenames=("dash_cohort_def_map.csv",),
    ),
    TableImportSpec(
        table=Indicator.__table__,
        filenames=("indicator.csv",),
    ),
    TableImportSpec(
        table=Report.__table__,
        filenames=("report.csv",),
    ),
    TableImportSpec(
        table=ReportCohortMap.__table__,
        filenames=("report_cohort_map.csv",),
        value_transforms={
            "primary_cohort": _parse_bool,
        },
    ),
    TableImportSpec(
        table=ReportVersion.__table__,
        filenames=("report_version.csv",),
        value_transforms={
            "report_status": lambda value: value.removeprefix("st_") if isinstance(value, str) else value,
        },
    ),
    TableImportSpec(
        table=report_indicator_map,
        filenames=("report_indicator_map.csv",),
    ),
)


def import_config_directory(
    config_path: Path,
    session: so.Session,
    *,
    dedupe: bool = True,
    create_tables: bool = True,
    dry_run: bool = False,
    progress_callback: ImportProgressCallback | None = None,
) -> list[TableImportResult]:
    config_path = config_path.expanduser().resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config path does not exist: {config_path}")
    if not config_path.is_dir():
        raise NotADirectoryError(f"Config path is not a directory: {config_path}")

    if create_tables and not dry_run:
        Base.metadata.create_all(session.get_bind())

    results: list[TableImportResult] = []
    table_count = len(CONFIG_IMPORT_SPECS)
    _emit_progress(
        progress_callback,
        phase="start",
        table_name=None,
        table_index=0,
        table_count=table_count,
        detail=f"Preparing config import for {table_count} table(s)",
        dry_run=dry_run,
    )
    for index, spec in enumerate(CONFIG_IMPORT_SPECS, start=1):
        path = spec.resolve_path(config_path)
        results.append(
            _sync_table(
                session,
                spec,
                path,
                table_index=index,
                table_count=table_count,
                dedupe=dedupe,
                dry_run=dry_run,
                progress_callback=progress_callback,
            )
        )
    _emit_progress(
        progress_callback,
        phase="complete",
        table_name=None,
        table_index=table_count,
        table_count=table_count,
        detail="Config import complete",
        dry_run=dry_run,
    )
    return results


__all__ = [
    "CONFIG_IMPORT_SPECS",
    "ImportProgressEvent",
    "TableImportResult",
    "TableImportSpec",
    "_clean_row",
    "import_config_directory",
]
