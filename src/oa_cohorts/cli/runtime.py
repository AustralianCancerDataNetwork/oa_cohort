from __future__ import annotations

import os

import sqlalchemy as sa
from omop_constructs.semantics import registry_engine
from rich.console import Console
from sqlalchemy.exc import SQLAlchemyError
import typer

from .ui import render_error


def resolve_engine(*, database_url: str | None) -> tuple[sa.Engine, str | None]:
    resolved_url = database_url or os.getenv("ENGINE")
    if resolved_url:
        return sa.create_engine(resolved_url), resolved_url
    return registry_engine, None


def handle_cli_error(console: Console, exc: Exception) -> None:
    if isinstance(exc, SQLAlchemyError):
        detail = str(exc).strip()
        message = f"Database operation failed: {exc.__class__.__name__}."
        if detail:
            message = f"{message} Detail: {detail}"
        console.print(render_error(message))
        raise typer.Exit(code=1) from exc

    if isinstance(exc, (RuntimeError, ValueError, FileNotFoundError, NotADirectoryError)):
        console.print(render_error(str(exc)))
        raise typer.Exit(code=1) from exc

    console.print(render_error(str(exc)))
    raise typer.Exit(code=1) from exc
