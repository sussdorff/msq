"""CLI entry point for msq."""

from pathlib import Path

import typer

from msq import __version__
from msq.attachments import extract_attachment, list_attachments
from msq.config import load_config, resolve_db
from msq.db import detect_schema, discover_databases, get_email, get_stats, open_db, search_emails
from msq.output import (
    OutputFormat,
    output_attachments,
    output_databases,
    output_email_detail,
    output_emails,
    output_stats,
    print_error,
    print_info,
    print_success,
)
from msq.parallel import search_all_databases

app = typer.Typer(
    name="msq",
    help="CLI tool for querying MailSteward SQLite email archives.",
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    """Zeigt die Version an."""
    if value:
        typer.echo(f"msq {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-V", callback=version_callback, is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """msq - Query MailSteward SQLite email archives from the command line."""


@app.command()
def dbs(
    fmt: OutputFormat = typer.Option(OutputFormat.TABLE, "--format", "-f", help="Output format."),
) -> None:
    """List all databases."""
    config = load_config()
    databases = discover_databases(config.db_dir)
    if not databases:
        print_info("No databases found.")
        raise typer.Exit()
    output_databases(databases, fmt)


@app.command()
def search(
    query: str = typer.Argument(..., help="Search term."),
    db: str | None = typer.Option(None, "--db", help="Specific database name or alias."),
    from_filter: str | None = typer.Option(None, "--from", help="Filter sender."),
    to_filter: str | None = typer.Option(None, "--to", help="Filter recipient."),
    subject: str | None = typer.Option(None, "--subject", help="Filter subject."),
    body: str | None = typer.Option(None, "--body", help="Filter body."),
    date_from: str | None = typer.Option(None, "--date-from", help="Date from (inclusive)."),
    date_to: str | None = typer.Option(None, "--date-to", help="Date to (inclusive)."),
    has_attachments: bool = typer.Option(False, "--has-attachments", help="Only with attachments."),
    limit: int = typer.Option(50, "--limit", "-n", help="Max results."),
    fmt: OutputFormat = typer.Option(OutputFormat.TABLE, "--format", "-f", help="Output format."),
) -> None:
    """Search emails."""
    config = load_config()

    if db:
        db_path = resolve_db(config, db)
        if db_path is None:
            print_error(f"Database not found: {db}")
            raise typer.Exit(1)
        conn = open_db(db_path)
        try:
            schema = detect_schema(conn)
            results = search_emails(
                conn, schema,
                query=query,
                from_filter=from_filter,
                to_filter=to_filter,
                subject_filter=subject,
                body_filter=body,
                date_from=date_from,
                date_to=date_to,
                has_attachments=has_attachments or None,
                limit=limit,
            )
        finally:
            conn.close()
    else:
        databases = discover_databases(config.db_dir)
        if not databases:
            print_info("No databases found.")
            raise typer.Exit()
        results = search_all_databases(
            databases,
            query=query,
            from_filter=from_filter,
            to_filter=to_filter,
            subject_filter=subject,
            body_filter=body,
            date_from=date_from,
            date_to=date_to,
            has_attachments=has_attachments or None,
            limit=limit,
        )

    if not results:
        print_info("No results found.")
        raise typer.Exit()
    output_emails(results, fmt)


@app.command()
def show(
    db_name: str = typer.Argument(..., help="Database name or alias."),
    email_id: int = typer.Argument(..., help="Email ID."),
) -> None:
    """Show email detail."""
    config = load_config()
    db_path = resolve_db(config, db_name)
    if db_path is None:
        print_error(f"Database not found: {db_name}")
        raise typer.Exit(1)

    conn = open_db(db_path)
    try:
        schema = detect_schema(conn)
        email = get_email(conn, schema, email_id)
    finally:
        conn.close()

    if email is None:
        print_error(f"Email not found: {email_id}")
        raise typer.Exit(1)

    output_email_detail(email)


@app.command()
def attachments(
    db_name: str = typer.Argument(..., help="Database name or alias."),
    email_id: int = typer.Argument(..., help="Email ID."),
    fmt: OutputFormat = typer.Option(OutputFormat.TABLE, "--format", "-f", help="Output format."),
) -> None:
    """List attachments of an email."""
    config = load_config()
    db_path = resolve_db(config, db_name)
    if db_path is None:
        print_error(f"Database not found: {db_name}")
        raise typer.Exit(1)

    conn = open_db(db_path)
    try:
        schema = detect_schema(conn)
        atts = list_attachments(conn, schema, email_id)
    finally:
        conn.close()

    if not atts:
        print_info("No attachments found.")
        raise typer.Exit()

    output_attachments(atts, fmt)


@app.command()
def extract(
    db_name: str = typer.Argument(..., help="Database name or alias."),
    email_id: int = typer.Argument(..., help="Email ID."),
    output_dir: Path = typer.Option(Path("."), "--output", "-o", help="Output directory."),
    index: int | None = typer.Option(
        None, "--index", "-i", help="Extract specific attachment index (0-based).",
    ),
) -> None:
    """Extract attachments from an email."""
    config = load_config()
    db_path = resolve_db(config, db_name)
    if db_path is None:
        print_error(f"Database not found: {db_name}")
        raise typer.Exit(1)

    conn = open_db(db_path)
    try:
        schema = detect_schema(conn)
        atts = list_attachments(conn, schema, email_id)
        if not atts:
            print_error("No attachments found.")
            raise typer.Exit(1)

        if index is not None:
            if index < 0 or index >= len(atts):
                print_error(f"Index {index} out of range (0-{len(atts) - 1}).")
                raise typer.Exit(1)
            path = extract_attachment(conn, schema, email_id, index, output_dir)
            print_success(f"Extracted: {path}")
        else:
            for i in range(len(atts)):
                path = extract_attachment(conn, schema, email_id, i, output_dir)
                print_success(f"Extracted: {path}")
    finally:
        conn.close()


@app.command()
def stats(
    db_name: str = typer.Argument(..., help="Database name or alias."),
) -> None:
    """Show database statistics."""
    config = load_config()
    db_path = resolve_db(config, db_name)
    if db_path is None:
        print_error(f"Database not found: {db_name}")
        raise typer.Exit(1)

    conn = open_db(db_path)
    try:
        schema = detect_schema(conn)
        db_stats = get_stats(conn, schema)
    finally:
        conn.close()

    output_stats(db_stats)
