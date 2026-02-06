"""Parallele Suche ueber mehrere MailSteward-Datenbanken."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn

from msq.db import detect_schema, open_db, search_emails
from msq.models import DatabaseInfo, EmailResult

log = logging.getLogger(__name__)


def search_all_databases(
    databases: list[DatabaseInfo],
    *,
    query: str | None = None,
    from_filter: str | None = None,
    to_filter: str | None = None,
    subject_filter: str | None = None,
    body_filter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    has_attachments: bool | None = None,
    limit: int = 50,
) -> list[EmailResult]:
    """Durchsucht mehrere Datenbanken parallel.

    Args:
        databases: Liste der zu durchsuchenden Datenbanken
        query: Allgemeine Suche in from/to/subject
        from_filter: Filter fuer Absender
        to_filter: Filter fuer Empfaenger
        subject_filter: Filter fuer Betreff
        body_filter: Filter fuer Body
        date_from: Fruehestes Datum (inklusiv)
        date_to: Spaetestes Datum (inklusiv)
        has_attachments: Filter fuer Anhaenge
        limit: Maximale Anzahl Ergebnisse pro Datenbank

    Returns:
        Zusammengefuehrte, nach Datum absteigende Liste von EmailResult
    """
    if not databases:
        return []

    results: list[EmailResult] = []
    max_workers = min(len(databases), 4)

    def _search_single(db_info: DatabaseInfo) -> list[EmailResult]:
        conn = open_db(db_info.path)
        try:
            schema = detect_schema(conn)
            return search_emails(
                conn,
                schema,
                query=query,
                from_filter=from_filter,
                to_filter=to_filter,
                subject_filter=subject_filter,
                body_filter=body_filter,
                date_from=date_from,
                date_to=date_to,
                has_attachments=has_attachments,
                limit=limit,
            )
        finally:
            conn.close()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task("Searching databases...", total=len(databases))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_db = {
                executor.submit(_search_single, db_info): db_info
                for db_info in databases
            }

            for future in as_completed(future_to_db):
                db_info = future_to_db[future]
                try:
                    results.extend(future.result())
                except Exception:
                    log.warning("Fehler beim Durchsuchen von %s", db_info.name, exc_info=True)
                progress.advance(task)

    results.sort(key=lambda e: e.date, reverse=True)
    return results
