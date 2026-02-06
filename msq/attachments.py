"""Attachment-Zugriff fuer MailSteward SQLite-Archive."""

import sqlite3
from pathlib import Path

from msq.db import SchemaMapping, decode_filename
from msq.models import AttachmentInfo


def list_attachments(
    conn: sqlite3.Connection, schema: SchemaMapping, email_id: int
) -> list[AttachmentInfo]:
    """Listet alle Attachments einer Email auf.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping
        email_id: ID der Email

    Returns:
        Liste von AttachmentInfo-Objekten
    """
    if not schema.attach_table:
        return []

    s = schema

    # SELECT-Teile aufbauen
    select_parts = ["rowid AS rid", f"{s.attach_filename_col} AS filename"]
    if s.attach_size_col:
        select_parts.append(f"{s.attach_size_col} AS size")
    else:
        select_parts.append(f"LENGTH({s.attach_data_col}) AS size")

    select = ", ".join(select_parts)
    sql = f"SELECT {select} FROM {s.attach_table} WHERE {s.attach_fk_col} = ?"
    rows = conn.execute(sql, (email_id,)).fetchall()

    results: list[AttachmentInfo] = []
    for row in rows:
        raw_filename = row["filename"]
        if isinstance(raw_filename, bytes):
            filename = decode_filename(raw_filename)
        else:
            filename = raw_filename or "unnamed"
        results.append(AttachmentInfo(id=row["rid"], filename=filename, size=row["size"] or 0))

    return results


def extract_attachment(
    conn: sqlite3.Connection,
    schema: SchemaMapping,
    email_id: int,
    attachment_idx: int,
    output_dir: Path,
) -> Path:
    """Extrahiert ein Attachment und schreibt es als Datei.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping
        email_id: ID der Email
        attachment_idx: 0-basierter Index des Attachments
        output_dir: Verzeichnis fuer die Ausgabe-Datei

    Returns:
        Pfad der geschriebenen Datei

    Raises:
        IndexError: Wenn attachment_idx ausserhalb des Bereichs liegt
        ValueError: Wenn Attachment-Daten NULL sind
    """
    if not schema.attach_table:
        msg = "Keine Attachment-Tabelle vorhanden"
        raise ValueError(msg)

    s = schema
    sql = (
        f"SELECT rowid AS rid, {s.attach_filename_col}, {s.attach_data_col} "
        f"FROM {s.attach_table} WHERE {s.attach_fk_col} = ?"
    )
    rows = conn.execute(sql, (email_id,)).fetchall()

    if attachment_idx < 0 or attachment_idx >= len(rows):
        msg = f"Attachment-Index {attachment_idx} ausserhalb des Bereichs (0-{len(rows) - 1})"
        raise IndexError(msg)

    row = rows[attachment_idx]
    raw_filename = row[1]
    raw_data = row[2]

    if raw_data is None:
        msg = f"Attachment-Daten sind NULL fuer Index {attachment_idx}"
        raise ValueError(msg)

    if isinstance(raw_filename, bytes):
        filename = decode_filename(raw_filename)
    else:
        filename = raw_filename or "unnamed"
    output_path = output_dir / filename
    output_path.write_bytes(raw_data)
    return output_path
