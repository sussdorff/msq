"""Attachment-Zugriff fuer MailSteward SQLite-Archive."""

import sqlite3
from pathlib import Path

from msq.db import decode_filename
from msq.models import AttachmentInfo


def _get_attachment_columns(conn: sqlite3.Connection) -> dict[str, str]:
    """Ermittelt die tatsaechlichen Spaltennamen der attachdata-Tabelle.

    Args:
        conn: Offene Datenbankverbindung

    Returns:
        Dict mit logischen Keys (filename, data, size, email_id) -> tatsaechliche Spaltennamen
    """
    columns = {row[1] for row in conn.execute("PRAGMA table_info(attachdata)")}

    mapping: dict[str, str] = {}

    # Filename-Spalte
    if "filename_fld" in columns:
        mapping["filename"] = "filename_fld"
    elif "name" in columns:
        mapping["filename"] = "name"
    else:
        mapping["filename"] = "filename"

    # Data-Spalte
    if "attach_fld" in columns:
        mapping["data"] = "attach_fld"
    elif "data" in columns:
        mapping["data"] = "data"
    else:
        mapping["data"] = "filedata"

    # Size-Spalte
    if "filesize_fld" in columns:
        mapping["size"] = "filesize_fld"
    elif "filesize" in columns:
        mapping["size"] = "filesize"
    else:
        mapping["size"] = None  # type: ignore[assignment]

    # Email-ID-Spalte (fuer Filterung)
    if "mail_fld" in columns:
        mapping["email_id"] = "mail_fld"
    elif "emailid" in columns:
        mapping["email_id"] = "emailid"
    elif "message_id" in columns:
        mapping["email_id"] = "message_id"
    else:
        mapping["email_id"] = None  # type: ignore[assignment]

    return mapping


def list_attachments(
    conn: sqlite3.Connection, schema: str, email_id: int
) -> list[AttachmentInfo]:
    """Listet alle Attachments einer Email auf.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Typ ('modern' oder 'legacy')
        email_id: ID der Email

    Returns:
        Liste von AttachmentInfo-Objekten
    """
    cols = _get_attachment_columns(conn)

    filename_col = cols["filename"]
    size_col = cols["size"]
    email_id_col = cols["email_id"]

    # SELECT-Teile aufbauen
    select_parts = ["rowid AS id", f"{filename_col} AS filename"]
    if size_col:
        select_parts.append(f"{size_col} AS size")
    else:
        select_parts.append(f"LENGTH({cols['data']}) AS size")

    select = ", ".join(select_parts)

    if email_id_col:
        sql = f"SELECT {select} FROM attachdata WHERE {email_id_col} = ?"
        rows = conn.execute(sql, (email_id,)).fetchall()
    else:
        # Kein email_id Feld -> alle Attachments zurueckgeben (Legacy-Fallback)
        sql = f"SELECT {select} FROM attachdata"
        rows = conn.execute(sql).fetchall()

    results: list[AttachmentInfo] = []
    for row in rows:
        raw_filename = row["filename"] if isinstance(row, sqlite3.Row) else row[1]
        raw_id = row["id"] if isinstance(row, sqlite3.Row) else row[0]
        raw_size = row["size"] if isinstance(row, sqlite3.Row) else row[2]

        if isinstance(raw_filename, bytes):
            filename = decode_filename(raw_filename)
        else:
            filename = raw_filename or "unnamed"
        results.append(AttachmentInfo(id=raw_id, filename=filename, size=raw_size or 0))

    return results


def extract_attachment(
    conn: sqlite3.Connection,
    schema: str,
    email_id: int,
    attachment_idx: int,
    output_dir: Path,
) -> Path:
    """Extrahiert ein Attachment und schreibt es als Datei.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Typ ('modern' oder 'legacy')
        email_id: ID der Email
        attachment_idx: 0-basierter Index des Attachments
        output_dir: Verzeichnis fuer die Ausgabe-Datei

    Returns:
        Pfad der geschriebenen Datei

    Raises:
        IndexError: Wenn attachment_idx ausserhalb des Bereichs liegt
        ValueError: Wenn Attachment-Daten NULL sind
    """
    cols = _get_attachment_columns(conn)

    filename_col = cols["filename"]
    data_col = cols["data"]
    email_id_col = cols["email_id"]

    if email_id_col:
        sql = f"SELECT rowid, {filename_col}, {data_col} FROM attachdata WHERE {email_id_col} = ?"
        rows = conn.execute(sql, (email_id,)).fetchall()
    else:
        sql = f"SELECT rowid, {filename_col}, {data_col} FROM attachdata"
        rows = conn.execute(sql).fetchall()

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
