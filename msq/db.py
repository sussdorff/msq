"""Datenbankzugriff fuer MailSteward SQLite-Archive."""

import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from msq.models import DatabaseInfo, DatabaseStats, EmailDetail, EmailResult


@dataclass(frozen=True, slots=True)
class SchemaMapping:
    """Mapping der logischen Spaltennamen auf die tatsaechlichen Spaltennamen."""

    table: str
    id_col: str
    from_col: str
    to_col: str
    subject_col: str
    date_col: str
    mailbox_col: str
    body_col: str
    attach_table: str
    attach_fk_col: str
    attach_filename_col: str
    attach_data_col: str
    attach_size_col: str | None
    headings_col: str | None


def open_db(path: Path) -> sqlite3.Connection:
    """Oeffnet eine MailSteward-Datenbank read-only.

    Args:
        path: Pfad zur SQLite-Datei

    Returns:
        Verbindung mit Row-Factory
    """
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Holt Spaltennamen einer Tabelle."""
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def detect_schema(conn: sqlite3.Connection) -> SchemaMapping:
    """Erkennt das Schema-Format und gibt ein Mapping zurueck.

    Unterstuetzt verschiedene MailSteward-Schema-Varianten:
    - Modern: Tabelle 'email' mit id, subj_fld, date_fld, mailbox
    - Legacy: Tabelle 'mail' mit emailid/rowid, subject_fld, datesent_fld, mailbox_fld

    Args:
        conn: Offene Datenbankverbindung

    Returns:
        SchemaMapping mit den tatsaechlichen Spaltennamen
    """
    # Email-Tabelle erkennen
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}

    if "email" in tables:
        email_table = "email"
    elif "mail" in tables:
        email_table = "mail"
    else:
        msg = "Keine bekannte Email-Tabelle gefunden (weder 'email' noch 'mail')"
        raise ValueError(msg)

    cols = _get_table_columns(conn, email_table)

    # ID-Spalte
    if "id" in cols:
        id_col = "id"
    elif "emailid" in cols:
        id_col = "emailid"
    else:
        id_col = "rowid"

    # Subject-Spalte
    subject_col = "subj_fld" if "subj_fld" in cols else "subject_fld"

    # Date-Spalte
    date_col = "date_fld" if "date_fld" in cols else "datesent_fld"

    # Mailbox-Spalte
    mailbox_col = "mailbox" if "mailbox" in cols else "mailbox_fld"

    # Body-Spalte (immer body_fld in beiden Schemas)
    body_col = "body_fld"

    # Attachment-Tabelle
    if "attachments" in tables:
        attach_table = "attachments"
    elif "attachdata" in tables:
        attach_table = "attachdata"
    else:
        attach_table = ""

    # Attachment-Spalten ermitteln
    attach_fk_col = "id"
    attach_filename_col = "filename_fld"
    attach_data_col = "attach_fld"
    attach_size_col: str | None = None

    if attach_table:
        attach_cols = _get_table_columns(conn, attach_table)

        # FK-Spalte
        if "id" in attach_cols:
            attach_fk_col = "id"
        elif "emailid" in attach_cols:
            attach_fk_col = "emailid"
        elif "mail_fld" in attach_cols:
            attach_fk_col = "mail_fld"
        elif "message_id" in attach_cols:
            attach_fk_col = "message_id"

        # Filename-Spalte
        if "filename_fld" in attach_cols:
            attach_filename_col = "filename_fld"
        elif "name" in attach_cols:
            attach_filename_col = "name"

        # Data-Spalte
        if "attach_fld" in attach_cols:
            attach_data_col = "attach_fld"
        elif "data" in attach_cols:
            attach_data_col = "data"

        # Size-Spalte
        if "filesize_fld" in attach_cols:
            attach_size_col = "filesize_fld"
        elif "filesize" in attach_cols:
            attach_size_col = "filesize"

    # Headings-Spalte (Original-RFC822-Header)
    headings_col = "headings" if "headings" in cols else None

    return SchemaMapping(
        table=email_table,
        id_col=id_col,
        from_col="from_fld",
        to_col="to_fld",
        subject_col=subject_col,
        date_col=date_col,
        mailbox_col=mailbox_col,
        body_col=body_col,
        attach_table=attach_table,
        attach_fk_col=attach_fk_col,
        attach_filename_col=attach_filename_col,
        attach_data_col=attach_data_col,
        attach_size_col=attach_size_col,
        headings_col=headings_col,
    )


def schema_type_label(schema: SchemaMapping) -> str:
    """Gibt ein Label fuer den Schema-Typ zurueck."""
    if schema.table == "email":
        return "modern"
    return "legacy"


def decode_filename(value: bytes | str) -> str:
    """Dekodiert einen Dateinamen aus der Datenbank.

    Args:
        value: Roher Wert (bytes oder str)

    Returns:
        Dekodierter String
    """
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def discover_databases(db_dir: Path) -> list[DatabaseInfo]:
    """Findet alle gueltigen MailSteward-Datenbanken in einem Verzeichnis.

    Args:
        db_dir: Verzeichnis mit Datenbank-Dateien

    Returns:
        Sortierte Liste von DatabaseInfo-Objekten
    """
    sqlite_magic = b"SQLite format 3\x00"
    results: list[DatabaseInfo] = []

    if not db_dir.is_dir():
        return results

    for entry in db_dir.iterdir():
        if not entry.is_file():
            continue
        try:
            header = entry.read_bytes()[:16]
        except OSError:
            continue
        if header != sqlite_magic:
            continue

        try:
            conn = open_db(entry)
        except sqlite3.Error:
            continue

        try:
            schema = detect_schema(conn)
            count_row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM {schema.table}"
            ).fetchone()
            email_count = count_row["cnt"] if count_row else 0

            range_row = conn.execute(
                f"SELECT MIN({schema.date_col}) AS min_d, "
                f"MAX({schema.date_col}) AS max_d FROM {schema.table}"
            ).fetchone()
            date_from = range_row["min_d"] or "" if range_row else ""
            date_to = range_row["max_d"] or "" if range_row else ""

            results.append(
                DatabaseInfo(
                    name=entry.name,
                    path=entry,
                    email_count=email_count,
                    date_range=(date_from, date_to),
                    size_bytes=entry.stat().st_size,
                    schema_type=schema_type_label(schema),
                )
            )
        except (sqlite3.Error, ValueError):
            continue
        finally:
            conn.close()

    results.sort(key=lambda db: db.name)
    return results


def search_emails(
    conn: sqlite3.Connection,
    schema: SchemaMapping,
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
    """Sucht Emails mit verschiedenen Filtern.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping
        query: Allgemeine Suche in from/to/subject
        from_filter: Filter fuer Absender
        to_filter: Filter fuer Empfaenger
        subject_filter: Filter fuer Betreff
        body_filter: Filter fuer Body
        date_from: Fruehestes Datum (inklusiv)
        date_to: Spaetestes Datum (inklusiv)
        has_attachments: Filter fuer Anhaenge
        limit: Maximale Anzahl Ergebnisse

    Returns:
        Liste von EmailResult-Objekten
    """
    t = schema.table
    s = schema

    select = (
        f"SELECT m.{s.id_col} AS id, m.{s.from_col}, m.{s.to_col}, "
        f"m.{s.subject_col}, m.{s.date_col}, m.{s.mailbox_col}"
    )
    from_clause = f" FROM {t} m"
    conditions: list[str] = []
    params: list[str] = []

    if query:
        conditions.append(
            f"(m.{s.from_col} LIKE ? OR m.{s.to_col} LIKE ? OR m.{s.subject_col} LIKE ?)"
        )
        like_val = f"%{query}%"
        params.extend([like_val, like_val, like_val])

    if from_filter:
        conditions.append(f"m.{s.from_col} LIKE ?")
        params.append(f"%{from_filter}%")

    if to_filter:
        conditions.append(f"m.{s.to_col} LIKE ?")
        params.append(f"%{to_filter}%")

    if subject_filter:
        conditions.append(f"m.{s.subject_col} LIKE ?")
        params.append(f"%{subject_filter}%")

    if body_filter:
        conditions.append(f"m.{s.body_col} LIKE ?")
        params.append(f"%{body_filter}%")

    if date_from:
        conditions.append(f"m.{s.date_col} >= ?")
        params.append(date_from)

    if date_to:
        conditions.append(f"m.{s.date_col} <= ?")
        params.append(date_to)

    if has_attachments is True and s.attach_table:
        from_clause += (
            f" INNER JOIN {s.attach_table} a ON a.{s.attach_fk_col} = m.{s.id_col}"
        )
    elif has_attachments is False and s.attach_table:
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM {s.attach_table} a "
            f"WHERE a.{s.attach_fk_col} = m.{s.id_col})"
        )

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    # Deduplizieren falls JOIN auf attachments mehrere Zeilen erzeugt
    group_by = f" GROUP BY m.{s.id_col}" if has_attachments is True else ""

    sql = f"{select}{from_clause}{where}{group_by} ORDER BY m.{s.date_col} DESC LIMIT ?"
    params.append(str(limit))

    rows = conn.execute(sql, params).fetchall()

    # Attachment-Check: sammle alle IDs fuer Batch-Lookup
    ids = [row["id"] for row in rows]
    attach_ids: set[int] = set()
    if ids and s.attach_table:
        try:
            placeholders = ",".join("?" * len(ids))
            attach_rows = conn.execute(
                f"SELECT DISTINCT {s.attach_fk_col} FROM {s.attach_table} "
                f"WHERE {s.attach_fk_col} IN ({placeholders})",
                ids,
            ).fetchall()
            attach_ids = {r[0] for r in attach_rows}
        except sqlite3.OperationalError:
            pass

    return [
        EmailResult(
            id=row["id"],
            from_=row[s.from_col] or "",
            to=row[s.to_col] or "",
            subject=row[s.subject_col] or "",
            date=row[s.date_col] or "",
            mailbox=row[s.mailbox_col] or "",
            has_attachments=row["id"] in attach_ids,
        )
        for row in rows
    ]


def get_email(
    conn: sqlite3.Connection, schema: SchemaMapping, email_id: int
) -> EmailDetail | None:
    """Holt eine vollstaendige Email mit Body.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping
        email_id: ID der Email

    Returns:
        EmailDetail oder None wenn nicht gefunden
    """
    s = schema

    row = conn.execute(
        f"SELECT {s.id_col} AS id, {s.from_col}, {s.to_col}, {s.subject_col}, "
        f"{s.date_col}, {s.body_col}, {s.mailbox_col} "
        f"FROM {s.table} WHERE {s.id_col} = ?",
        (email_id,),
    ).fetchone()

    if row is None:
        return None

    # Attachment-Check
    has_attachments = False
    if s.attach_table:
        try:
            attach_row = conn.execute(
                f"SELECT 1 FROM {s.attach_table} WHERE {s.attach_fk_col} = ? LIMIT 1",
                (email_id,),
            ).fetchone()
            has_attachments = attach_row is not None
        except sqlite3.OperationalError:
            pass

    return EmailDetail(
        id=row["id"],
        from_=row[s.from_col] or "",
        to=row[s.to_col] or "",
        subject=row[s.subject_col] or "",
        date=row[s.date_col] or "",
        mailbox=row[s.mailbox_col] or "",
        has_attachments=has_attachments,
        body=row[s.body_col] or "",
    )


def get_stats(conn: sqlite3.Connection, schema: SchemaMapping) -> DatabaseStats:
    """Berechnet Statistiken fuer eine MailSteward-Datenbank.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping

    Returns:
        DatabaseStats mit Mailbox-, Sender- und Datumsverteilung
    """
    s = schema

    mailbox_counts: dict[str, int] = {}
    for row in conn.execute(
        f"SELECT {s.mailbox_col}, COUNT(*) AS cnt FROM {s.table} GROUP BY {s.mailbox_col}"
    ):
        mailbox_counts[row[0] or "(empty)"] = row["cnt"]

    sender_counts: dict[str, int] = {}
    for row in conn.execute(
        f"SELECT {s.from_col}, COUNT(*) AS cnt FROM {s.table} "
        f"GROUP BY {s.from_col} ORDER BY cnt DESC LIMIT 20"
    ):
        sender_counts[row[0] or "(empty)"] = row["cnt"]

    date_distribution: dict[str, int] = {}
    for row in conn.execute(
        f"SELECT strftime('%Y-%m', {s.date_col}) AS period, COUNT(*) AS cnt "
        f"FROM {s.table} GROUP BY period"
    ):
        date_distribution[row["period"] or "(unknown)"] = row["cnt"]

    return DatabaseStats(
        mailbox_counts=mailbox_counts,
        sender_counts=sender_counts,
        date_distribution=date_distribution,
    )


def count_emails(conn: sqlite3.Connection, schema: SchemaMapping) -> int:
    """Zaehlt die Anzahl der Emails in einer Datenbank."""
    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {schema.table}").fetchone()
    return row["cnt"] if row else 0


def iter_emails_for_export(
    conn: sqlite3.Connection, schema: SchemaMapping
) -> Iterator[sqlite3.Row]:
    """Iteriert ueber alle Emails fuer den EML-Export.

    Yieldet pro Email eine Row mit id, headings, body_fld, mailbox, date, subject.
    Memory-effizient via Cursor (kein fetchall).

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping

    Yields:
        sqlite3.Row mit den Export-relevanten Spalten
    """
    s = schema
    headings_select = f", {s.headings_col}" if s.headings_col else ", '' AS headings"

    sql = (
        f"SELECT {s.id_col} AS id, {s.body_col} AS body_fld, "
        f"{s.mailbox_col} AS mailbox, {s.date_col} AS date_fld, "
        f"{s.subject_col} AS subject_fld"
        f"{headings_select} "
        f"FROM {s.table} ORDER BY {s.id_col}"
    )

    cursor = conn.execute(sql)
    yield from cursor
