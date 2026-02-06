"""Datenbankzugriff fuer MailSteward SQLite-Archive."""

import sqlite3
from pathlib import Path

from msq.models import DatabaseInfo, EmailDetail, EmailResult


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


def detect_schema(conn: sqlite3.Connection) -> str:
    """Erkennt das Schema-Format der Datenbank.

    Args:
        conn: Offene Datenbankverbindung

    Returns:
        'modern' wenn emailid-Spalte existiert, sonst 'legacy'
    """
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(mail)")]
    return "modern" if "emailid" in columns else "legacy"


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
            count_row = conn.execute("SELECT COUNT(*) AS cnt FROM mail").fetchone()
            email_count = count_row["cnt"] if count_row else 0

            range_row = conn.execute(
                "SELECT MIN(datesent_fld) AS min_d, MAX(datesent_fld) AS max_d FROM mail"
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
                    schema_type=schema,
                )
            )
        except sqlite3.Error:
            continue
        finally:
            conn.close()

    results.sort(key=lambda db: db.name)
    return results


def search_emails(
    conn: sqlite3.Connection,
    schema: str,
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
        schema: Schema-Typ ('modern' oder 'legacy')
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
    id_col = "emailid" if schema == "modern" else "rowid"

    select = (
        f"SELECT m.{id_col} AS id, m.from_fld, m.to_fld, "
        "m.subject_fld, m.datesent_fld, m.mailbox_fld"
    )
    from_clause = " FROM mail m"
    conditions: list[str] = []
    params: list[str] = []

    if query:
        conditions.append(
            "(m.from_fld LIKE ? OR m.to_fld LIKE ? OR m.subject_fld LIKE ?)"
        )
        like_val = f"%{query}%"
        params.extend([like_val, like_val, like_val])

    if from_filter:
        conditions.append("m.from_fld LIKE ?")
        params.append(f"%{from_filter}%")

    if to_filter:
        conditions.append("m.to_fld LIKE ?")
        params.append(f"%{to_filter}%")

    if subject_filter:
        conditions.append("m.subject_fld LIKE ?")
        params.append(f"%{subject_filter}%")

    if body_filter:
        conditions.append("m.body_fld LIKE ?")
        params.append(f"%{body_filter}%")

    if date_from:
        conditions.append("m.datesent_fld >= ?")
        params.append(date_from)

    if date_to:
        conditions.append("m.datesent_fld <= ?")
        params.append(date_to)

    if has_attachments is True:
        from_clause += (
            f" INNER JOIN attachdata a ON a.message_id = m.{id_col}"
        )
    elif has_attachments is False:
        conditions.append(
            f"NOT EXISTS (SELECT 1 FROM attachdata a WHERE a.message_id = m.{id_col})"
        )

    where = (" WHERE " + " AND ".join(conditions)) if conditions else ""

    # Deduplizieren falls JOIN auf attachdata mehrere Zeilen erzeugt
    group_by = f" GROUP BY m.{id_col}" if has_attachments is True else ""

    sql = f"{select}{from_clause}{where}{group_by} ORDER BY m.datesent_fld DESC LIMIT ?"
    params.append(str(limit))

    rows = conn.execute(sql, params).fetchall()

    # Attachment-Check: sammle alle IDs fuer Batch-Lookup
    ids = [row["id"] for row in rows]
    attach_ids: set[int] = set()
    if ids:
        try:
            placeholders = ",".join("?" * len(ids))
            attach_rows = conn.execute(
                f"SELECT DISTINCT message_id FROM attachdata WHERE message_id IN ({placeholders})",
                ids,
            ).fetchall()
            attach_ids = {r["message_id"] for r in attach_rows}
        except sqlite3.OperationalError:
            # attachdata-Tabelle existiert nicht
            pass

    return [
        EmailResult(
            id=row["id"],
            from_=row["from_fld"] or "",
            to=row["to_fld"] or "",
            subject=row["subject_fld"] or "",
            date=row["datesent_fld"] or "",
            mailbox=row["mailbox_fld"] or "",
            has_attachments=row["id"] in attach_ids,
        )
        for row in rows
    ]


def get_email(
    conn: sqlite3.Connection, schema: str, email_id: int
) -> EmailDetail | None:
    """Holt eine vollstaendige Email mit Body.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Typ ('modern' oder 'legacy')
        email_id: ID der Email

    Returns:
        EmailDetail oder None wenn nicht gefunden
    """
    id_col = "emailid" if schema == "modern" else "rowid"

    row = conn.execute(
        f"SELECT {id_col} AS id, from_fld, to_fld, subject_fld, "
        "datesent_fld, body_fld, mailbox_fld, cc_fld, bcc_fld "
        f"FROM mail WHERE {id_col} = ?",
        (email_id,),
    ).fetchone()

    if row is None:
        return None

    # Attachment-Check
    has_attachments = False
    try:
        attach_row = conn.execute(
            "SELECT 1 FROM attachdata WHERE message_id = ? LIMIT 1",
            (email_id,),
        ).fetchone()
        has_attachments = attach_row is not None
    except sqlite3.OperationalError:
        pass

    return EmailDetail(
        id=row["id"],
        from_=row["from_fld"] or "",
        to=row["to_fld"] or "",
        subject=row["subject_fld"] or "",
        date=row["datesent_fld"] or "",
        mailbox=row["mailbox_fld"] or "",
        has_attachments=has_attachments,
        body=row["body_fld"] or "",
        cc=row["cc_fld"] or "",
        bcc=row["bcc_fld"] or "",
    )
