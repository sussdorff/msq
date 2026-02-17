"""EML-Export aus MailSteward SQLite-Archiven."""

import logging
import re
import sqlite3
from email import encoders, generator, message, parser, policy
from email.charset import QP, Charset
from email.utils import make_msgid
from io import StringIO
from pathlib import Path

from msq.db import SchemaMapping, count_emails, decode_filename, iter_emails_for_export
from msq.models import ExportStats

log = logging.getLogger(__name__)

_PARSER = parser.Parser(policy=policy.compat32)


def sanitize_path(mailbox: str) -> str:
    """Macht einen Mailbox-Pfad filesystem-sicher.

    Wandelt z.B. 'V10/Balboacastlecamp/INBOX.mbox/2018.mbox/Done'
    in 'V10/Balboacastlecamp/INBOX/2018/Done' um.

    Args:
        mailbox: Roher Mailbox-Pfad aus der Datenbank

    Returns:
        Bereinigter Pfad-String
    """
    if not mailbox:
        return "_unsorted"

    # .mbox-Suffix entfernen
    cleaned = mailbox.replace(".mbox", "")

    # Jede Pfad-Komponente bereinigen
    parts = []
    for part in cleaned.split("/"):
        # Nicht-druckbare und Filesystem-unsichere Zeichen entfernen
        part = re.sub(r'[<>:"|?*\x00-\x1f]', "_", part)
        part = part.strip(". ")
        if part:
            parts.append(part)

    return "/".join(parts) if parts else "_unsorted"


def _make_subject_slug(subject: str) -> str:
    """Erzeugt einen kurzen Slug aus dem Betreff fuer den Dateinamen."""
    if not subject:
        return "no-subject"
    slug = re.sub(r"[^\w\s-]", "", subject.lower())
    slug = re.sub(r"[\s_]+", "-", slug).strip("-")
    return slug[:50] if slug else "no-subject"


def _ensure_str(value: str | bytes) -> str:
    """Konvertiert str oder bytes zu str (UTF-8 mit replace fuer invalide Bytes)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value or ""


def build_eml(
    headings: str,
    body: str | bytes,
    attachments: list[tuple[str, bytes, str]],
) -> bytes:
    """Rekonstruiert eine EML-Datei aus DB-Feldern.

    Args:
        headings: Originale RFC822-Header als String
        body: Email-Body (Plain-Text oder HTML, als str oder bytes)
        attachments: Liste von (filename, data, content_type) Tupeln

    Returns:
        Vollstaendige EML als Bytes
    """
    if headings:
        msg = _PARSER.parsestr(headings, headersonly=True)
    else:
        msg = message.Message()

    # Content-Type aus Original-Headers extrahieren
    original_ct = msg.get_content_type() if headings else "text/plain"
    body_str = _ensure_str(body)

    if attachments:
        # Multipart aufbauen: Original-Header behalten, aber Payload ersetzen
        # Alle Content-* Header entfernen (werden durch multipart ersetzt)
        for key in list(msg.keys()):
            if key.lower().startswith("content-") or key.lower() == "mime-version":
                del msg[key]

        msg["MIME-Version"] = "1.0"
        boundary = make_msgid(domain="msq.export").strip("<>").replace("@", ".")
        msg["Content-Type"] = f'multipart/mixed; boundary="{boundary}"'
        msg.set_payload([])

        # Body als erste Part
        body_part = message.Message()
        body_part["Content-Type"] = original_ct + "; charset=utf-8"
        body_part["Content-Transfer-Encoding"] = "quoted-printable"
        cs = Charset("utf-8")
        cs.body_encoding = QP
        body_part.set_payload(body_str, charset=cs)
        msg.attach(body_part)

        # Attachments
        for filename, data, content_type in attachments:
            att_part = message.Message()
            att_part["Content-Type"] = content_type
            att_part["Content-Disposition"] = f'attachment; filename="{filename}"'
            att_part["Content-Transfer-Encoding"] = "base64"
            att_part.set_payload(data)
            encoders.encode_base64(att_part)
            msg.attach(att_part)
    else:
        # Einfacher single-part: Header behalten, Body setzen
        for key in list(msg.keys()):
            if key.lower().startswith("content-") or key.lower() == "mime-version":
                del msg[key]

        msg["MIME-Version"] = "1.0"
        cs = Charset("utf-8")
        cs.body_encoding = QP
        msg.set_payload(body_str, charset=cs)
        # set_payload overrides Content-Type to text/plain; restore original
        if original_ct != "text/plain":
            msg.replace_header("Content-Type", original_ct + "; charset=utf-8")

    # Serialisieren
    buf = StringIO()
    gen = generator.Generator(buf, mangle_from_=False)
    gen.flatten(msg)
    return buf.getvalue().encode("utf-8")


def _get_attachments_for_email(
    conn: sqlite3.Connection, schema: SchemaMapping, email_id: int
) -> list[tuple[str, bytes, str]]:
    """Holt alle Attachments einer Email als (filename, data, content_type) Tupel."""
    if not schema.attach_table:
        return []

    s = schema
    # type_fld existiert nur im modernen Schema
    has_type = "type_fld" in {
        row[1] for row in conn.execute(f"PRAGMA table_info({s.attach_table})")
    }

    if has_type:
        sql = (
            f"SELECT {s.attach_filename_col}, {s.attach_data_col}, type_fld "
            f"FROM {s.attach_table} WHERE {s.attach_fk_col} = ?"
        )
    else:
        sql = (
            f"SELECT {s.attach_filename_col}, {s.attach_data_col} "
            f"FROM {s.attach_table} WHERE {s.attach_fk_col} = ?"
        )

    result = []
    for row in conn.execute(sql, (email_id,)):
        raw_filename = row[0]
        data = row[1]
        if data is None:
            continue

        if isinstance(raw_filename, bytes):
            filename = decode_filename(raw_filename)
        else:
            filename = raw_filename or "unnamed"

        content_type = row[2] if has_type and row[2] else "application/octet-stream"
        result.append((filename, data, content_type))

    return result


def export_database(
    conn: sqlite3.Connection,
    schema: SchemaMapping,
    output_dir: Path,
    db_name: str,
    *,
    dry_run: bool = False,
    progress_callback: object | None = None,
) -> ExportStats:
    """Exportiert alle Emails einer Datenbank als EML-Dateien.

    Args:
        conn: Offene Datenbankverbindung
        schema: Schema-Mapping
        output_dir: Basis-Ausgabeverzeichnis
        db_name: Name der Datenbank (fuer Unterordner)
        dry_run: Wenn True, keine Dateien schreiben
        progress_callback: Callable das pro Email aufgerufen wird (fuer Rich Progress)

    Returns:
        ExportStats mit Zaehlerstaenden
    """
    stats = ExportStats()
    stats.total = count_emails(conn, schema)
    db_dir = output_dir / db_name

    for row in iter_emails_for_export(conn, schema):
        email_id = row["id"]
        headings = _ensure_str(row["headings"] or "")
        body = row["body_fld"] or b""
        mailbox = _ensure_str(row["mailbox"] or "")
        date_str = _ensure_str(row["date_fld"] or "")
        subject = _ensure_str(row["subject_fld"] or "")

        if not dry_run:
            try:
                attachments = _get_attachments_for_email(conn, schema, email_id)
                eml_bytes = build_eml(headings, body, attachments)

                # Pfad aufbauen
                sanitized_mailbox = sanitize_path(mailbox)
                date_slug = date_str[:10].replace("-", "") if date_str else "nodate"
                subject_slug = _make_subject_slug(subject)
                filename = f"{email_id}_{date_slug}_{subject_slug}.eml"

                eml_path = db_dir / sanitized_mailbox / filename
                eml_path.parent.mkdir(parents=True, exist_ok=True)
                eml_path.write_bytes(eml_bytes)

                stats.exported += 1
            except Exception:
                log.warning("Fehler beim Export von Email %d", email_id, exc_info=True)
                stats.errors += 1
        else:
            stats.exported += 1

        if progress_callback is not None:
            progress_callback()

    return stats
