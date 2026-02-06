"""Tests fuer msq.db."""

import sqlite3
from pathlib import Path

import pytest

from msq.db import (
    SchemaMapping,
    decode_filename,
    detect_schema,
    discover_databases,
    get_email,
    get_stats,
    schema_type_label,
    search_emails,
)


def _create_modern_db(conn: sqlite3.Connection) -> None:
    """Erstellt eine moderne MailSteward-DB (Tabelle 'email')."""
    conn.execute("""CREATE TABLE email (
        id INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subj_fld TEXT,
        date_fld TEXT, body_fld TEXT, mailbox TEXT,
        mailto TEXT, numAttach INTEGER DEFAULT 0,
        attachNames TEXT DEFAULT '', attachText TEXT DEFAULT '',
        headings TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachments (
        id INTEGER, type_fld TEXT, filename_fld TEXT,
        encode_fld INTEGER DEFAULT 0, attach_fld BLOB
    )""")


def _create_legacy_db(conn: sqlite3.Connection) -> None:
    """Erstellt eine Legacy-MailSteward-DB (Tabelle 'mail')."""
    conn.execute("""CREATE TABLE mail (
        emailid INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT,
        cc_fld TEXT DEFAULT '', bcc_fld TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachdata (
        message_id INTEGER, name TEXT, data BLOB, filesize INTEGER
    )""")


@pytest.fixture
def modern_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_modern_db(conn)
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox) "
        "VALUES (1, 'alice@example.com', 'bob@example.com', 'Meeting Notes', "
        "'2024-01-15', 'Discussing Q1 plans', 'INBOX')"
    )
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox) "
        "VALUES (2, 'charlie@example.com', 'alice@example.com', 'Invoice #42', "
        "'2024-02-20', 'Please find attached', 'Work')"
    )
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox) "
        "VALUES (3, 'dave@example.com', 'team@example.com', 'Lunch?', "
        "'2024-03-01', 'Anyone up for lunch?', 'INBOX')"
    )
    conn.execute(
        "INSERT INTO attachments (id, type_fld, filename_fld, attach_fld) "
        "VALUES (1, 'application/pdf', 'doc.pdf', X'255044462D')"
    )
    conn.commit()
    return conn


@pytest.fixture
def legacy_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_legacy_db(conn)
    cols = "emailid, from_fld, to_fld, subject_fld, datesent_fld, body_fld, mailbox_fld"
    conn.execute(
        f"INSERT INTO mail ({cols}) "
        "VALUES (1, 'alice@example.com', 'bob@example.com', 'Meeting Notes', "
        "'2024-01-15', 'Discussing Q1 plans', 'INBOX')"
    )
    conn.execute(
        f"INSERT INTO mail ({cols}) "
        "VALUES (2, 'charlie@example.com', 'alice@example.com', 'Invoice #42', "
        "'2024-02-20', 'Please find attached', 'Work')"
    )
    conn.execute(
        "INSERT INTO attachdata (message_id, name, data, filesize) "
        "VALUES (1, 'report.pdf', X'255044462D', 5)"
    )
    conn.commit()
    return conn


class TestDetectSchema:
    def test_modern_schema(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        assert isinstance(schema, SchemaMapping)
        assert schema.table == "email"
        assert schema.id_col == "id"
        assert schema.subject_col == "subj_fld"
        assert schema.date_col == "date_fld"
        assert schema.mailbox_col == "mailbox"
        assert schema.attach_table == "attachments"
        assert schema.attach_fk_col == "id"
        assert schema_type_label(schema) == "modern"

    def test_legacy_schema(self, legacy_db: sqlite3.Connection) -> None:
        schema = detect_schema(legacy_db)
        assert isinstance(schema, SchemaMapping)
        assert schema.table == "mail"
        assert schema.id_col == "emailid"
        assert schema.subject_col == "subject_fld"
        assert schema.date_col == "datesent_fld"
        assert schema.mailbox_col == "mailbox_fld"
        assert schema.attach_table == "attachdata"
        assert schema_type_label(schema) == "legacy"

    def test_no_known_table(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE other (id INTEGER)")
        with pytest.raises(ValueError, match="Keine bekannte Email-Tabelle"):
            detect_schema(conn)


class TestDecodeFilename:
    def test_str_passthrough(self) -> None:
        assert decode_filename("report.pdf") == "report.pdf"

    def test_bytes_utf8(self) -> None:
        assert decode_filename(b"report.pdf") == "report.pdf"

    def test_bytes_invalid_utf8(self) -> None:
        result = decode_filename(b"file\xff\xfename.doc")
        assert "file" in result
        assert "name.doc" in result


class TestSearchEmails:
    def test_search_all(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema)
        assert len(results) == 3

    def test_search_all_legacy(self, legacy_db: sqlite3.Connection) -> None:
        schema = detect_schema(legacy_db)
        results = search_emails(legacy_db, schema)
        assert len(results) == 2

    def test_general_query(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, query="alice")
        assert len(results) >= 2

    def test_from_filter(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, from_filter="charlie")
        assert len(results) == 1
        assert results[0].from_ == "charlie@example.com"

    def test_to_filter(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, to_filter="bob")
        assert len(results) == 1

    def test_subject_filter(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, subject_filter="Invoice")
        assert len(results) == 1

    def test_body_filter(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, body_filter="lunch")
        assert len(results) == 1

    def test_date_range(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(
            modern_db, schema, date_from="2024-02-01", date_to="2024-02-28"
        )
        assert len(results) == 1
        assert results[0].subject == "Invoice #42"

    def test_limit(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, limit=1)
        assert len(results) == 1

    def test_has_attachments_true(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, has_attachments=True)
        assert len(results) == 1
        assert results[0].id == 1

    def test_has_attachments_false(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema, has_attachments=False)
        assert len(results) == 2

    def test_no_body_in_results(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema)
        assert not hasattr(results[0], "body")

    def test_modern_uses_id(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        results = search_emails(modern_db, schema)
        ids = {r.id for r in results}
        assert ids == {1, 2, 3}

    def test_legacy_uses_emailid(self, legacy_db: sqlite3.Connection) -> None:
        schema = detect_schema(legacy_db)
        results = search_emails(legacy_db, schema)
        ids = {r.id for r in results}
        assert ids == {1, 2}


class TestGetEmail:
    def test_found_modern(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        email = get_email(modern_db, schema, 1)
        assert email is not None
        assert email.from_ == "alice@example.com"
        assert email.subject == "Meeting Notes"
        assert email.body == "Discussing Q1 plans"

    def test_found_legacy(self, legacy_db: sqlite3.Connection) -> None:
        schema = detect_schema(legacy_db)
        email = get_email(legacy_db, schema, 1)
        assert email is not None
        assert email.from_ == "alice@example.com"

    def test_not_found(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        email = get_email(modern_db, schema, 999)
        assert email is None

    def test_has_attachments_flag(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        email1 = get_email(modern_db, schema, 1)
        email2 = get_email(modern_db, schema, 2)
        assert email1 is not None and email1.has_attachments is True
        assert email2 is not None and email2.has_attachments is False


class TestGetStats:
    def test_stats(self, modern_db: sqlite3.Connection) -> None:
        schema = detect_schema(modern_db)
        stats = get_stats(modern_db, schema)
        assert "INBOX" in stats.mailbox_counts
        assert stats.mailbox_counts["INBOX"] == 2
        assert "alice@example.com" in stats.sender_counts


class TestDiscoverDatabases:
    def test_finds_sqlite_files(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        _create_modern_db(conn)
        conn.execute(
            "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox) "
            "VALUES (1, 'a@b.com', 'c@d.com', 'Test', '2024-01-01', 'Body', 'INBOX')"
        )
        conn.commit()
        conn.close()

        results = discover_databases(tmp_path)
        assert len(results) == 1
        assert results[0].name == "test.db"
        assert results[0].email_count == 1
        assert results[0].schema_type == "modern"

    def test_empty_dir(self, tmp_path: Path) -> None:
        results = discover_databases(tmp_path)
        assert results == []

    def test_nonexistent_dir(self, tmp_path: Path) -> None:
        results = discover_databases(tmp_path / "nonexistent")
        assert results == []

    def test_sorted_by_name(self, tmp_path: Path) -> None:
        for name in ["zebra", "alpha", "gamma"]:
            db_path = tmp_path / name
            conn = sqlite3.connect(str(db_path))
            _create_modern_db(conn)
            conn.commit()
            conn.close()

        results = discover_databases(tmp_path)
        names = [r.name for r in results]
        assert names == sorted(names)
