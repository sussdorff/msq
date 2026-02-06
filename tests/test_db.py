"""Tests fuer msq.db."""

import sqlite3

import pytest

from msq.db import (
    decode_filename,
    detect_schema,
    discover_databases,
    get_email,
    search_emails,
)


def _make_legacy_db() -> sqlite3.Connection:
    """Erstellt eine Legacy-Schema In-Memory-DB."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE mail (
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT,
        cc_fld TEXT DEFAULT '', bcc_fld TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachdata (
        message_id INTEGER, filename TEXT, filedata BLOB
    )""")
    return conn


def _make_modern_db() -> sqlite3.Connection:
    """Erstellt eine Modern-Schema In-Memory-DB."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE mail (
        emailid INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT,
        cc_fld TEXT DEFAULT '', bcc_fld TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachdata (
        message_id INTEGER, filename TEXT, filedata BLOB
    )""")
    return conn


def _insert_email(conn, *, from_="alice@example.com", to="bob@example.com",
                  subject="Test", date="2024-01-15", body="Hello",
                  mailbox="INBOX", cc="", bcc=""):
    conn.execute(
        "INSERT INTO mail (from_fld, to_fld, subject_fld, datesent_fld, "
        "body_fld, mailbox_fld, cc_fld, bcc_fld) VALUES (?,?,?,?,?,?,?,?)",
        (from_, to, subject, date, body, mailbox, cc, bcc),
    )


@pytest.fixture
def legacy_db():
    conn = _make_legacy_db()
    _insert_email(conn, from_="alice@example.com", to="bob@example.com",
                  subject="Meeting Notes", date="2024-01-15",
                  body="Discussing Q1 plans", mailbox="INBOX")
    _insert_email(conn, from_="charlie@example.com", to="alice@example.com",
                  subject="Invoice #42", date="2024-02-20",
                  body="Please find attached", mailbox="Work")
    _insert_email(conn, from_="dave@example.com", to="team@example.com",
                  subject="Lunch?", date="2024-03-01",
                  body="Anyone up for lunch?", mailbox="INBOX",
                  cc="alice@example.com")
    conn.commit()
    return conn


@pytest.fixture
def modern_db():
    conn = _make_modern_db()
    _insert_email(conn, from_="alice@example.com", to="bob@example.com",
                  subject="Meeting Notes", date="2024-01-15",
                  body="Discussing Q1 plans", mailbox="INBOX")
    _insert_email(conn, from_="charlie@example.com", to="alice@example.com",
                  subject="Invoice #42", date="2024-02-20",
                  body="Please find attached", mailbox="Work")
    _insert_email(conn, from_="dave@example.com", to="team@example.com",
                  subject="Lunch?", date="2024-03-01",
                  body="Anyone up for lunch?", mailbox="INBOX",
                  cc="alice@example.com")
    conn.commit()
    return conn


# --- detect_schema ---

class TestDetectSchema:
    def test_legacy_schema(self, legacy_db):
        assert detect_schema(legacy_db) == "legacy"

    def test_modern_schema(self, modern_db):
        assert detect_schema(modern_db) == "modern"


# --- decode_filename ---

class TestDecodeFilename:
    def test_str_passthrough(self):
        assert decode_filename("report.pdf") == "report.pdf"

    def test_bytes_utf8(self):
        assert decode_filename(b"report.pdf") == "report.pdf"

    def test_bytes_invalid_utf8(self):
        result = decode_filename(b"file\xff\xfename.doc")
        assert "file" in result
        assert "name.doc" in result


# --- search_emails ---

class TestSearchEmails:
    def test_search_all(self, legacy_db):
        results = search_emails(legacy_db, "legacy")
        assert len(results) == 3

    def test_search_all_modern(self, modern_db):
        results = search_emails(modern_db, "modern")
        assert len(results) == 3

    def test_general_query(self, legacy_db):
        results = search_emails(legacy_db, "legacy", query="alice")
        # alice in from_fld (email 1) und to_fld (email 2), nicht in cc
        assert len(results) == 2

    def test_from_filter(self, modern_db):
        results = search_emails(modern_db, "modern", from_filter="charlie")
        assert len(results) == 1
        assert results[0].from_ == "charlie@example.com"

    def test_to_filter(self, legacy_db):
        results = search_emails(legacy_db, "legacy", to_filter="bob")
        assert len(results) == 1
        assert results[0].subject == "Meeting Notes"

    def test_subject_filter(self, modern_db):
        results = search_emails(modern_db, "modern", subject_filter="Invoice")
        assert len(results) == 1
        assert "Invoice" in results[0].subject

    def test_body_filter(self, legacy_db):
        results = search_emails(legacy_db, "legacy", body_filter="lunch")
        assert len(results) == 1
        assert results[0].subject == "Lunch?"

    def test_date_range(self, modern_db):
        results = search_emails(
            modern_db, "modern", date_from="2024-02-01", date_to="2024-02-28"
        )
        assert len(results) == 1
        assert results[0].subject == "Invoice #42"

    def test_limit(self, legacy_db):
        results = search_emails(legacy_db, "legacy", limit=2)
        assert len(results) == 2

    def test_has_attachments_true(self, modern_db):
        # Fuege Attachment hinzu fuer emailid=2
        modern_db.execute(
            "INSERT INTO attachdata (message_id, filename, filedata) VALUES (?,?,?)",
            (2, "invoice.pdf", b"fake-pdf"),
        )
        modern_db.commit()
        results = search_emails(modern_db, "modern", has_attachments=True)
        assert len(results) == 1
        assert results[0].has_attachments is True

    def test_has_attachments_false(self, modern_db):
        modern_db.execute(
            "INSERT INTO attachdata (message_id, filename, filedata) VALUES (?,?,?)",
            (1, "notes.txt", b"data"),
        )
        modern_db.commit()
        results = search_emails(modern_db, "modern", has_attachments=False)
        assert len(results) == 2
        assert all(not r.has_attachments for r in results)

    def test_no_body_in_results(self, legacy_db):
        """EmailResult hat kein body-Feld."""
        results = search_emails(legacy_db, "legacy")
        assert not hasattr(results[0], "body")

    def test_legacy_uses_rowid(self, legacy_db):
        results = search_emails(legacy_db, "legacy")
        assert all(isinstance(r.id, int) for r in results)
        assert all(r.id > 0 for r in results)

    def test_modern_uses_emailid(self, modern_db):
        results = search_emails(modern_db, "modern")
        ids = {r.id for r in results}
        assert ids == {1, 2, 3}


# --- get_email ---

class TestGetEmail:
    def test_found_legacy(self, legacy_db):
        result = get_email(legacy_db, "legacy", 1)
        assert result is not None
        assert result.from_ == "alice@example.com"
        assert result.body == "Discussing Q1 plans"

    def test_found_modern(self, modern_db):
        result = get_email(modern_db, "modern", 2)
        assert result is not None
        assert result.from_ == "charlie@example.com"
        assert result.subject == "Invoice #42"

    def test_not_found(self, legacy_db):
        result = get_email(legacy_db, "legacy", 999)
        assert result is None

    def test_cc_bcc(self, legacy_db):
        result = get_email(legacy_db, "legacy", 3)
        assert result is not None
        assert result.cc == "alice@example.com"

    def test_has_attachments_flag(self, modern_db):
        modern_db.execute(
            "INSERT INTO attachdata (message_id, filename, filedata) VALUES (?,?,?)",
            (1, "doc.pdf", b"data"),
        )
        modern_db.commit()
        with_attach = get_email(modern_db, "modern", 1)
        without_attach = get_email(modern_db, "modern", 2)
        assert with_attach is not None
        assert with_attach.has_attachments is True
        assert without_attach is not None
        assert without_attach.has_attachments is False


# --- discover_databases ---

class TestDiscoverDatabases:
    def test_finds_sqlite_files(self, tmp_path):
        # Erstelle eine echte SQLite-Datei
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("""CREATE TABLE mail (
            emailid INTEGER PRIMARY KEY,
            from_fld TEXT, to_fld TEXT, subject_fld TEXT,
            datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT,
            cc_fld TEXT DEFAULT '', bcc_fld TEXT DEFAULT ''
        )""")
        conn.execute(
            "INSERT INTO mail (from_fld, to_fld, subject_fld, datesent_fld, "
            "body_fld, mailbox_fld) VALUES (?,?,?,?,?,?)",
            ("a@b.com", "c@d.com", "Hello", "2024-01-01", "Body", "INBOX"),
        )
        conn.commit()
        conn.close()

        # Erstelle eine Nicht-SQLite-Datei
        (tmp_path / "notes.txt").write_text("not a database")

        results = discover_databases(tmp_path)
        assert len(results) == 1
        assert results[0].name == "test.db"
        assert results[0].email_count == 1
        assert results[0].schema_type == "modern"
        assert results[0].date_range == ("2024-01-01", "2024-01-01")
        assert results[0].size_bytes > 0

    def test_empty_dir(self, tmp_path):
        results = discover_databases(tmp_path)
        assert results == []

    def test_nonexistent_dir(self, tmp_path):
        results = discover_databases(tmp_path / "nope")
        assert results == []

    def test_sorted_by_name(self, tmp_path):
        for name in ["zebra.db", "alpha.db"]:
            db_path = tmp_path / name
            conn = sqlite3.connect(str(db_path))
            conn.execute("""CREATE TABLE mail (
                emailid INTEGER PRIMARY KEY,
                from_fld TEXT, to_fld TEXT, subject_fld TEXT,
                datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT
            )""")
            conn.commit()
            conn.close()

        results = discover_databases(tmp_path)
        assert [r.name for r in results] == ["alpha.db", "zebra.db"]
