"""Tests fuer msq.export."""

import email
import sqlite3

import pytest

from msq.db import detect_schema
from msq.export import build_eml, export_database, sanitize_path


def _create_export_db(conn: sqlite3.Connection) -> None:
    """Erstellt eine moderne DB mit headings-Spalte fuer Export-Tests."""
    conn.execute("""CREATE TABLE email (
        id INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subj_fld TEXT,
        date_fld TEXT, body_fld TEXT, mailbox TEXT,
        headings TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachments (
        id INTEGER, type_fld TEXT, filename_fld TEXT,
        encode_fld INTEGER DEFAULT 0, attach_fld BLOB
    )""")


@pytest.fixture
def export_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_export_db(conn)

    # Email mit RFC822-Headers
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox, headings) "
        "VALUES (1, 'alice@example.com', 'bob@example.com', 'Test Subject', "
        "'2024-01-15 10:30:00', 'Hello World', 'INBOX', "
        "'From: alice@example.com\r\nTo: bob@example.com\r\n"
        "Subject: Test Subject\r\nDate: Mon, 15 Jan 2024 10:30:00 +0100\r\n"
        "Message-ID: <test-001@example.com>\r\nContent-Type: text/plain; charset=utf-8\r\n')"
    )

    # HTML-Email
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox, headings) "
        "VALUES (2, 'charlie@example.com', 'alice@example.com', 'HTML Mail', "
        "'2024-02-20 14:00:00', '<h1>Hello</h1><p>World</p>', 'Work', "
        "'From: charlie@example.com\r\nTo: alice@example.com\r\n"
        "Subject: HTML Mail\r\nDate: Tue, 20 Feb 2024 14:00:00 +0100\r\n"
        "Message-ID: <test-002@example.com>\r\nContent-Type: text/html; charset=utf-8\r\n')"
    )

    # Email mit Attachment
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox, headings) "
        "VALUES (3, 'dave@example.com', 'team@example.com', 'With Attachment', "
        "'2024-03-01 09:00:00', 'See attached file', 'INBOX/Sub', "
        "'From: dave@example.com\r\nTo: team@example.com\r\n"
        "Subject: With Attachment\r\nDate: Fri, 1 Mar 2024 09:00:00 +0100\r\n"
        "Message-ID: <test-003@example.com>\r\nContent-Type: text/plain\r\n')"
    )
    conn.execute(
        "INSERT INTO attachments VALUES (3, 'application/pdf', 'report.pdf', 0, X'255044462D')"
    )

    # Email ohne Headers
    conn.execute(
        "INSERT INTO email (id, from_fld, to_fld, subj_fld, date_fld, body_fld, mailbox, headings) "
        "VALUES (4, 'x@y.com', 'z@w.com', 'No Headers', "
        "'2024-04-01 12:00:00', 'Plain body only', 'Sent', '')"
    )

    conn.commit()
    return conn


class TestSanitizePath:
    def test_simple_path(self):
        assert sanitize_path("INBOX") == "INBOX"

    def test_mbox_suffix_removed(self):
        assert sanitize_path("INBOX.mbox/2018.mbox/Done") == "INBOX/2018/Done"

    def test_nested_path(self):
        result = sanitize_path("V10/Balboacastlecamp/INBOX.mbox/2018.mbox/Done")
        assert result == "V10/Balboacastlecamp/INBOX/2018/Done"

    def test_empty_returns_unsorted(self):
        assert sanitize_path("") == "_unsorted"

    def test_none_returns_unsorted(self):
        assert sanitize_path(None) == "_unsorted"

    def test_unsafe_chars_replaced(self):
        result = sanitize_path('Folder<1>:test|"bad"')
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result
        assert "|" not in result
        assert '"' not in result


class TestBuildEml:
    def test_plain_text_with_headers(self):
        headers = (
            "From: alice@example.com\r\n"
            "To: bob@example.com\r\n"
            "Subject: Test\r\n"
            "Date: Mon, 15 Jan 2024 10:30:00 +0100\r\n"
            "Message-ID: <test@example.com>\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
        )
        eml_bytes = build_eml(headers, "Hello World", [])
        msg = email.message_from_bytes(eml_bytes)
        assert msg["From"] == "alice@example.com"
        assert msg["To"] == "bob@example.com"
        assert msg["Subject"] == "Test"
        assert msg["Message-ID"] == "<test@example.com>"
        assert b"Hello World" in eml_bytes

    def test_html_body(self):
        headers = (
            "From: a@b.com\r\n"
            "Content-Type: text/html\r\n"
        )
        eml_bytes = build_eml(headers, "<h1>Hi</h1>", [])
        msg = email.message_from_bytes(eml_bytes)
        assert msg.get_content_type() == "text/html"
        assert b"<h1>Hi</h1>" in eml_bytes

    def test_with_attachment(self):
        headers = (
            "From: a@b.com\r\n"
            "Subject: Test\r\n"
            "Content-Type: text/plain\r\n"
        )
        attachments = [("report.pdf", b"%PDF-", "application/pdf")]
        eml_bytes = build_eml(headers, "See attached", attachments)
        msg = email.message_from_bytes(eml_bytes)
        assert msg.is_multipart()
        parts = msg.get_payload()
        assert len(parts) == 2
        # First part is body
        assert parts[0].get_content_type() == "text/plain"
        # Second part is attachment
        assert parts[1].get_filename() == "report.pdf"
        assert parts[1].get_content_type() == "application/pdf"

    def test_no_headers_fallback(self):
        eml_bytes = build_eml("", "Just a body", [])
        msg = email.message_from_bytes(eml_bytes)
        assert msg.get_content_type() == "text/plain"
        assert b"Just a body" in eml_bytes

    def test_empty_body(self):
        headers = "From: a@b.com\r\nSubject: Empty\r\n"
        eml_bytes = build_eml(headers, "", [])
        msg = email.message_from_bytes(eml_bytes)
        assert msg["Subject"] == "Empty"

    def test_multiple_attachments(self):
        headers = "From: a@b.com\r\nContent-Type: text/plain\r\n"
        attachments = [
            ("file1.txt", b"Hello", "text/plain"),
            ("file2.pdf", b"%PDF-", "application/pdf"),
        ]
        eml_bytes = build_eml(headers, "Body", attachments)
        msg = email.message_from_bytes(eml_bytes)
        assert msg.is_multipart()
        parts = msg.get_payload()
        assert len(parts) == 3  # body + 2 attachments


class TestExportDatabase:
    def test_export_creates_files(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        stats = export_database(export_db, schema, tmp_path, "TestDB")
        assert stats.exported == 4
        assert stats.errors == 0

        # Check file structure
        eml_files = list(tmp_path.rglob("*.eml"))
        assert len(eml_files) == 4

    def test_dry_run_no_files(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        stats = export_database(export_db, schema, tmp_path, "TestDB", dry_run=True)
        assert stats.exported == 4

        eml_files = list(tmp_path.rglob("*.eml"))
        assert len(eml_files) == 0

    def test_mailbox_directories(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        export_database(export_db, schema, tmp_path, "TestDB")

        # INBOX should exist as a directory
        assert (tmp_path / "TestDB" / "INBOX").is_dir()

    def test_eml_is_parseable(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        export_database(export_db, schema, tmp_path, "TestDB")

        eml_files = list(tmp_path.rglob("*.eml"))
        for eml_file in eml_files:
            msg = email.message_from_bytes(eml_file.read_bytes())
            assert msg is not None

    def test_progress_callback(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        calls = []
        stats = export_database(
            export_db, schema, tmp_path, "TestDB",
            progress_callback=lambda: calls.append(1),
        )
        assert len(calls) == stats.total

    def test_attachment_in_eml(self, export_db, tmp_path):
        schema = detect_schema(export_db)
        export_database(export_db, schema, tmp_path, "TestDB")

        # Find the "With Attachment" EML
        eml_files = [f for f in tmp_path.rglob("*.eml") if "with-attachment" in f.name]
        assert len(eml_files) == 1
        msg = email.message_from_bytes(eml_files[0].read_bytes())
        assert msg.is_multipart()
        # Should have body + 1 attachment
        parts = msg.get_payload()
        assert len(parts) == 2

    def test_empty_database(self, tmp_path):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _create_export_db(conn)
        conn.commit()
        schema = detect_schema(conn)
        stats = export_database(conn, schema, tmp_path, "Empty")
        assert stats.total == 0
        assert stats.exported == 0
