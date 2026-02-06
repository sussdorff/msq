"""Tests fuer msq.attachments."""

import sqlite3

import pytest

from msq.attachments import extract_attachment, list_attachments


@pytest.fixture
def legacy_db_with_attachments():
    """Legacy-Schema DB mit Attachments (BLOB filename, mail_fld)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE mail (
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT
    )""")
    conn.execute("""CREATE TABLE attachdata (
        mail_fld INTEGER, filename_fld BLOB, attach_fld BLOB, filesize_fld INTEGER
    )""")
    conn.execute(
        "INSERT INTO mail VALUES ('a@b.com','c@d.com','Test','2024-01-01','Body','INBOX')"
    )
    # test.txt mit Inhalt "Hello"
    conn.execute(
        "INSERT INTO attachdata VALUES (1, X'746573742E747874', X'48656C6C6F', 5)"
    )
    # notes.md mit Inhalt "World!"
    conn.execute(
        "INSERT INTO attachdata VALUES (1, X'6E6F7465732E6D64', X'576F726C6421', 6)"
    )
    conn.commit()
    return conn


@pytest.fixture
def modern_db_with_attachments():
    """Modern-Schema DB mit Attachments (TEXT name, emailid)."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE mail (
        emailid INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT
    )""")
    conn.execute("""CREATE TABLE attachdata (
        emailid INTEGER, name TEXT, data BLOB, filesize INTEGER
    )""")
    conn.execute(
        "INSERT INTO mail VALUES (1,'a@b.com','c@d.com','Test','2024-01-01','Body','INBOX')"
    )
    conn.execute(
        "INSERT INTO mail VALUES (2,'x@y.com','z@w.com','Other','2024-02-01','Body2','INBOX')"
    )
    # report.pdf (5 Bytes) fuer emailid=1
    conn.execute("INSERT INTO attachdata VALUES (1, 'report.pdf', X'255044462D', 5)")
    conn.commit()
    return conn


class TestListAttachments:
    def test_legacy_lists_attachments(self, legacy_db_with_attachments):
        result = list_attachments(legacy_db_with_attachments, "legacy", 1)
        assert len(result) == 2
        assert result[0].filename == "test.txt"
        assert result[0].size == 5
        assert result[1].filename == "notes.md"
        assert result[1].size == 6

    def test_modern_lists_attachments(self, modern_db_with_attachments):
        result = list_attachments(modern_db_with_attachments, "modern", 1)
        assert len(result) == 1
        assert result[0].filename == "report.pdf"
        assert result[0].size == 5

    def test_legacy_no_attachments(self, legacy_db_with_attachments):
        """Email ohne Attachments liefert leere Liste."""
        result = list_attachments(legacy_db_with_attachments, "legacy", 999)
        assert result == []

    def test_modern_no_attachments(self, modern_db_with_attachments):
        """Email ohne Attachments liefert leere Liste."""
        result = list_attachments(modern_db_with_attachments, "modern", 2)
        assert result == []

    def test_attachment_has_rowid(self, modern_db_with_attachments):
        result = list_attachments(modern_db_with_attachments, "modern", 1)
        assert result[0].id > 0


class TestExtractAttachment:
    def test_legacy_extract(self, legacy_db_with_attachments, tmp_path):
        path = extract_attachment(legacy_db_with_attachments, "legacy", 1, 0, tmp_path)
        assert path == tmp_path / "test.txt"
        assert path.read_bytes() == b"Hello"

    def test_legacy_extract_second(self, legacy_db_with_attachments, tmp_path):
        path = extract_attachment(legacy_db_with_attachments, "legacy", 1, 1, tmp_path)
        assert path == tmp_path / "notes.md"
        assert path.read_bytes() == b"World!"

    def test_modern_extract(self, modern_db_with_attachments, tmp_path):
        path = extract_attachment(modern_db_with_attachments, "modern", 1, 0, tmp_path)
        assert path == tmp_path / "report.pdf"
        assert path.read_bytes() == b"%PDF-"

    def test_index_out_of_range(self, modern_db_with_attachments, tmp_path):
        with pytest.raises(IndexError, match="ausserhalb"):
            extract_attachment(modern_db_with_attachments, "modern", 1, 5, tmp_path)

    def test_null_data_raises(self, modern_db_with_attachments, tmp_path):
        """Attachment mit NULL data wirft ValueError."""
        modern_db_with_attachments.execute(
            "INSERT INTO attachdata VALUES (1, 'empty.bin', NULL, 0)"
        )
        modern_db_with_attachments.commit()
        with pytest.raises(ValueError, match="NULL"):
            extract_attachment(modern_db_with_attachments, "modern", 1, 1, tmp_path)
