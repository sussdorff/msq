"""Tests fuer msq.parallel."""

import sqlite3

from msq.db import discover_databases
from msq.models import DatabaseInfo
from msq.parallel import search_all_databases


def _create_db(path, emails):
    """Erstellt eine SQLite-Datenbank mit Emails.

    Args:
        path: Pfad zur Datenbank-Datei
        emails: Liste von (from_, to, subject, date, body, mailbox) Tupeln
    """
    conn = sqlite3.connect(str(path))
    conn.execute("""CREATE TABLE mail (
        emailid INTEGER PRIMARY KEY,
        from_fld TEXT, to_fld TEXT, subject_fld TEXT,
        datesent_fld TEXT, body_fld TEXT, mailbox_fld TEXT,
        cc_fld TEXT DEFAULT '', bcc_fld TEXT DEFAULT ''
    )""")
    conn.execute("""CREATE TABLE attachdata (
        message_id INTEGER, filename TEXT, filedata BLOB
    )""")
    for email in emails:
        conn.execute(
            "INSERT INTO mail (from_fld, to_fld, subject_fld, datesent_fld, "
            "body_fld, mailbox_fld) VALUES (?,?,?,?,?,?)",
            email,
        )
    conn.commit()
    conn.close()


class TestSearchAllDatabases:
    def test_merges_results_from_multiple_dbs(self, tmp_path):
        _create_db(tmp_path / "db1.db", [
            ("alice@x.com", "bob@x.com", "Hello", "2024-01-15", "Hi", "INBOX"),
        ])
        _create_db(tmp_path / "db2.db", [
            ("charlie@x.com", "dave@x.com", "Meeting", "2024-02-20", "Go", "Work"),
        ])
        _create_db(tmp_path / "db3.db", [
            ("eve@x.com", "frank@x.com", "Report", "2024-03-10", "See", "INBOX"),
        ])

        databases = discover_databases(tmp_path)
        assert len(databases) == 3

        results = search_all_databases(databases)
        assert len(results) == 3

    def test_sorted_by_date_descending(self, tmp_path):
        _create_db(tmp_path / "old.db", [
            ("a@b.com", "c@d.com", "Old", "2023-01-01", "Old email", "INBOX"),
        ])
        _create_db(tmp_path / "new.db", [
            ("a@b.com", "c@d.com", "New", "2025-06-01", "New email", "INBOX"),
        ])
        _create_db(tmp_path / "mid.db", [
            ("a@b.com", "c@d.com", "Mid", "2024-06-15", "Mid email", "INBOX"),
        ])

        databases = discover_databases(tmp_path)
        results = search_all_databases(databases)

        dates = [r.date for r in results]
        assert dates == sorted(dates, reverse=True)
        assert results[0].subject == "New"
        assert results[1].subject == "Mid"
        assert results[2].subject == "Old"

    def test_filters_applied(self, tmp_path):
        _create_db(tmp_path / "db1.db", [
            ("alice@x.com", "bob@x.com", "Alpha", "2024-01-15", "D", "INBOX"),
            ("charlie@x.com", "dave@x.com", "Lunch", "2024-01-16", "L", "INBOX"),
        ])
        _create_db(tmp_path / "db2.db", [
            ("alice@x.com", "eve@x.com", "Beta", "2024-02-10", "More", "Work"),
        ])

        databases = discover_databases(tmp_path)
        results = search_all_databases(databases, from_filter="alice")
        assert len(results) == 2
        assert all("alice" in r.from_ for r in results)

    def test_graceful_error_handling(self, tmp_path):
        """Eine korrupte DB soll die Suche nicht abbrechen."""
        _create_db(tmp_path / "good.db", [
            ("a@b.com", "c@d.com", "Good", "2024-01-01", "OK", "INBOX"),
        ])

        # Erstelle DB ohne mail-Tabelle
        corrupt_path = tmp_path / "corrupt.db"
        corrupt_conn = sqlite3.connect(str(corrupt_path))
        corrupt_conn.execute("CREATE TABLE other (id INTEGER)")
        corrupt_conn.commit()
        corrupt_conn.close()

        databases = discover_databases(tmp_path)
        corrupt_info = DatabaseInfo(
            name="corrupt.db",
            path=corrupt_path,
            email_count=0,
            date_range=("", ""),
            size_bytes=corrupt_path.stat().st_size,
            schema_type="modern",
        )
        all_dbs = databases + [corrupt_info]

        results = search_all_databases(all_dbs)
        assert len(results) == 1
        assert results[0].subject == "Good"

    def test_empty_databases(self, tmp_path):
        _create_db(tmp_path / "empty1.db", [])
        _create_db(tmp_path / "empty2.db", [])

        databases = discover_databases(tmp_path)
        results = search_all_databases(databases)
        assert results == []

    def test_empty_database_list(self):
        results = search_all_databases([])
        assert results == []

    def test_query_filter(self, tmp_path):
        _create_db(tmp_path / "db1.db", [
            ("alice@x.com", "bob@x.com", "Important", "2024-01-15", "Hi", "INBOX"),
            ("charlie@x.com", "dave@x.com", "Spam", "2024-01-16", "Buy", "INBOX"),
        ])

        databases = discover_databases(tmp_path)
        results = search_all_databases(databases, query="alice")
        assert len(results) == 1
        assert results[0].subject == "Important"

    def test_date_range_filter(self, tmp_path):
        _create_db(tmp_path / "db1.db", [
            ("a@b.com", "c@d.com", "Jan", "2024-01-15", "January", "INBOX"),
            ("a@b.com", "c@d.com", "Mar", "2024-03-15", "March", "INBOX"),
        ])
        _create_db(tmp_path / "db2.db", [
            ("a@b.com", "c@d.com", "Feb", "2024-02-15", "February", "Work"),
        ])

        databases = discover_databases(tmp_path)
        results = search_all_databases(
            databases, date_from="2024-02-01", date_to="2024-02-28"
        )
        assert len(results) == 1
        assert results[0].subject == "Feb"
