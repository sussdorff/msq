"""Tests fuer msq.output."""

import json
from pathlib import Path

from msq.models import AttachmentInfo, DatabaseInfo, DatabaseStats, EmailDetail, EmailResult
from msq.output import (
    OutputFormat,
    _format_size,
    output_attachments,
    output_databases,
    output_email_detail,
    output_emails,
    output_stats,
    print_error,
    print_info,
    print_success,
    print_warning,
)


def _sample_database() -> DatabaseInfo:
    return DatabaseInfo(
        name="test.db",
        path=Path("/tmp/test.db"),
        email_count=100,
        date_range=("2020-01-01", "2024-12-31"),
        size_bytes=1048576,
        schema_type="v3",
    )


def _sample_email() -> EmailResult:
    return EmailResult(
        id=1,
        from_="alice@example.com",
        to="bob@example.com",
        subject="Test Subject",
        date="2024-01-15",
        mailbox="INBOX",
        has_attachments=True,
    )


def _sample_email_detail() -> EmailDetail:
    return EmailDetail(
        id=1,
        from_="alice@example.com",
        to="bob@example.com",
        subject="Test Subject",
        date="2024-01-15",
        mailbox="INBOX",
        has_attachments=False,
        body="Hello World",
        cc="carol@example.com",
        bcc="",
    )


def _sample_attachment() -> AttachmentInfo:
    return AttachmentInfo(id=1, filename="report.pdf", size=204800)


class TestFormatSize:
    def test_bytes(self):
        assert _format_size(500) == "500 B"

    def test_kilobytes(self):
        assert _format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert _format_size(1048576) == "1.0 MB"


class TestOutputDatabases:
    def test_table_format(self, capsys):
        output_databases([_sample_database()], OutputFormat.TABLE)
        captured = capsys.readouterr()
        assert "test.db" in captured.out
        assert "100" in captured.out
        assert "v3" in captured.out

    def test_json_format(self, capsys):
        output_databases([_sample_database()], OutputFormat.JSON)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 1
        assert data[0]["name"] == "test.db"
        assert data[0]["email_count"] == 100

    def test_csv_format(self, capsys):
        output_databases([_sample_database()], OutputFormat.CSV)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert "name" in lines[0]
        assert "test.db" in lines[1]

    def test_empty_list(self, capsys):
        output_databases([], OutputFormat.JSON)
        captured = capsys.readouterr()
        assert json.loads(captured.out) == []


class TestOutputEmails:
    def test_table_format(self, capsys):
        output_emails([_sample_email()], OutputFormat.TABLE)
        captured = capsys.readouterr()
        assert "alice@example.com" in captured.out
        assert "Test Subject" in captured.out
        assert "\u2713" in captured.out

    def test_json_format(self, capsys):
        output_emails([_sample_email()], OutputFormat.JSON)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert len(data) == 1
        assert data[0]["from"] == "alice@example.com"
        assert data[0]["has_attachments"] is True

    def test_csv_format(self, capsys):
        output_emails([_sample_email()], OutputFormat.CSV)
        captured = capsys.readouterr()
        lines = captured.out.strip().split("\n")
        assert "from" in lines[0]
        assert "alice@example.com" in lines[1]

    def test_no_attachments(self, capsys):
        email = EmailResult(
            id=2,
            from_="x@y.com",
            to="a@b.com",
            subject="No Att",
            date="2024-01-01",
            mailbox="INBOX",
            has_attachments=False,
        )
        output_emails([email], OutputFormat.TABLE)
        captured = capsys.readouterr()
        assert "\u2713" not in captured.out


class TestOutputEmailDetail:
    def test_detail_output(self, capsys):
        output_email_detail(_sample_email_detail())
        captured = capsys.readouterr()
        assert "alice@example.com" in captured.out
        assert "bob@example.com" in captured.out
        assert "carol@example.com" in captured.out
        assert "Hello World" in captured.out
        assert "Test Subject" in captured.out


class TestOutputAttachments:
    def test_table_format(self, capsys):
        output_attachments([_sample_attachment()], OutputFormat.TABLE)
        captured = capsys.readouterr()
        assert "report.pdf" in captured.out
        assert "200.0 KB" in captured.out

    def test_json_format(self, capsys):
        output_attachments([_sample_attachment()], OutputFormat.JSON)
        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data[0]["filename"] == "report.pdf"
        assert data[0]["size"] == 204800


class TestOutputStats:
    def test_stats_output(self, capsys):
        stats = DatabaseStats(
            mailbox_counts={"INBOX": 50, "Sent": 30},
            sender_counts={"alice@example.com": 20, "bob@example.com": 10},
            date_distribution={"2024-01": 15, "2024-02": 25},
        )
        output_stats(stats)
        captured = capsys.readouterr()
        assert "INBOX" in captured.out
        assert "50" in captured.out
        assert "alice@example.com" in captured.out
        assert "2024-01" in captured.out

    def test_empty_stats(self, capsys):
        output_stats(DatabaseStats())
        captured = capsys.readouterr()
        assert captured.out == ""


class TestHelpers:
    def test_print_success(self, capsys):
        print_success("done")
        captured = capsys.readouterr()
        assert "done" in captured.out

    def test_print_error(self, capsys):
        print_error("fail")
        captured = capsys.readouterr()
        assert "fail" in captured.err

    def test_print_warning(self, capsys):
        print_warning("warn")
        captured = capsys.readouterr()
        assert "warn" in captured.err

    def test_print_info(self, capsys):
        print_info("info")
        captured = capsys.readouterr()
        assert "info" in captured.out
