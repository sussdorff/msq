"""Tests fuer msq CLI commands."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from msq.cli import app
from msq.config import Config
from msq.models import DatabaseInfo, EmailDetail, EmailResult

runner = CliRunner()

SAMPLE_DB = DatabaseInfo(
    name="test.db",
    path=Path("/tmp/test.db"),
    email_count=42,
    date_range=("2020-01-01", "2024-12-31"),
    size_bytes=1024 * 1024,
    schema_type="modern",
)

SAMPLE_EMAIL = EmailResult(
    id=1,
    from_="alice@example.com",
    to="bob@example.com",
    subject="Test Subject",
    date="2024-01-15",
    mailbox="INBOX",
    has_attachments=False,
)

SAMPLE_DETAIL = EmailDetail(
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

DEFAULT_CONFIG = Config(db_dir=Path("/tmp/mailsteward"))


class TestVersion:
    def test_version_flag(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "msq" in result.output


class TestDbs:
    @patch("msq.cli.discover_databases", return_value=[SAMPLE_DB])
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_dbs_table(self, _mock_config, _mock_discover) -> None:
        result = runner.invoke(app, ["dbs"])
        assert result.exit_code == 0
        assert "test.db" in result.output

    @patch("msq.cli.discover_databases", return_value=[SAMPLE_DB])
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_dbs_json(self, _mock_config, _mock_discover) -> None:
        result = runner.invoke(app, ["dbs", "--format", "json"])
        assert result.exit_code == 0
        assert "test.db" in result.output

    @patch("msq.cli.discover_databases", return_value=[])
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_dbs_empty(self, _mock_config, _mock_discover) -> None:
        result = runner.invoke(app, ["dbs"])
        assert result.exit_code == 0
        assert "No databases found" in result.output


class TestSearch:
    @patch("msq.cli.search_emails", return_value=[SAMPLE_EMAIL])
    @patch("msq.cli.detect_schema", return_value="modern")
    @patch("msq.cli.open_db")
    @patch("msq.cli.resolve_db", return_value=Path("/tmp/test.db"))
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_search_with_db(
        self, _cfg, _resolve, _open, _schema, _search
    ) -> None:
        result = runner.invoke(app, ["search", "hello", "--db", "test.db"])
        assert result.exit_code == 0
        assert "alice@example.com" in result.output

    @patch("msq.cli.search_emails", return_value=[SAMPLE_EMAIL])
    @patch("msq.cli.detect_schema", return_value="modern")
    @patch("msq.cli.open_db")
    @patch("msq.cli.discover_databases", return_value=[SAMPLE_DB])
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_search_all_dbs(
        self, _cfg, _discover, _open, _schema, _search
    ) -> None:
        result = runner.invoke(app, ["search", "hello"])
        assert result.exit_code == 0
        assert "alice@example.com" in result.output

    @patch("msq.cli.resolve_db", return_value=None)
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_search_db_not_found(self, _cfg, _resolve) -> None:
        result = runner.invoke(app, ["search", "hello", "--db", "nonexistent.db"])
        assert result.exit_code == 1
        assert "Database not found" in result.output

    @patch("msq.cli.search_emails", return_value=[])
    @patch("msq.cli.detect_schema", return_value="modern")
    @patch("msq.cli.open_db")
    @patch("msq.cli.resolve_db", return_value=Path("/tmp/test.db"))
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_search_no_results(
        self, _cfg, _resolve, _open, _schema, _search
    ) -> None:
        result = runner.invoke(app, ["search", "nothing", "--db", "test.db"])
        assert result.exit_code == 0
        assert "No results found" in result.output

    @patch("msq.cli.discover_databases", return_value=[])
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_search_no_dbs(self, _cfg, _discover) -> None:
        result = runner.invoke(app, ["search", "hello"])
        assert result.exit_code == 0
        assert "No databases found" in result.output


class TestShow:
    @patch("msq.cli.get_email", return_value=SAMPLE_DETAIL)
    @patch("msq.cli.detect_schema", return_value="modern")
    @patch("msq.cli.open_db")
    @patch("msq.cli.resolve_db", return_value=Path("/tmp/test.db"))
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_show_email(self, _cfg, _resolve, _open, _schema, _get) -> None:
        result = runner.invoke(app, ["show", "test.db", "1"])
        assert result.exit_code == 0
        assert "alice@example.com" in result.output
        assert "Hello World" in result.output

    @patch("msq.cli.resolve_db", return_value=None)
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_show_db_not_found(self, _cfg, _resolve) -> None:
        result = runner.invoke(app, ["show", "nonexistent.db", "1"])
        assert result.exit_code == 1
        assert "Database not found" in result.output

    @patch("msq.cli.get_email", return_value=None)
    @patch("msq.cli.detect_schema", return_value="modern")
    @patch("msq.cli.open_db")
    @patch("msq.cli.resolve_db", return_value=Path("/tmp/test.db"))
    @patch("msq.cli.load_config", return_value=DEFAULT_CONFIG)
    def test_show_email_not_found(
        self, _cfg, _resolve, _open, _schema, _get
    ) -> None:
        result = runner.invoke(app, ["show", "test.db", "999"])
        assert result.exit_code == 1
        assert "Email not found" in result.output
