"""Tests fuer msq.models."""

from pathlib import Path

from msq.models import (
    AttachmentInfo,
    DatabaseInfo,
    DatabaseStats,
    EmailDetail,
    EmailResult,
)


class TestEmailResult:
    def test_creation(self) -> None:
        result = EmailResult(
            id=1,
            from_="sender@example.com",
            to="receiver@example.com",
            subject="Test",
            date="2024-01-15",
            mailbox="INBOX",
            has_attachments=False,
        )
        assert result.id == 1
        assert result.from_ == "sender@example.com"
        assert result.to == "receiver@example.com"
        assert result.subject == "Test"
        assert result.date == "2024-01-15"
        assert result.mailbox == "INBOX"
        assert result.has_attachments is False

    def test_to_dict(self) -> None:
        result = EmailResult(
            id=42,
            from_="alice@example.com",
            to="bob@example.com",
            subject="Hello",
            date="2024-03-01",
            mailbox="Sent",
            has_attachments=True,
        )
        d = result.to_dict()
        assert d == {
            "id": 42,
            "from": "alice@example.com",
            "to": "bob@example.com",
            "subject": "Hello",
            "date": "2024-03-01",
            "mailbox": "Sent",
            "has_attachments": True,
        }

    def test_frozen(self) -> None:
        result = EmailResult(
            id=1,
            from_="a@b.com",
            to="c@d.com",
            subject="X",
            date="2024-01-01",
            mailbox="INBOX",
            has_attachments=False,
        )
        try:
            result.id = 99  # type: ignore[misc]
            msg = "Should have raised FrozenInstanceError"
            raise AssertionError(msg)
        except AttributeError:
            pass


class TestEmailDetail:
    def test_creation(self) -> None:
        detail = EmailDetail(
            id=5,
            from_="sender@example.com",
            to="receiver@example.com",
            subject="Details",
            date="2024-06-15",
            mailbox="INBOX",
            has_attachments=True,
            body="Hello World",
        )
        assert detail.id == 5
        assert detail.body == "Hello World"
        assert detail.cc == ""
        assert detail.bcc == ""

    def test_creation_with_cc_bcc(self) -> None:
        detail = EmailDetail(
            id=5,
            from_="sender@example.com",
            to="receiver@example.com",
            subject="Details",
            date="2024-06-15",
            mailbox="INBOX",
            has_attachments=False,
            body="Content",
            cc="cc@example.com",
            bcc="bcc@example.com",
        )
        assert detail.cc == "cc@example.com"
        assert detail.bcc == "bcc@example.com"

    def test_to_dict(self) -> None:
        detail = EmailDetail(
            id=10,
            from_="a@b.com",
            to="c@d.com",
            subject="Sub",
            date="2024-02-01",
            mailbox="Archive",
            has_attachments=False,
            body="Body text",
            cc="cc@x.com",
            bcc="",
        )
        d = detail.to_dict()
        assert d == {
            "id": 10,
            "from": "a@b.com",
            "to": "c@d.com",
            "subject": "Sub",
            "date": "2024-02-01",
            "mailbox": "Archive",
            "has_attachments": False,
            "body": "Body text",
            "cc": "cc@x.com",
            "bcc": "",
        }


class TestAttachmentInfo:
    def test_creation(self) -> None:
        att = AttachmentInfo(id=1, filename="doc.pdf", size=1024)
        assert att.id == 1
        assert att.filename == "doc.pdf"
        assert att.size == 1024

    def test_to_dict(self) -> None:
        att = AttachmentInfo(id=3, filename="image.png", size=2048)
        d = att.to_dict()
        assert d == {
            "id": 3,
            "filename": "image.png",
            "size": 2048,
        }


class TestDatabaseInfo:
    def test_creation(self) -> None:
        info = DatabaseInfo(
            name="mail_2024",
            path=Path("/data/mail_2024.db"),
            email_count=5000,
            date_range=("2024-01-01", "2024-12-31"),
            size_bytes=10_000_000,
            schema_type="modern",
        )
        assert info.name == "mail_2024"
        assert info.path == Path("/data/mail_2024.db")
        assert info.email_count == 5000
        assert info.date_range == ("2024-01-01", "2024-12-31")
        assert info.size_bytes == 10_000_000
        assert info.schema_type == "modern"

    def test_to_dict(self) -> None:
        info = DatabaseInfo(
            name="legacy_mail",
            path=Path("/old/legacy.db"),
            email_count=100,
            date_range=("2020-01-01", "2020-06-30"),
            size_bytes=500_000,
            schema_type="legacy",
        )
        d = info.to_dict()
        assert d == {
            "name": "legacy_mail",
            "path": "/old/legacy.db",
            "email_count": 100,
            "date_range": ["2020-01-01", "2020-06-30"],
            "size_bytes": 500_000,
            "schema_type": "legacy",
        }

    def test_path_serialized_as_string(self) -> None:
        info = DatabaseInfo(
            name="test",
            path=Path("/some/path/db.sqlite"),
            email_count=0,
            date_range=("", ""),
            size_bytes=0,
            schema_type="modern",
        )
        d = info.to_dict()
        assert isinstance(d["path"], str)


class TestDatabaseStats:
    def test_creation_defaults(self) -> None:
        stats = DatabaseStats()
        assert stats.mailbox_counts == {}
        assert stats.sender_counts == {}
        assert stats.date_distribution == {}

    def test_mutable(self) -> None:
        stats = DatabaseStats()
        stats.mailbox_counts["INBOX"] = 100
        stats.sender_counts["alice@example.com"] = 50
        stats.date_distribution["2024-01"] = 30
        assert stats.mailbox_counts == {"INBOX": 100}
        assert stats.sender_counts == {"alice@example.com": 50}
        assert stats.date_distribution == {"2024-01": 30}

    def test_to_dict(self) -> None:
        stats = DatabaseStats(
            mailbox_counts={"INBOX": 200, "Sent": 50},
            sender_counts={"a@b.com": 100},
            date_distribution={"2024-01": 80, "2024-02": 120},
        )
        d = stats.to_dict()
        assert d == {
            "mailbox_counts": {"INBOX": 200, "Sent": 50},
            "sender_counts": {"a@b.com": 100},
            "date_distribution": {"2024-01": 80, "2024-02": 120},
        }
