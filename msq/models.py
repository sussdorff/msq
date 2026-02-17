"""Datenmodelle fuer msq."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class EmailResult:
    """Suchergebnis ohne Body."""

    id: int
    from_: str
    to: str
    subject: str
    date: str
    mailbox: str
    has_attachments: bool

    def to_dict(self) -> dict[str, Any]:
        """Konvertiert zu einem dict fuer JSON-Output."""
        return {
            "id": self.id,
            "from": self.from_,
            "to": self.to,
            "subject": self.subject,
            "date": self.date,
            "mailbox": self.mailbox,
            "has_attachments": self.has_attachments,
        }


@dataclass(frozen=True, slots=True)
class EmailDetail:
    """Vollstaendige Email mit Body."""

    id: int
    from_: str
    to: str
    subject: str
    date: str
    mailbox: str
    has_attachments: bool
    body: str
    cc: str = ""
    bcc: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Konvertiert zu einem dict fuer JSON-Output."""
        return {
            "id": self.id,
            "from": self.from_,
            "to": self.to,
            "subject": self.subject,
            "date": self.date,
            "mailbox": self.mailbox,
            "has_attachments": self.has_attachments,
            "body": self.body,
            "cc": self.cc,
            "bcc": self.bcc,
        }


@dataclass(frozen=True, slots=True)
class AttachmentInfo:
    """Informationen zu einem Email-Anhang."""

    id: int
    filename: str
    size: int

    def to_dict(self) -> dict[str, Any]:
        """Konvertiert zu einem dict fuer JSON-Output."""
        return {
            "id": self.id,
            "filename": self.filename,
            "size": self.size,
        }


@dataclass(frozen=True, slots=True)
class DatabaseInfo:
    """Informationen ueber eine MailSteward-Datenbank."""

    name: str
    path: Path
    email_count: int
    date_range: tuple[str, str]
    size_bytes: int
    schema_type: str

    def to_dict(self) -> dict[str, Any]:
        """Konvertiert zu einem dict fuer JSON-Output."""
        return {
            "name": self.name,
            "path": str(self.path),
            "email_count": self.email_count,
            "date_range": list(self.date_range),
            "size_bytes": self.size_bytes,
            "schema_type": self.schema_type,
        }


@dataclass
class ExportStats:
    """Statistiken eines EML-Exports."""

    total: int = 0
    exported: int = 0
    skipped: int = 0
    errors: int = 0


@dataclass
class DatabaseStats:
    """Statistiken ueber eine MailSteward-Datenbank."""

    mailbox_counts: dict[str, int] = field(default_factory=dict)
    sender_counts: dict[str, int] = field(default_factory=dict)
    date_distribution: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Konvertiert zu einem dict fuer JSON-Output."""
        return {
            "mailbox_counts": dict(self.mailbox_counts),
            "sender_counts": dict(self.sender_counts),
            "date_distribution": dict(self.date_distribution),
        }
