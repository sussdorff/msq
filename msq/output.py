"""Ausgabe-Funktionen fuer msq."""

import csv
import json
import sys
from enum import StrEnum, auto

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from msq.models import AttachmentInfo, DatabaseInfo, DatabaseStats, EmailDetail, EmailResult

console = Console()
err_console = Console(stderr=True)


class OutputFormat(StrEnum):
    TABLE = auto()
    JSON = auto()
    CSV = auto()


def _format_size(size_bytes: int) -> str:
    """Formatiert Bytes in menschenlesbare Groesse."""
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    if size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    return f"{size_bytes} B"


def output_databases(databases: list[DatabaseInfo], fmt: OutputFormat = OutputFormat.TABLE) -> None:
    """Gibt Datenbank-Informationen aus."""
    match fmt:
        case OutputFormat.TABLE:
            table = Table(title="Databases")
            table.add_column("Name")
            table.add_column("Emails", justify="right")
            table.add_column("Date Range")
            table.add_column("Size", justify="right")
            table.add_column("Schema")
            for db in databases:
                table.add_row(
                    db.name,
                    str(db.email_count),
                    f"{db.date_range[0]} - {db.date_range[1]}",
                    _format_size(db.size_bytes),
                    db.schema_type,
                )
            console.print(table)
        case OutputFormat.JSON:
            console.print(json.dumps([d.to_dict() for d in databases], indent=2, default=str))
        case OutputFormat.CSV:
            writer = csv.writer(sys.stdout)
            writer.writerow(["name", "email_count", "date_from", "date_to", "size_bytes", "schema"])
            for db in databases:
                writer.writerow([
                    db.name,
                    db.email_count,
                    db.date_range[0],
                    db.date_range[1],
                    db.size_bytes,
                    db.schema_type,
                ])


def output_emails(emails: list[EmailResult], fmt: OutputFormat = OutputFormat.TABLE) -> None:
    """Gibt Email-Suchergebnisse aus."""
    match fmt:
        case OutputFormat.TABLE:
            table = Table(title="Emails")
            table.add_column("ID", justify="right")
            table.add_column("From")
            table.add_column("Subject")
            table.add_column("Date")
            table.add_column("Mailbox")
            table.add_column("Att", justify="center")
            for email in emails:
                table.add_row(
                    str(email.id),
                    email.from_,
                    email.subject,
                    email.date,
                    email.mailbox,
                    "\u2713" if email.has_attachments else "",
                )
            console.print(table)
        case OutputFormat.JSON:
            console.print(json.dumps([e.to_dict() for e in emails], indent=2, default=str))
        case OutputFormat.CSV:
            writer = csv.writer(sys.stdout)
            writer.writerow(["id", "from", "subject", "date", "mailbox", "has_attachments"])
            for email in emails:
                writer.writerow([
                    email.id,
                    email.from_,
                    email.subject,
                    email.date,
                    email.mailbox,
                    email.has_attachments,
                ])


def output_email_detail(email: EmailDetail) -> None:
    """Gibt eine vollstaendige Email aus."""
    headers = Text()
    headers.append("From: ", style="bold")
    headers.append(f"{email.from_}\n")
    headers.append("To: ", style="bold")
    headers.append(f"{email.to}\n")
    if email.cc:
        headers.append("CC: ", style="bold")
        headers.append(f"{email.cc}\n")
    if email.bcc:
        headers.append("BCC: ", style="bold")
        headers.append(f"{email.bcc}\n")
    headers.append("Date: ", style="bold")
    headers.append(f"{email.date}\n")
    headers.append("Subject: ", style="bold")
    headers.append(email.subject)

    console.print(Panel(headers, title="Headers"))
    console.print(Panel(email.body, title="Body"))


def output_attachments(
    attachments: list[AttachmentInfo], fmt: OutputFormat = OutputFormat.TABLE
) -> None:
    """Gibt Anhang-Informationen aus."""
    match fmt:
        case OutputFormat.TABLE:
            table = Table(title="Attachments")
            table.add_column("#", justify="right")
            table.add_column("Filename")
            table.add_column("Size", justify="right")
            for att in attachments:
                table.add_row(str(att.id), att.filename, _format_size(att.size))
            console.print(table)
        case OutputFormat.JSON:
            console.print(
                json.dumps([a.to_dict() for a in attachments], indent=2, default=str)
            )
        case OutputFormat.CSV:
            writer = csv.writer(sys.stdout)
            writer.writerow(["id", "filename", "size"])
            for att in attachments:
                writer.writerow([att.id, att.filename, att.size])


def output_stats(stats: DatabaseStats) -> None:
    """Gibt Datenbank-Statistiken aus."""
    if stats.mailbox_counts:
        table = Table(title="Mailbox Counts")
        table.add_column("Mailbox")
        table.add_column("Count", justify="right")
        for mailbox, count in sorted(stats.mailbox_counts.items(), key=lambda x: -x[1]):
            table.add_row(mailbox, str(count))
        console.print(table)

    if stats.sender_counts:
        table = Table(title="Top Senders")
        table.add_column("Sender")
        table.add_column("Count", justify="right")
        for sender, count in sorted(stats.sender_counts.items(), key=lambda x: -x[1]):
            table.add_row(sender, str(count))
        console.print(table)

    if stats.date_distribution:
        table = Table(title="Date Distribution")
        table.add_column("Period")
        table.add_column("Count", justify="right")
        for period, count in sorted(stats.date_distribution.items()):
            table.add_row(period, str(count))
        console.print(table)


def print_success(msg: str) -> None:
    """Gibt eine Erfolgsmeldung aus."""
    console.print(f"[green]{msg}[/green]")


def print_error(msg: str) -> None:
    """Gibt eine Fehlermeldung auf stderr aus."""
    err_console.print(f"[red]{msg}[/red]")


def print_warning(msg: str) -> None:
    """Gibt eine Warnung auf stderr aus."""
    err_console.print(f"[yellow]{msg}[/yellow]")


def print_info(msg: str) -> None:
    """Gibt eine Info-Meldung aus."""
    console.print(msg)
