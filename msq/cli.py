"""CLI entry point for msq."""

import typer

from msq import __version__

app = typer.Typer(
    name="msq",
    help="CLI tool for querying MailSteward SQLite email archives.",
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    """Zeigt die Version an."""
    if value:
        typer.echo(f"msq {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-V", callback=version_callback, is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """msq - Query MailSteward SQLite email archives from the command line."""
