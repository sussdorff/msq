"""Configuration management for msq."""

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH: Path = Path.home() / ".config" / "msq" / "config.toml"


@dataclass
class Config:
    """msq-Konfiguration.

    Args:
        db_dir: Verzeichnis mit MailSteward-Datenbanken
        aliases: Mapping von Alias-Namen zu Datenbank-Dateinamen
    """

    db_dir: Path = field(default_factory=lambda: Path.home() / "MailSteward")
    aliases: dict[str, str] = field(default_factory=dict)


def load_config(path: Path | None = None) -> Config:
    """Laedt die Konfiguration aus einer TOML-Datei.

    Args:
        path: Pfad zur Config-Datei (default: ~/.config/msq/config.toml)

    Returns:
        Config mit geladenen oder Default-Werten
    """
    config_path = path or DEFAULT_CONFIG_PATH

    if not config_path.exists():
        return Config()

    data = tomllib.loads(config_path.read_text(encoding="utf-8"))

    db_dir_str = data.get("db_dir")
    db_dir = Path(db_dir_str) if db_dir_str else Path.home() / "MailSteward"

    aliases = data.get("aliases", {})

    return Config(db_dir=db_dir, aliases=aliases)


def save_config(config: Config, path: Path | None = None) -> None:
    """Schreibt die Konfiguration als TOML-Datei.

    Args:
        config: Zu speichernde Konfiguration
        path: Pfad zur Config-Datei (default: ~/.config/msq/config.toml)
    """
    config_path = path or DEFAULT_CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [f'db_dir = "{config.db_dir}"', ""]

    if config.aliases:
        lines.append("[aliases]")
        for alias, db_name in sorted(config.aliases.items()):
            lines.append(f'{alias} = "{db_name}"')
        lines.append("")

    config_path.write_text("\n".join(lines), encoding="utf-8")


def resolve_db(config: Config, name: str) -> Path | None:
    """Loest einen Datenbank-Namen zu einem Pfad auf.

    Suche in dieser Reihenfolge:
    1. Alias-Lookup (exakt)
    2. Exakter Dateiname im db_dir
    3. Case-insensitive Match im db_dir

    Args:
        config: Aktive Konfiguration
        name: Datenbank-Name oder Alias

    Returns:
        Pfad zur Datenbank oder None
    """
    # 1. Alias-Lookup
    if name in config.aliases:
        alias_target = config.aliases[name]
        alias_path = config.db_dir / alias_target
        if alias_path.exists():
            return alias_path

    # 2. Exakter Dateiname
    exact_path = config.db_dir / name
    if exact_path.exists():
        return exact_path

    # 3. Case-insensitive Match
    if config.db_dir.is_dir():
        name_lower = name.lower()
        for entry in config.db_dir.iterdir():
            if entry.name.lower() == name_lower:
                return entry

    return None
