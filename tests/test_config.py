"""Tests fuer msq.config."""

from pathlib import Path

from msq.config import Config, load_config, resolve_db, save_config


class TestLoadConfig:
    """Tests fuer load_config."""

    def test_load_existing_toml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            'db_dir = "/data/mail"\n\n[aliases]\nwork = "WorkMail.db"\n',
            encoding="utf-8",
        )

        config = load_config(config_file)

        assert config.db_dir == Path("/data/mail")
        assert config.aliases == {"work": "WorkMail.db"}

    def test_load_nonexistent_returns_defaults(self, tmp_path: Path) -> None:
        config = load_config(tmp_path / "missing.toml")

        assert config.db_dir == Path.home() / "MailSteward"
        assert config.aliases == {}

    def test_load_minimal_toml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text('db_dir = "/tmp/dbs"\n', encoding="utf-8")

        config = load_config(config_file)

        assert config.db_dir == Path("/tmp/dbs")
        assert config.aliases == {}

    def test_load_empty_toml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        config_file.write_text("", encoding="utf-8")

        config = load_config(config_file)

        assert config.db_dir == Path.home() / "MailSteward"
        assert config.aliases == {}


class TestSaveConfig:
    """Tests fuer save_config."""

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        config_file = tmp_path / "sub" / "dir" / "config.toml"
        config = Config(db_dir=Path("/data/mail"), aliases={"a": "b.db"})

        save_config(config, config_file)

        assert config_file.exists()

    def test_roundtrip(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        original = Config(
            db_dir=Path("/data/mail"),
            aliases={"work": "WorkMail.db", "personal": "Personal.db"},
        )

        save_config(original, config_file)
        loaded = load_config(config_file)

        assert loaded.db_dir == original.db_dir
        assert loaded.aliases == original.aliases

    def test_roundtrip_no_aliases(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.toml"
        original = Config(db_dir=Path("/data/mail"))

        save_config(original, config_file)
        loaded = load_config(config_file)

        assert loaded.db_dir == original.db_dir
        assert loaded.aliases == {}


class TestResolveDb:
    """Tests fuer resolve_db."""

    def test_resolve_via_alias(self, tmp_path: Path) -> None:
        db_file = tmp_path / "WorkMail.db"
        db_file.touch()
        config = Config(db_dir=tmp_path, aliases={"work": "WorkMail.db"})

        result = resolve_db(config, "work")

        assert result == db_file

    def test_resolve_exact_name(self, tmp_path: Path) -> None:
        db_file = tmp_path / "MyMail.db"
        db_file.touch()
        config = Config(db_dir=tmp_path)

        result = resolve_db(config, "MyMail.db")

        assert result == db_file

    def test_resolve_case_insensitive(self, tmp_path: Path) -> None:
        db_file = tmp_path / "MyMail.db"
        db_file.touch()
        config = Config(db_dir=tmp_path)

        result = resolve_db(config, "mymail.db")

        # Auf case-insensitive Filesystemen (macOS) greift schon der exakte Match,
        # auf case-sensitive (Linux) der iterdir-Fallback.
        assert result is not None
        assert result.name.lower() == "mymail.db"

    def test_resolve_not_found(self, tmp_path: Path) -> None:
        config = Config(db_dir=tmp_path)

        result = resolve_db(config, "nonexistent.db")

        assert result is None

    def test_resolve_alias_missing_file(self, tmp_path: Path) -> None:
        """Alias existiert, aber die Datei nicht -> fallback auf exakt/case-insensitive."""
        config = Config(db_dir=tmp_path, aliases={"work": "Gone.db"})

        result = resolve_db(config, "work")

        assert result is None

    def test_resolve_alias_priority_over_exact(self, tmp_path: Path) -> None:
        """Alias hat Vorrang vor exaktem Dateinamen."""
        (tmp_path / "work").touch()
        (tmp_path / "WorkAlias.db").touch()
        config = Config(db_dir=tmp_path, aliases={"work": "WorkAlias.db"})

        result = resolve_db(config, "work")

        assert result == tmp_path / "WorkAlias.db"
