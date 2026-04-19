"""Tests for Dolt server management."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from thread.dolt import detect_dolt_backend, find_beads_dir, find_dolt_db_dir


class TestFindBeadsDir:
    def test_explicit_arg(self, tmp_path):
        bd = tmp_path / ".beads"
        bd.mkdir()
        result = find_beads_dir(str(bd))
        assert result == bd.resolve()

    def test_env_var(self, tmp_path, monkeypatch):
        bd = tmp_path / ".beads"
        bd.mkdir()
        monkeypatch.setenv("BEADS_DIR", str(bd))
        result = find_beads_dir()
        assert result == bd.resolve()

    def test_explicit_overrides_env(self, tmp_path, monkeypatch):
        env_bd = tmp_path / "env-beads"
        env_bd.mkdir()
        arg_bd = tmp_path / "arg-beads"
        arg_bd.mkdir()
        monkeypatch.setenv("BEADS_DIR", str(env_bd))
        result = find_beads_dir(str(arg_bd))
        assert result == arg_bd.resolve()

    def test_not_found_raises(self):
        with pytest.raises(FileNotFoundError):
            find_beads_dir("/nonexistent/path")


class TestFindDoltDbDir:
    def test_finds_db_with_dolt_dir(self, tmp_path):
        embedded = tmp_path / "embeddeddolt"
        embedded.mkdir()
        db = embedded / "my_project"
        db.mkdir()
        (db / ".dolt").mkdir()

        result = find_dolt_db_dir(tmp_path)
        assert result == db

    def test_no_embeddeddolt_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="embeddeddolt"):
            find_dolt_db_dir(tmp_path)

    def test_no_dolt_db_raises(self, tmp_path):
        embedded = tmp_path / "embeddeddolt"
        embedded.mkdir()
        (embedded / "not_a_db").mkdir()

        with pytest.raises(FileNotFoundError, match="No Dolt database"):
            find_dolt_db_dir(tmp_path)


class TestDetectDoltBackend:
    """Detect whether .beads/ uses embedded or server-mode Dolt.

    Embedded (legacy default): .beads/embeddeddolt/<db>/.dolt — thread spawns
    its own dolt sql-server against the on-disk database.

    Server (new bd default): .beads/dolt/ — bd runs/manages the dolt sql-server;
    thread connects to it as a pymysql client. Required for team deployments
    where the Dolt server is shared (possibly remote).
    """

    def test_embedded_mode_when_only_embeddeddolt(self, tmp_path):
        bd = tmp_path / ".beads"
        bd.mkdir()
        (bd / "embeddeddolt").mkdir()

        assert detect_dolt_backend(bd) == "embedded"

    def test_server_mode_when_only_dolt(self, tmp_path):
        bd = tmp_path / ".beads"
        bd.mkdir()
        (bd / "dolt").mkdir()

        assert detect_dolt_backend(bd) == "server"

    def test_raises_when_both_exist(self, tmp_path):
        """Ambiguous state — user should clean up rather than have thread guess."""
        bd = tmp_path / ".beads"
        bd.mkdir()
        (bd / "embeddeddolt").mkdir()
        (bd / "dolt").mkdir()

        with pytest.raises(ValueError, match="both embeddeddolt and dolt"):
            detect_dolt_backend(bd)

    def test_raises_when_neither_present(self, tmp_path):
        bd = tmp_path / ".beads"
        bd.mkdir()

        with pytest.raises(FileNotFoundError, match="embeddeddolt.*dolt"):
            detect_dolt_backend(bd)
