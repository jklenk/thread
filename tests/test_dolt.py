"""Tests for Dolt server management."""

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from thread.dolt import (
    ServerConfig,
    detect_dolt_backend,
    find_beads_dir,
    find_dolt_db_dir,
    read_server_config,
)


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


class TestReadServerConfig:
    """Read resolved Dolt server config via `bd dolt show --json`.

    Thread delegates config-priority resolution (env vars → metadata.json →
    config.yaml) to bd itself rather than reimplementing the cascade.
    """

    # Real output shape from `bd dolt show --json` as of bd v1.x
    SAMPLE_JSON = {
        "backend": "dolt",
        "connection_ok": True,
        "database": "migration",
        "host": "127.0.0.1",
        "port": 3307,
        "user": "root",
    }

    def test_parses_host_port_database_user(self, tmp_path):
        """Well-formed JSON from bd dolt show --json maps to ServerConfig."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=json.dumps(self.SAMPLE_JSON),
                returncode=0,
            )
            cfg = read_server_config(bd)

        assert cfg.host == "127.0.0.1"
        assert cfg.port == 3307
        assert cfg.database == "migration"
        assert cfg.user == "root"

    def test_returns_server_config_instance(self, tmp_path):
        """Return type is the public ServerConfig dataclass."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=json.dumps(self.SAMPLE_JSON),
                returncode=0,
            )
            cfg = read_server_config(bd)

        assert isinstance(cfg, ServerConfig)

    def test_runs_bd_in_parent_of_beads_dir(self, tmp_path):
        """bd auto-discovers .beads/ in its cwd; run it in the parent so
        bd finds the same .beads/ we're asking about."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=json.dumps(self.SAMPLE_JSON),
                returncode=0,
            )
            read_server_config(bd)

        call = mock_run.call_args
        assert call.kwargs.get("cwd") == str(bd.parent)

    def test_invokes_bd_dolt_show_json(self, tmp_path):
        """The subprocess call is `bd dolt show --json`."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout=json.dumps(self.SAMPLE_JSON),
                returncode=0,
            )
            read_server_config(bd)

        # First positional arg is the command list
        cmd = mock_run.call_args.args[0]
        assert cmd == ["bd", "dolt", "show", "--json"]

    def test_raises_if_bd_not_installed(self, tmp_path):
        """Missing `bd` binary surfaces as FileNotFoundError."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.side_effect = FileNotFoundError("bd not on PATH")

            with pytest.raises(FileNotFoundError):
                read_server_config(bd)

    def test_raises_if_bd_returns_nonzero(self, tmp_path):
        """Non-zero exit from bd surfaces as CalledProcessError (check=True)."""
        bd = tmp_path / ".beads"
        bd.mkdir()

        with patch("thread.dolt.subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1,
                cmd=["bd", "dolt", "show", "--json"],
            )

            with pytest.raises(subprocess.CalledProcessError):
                read_server_config(bd)
