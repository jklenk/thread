"""Dolt connection management for Thread extraction.

Supports both Beads Dolt backends, detecting mode automatically from
the on-disk layout under .beads/:

- Embedded: locate the database in .beads/embeddeddolt/, spawn a
  dolt sql-server on a free port, shut it down on exit.
- Server: read bd's resolved connection config via `bd dolt show --json`
  and connect to the already-running bd-managed server. Never spawns —
  bd owns the server's lifecycle (required for shared/remote team
  deployments).

All flows yield a pymysql connection; callers don't care which mode.
"""

import json
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pymysql


@dataclass(frozen=True)
class ServerConfig:
    """Resolved connection info for a bd-managed Dolt server.

    Fields mirror ``bd dolt show --json`` output.
    """

    host: str
    port: int
    database: str
    user: str


def find_beads_dir(beads_dir: str | None = None) -> Path:
    """Locate the .beads directory.

    Checks BEADS_DIR env var, then explicit argument, then .beads/ in cwd.
    """
    if beads_dir:
        p = Path(beads_dir)
    elif os.environ.get("BEADS_DIR"):
        p = Path(os.environ["BEADS_DIR"])
    else:
        p = Path(".beads")

    if not p.is_dir():
        raise FileNotFoundError(f"Beads directory not found: {p}")
    return p.resolve()


def detect_dolt_backend(beads_dir: Path) -> Literal["embedded", "server"]:
    """Detect whether this .beads/ uses embedded or server-mode Dolt.

    Embedded (legacy default): ``.beads/embeddeddolt/<db>/.dolt`` — thread
    spawns its own dolt sql-server against the on-disk database.

    Server (new bd default): ``.beads/dolt/`` — bd runs/manages the
    dolt sql-server; thread connects as a pymysql client. Required for
    team deployments where the Dolt server is shared (possibly remote).

    Raises:
        ValueError: if both directories exist (ambiguous state).
        FileNotFoundError: if neither exists.
    """
    embedded = beads_dir / "embeddeddolt"
    server = beads_dir / "dolt"
    has_embedded = embedded.is_dir()
    has_server = server.is_dir()

    if has_embedded and has_server:
        raise ValueError(
            f"Ambiguous Dolt state: both embeddeddolt and dolt directories "
            f"exist in {beads_dir}. Remove one to disambiguate."
        )
    if has_embedded:
        return "embedded"
    if has_server:
        return "server"
    raise FileNotFoundError(
        f"No embeddeddolt or dolt directory in {beads_dir}"
    )


def read_server_config(beads_dir: Path) -> ServerConfig:
    """Resolve Dolt connection info for a server-mode ``.beads/`` directory.

    Shells out to ``bd dolt show --json`` in the parent of ``beads_dir``
    (so bd's auto-discovery finds the correct board) and parses the
    resolved config. Delegates the config-priority cascade (env vars →
    ``metadata.json`` → ``config.yaml``) to bd itself.

    Raises:
        FileNotFoundError: if ``bd`` is not on PATH.
        subprocess.CalledProcessError: if ``bd dolt show --json`` exits
            non-zero.
        ValueError: if bd's output is not valid JSON, is missing any of
            the required fields (host, port, database, user), or reports
            a non-integer port.
    """
    result = subprocess.run(
        ["bd", "dolt", "show", "--json"],
        cwd=str(beads_dir.parent),
        capture_output=True,
        text=True,
        check=True,
    )
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"bd dolt show --json did not return valid JSON; "
            f"got: {result.stdout!r}"
        ) from exc

    required = ("host", "port", "database", "user")
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(
            f"bd dolt show --json output is missing required fields: "
            f"{missing}. Got keys: {sorted(data.keys())}"
        )

    try:
        port = int(data["port"])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"bd dolt show --json 'port' is not an integer: "
            f"{data['port']!r}"
        ) from exc

    return ServerConfig(
        host=data["host"],
        port=port,
        database=data["database"],
        user=data["user"],
    )


def find_dolt_db_dir(beads_dir: Path) -> Path:
    """Find the embedded Dolt database directory inside .beads/embeddeddolt/."""
    embedded = beads_dir / "embeddeddolt"
    if not embedded.is_dir():
        raise FileNotFoundError(f"No embeddeddolt directory in {beads_dir}")

    # Find the first subdirectory that contains a .dolt folder
    for child in sorted(embedded.iterdir()):
        if child.is_dir() and (child / ".dolt").is_dir():
            return child

    raise FileNotFoundError(f"No Dolt database found in {embedded}")


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for_server(host: str, port: int, timeout: float = 10.0) -> None:
    """Wait until the Dolt server accepts connections."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"Dolt server did not start on {host}:{port} within {timeout}s")


@contextmanager
def dolt_server(dolt_db_dir: Path):
    """Context manager that starts a dolt sql-server and yields (host, port).

    The server is shut down when the context exits, even on error.
    """
    port = _find_free_port()
    host = "127.0.0.1"

    proc = subprocess.Popen(
        ["dolt", "sql-server", f"--host={host}", f"--port={port}"],
        cwd=str(dolt_db_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        _wait_for_server(host, port)
        yield host, port
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@contextmanager
def dolt_connection(beads_dir: str | None = None):
    """Context manager that yields a pymysql connection to the Dolt database.

    Dispatches based on backend mode:

    - **embedded**: spawns a dolt sql-server against
      ``.beads/embeddeddolt/<db>/`` and connects pymysql to the spawned
      instance (shutdown handled on context exit).
    - **server**: reads connection info from bd's own configuration
      (via ``bd dolt show --json``) and connects pymysql directly.
      Never spawns — bd owns the server's lifecycle.
    """
    bd = find_beads_dir(beads_dir)
    mode = detect_dolt_backend(bd)

    if mode == "embedded":
        db_dir = find_dolt_db_dir(bd)
        db_name = db_dir.name
        with dolt_server(db_dir) as (host, port):
            conn = pymysql.connect(
                host=host,
                port=port,
                user="root",
                database=db_name,
                cursorclass=pymysql.cursors.DictCursor,
            )
            try:
                yield conn
            finally:
                conn.close()
    else:  # "server"
        cfg = read_server_config(bd)
        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            database=cfg.database,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            yield conn
        finally:
            conn.close()
