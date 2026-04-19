"""Dolt server management for Thread extraction.

Finds the embedded Dolt database directory, starts a dolt sql-server
on a free port, provides a pymysql connection, and ensures clean shutdown.
"""

import os
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Literal

import pymysql


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

    Handles server lifecycle automatically.
    """
    bd = find_beads_dir(beads_dir)
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
