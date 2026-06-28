"""Tendwire runtime configuration.

Loads settings from a simple defaults + optional environment override set.
No external config-file parser is required.
"""

from __future__ import annotations

import os
import platform
import socket
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Config:
    """Neutral runtime configuration for Tendwire."""

    host_id: str = field(default_factory=lambda: platform.node() or "unknown")
    herdr_bin: str = "herdr"
    data_dir: Path = field(default_factory=lambda: Path.home() / ".local" / "share" / "tendwire")
    db_path: Path | None = None
    herdr_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        object.__setattr__(self, "herdr_bin", os.path.expanduser(self.herdr_bin))
        object.__setattr__(self, "data_dir", Path(self.data_dir).expanduser())
        if self.db_path is None:
            object.__setattr__(
                self,
                "db_path",
                self.data_dir / "tendwire.db",
            )
        else:
            object.__setattr__(self, "db_path", Path(self.db_path).expanduser())
        if self.herdr_timeout_seconds <= 0:
            raise ValueError("herdr_timeout_seconds must be positive")


def load_config(
    *,
    host_id: str | None = None,
    herdr_bin: str | None = None,
    data_dir: str | Path | None = None,
    db_path: str | Path | None = None,
    herdr_timeout_seconds: float | str | None = None,
) -> Config:
    """Build a Config from explicit args, then environment, then defaults."""
    env_host_id = os.environ.get("TENDWIRE_HOST_ID")
    env_herdr_bin = os.environ.get("TENDWIRE_HERDR_BIN")
    env_data_dir = os.environ.get("TENDWIRE_DATA_DIR")
    env_db_path = os.environ.get("TENDWIRE_DB_PATH")
    env_herdr_timeout_seconds = os.environ.get("TENDWIRE_HERDR_TIMEOUT_SECONDS")

    resolved_host_id = host_id or env_host_id or (platform.node() or "unknown")
    resolved_herdr_bin = herdr_bin or env_herdr_bin or "herdr"

    if db_path is not None:
        resolved_db_path = Path(db_path)
    elif env_db_path is not None:
        resolved_db_path = Path(env_db_path)
    else:
        resolved_db_path = None

    if data_dir is not None:
        resolved_data_dir = Path(data_dir)
    elif env_data_dir is not None:
        resolved_data_dir = Path(env_data_dir)
    else:
        resolved_data_dir = Path.home() / ".local" / "share" / "tendwire"

    raw_timeout = herdr_timeout_seconds
    if raw_timeout is None:
        raw_timeout = env_herdr_timeout_seconds
    if raw_timeout is None:
        resolved_herdr_timeout_seconds = 5.0
    else:
        try:
            resolved_herdr_timeout_seconds = float(raw_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError("herdr timeout must be a positive number") from exc

    return Config(
        host_id=resolved_host_id,
        herdr_bin=resolved_herdr_bin,
        data_dir=resolved_data_dir,
        db_path=resolved_db_path,
        herdr_timeout_seconds=resolved_herdr_timeout_seconds,
    )
