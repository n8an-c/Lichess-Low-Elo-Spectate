"""Load config.toml into typed settings objects."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class LichessSettings:
    user_agent: str = "lichess-low-elo-spectate (local, personal use)"
    courtesy_delay_seconds: float = 1.0
    rate_limit_pause_seconds: float = 60.0
    token: str = ""


@dataclass(frozen=True)
class PoolSettings:
    team_ids: list[str] = field(default_factory=list)
    team_keywords: list[str] = field(default_factory=list)
    keyword_max_pages: int = 1
    max_members_per_team: int = 1500
    rating_cap: int = 1000
    active_days: int = 7
    pool_refresh_hours: float = 3.0
    max_profile_refresh_batches: int = 20


@dataclass(frozen=True)
class DetectionSettings:
    max_start_plies: int = 10
    poll_interval_seconds: float = 2.0
    time_control: str = "3+0"
    max_game_age_seconds: float = 120.0
    replay_idle_seconds: float = 1.0
    stream_open_timeout_seconds: float = 15.0
    candidate_ttl_seconds: float = 60.0


@dataclass(frozen=True)
class ViewerSettings:
    engine_depth: int = 12


@dataclass(frozen=True)
class StorageSettings:
    sqlite_path: str = "spectate.sqlite3"


@dataclass(frozen=True)
class Settings:
    lichess: LichessSettings = field(default_factory=LichessSettings)
    pool: PoolSettings = field(default_factory=PoolSettings)
    detection: DetectionSettings = field(default_factory=DetectionSettings)
    viewer: ViewerSettings = field(default_factory=ViewerSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)


def _section(data: dict, name: str, cls):
    raw = data.get(name, {})
    if not isinstance(raw, dict):
        raise ValueError(f"config section [{name}] must be a table")
    known = cls.__dataclass_fields__
    unknown = set(raw) - set(known)
    if unknown:
        raise ValueError(f"unknown keys in [{name}]: {', '.join(sorted(unknown))}")
    return cls(**raw)


def load_settings(path: str | Path = "config.toml") -> Settings:
    """Read a TOML config file. Missing sections/keys fall back to defaults."""
    path = Path(path)
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    return Settings(
        lichess=_section(data, "lichess", LichessSettings),
        pool=_section(data, "pool", PoolSettings),
        detection=_section(data, "detection", DetectionSettings),
        viewer=_section(data, "viewer", ViewerSettings),
        storage=_section(data, "storage", StorageSettings),
    )
