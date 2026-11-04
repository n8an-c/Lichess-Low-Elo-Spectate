"""Load config.toml into typed settings objects."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path


def parse_time_control(time_control: str) -> tuple[int, int] | None:
    """'3+0' -> (180, 0); None if the string is not minutes+increment."""
    try:
        minutes, inc = time_control.split("+")
        return int(float(minutes) * 60), int(inc)
    except ValueError:
        return None


def speed_for(initial: int, increment: int) -> str:
    """Lichess speed class for a clock: estimated duration = initial + 40 * increment."""
    total = initial + 40 * increment
    if total < 30:
        return "ultraBullet"
    if total < 180:
        return "bullet"
    if total < 480:
        return "blitz"
    if total < 1500:
        return "rapid"
    return "classical"


@dataclass(frozen=True)
class LichessSettings:
    user_agent: str = "lichess-low-elo-spectate (local, personal use)"
    # Minimum spacing between the start of consecutive requests (Lichess asks
    # for one request at a time; this keeps us polite without idle sleeps).
    courtesy_delay_seconds: float = 1.0
    rate_limit_pause_seconds: float = 60.0
    token: str = ""


POOL_SOURCES = ("games", "teams", "both")


@dataclass(frozen=True)
class PoolSettings:
    # "games": snowball from rated games (fast, every member is a proven player
    # of the configured time controls); "teams": scan team rosters; "both".
    source: str = "games"
    team_ids: list[str] = field(default_factory=list)
    team_keywords: list[str] = field(default_factory=list)
    keyword_max_pages: int = 1
    max_members_per_team: int = 1500
    rating_cap: int = 1000
    active_days: int = 7
    pool_refresh_hours: float = 3.0
    max_profile_refresh_batches: int = 20
    # -- games source (snowball crawl) --
    seed_players: list[str] = field(default_factory=list)
    max_members: int = 2000
    crawl_request_budget: int = 300
    games_since_days: int = 14
    games_per_request: int = 75
    expand_rating_max: int = 950
    trickle_interval_minutes: float = 5.0
    trickle_players: int = 10
    trickle_hours: float = 2.0
    light_refresh_minutes: float = 15.0
    profile_recheck_hours: float = 6.0

    def __post_init__(self) -> None:
        if self.source not in POOL_SOURCES:
            raise ValueError(f'[pool] source must be one of {POOL_SOURCES}, not {self.source!r}')
        if self.games_per_request <= 0 or self.max_members <= 0:
            raise ValueError("[pool] games_per_request and max_members must be positive")

    @property
    def games_enabled(self) -> bool:
        return self.source in ("games", "both")

    @property
    def teams_enabled(self) -> bool:
        return self.source in ("teams", "both")


@dataclass(frozen=True)
class DetectionSettings:
    mode: str = "poll"  # "poll" (status polling), "stream" (games-by-users streams) or "both"
    time_controls: list[str] = field(default_factory=lambda: ["3+0"])
    time_control: str | None = None  # legacy single value; if set it replaces time_controls
    rating_rule: str = "both"  # "both" players under the cap, or "either" (at least one)
    max_start_plies: int = 20
    poll_interval_seconds: float = 0.0  # minimum seconds per poll tick (0 = as fast as request spacing allows)
    hot_poll_seconds: float = 3.0  # how often players seen online are re-polled
    cold_batches_per_tick: int = 2  # 100-player batches of offline players swept per tick
    verify_concurrency: int = 4  # game streams opened in parallel for verification (Lichess allows 8)
    max_game_age_seconds: float = 120.0
    replay_idle_seconds: float = 0.3
    stream_open_timeout_seconds: float = 15.0
    candidate_ttl_seconds: float = 180.0
    stream_backoff_seconds: float = 5.0
    stream_backoff_max_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.mode not in ("poll", "stream", "both"):
            raise ValueError(f'[detection] mode must be "poll", "stream" or "both", not {self.mode!r}')
        if self.rating_rule not in ("both", "either"):
            raise ValueError(f'[detection] rating_rule must be "both" or "either", not {self.rating_rule!r}')
        tcs = [self.time_control] if self.time_control else list(self.time_controls)
        if not tcs:
            raise ValueError("[detection] time_controls must list at least one time control, e.g. [\"3+0\"]")
        for tc in tcs:
            if parse_time_control(tc) is None:
                raise ValueError(f"[detection] time control {tc!r} is not minutes+increment, e.g. \"3+0\"")
        object.__setattr__(self, "time_controls", tcs)
        if self.verify_concurrency < 1 or self.cold_batches_per_tick < 1:
            raise ValueError("[detection] verify_concurrency and cold_batches_per_tick must be at least 1")

    @property
    def poll_enabled(self) -> bool:
        return self.mode in ("poll", "both")

    @property
    def stream_enabled(self) -> bool:
        return self.mode in ("stream", "both")

    @property
    def label(self) -> str:
        return "/".join(self.time_controls)

    def clock_pairs(self) -> set[tuple[int, int]]:
        """Allowed clocks as (initial seconds, increment seconds)."""
        return {parse_time_control(tc) for tc in self.time_controls}  # type: ignore[misc]

    def initial_seconds(self) -> set[int]:
        return {pair[0] for pair in self.clock_pairs()}

    def speeds(self) -> set[str]:
        """Lichess speed classes the allowed clocks fall into (blitz, rapid, ...)."""
        return {speed_for(*pair) for pair in self.clock_pairs()}


@dataclass(frozen=True)
class ViewerSettings:
    engine_depth: int = 12


@dataclass(frozen=True)
class StorageSettings:
    sqlite_path: str = "spectate.sqlite3"


@dataclass(frozen=True)
class ServerSettings:
    host: str = "127.0.0.1"
    port: int = 8000


@dataclass(frozen=True)
class Settings:
    lichess: LichessSettings = field(default_factory=LichessSettings)
    pool: PoolSettings = field(default_factory=PoolSettings)
    detection: DetectionSettings = field(default_factory=DetectionSettings)
    viewer: ViewerSettings = field(default_factory=ViewerSettings)
    storage: StorageSettings = field(default_factory=StorageSettings)
    server: ServerSettings = field(default_factory=ServerSettings)


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
        server=_section(data, "server", ServerSettings),
    )
