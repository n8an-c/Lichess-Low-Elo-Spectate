"""Command-line entry point: ``python -m backend.cli <command>``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone

from .config import load_settings
from .db import Database
from .lichess import LichessClient
from .pool import build_pool
from .service import Spectator


def _iso(ms: int | None) -> str:
    if ms is None:
        return "-"
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")


async def cmd_build_pool(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    async with LichessClient(settings.lichess) as client:
        with Database(settings.storage.sqlite_path) as db:
            result = await build_pool(client, db, settings.pool, max_teams=args.max_teams)
    print()
    print(result.summary())
    return 0


async def cmd_watch(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    spectator = Spectator(settings, skip_pool_build=args.skip_pool_build)
    print("watching for games; press Ctrl+C to stop", file=sys.stderr)
    try:
        await spectator.run()
    except asyncio.CancelledError:
        pass
    finally:
        print(
            f"accepted {spectator.accepted_total}, rejected {spectator.rejected_total}, "
            f"poll passes {spectator.poller.passes}",
            file=sys.stderr,
        )
    return 0


def cmd_show_pool(args: argparse.Namespace) -> int:
    settings = load_settings(args.config)
    with Database(settings.storage.sqlite_path) as db:
        stats = db.pool_stats()
        print(f"database: {db.path}")
        print(f"teams: {stats['teams_scanned']}/{stats['teams_total']} scanned")
        print(
            f"players: {stats['players_active']} active of {stats['players_total']} known"
            f" (last update {_iso(stats['last_updated'])} UTC)"
        )
        if stats["players_active"]:
            print(
                f"active blitz rating: min {stats['min_rating']}, avg {stats['avg_rating']}, "
                f"max {stats['max_rating']}"
            )
        rows = db.active_players(limit=args.limit)
        if rows:
            print()
            print(f"{'id':<22} {'rating':>6} {'prov':>4}  {'seen (UTC)':<16} source team")
            for r in rows:
                print(
                    f"{r['id']:<22} {r['blitz_rating']:>6} {r['provisional']:>4}  "
                    f"{_iso(r['seen_at']):<16} {r['source_team'] or '-'}"
                )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m backend.cli", description="Low-ELO Lichess Spectator")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", default="config.toml", help="path to config.toml (default: ./config.toml)")
    common.add_argument("--log-level", default="INFO", help="DEBUG logs every Lichess request")
    sub = parser.add_subparsers(dest="command", required=True)

    p_build = sub.add_parser(
        "build-pool", parents=[common], help="discover teams and write qualifying players to SQLite"
    )
    p_build.add_argument(
        "--max-teams", type=int, default=None, help="only scan the first N teams (quick trial run)"
    )
    p_build.set_defaults(func=lambda a: asyncio.run(cmd_build_pool(a)))

    p_watch = sub.add_parser("watch", parents=[common], help="poll the pool, verify games and log qualifying ones (milestone 2)")
    p_watch.add_argument(
        "--skip-pool-build",
        action="store_true",
        help="do not (re)build the pool on startup even if it is stale; poll whatever is in the database",
    )
    p_watch.set_defaults(func=lambda a: asyncio.run(cmd_watch(a)))

    p_show = sub.add_parser("show-pool", parents=[common], help="print pool statistics and a sample of active players")
    p_show.add_argument("--limit", type=int, default=20, help="number of players to list (default 20)")
    p_show.set_defaults(func=cmd_show_pool)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "httpcore", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
