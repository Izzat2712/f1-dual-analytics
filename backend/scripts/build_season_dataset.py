from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.season_data import SUPPORTED_SEASONS, load_or_build_season  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build cached F1 season dataset(s).")
    parser.add_argument("--season", type=int, help="Single season to build.")
    parser.add_argument("--start", type=int, help="Start season (inclusive).")
    parser.add_argument("--end", type=int, help="End season (inclusive).")
    parser.add_argument("--force-refresh", action="store_true", help="Re-fetch and overwrite existing cache.")
    return parser.parse_args()


def build_many(start: int, end: int, force_refresh: bool) -> None:
    for season in range(start, end + 1):
        if season not in SUPPORTED_SEASONS:
            print(f"Skip {season}: outside supported range {SUPPORTED_SEASONS[0]}-{SUPPORTED_SEASONS[-1]}")
            continue
        load_or_build_season(season, force_refresh=force_refresh)
        print(f"Built season {season}")


def main() -> None:
    args = parse_args()

    if args.season is not None:
        load_or_build_season(args.season, force_refresh=args.force_refresh)
        print(f"Built season {args.season}")
        return

    if args.start is not None and args.end is not None:
        build_many(args.start, args.end, args.force_refresh)
        return

    load_or_build_season(2026, force_refresh=args.force_refresh)
    print("Built season 2026")


if __name__ == "__main__":
    main()
