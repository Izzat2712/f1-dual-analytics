from __future__ import annotations

import asyncio
import json
import os
import random
import time
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import numpy as np
from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sklearn.linear_model import LinearRegression

from .season_data import BASE, SUPPORTED_SEASONS, fetch, load_or_build_season, map_driver, normalize_constructor_name, season_file

app = FastAPI(title="F1 Dual-Mode Analytics API", version="0.1.0")
OPENF1_BASE = "https://api.openf1.org/v1"

raw_origins = os.getenv("CORS_ALLOW_ORIGINS", "*")
allow_origins = [item.strip() for item in raw_origins.split(",") if item.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins or ["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TelemetryPayload(BaseModel):
    speed: list[float] = Field(default_factory=list)
    throttle: list[float] = Field(default_factory=list)
    brake: list[float] = Field(default_factory=list)
    window: int = 5


class LapPredictionInput(BaseModel):
    tyre_compound: Literal["SOFT", "MEDIUM", "HARD"]
    sector1: float
    sector2: float
    sector3: float
    tyre_age_laps: int
    track_temp_c: float


class StrategyInput(BaseModel):
    total_laps: int = 57
    pit_window_start: int = 15
    pit_window_end: int = 32
    simulations: int = 500


class NetworkInput(BaseModel):
    packets: int = 120
    base_latency_ms: float = 45.0
    jitter_ms: float = 12.0
    packet_loss_rate: float = 0.03
    bandwidth_mbps: float = 4.0


class EngineeringDriverInput(BaseModel):
    season: int = 2025
    round_no: int
    driver: str

LAPS_OVERRIDE_BY_SEASON_ROUND = {
    (2022, 8): 51,   # Azerbaijan
    (2022, 9): 70,   # Canada
    (2022, 10): 52,  # British
    (2022, 16): 53,  # Italian
    (2023, 3): 58,   # Australia
    (2023, 9): 71,   # Austrian
    (2023, 10): 52,  # British
    (2023, 11): 70,  # Hungarian
    (2023, 12): 44,  # Belgian
    (2023, 13): 72,  # Dutch
    (2023, 14): 51,  # Italian
    (2023, 15): 62,  # Singapore
    (2023, 16): 53,  # Japanese
    (2023, 17): 57,  # Qatar
    (2023, 18): 56,  # United States
    (2023, 19): 71,  # Mexico City
    (2023, 20): 71,  # Sao Paulo
    (2023, 21): 50,  # Las Vegas
    (2023, 22): 58,  # Abu Dhabi
}


def is_classified_finisher_status(status: str | None) -> bool:
    value = str(status or "").strip().lower()
    if value == "finished":
        return True
    if value == "lapped":
        return True
    if value.startswith("+"):
        return True
    if "lap" in value and any(ch.isdigit() for ch in value):
        return True
    return False


def is_non_starter_status(status: str | None) -> bool:
    value = str(status or "").strip().lower()
    return (
        "did not start" in value
        or value == "dns"
        or "withdrawn" in value
        or "did not qualify" in value
        or value == "dnq"
        or "excluded" in value
        or value == "dsq"
    )


def moving_average(signal: list[float], window: int) -> list[float]:
    if not signal:
        return []
    w = max(1, window)
    padded = np.pad(np.array(signal, dtype=float), (w - 1, 0), mode="edge")
    kernel = np.ones(w) / w
    filtered = np.convolve(padded, kernel, mode="valid")
    return filtered.tolist()


def create_training_data() -> tuple[np.ndarray, np.ndarray]:
    rows = []
    targets = []
    compound_map = {"SOFT": 0, "MEDIUM": 1, "HARD": 2}
    rng = np.random.default_rng(7)

    for _ in range(450):
        compound = rng.choice(["SOFT", "MEDIUM", "HARD"])
        s1 = rng.normal(30.6, 0.8)
        s2 = rng.normal(37.1, 1.0)
        s3 = rng.normal(24.8, 0.7)
        tyre_age = int(rng.integers(1, 28))
        temp = rng.normal(34.0, 5.0)

        base = s1 + s2 + s3
        compound_factor = [0.0, 0.35, 0.75][compound_map[compound]]
        deg_penalty = 0.045 * tyre_age
        temp_penalty = max(0, temp - 38) * 0.03
        noise = rng.normal(0, 0.22)
        lap = base + compound_factor + deg_penalty + temp_penalty + noise

        rows.append([compound_map[compound], s1, s2, s3, tyre_age, temp])
        targets.append(lap)

    return np.array(rows), np.array(targets)


X_train, y_train = create_training_data()
model = LinearRegression()
model.fit(X_train, y_train)

SEASON_CACHE: dict[int, dict] = {}
SEASON_CACHE_MTIME_NS: dict[int, int | None] = {}
POSITIONS_CACHE: dict[tuple[int, int, str], dict] = {}
TYRE_STRATEGY_CACHE: dict[tuple[int, int], dict] = {}
TYRE_STRATEGY_CACHE_VERSION = 9
H2H_CACHE: dict[tuple[int, int, str, str, str], dict] = {}
TELEMETRY_CATALOG_CACHE: dict[tuple[int, int, str], dict] = {}
OPENF1_SESSIONS_CACHE: dict[tuple[int, str], list[dict]] = {}
ROUND_SESSION_SCHEDULE_CACHE: dict[tuple[int, int], list[dict]] = {}
SEASON_SESSION_SCHEDULE_CACHE: dict[int, list[dict]] = {}
QUALI_SIM_CACHE: dict[tuple[int, int, int, int, int], dict] = {}
POSITIONS_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "positions_cache"
POSITIONS_CACHE_VERSION = 15


def get_season_cache_mtime_ns(season: int) -> int | None:
    target = season_file(season)
    if not target.exists():
        return None
    try:
        return target.stat().st_mtime_ns
    except OSError:
        return None


def get_season_data(season: int) -> dict:
    if season not in SUPPORTED_SEASONS:
        raise HTTPException(
            status_code=400,
            detail=f"Season {season} is not supported. Supported: {SUPPORTED_SEASONS[0]}-{SUPPORTED_SEASONS[-1]}",
        )
    current_mtime_ns = get_season_cache_mtime_ns(season)
    cached_mtime_ns = SEASON_CACHE_MTIME_NS.get(season)
    if season not in SEASON_CACHE or cached_mtime_ns != current_mtime_ns:
        try:
            SEASON_CACHE[season] = load_or_build_season(season)
            SEASON_CACHE_MTIME_NS[season] = get_season_cache_mtime_ns(season)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to load season {season}: {exc}") from exc
    return SEASON_CACHE[season]


def to_session_iso8601(date_value: str | None, time_value: str | None) -> str | None:
    if not date_value:
        return None
    raw_time = str(time_value or "00:00:00Z").strip()
    if not raw_time:
        raw_time = "00:00:00Z"
    if raw_time.endswith("Z"):
        return f"{date_value}T{raw_time}"
    if "+" in raw_time or "-" in raw_time[1:]:
        return f"{date_value}T{raw_time}"
    return f"{date_value}T{raw_time}Z"


def extract_round_session_schedule(race: dict) -> list[dict]:
    has_sprint = bool(race.get("Sprint") or race.get("SprintQualifying"))
    if has_sprint:
        ordered_fields = [
            ("practice_1", "Practice 1", race.get("FirstPractice")),
            ("sprint_qualifying", "Sprint Qualifying", race.get("SprintQualifying")),
            ("sprint", "Sprint", race.get("Sprint")),
            ("qualifying", "Qualifying", race.get("Qualifying")),
            ("race", "Race", {"date": race.get("date"), "time": race.get("time")}),
        ]
    else:
        ordered_fields = [
            ("practice_1", "Practice 1", race.get("FirstPractice")),
            ("practice_2", "Practice 2", race.get("SecondPractice")),
            ("practice_3", "Practice 3", race.get("ThirdPractice")),
            ("qualifying", "Qualifying", race.get("Qualifying")),
            ("race", "Race", {"date": race.get("date"), "time": race.get("time")}),
        ]

    sessions: list[dict] = []
    for code, label, payload in ordered_fields:
        if not isinstance(payload, dict):
            continue
        start_utc = to_session_iso8601(payload.get("date"), payload.get("time"))
        if not start_utc:
            continue
        sessions.append(
            {
                "code": code,
                "label": label,
                "start_utc": start_utc,
            }
        )
    return sessions


def get_round_session_schedule(season: int, round_no: int) -> list[dict]:
    key = (season, round_no)
    cached = ROUND_SESSION_SCHEDULE_CACHE.get(key)
    if cached:
        return cached

    try:
        payload = fetch(f"{season}/{round_no}.json")
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        race = races[0] if races else {}
        schedule = extract_round_session_schedule(race)
    except Exception:
        schedule = []

    # Avoid caching empty schedules from transient fetch failures.
    if schedule:
        ROUND_SESSION_SCHEDULE_CACHE[key] = schedule
    return schedule


def get_season_session_schedule(season: int) -> list[dict]:
    cached = SEASON_SESSION_SCHEDULE_CACHE.get(season)
    if cached:
        return cached

    try:
        payload = fetch(f"{season}.json", limit=100)
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        rounds: list[dict] = []
        for race in races:
            schedule = extract_round_session_schedule(race)
            rounds.append(
                {
                    "round": int(race.get("round", 0) or 0),
                    "race_name": race.get("raceName"),
                    "track": {
                        "name": race.get("Circuit", {}).get("circuitName"),
                        "country": race.get("Circuit", {}).get("Location", {}).get("country"),
                        "locality": race.get("Circuit", {}).get("Location", {}).get("locality"),
                    },
                    "session_schedule": schedule,
                }
            )
        rounds.sort(key=lambda item: item.get("round", 0))
    except Exception:
        rounds = []

    # Avoid caching empty schedules from transient fetch failures.
    if rounds:
        SEASON_SESSION_SCHEDULE_CACHE[season] = rounds
    return rounds


def parse_warm_seasons() -> list[int]:
    raw = os.getenv("WARM_SEASONS", "2025")
    values: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            season = int(token)
        except ValueError:
            continue
        if season in SUPPORTED_SEASONS:
            values.append(season)
    return list(dict.fromkeys(values))


def parse_warm_rounds() -> list[int] | None:
    raw = os.getenv("WARM_POSITION_ROUNDS", "").strip().lower()
    if not raw or raw == "all":
        return None
    values: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            round_no = int(token)
        except ValueError:
            continue
        if round_no > 0:
            values.append(round_no)
    return list(dict.fromkeys(values))


def parse_warm_position_seasons() -> list[int]:
    raw = os.getenv("WARM_POSITION_SEASONS", os.getenv("WARM_SEASONS", "2025"))
    values: list[int] = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            season = int(token)
        except ValueError:
            continue
        if season in SUPPORTED_SEASONS:
            values.append(season)
    return list(dict.fromkeys(values))


def positions_cache_file(season: int, round_no: int, session_kind: str = "race") -> Path:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    return POSITIONS_CACHE_DIR / f"{season}_{round_no}_{normalized_session}.json"


def load_positions_from_disk(season: int, round_no: int, session_kind: str = "race") -> dict | None:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    target = positions_cache_file(season, round_no, normalized_session)
    if not target.exists():
        return None
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if payload.get("season") != season or payload.get("round") != round_no:
        return None
    payload_session = str(payload.get("session", "race")).strip().lower()
    if payload_session not in {"race", "sprint"}:
        payload_session = "race"
    if payload_session != normalized_session:
        return None
    if int(payload.get("cache_version", 0)) != POSITIONS_CACHE_VERSION:
        return None
    return payload


def save_positions_to_disk(payload: dict) -> None:
    season = payload.get("season")
    round_no = payload.get("round")
    if not isinstance(season, int) or not isinstance(round_no, int):
        return
    POSITIONS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    session_kind = str(payload.get("session", "race")).strip().lower()
    if session_kind not in {"race", "sprint"}:
        session_kind = "race"
    target = positions_cache_file(season, round_no, session_kind)
    stored = {**payload, "cache_version": POSITIONS_CACHE_VERSION}
    target.write_text(json.dumps(stored, separators=(",", ":")), encoding="utf-8")
def expected_lap_count(season: int, round_no: int, raw_results: list[dict] | None = None) -> int:
    override = LAPS_OVERRIDE_BY_SEASON_ROUND.get((season, round_no))
    if override is not None:
        return override
    rows = raw_results or []
    if rows:
        try:
            return max(0, int(rows[0].get("laps", 0) or 0))
        except (TypeError, ValueError):
            return 0
    return 0


def parse_lap_time_to_seconds(raw: str | None) -> float | None:
    value = str(raw or "").strip()
    if not value:
        return None
    parts = value.split(":")
    try:
        if len(parts) == 3:
            hours = int(parts[0])
            mins = int(parts[1])
            secs = float(parts[2])
            return (hours * 3600) + (mins * 60) + secs
        if len(parts) == 2:
            mins = int(parts[0])
            secs = float(parts[1])
            return (mins * 60) + secs
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_driver_name_key(name: str | None) -> str:
    normalized = unicodedata.normalize("NFKD", str(name or "").strip())
    ascii_only = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    return " ".join(ascii_only.casefold().split())


def resolve_canonical_driver_name(name: str | None, canonical_names: dict[str, str]) -> str | None:
    normalized_name = normalize_driver_name_key(name)
    if not normalized_name:
        return None
    direct = canonical_names.get(normalized_name)
    if direct:
        return direct

    name_tokens = set(normalized_name.split())
    candidates = []
    for canonical_key, canonical_name in canonical_names.items():
        canonical_tokens = set(canonical_key.split())
        if canonical_tokens and (canonical_tokens <= name_tokens or name_tokens <= canonical_tokens):
            candidates.append(canonical_name)

    if len(candidates) == 1:
        return candidates[0]
    return str(name) if name else None


def canonicalize_position_rows(
    rows: list[dict],
    canonical_names: dict[str, str],
) -> list[dict]:
    normalized_rows: list[dict] = []
    for row in rows:
        normalized_row = {"lap": row.get("lap")}
        for key, value in row.items():
            if key == "lap":
                continue
            canonical_key = resolve_canonical_driver_name(key, canonical_names) or key
            normalized_row[canonical_key] = value
        normalized_rows.append(normalized_row)
    return normalized_rows


def repair_cached_positions_payload(
    payload: dict,
    round_payload: dict,
    session_kind: str = "race",
) -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    session_rows = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")
    normalized_session_rows = [row for row in (session_rows or []) if row.get("driver")]
    session_driver_names = {
        normalize_driver_name_key(row.get("driver")): row.get("driver")
        for row in normalized_session_rows
        if row.get("driver")
    }
    if not session_driver_names:
        return payload

    normalized_laps = canonicalize_position_rows(
        [row for row in (payload.get("laps") or []) if isinstance(row, dict)],
        session_driver_names,
    )
    grid_order = []
    for row in normalized_session_rows:
        try:
            grid = int(row.get("grid", 0) or 0)
        except (TypeError, ValueError):
            grid = 0
        if grid <= 0:
            try:
                grid = int(row.get("position", 999) or 999)
            except (TypeError, ValueError):
                grid = 999
        grid_order.append((row["driver"], grid))
    grid_order.sort(key=lambda item: (item[1], item[0]))
    if grid_order:
        lap0 = {"lap": 0}
        for idx, (driver_name, _) in enumerate(grid_order, start=1):
            lap0[driver_name] = idx
        first_lap_value = normalized_laps[0].get("lap") if normalized_laps else None
        try:
            first_lap_no = int(first_lap_value) if first_lap_value is not None else -1
        except (TypeError, ValueError):
            first_lap_no = -1
        if normalized_laps and first_lap_no == 0:
            normalized_laps[0] = lap0
        else:
            normalized_laps = [lap0, *normalized_laps]

    deduped_laps: list[dict] = []
    seen_laps: set[int] = set()
    for row in normalized_laps:
        raw_lap_value = row.get("lap")
        try:
            lap_no = int(raw_lap_value) if raw_lap_value is not None else -1
        except (TypeError, ValueError):
            continue
        if lap_no in seen_laps:
            continue
        seen_laps.add(lap_no)
        deduped_laps.append(row)
    normalized_laps = deduped_laps

    team_by_driver = {row["driver"]: row.get("team") for row in normalized_session_rows}
    completed_laps_by_driver: dict[str, int] = {}
    for driver_name, lap_count in (payload.get("completed_laps_by_driver") or {}).items():
        canonical = resolve_canonical_driver_name(driver_name, session_driver_names) or driver_name
        try:
            lap_value = int(lap_count or 0)
        except (TypeError, ValueError):
            continue
        completed_laps_by_driver[canonical] = max(completed_laps_by_driver.get(canonical, 0), lap_value)
    for row in normalized_session_rows:
        completed_laps_by_driver.setdefault(row["driver"], 0)

    drivers = []
    seen_drivers = set()
    for item in payload.get("drivers") or []:
        if not isinstance(item, dict):
            continue
        canonical = resolve_canonical_driver_name(item.get("driver"), session_driver_names)
        if not canonical or canonical in seen_drivers:
            continue
        seen_drivers.add(canonical)
        drivers.append({"driver": canonical, "team": team_by_driver.get(canonical) or item.get("team")})
    for row in normalized_session_rows:
        driver_name = row["driver"]
        if driver_name in seen_drivers:
            continue
        seen_drivers.add(driver_name)
        drivers.append({"driver": driver_name, "team": team_by_driver.get(driver_name)})

    max_position = 0
    try:
        max_position = int(payload.get("max_position", 0) or 0)
    except (TypeError, ValueError):
        max_position = 0
    for row in normalized_laps:
        for key, value in row.items():
            if key == "lap":
                continue
            if isinstance(value, int):
                max_position = max(max_position, value)
    max_position = max(max_position, len(drivers))

    return {
        **payload,
        "session": normalized_session,
        "drivers": drivers,
        "laps": normalized_laps,
        "max_position": max_position,
        "dnf_drivers": [
            row["driver"]
            for row in normalized_session_rows
            if not is_classified_finisher_status(row.get("status"))
        ],
        "completed_laps_by_driver": completed_laps_by_driver,
    }


def reconstruct_positions_from_lap_times(
    lap_timings: dict[int, dict[str, float]],
    target_laps: int,
) -> list[dict]:
    cumulative_times: dict[str, float] = {}
    completed_laps: dict[str, int] = {}
    rows: list[dict] = []

    for lap_no in range(1, target_laps + 1):
        for driver_name, lap_time_s in lap_timings.get(lap_no, {}).items():
            cumulative_times[driver_name] = cumulative_times.get(driver_name, 0.0) + lap_time_s
            completed_laps[driver_name] = lap_no

        ranked = sorted(
            (
                driver_name
                for driver_name, completed_lap in completed_laps.items()
                if completed_lap > 0 and completed_lap <= lap_no
            ),
            key=lambda driver_name: (
                -completed_laps.get(driver_name, 0),
                cumulative_times.get(driver_name, float("inf")),
                driver_name,
            ),
        )
        row = {"lap": lap_no}
        for position, driver_name in enumerate(ranked, start=1):
            row[driver_name] = position
        rows.append(row)

    return rows


def format_lap_time(seconds: float | None) -> str | None:
    if seconds is None or not np.isfinite(seconds) or seconds <= 0:
        return None
    mins = int(seconds // 60)
    secs = float(seconds - (mins * 60))
    return f"{mins}:{secs:06.3f}"


def estimate_compound_by_stint_length(length_laps: int) -> str:
    if length_laps <= 10:
        return "SOFT"
    if length_laps <= 22:
        return "MEDIUM"
    return "HARD"


def load_lap_times_by_driver(
    season: int,
    round_no: int,
    driver_id_to_name: dict[str, str],
) -> tuple[dict[str, dict[int, float]], int]:
    lap_times_by_driver: dict[str, dict[int, float]] = {}
    max_lap = 0

    first_page = fetch_positions_fast(f"{season}/{round_no}/laps.json", limit=100, offset=0)
    first_mr = first_page.get("MRData", {})
    try:
        total_rows = int(first_mr.get("total", 0))
        limit_rows = max(1, int(first_mr.get("limit", 100)))
    except (TypeError, ValueError):
        total_rows = 0
        limit_rows = 100

    all_pages = [first_page]
    for offset in range(limit_rows, total_rows, limit_rows):
        all_pages.append(fetch_positions_fast(f"{season}/{round_no}/laps.json", limit=limit_rows, offset=offset))

    for page in all_pages:
        page_races = page.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        page_payload = page_races[0] if page_races else {}
        for lap in page_payload.get("Laps", []):
            try:
                lap_no = int(lap.get("number", 0))
            except (TypeError, ValueError):
                continue
            if lap_no <= 0:
                continue
            max_lap = max(max_lap, lap_no)
            for timing in lap.get("Timings", []):
                driver_name = driver_id_to_name.get(timing.get("driverId", ""))
                if not driver_name:
                    continue
                lap_seconds = parse_lap_time_to_seconds(timing.get("time"))
                if lap_seconds is None:
                    continue
                bucket = lap_times_by_driver.setdefault(driver_name, {})
                bucket[lap_no] = lap_seconds

    return lap_times_by_driver, max_lap


def load_pit_laps_by_driver(
    season: int,
    round_no: int,
    driver_id_to_name: dict[str, str],
) -> dict[str, list[int]]:
    pit_laps_by_driver: dict[str, set[int]] = {}
    try:
        first_page = fetch_positions_fast(f"{season}/{round_no}/pitstops.json", limit=200, offset=0)
    except Exception:
        return {}

    first_mr = first_page.get("MRData", {})
    try:
        total_rows = int(first_mr.get("total", 0))
        limit_rows = max(1, int(first_mr.get("limit", 200)))
    except (TypeError, ValueError):
        total_rows = 0
        limit_rows = 200

    all_pages = [first_page]
    for offset in range(limit_rows, total_rows, limit_rows):
        try:
            all_pages.append(fetch_positions_fast(f"{season}/{round_no}/pitstops.json", limit=limit_rows, offset=offset))
        except Exception:
            break

    for page in all_pages:
        page_races = page.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        page_payload = page_races[0] if page_races else {}
        for stop in page_payload.get("PitStops", []):
            driver_name = driver_id_to_name.get(stop.get("driverId", ""))
            if not driver_name:
                continue
            try:
                lap_no = int(stop.get("lap", 0))
            except (TypeError, ValueError):
                continue
            if lap_no <= 0:
                continue
            bucket = pit_laps_by_driver.setdefault(driver_name, set())
            bucket.add(lap_no)

    return {driver: sorted(laps) for driver, laps in pit_laps_by_driver.items()}


def build_tyre_strategy(round_payload: dict, season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    key = (season, round_no, normalized_session, TYRE_STRATEGY_CACHE_VERSION)
    cached = TYRE_STRATEGY_CACHE.get(key)
    if cached is not None:
        return cached

    def _safe_position(value: object) -> int:
        try:
            return int(value or 999)
        except (TypeError, ValueError):
            return 999

    def _normalize_compound_label(value: object) -> str:
        compound = str(value or "").strip().upper()
        if compound in {"", "UNKNOWN", "NONE", "NULL", "N/A", "NA"}:
            return "UNKNOWN"
        return compound

    def _compute_stint_metrics(
        stint_index: int,
        compound: str,
        start_lap: int,
        end_lap: int,
        lap_times_by_lap: dict[int, float],
    ) -> dict:
        covered_laps = list(range(start_lap, end_lap + 1))
        measured_times = [
            lap_times_by_lap[lap_no]
            for lap_no in covered_laps
            if lap_no in lap_times_by_lap
        ]
        fastest = min(measured_times) if measured_times else None
        slowest = max(measured_times) if measured_times else None
        avg = float(np.mean(measured_times)) if measured_times else None
        consistency = float(np.std(measured_times)) if measured_times else None

        degradation = None
        if len(measured_times) >= 2:
            x = np.arange(len(measured_times), dtype=float)
            y = np.array(measured_times, dtype=float)
            x_mean = float(np.mean(x))
            y_mean = float(np.mean(y))
            denom = float(np.sum((x - x_mean) ** 2))
            if denom > 0:
                degradation = float(np.sum((x - x_mean) * (y - y_mean)) / denom)

        return {
            "stint_index": stint_index,
            "compound": compound,
            "start_lap": start_lap,
            "end_lap": end_lap,
            "laps_count": len(covered_laps),
            "laps_timed": len(measured_times),
            "fastest_lap_s": round(float(fastest), 3) if fastest is not None else None,
            "slowest_lap_s": round(float(slowest), 3) if slowest is not None else None,
            "avg_lap_s": round(float(avg), 3) if avg is not None else None,
            "consistency_s": round(float(consistency), 3) if consistency is not None else None,
            "degradation_s_per_lap": round(float(degradation), 4) if degradation is not None else None,
            "fastest_lap": format_lap_time(fastest),
            "slowest_lap": format_lap_time(slowest),
            "avg_lap": format_lap_time(avg),
        }

    def _empty_tyre_strategy_payload() -> dict:
        return {
            "season": season,
            "round": round_no,
            "race": round_payload.get("race"),
            "session": normalized_session,
            "total_laps": 0,
            "drivers": [],
            "source": "unavailable",
            "notes": {
                "compound": "",
                "metrics": "",
            },
        }

    rows_source = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")
    rows = sorted(rows_source or [], key=lambda item: _safe_position(item.get("position")))
    if not rows:
        raw_rows = load_official_results_rows(season, round_no, normalized_session)
        parsed_rows = []
        for item in raw_rows:
            driver_info = item.get("Driver", {})
            parsed_rows.append(
                {
                    "position": int(item.get("position", 999) or 999),
                    "driver": map_driver(driver_info) if driver_info else None,
                    "team": normalize_constructor_name(season, item.get("Constructor", {}).get("name")),
                    "status": item.get("status"),
                    "grid": int(item.get("grid", 0) or 0),
                }
            )
        rows = sorted([r for r in parsed_rows if r.get("driver")], key=lambda item: _safe_position(item.get("position")))
    if not rows:
        rows = load_openf1_session_driver_rows(season, round_payload, normalized_session)

    # Primary source: OpenF1 (real compound history from stints endpoint).
    try:
        openf1_session_name = "Sprint" if normalized_session == "sprint" else "Race"
        openf1_session = pick_openf1_session(season, round_payload, openf1_session_name)
        if openf1_session and openf1_session.get("session_key"):
            session_key = int(openf1_session["session_key"])
            openf1_drivers = fetch_openf1("drivers", session_key=session_key)
            openf1_stints = fetch_openf1("stints", session_key=session_key)
            openf1_laps = fetch_openf1("laps", session_key=session_key)

            driver_number_by_name: dict[str, int] = {}
            driver_number_by_code: dict[str, int] = {}
            team_by_number: dict[int, str] = {}

            for item in openf1_drivers:
                number = item.get("driver_number")
                if number is None:
                    continue
                try:
                    driver_number = int(number)
                except (TypeError, ValueError):
                    continue
                full_name = str(item.get("full_name", "")).strip()
                acronym = str(item.get("name_acronym", "")).strip().upper()
                if full_name:
                    driver_number_by_name[normalize_driver_label(full_name)] = driver_number
                if acronym:
                    driver_number_by_code[acronym] = driver_number
                team_name = str(item.get("team_name", "")).strip()
                if team_name:
                    team_by_number[driver_number] = team_name

            laps_by_driver_number: dict[int, dict[int, float]] = {}
            max_lap_seen = 0
            for lap in openf1_laps:
                number = lap.get("driver_number")
                lap_no = lap.get("lap_number")
                lap_duration = lap.get("lap_duration")
                try:
                    driver_number = int(number)
                    lap_number = int(lap_no)
                    lap_time_s = float(lap_duration)
                except (TypeError, ValueError):
                    continue
                if lap_number <= 0 or lap_time_s <= 0:
                    continue
                max_lap_seen = max(max_lap_seen, lap_number)
                bucket = laps_by_driver_number.setdefault(driver_number, {})
                bucket[lap_number] = lap_time_s

            stints_by_driver_number: dict[int, list[dict]] = {}
            for stint in openf1_stints:
                number = stint.get("driver_number")
                try:
                    driver_number = int(number)
                except (TypeError, ValueError):
                    continue
                stints_by_driver_number.setdefault(driver_number, []).append(stint)

            openf1_pit_laps_by_driver_number: dict[int, list[int]] = {}
            if normalized_session == "race":
                for stop in fetch_openf1("pit", session_key=session_key):
                    try:
                        driver_number = int(stop.get("driver_number"))
                        lap_number = int(stop.get("lap_number"))
                    except (TypeError, ValueError):
                        continue
                    if lap_number <= 0:
                        continue
                    bucket = openf1_pit_laps_by_driver_number.setdefault(driver_number, [])
                    bucket.append(lap_number)
                for driver_number, laps in openf1_pit_laps_by_driver_number.items():
                    openf1_pit_laps_by_driver_number[driver_number] = sorted(set(laps))

            raw_results = load_official_results_rows(season, round_no, normalized_session)
            driver_id_to_name: dict[str, str] = {}
            official_laps_by_driver: dict[str, int] = {}
            for row in raw_results:
                driver = row.get("Driver", {})
                driver_id = driver.get("driverId")
                driver_name = map_driver(driver) if driver else None
                if driver_id and driver_name:
                    driver_id_to_name[driver_id] = driver_name
                if driver_name:
                    try:
                        official_laps_by_driver[driver_name] = int(row.get("laps", 0) or 0)
                    except (TypeError, ValueError):
                        pass
            pit_laps_by_driver = load_pit_laps_by_driver(season, round_no, driver_id_to_name) if normalized_session == "race" else {}

            total_laps = expected_lap_count(season, round_no, raw_results) or max_lap_seen
            if total_laps <= 0:
                total_laps = 57

            drivers = []
            for result in rows:
                driver_name = result.get("driver")
                if not driver_name:
                    continue

                key_name = normalize_driver_label(driver_name)
                parts = str(driver_name).strip().split()
                drv_code = (parts[-1][:3] if parts else str(driver_name)[:3]).upper()
                driver_number = driver_number_by_name.get(key_name) or driver_number_by_code.get(drv_code)
                lap_times = laps_by_driver_number.get(driver_number, {}) if driver_number is not None else {}
                raw_stints_all = sorted(
                    stints_by_driver_number.get(driver_number, []),
                    key=lambda item: (
                        int(item.get("lap_start", 999) or 999),
                        int(item.get("stint_number", 999) or 999),
                    ),
                ) if driver_number is not None else []
                raw_stints = raw_stints_all

                # OpenF1 can emit duplicate micro-stints for the same lap_start.
                # Keep the most meaningful one (largest lap_end, then latest stint_number).
                by_start_lap: dict[int, dict] = {}
                for stint in raw_stints:
                    raw_start = stint.get("lap_start")
                    raw_end = stint.get("lap_end")
                    try:
                        start_lap_key = int(raw_start) if raw_start is not None else 0
                    except (TypeError, ValueError):
                        start_lap_key = 0
                    if start_lap_key <= 0:
                        continue
                    try:
                        end_lap_key = int(raw_end) if raw_end is not None else 0
                    except (TypeError, ValueError):
                        end_lap_key = 0
                    current = by_start_lap.get(start_lap_key)
                    if current is None:
                        by_start_lap[start_lap_key] = stint
                        continue
                    try:
                        curr_end = int(current.get("lap_end", 0) or 0)
                    except (TypeError, ValueError):
                        curr_end = 0
                    try:
                        curr_idx = int(current.get("stint_number", 0) or 0)
                    except (TypeError, ValueError):
                        curr_idx = 0
                    try:
                        next_idx = int(stint.get("stint_number", 0) or 0)
                    except (TypeError, ValueError):
                        next_idx = 0
                    if (end_lap_key, next_idx) >= (curr_end, curr_idx):
                        by_start_lap[start_lap_key] = stint
                raw_stints = sorted(
                    by_start_lap.values(),
                    key=lambda item: (
                        int(item.get("lap_start", 999) or 999),
                        int(item.get("stint_number", 999) or 999),
                    ),
                )

                is_dnf = not is_classified_finisher_status(result.get("status"))
                non_starter = is_non_starter_status(result.get("status"))
                max_lap_from_times = max(lap_times.keys(), default=0)
                max_lap_from_stints = 0
                for stint in raw_stints:
                    try:
                        lap_end = int(stint.get("lap_end", 0) or 0)
                    except (TypeError, ValueError):
                        lap_end = 0
                    max_lap_from_stints = max(max_lap_from_stints, lap_end)

                official_completed_laps = official_laps_by_driver.get(driver_name, 0)
                if is_dnf:
                    if official_completed_laps > 0:
                        driver_limit = min(official_completed_laps, total_laps)
                    else:
                        driver_limit = max(max_lap_from_times, max_lap_from_stints)
                        if driver_limit > 0:
                            driver_limit = min(driver_limit, total_laps)
                else:
                    driver_limit = total_laps if total_laps > 0 else max(max_lap_from_times, max_lap_from_stints)

                if non_starter:
                    drivers.append(
                        {
                            "driver": driver_name,
                            "team": result.get("team") or (team_by_number.get(driver_number) if driver_number is not None else None),
                            "position": _safe_position(result.get("position")),
                            "status": result.get("status"),
                            "completed_laps": 0,
                            "stints": [],
                        }
                    )
                    continue

                participated = not non_starter and (
                    official_completed_laps > 0 or max_lap_from_times > 0 or max_lap_from_stints > 0 or bool(raw_stints_all)
                )
                if not participated:
                    continue
                early_dnf = is_dnf and not non_starter and driver_limit <= 1

                stints = []
                if early_dnf:
                    start_compound = "UNKNOWN"
                    for stint in raw_stints_all:
                        compound = _normalize_compound_label(stint.get("compound"))
                        if compound != "UNKNOWN":
                            start_compound = compound
                            break
                    if start_compound == "UNKNOWN":
                        for stint in raw_stints:
                            compound = _normalize_compound_label(stint.get("compound"))
                            if compound != "UNKNOWN":
                                start_compound = compound
                                break
                    stints = [_compute_stint_metrics(1, start_compound, 1, 1, lap_times)]

                def _compound_for_start_lap(start_lap: int) -> str:
                    chosen = None
                    for item in raw_stints:
                        raw_start = item.get("lap_start")
                        try:
                            s = int(raw_start) if raw_start is not None else None
                        except (TypeError, ValueError):
                            s = None
                        if s is None:
                            continue
                        if s <= start_lap:
                            if chosen is None or s >= chosen[0]:
                                chosen = (s, _normalize_compound_label(item.get("compound")))
                    if chosen is not None:
                        return chosen[1]
                    if raw_stints:
                        return _normalize_compound_label(raw_stints[0].get("compound"))
                    return "UNKNOWN"

                def _initial_compound() -> str:
                    for item in raw_stints:
                        compound = _normalize_compound_label(item.get("compound"))
                        if compound != "UNKNOWN":
                            return compound
                    return "UNKNOWN"

                def _resolved_compound(raw_compound: str, start_lap: int, end_lap: int) -> str:
                    compound = _normalize_compound_label(raw_compound)
                    if compound != "UNKNOWN":
                        return compound
                    length_laps = max(1, end_lap - start_lap + 1)
                    return estimate_compound_by_stint_length(length_laps)

                def _compound_events_by_end() -> list[tuple[int, str, int]]:
                    events: list[tuple[int, str, int]] = []
                    for item in raw_stints:
                        raw_end = item.get("lap_end")
                        try:
                            end_lap = int(raw_end) if raw_end is not None else 0
                        except (TypeError, ValueError):
                            end_lap = 0
                        if end_lap <= 0:
                            continue
                        try:
                            stint_idx = int(item.get("stint_number", 0) or 0)
                        except (TypeError, ValueError):
                            stint_idx = 0
                        compound = _normalize_compound_label(item.get("compound"))
                        events.append((end_lap, compound, stint_idx))
                    events.sort(key=lambda it: (it[0], it[2]))
                    return events

                cursor = 1
                normalized_ranges: list[tuple[int, str, int, int]] = []
                if not early_dnf:
                    # Prefer official pit-stop boundaries for lap ranges; use OpenF1 for compound identity.
                    boundary_source = pit_laps_by_driver.get(driver_name) or (
                        openf1_pit_laps_by_driver_number.get(driver_number, []) if driver_number is not None else []
                    )
                    pit_boundaries = sorted(
                        lap for lap in (boundary_source or [])
                        if lap > 0 and (driver_limit <= 0 or lap < driver_limit)
                    )
                    if pit_boundaries and driver_limit > 0:
                        start_lap = 1
                        stint_idx = 1
                        segments: list[tuple[int, int, int]] = []
                        for pit_lap in pit_boundaries:
                            end_lap = min(pit_lap, driver_limit)
                            if end_lap >= start_lap:
                                segments.append((stint_idx, start_lap, end_lap))
                                stint_idx += 1
                            start_lap = pit_lap + 1
                            if start_lap > driver_limit:
                                break
                        if start_lap <= driver_limit:
                            segments.append((stint_idx, start_lap, driver_limit))

                        for stint_idx, seg_start, seg_end in segments:
                            compound = _resolved_compound(_compound_for_start_lap(seg_start), seg_start, seg_end)
                            normalized_ranges.append((stint_idx, compound, seg_start, seg_end))
                    else:
                        for stint in raw_stints:
                            try:
                                stint_index = int(stint.get("stint_number", len(stints) + 1))
                            except (TypeError, ValueError):
                                stint_index = len(stints) + 1
                            raw_start = stint.get("lap_start")
                            raw_end = stint.get("lap_end")
                            start_lap: int | None
                            end_lap: int | None
                            try:
                                start_lap = int(raw_start) if raw_start is not None else None
                            except (TypeError, ValueError):
                                start_lap = None
                            try:
                                end_lap = int(raw_end) if raw_end is not None else None
                            except (TypeError, ValueError):
                                end_lap = None

                            if start_lap is None and end_lap is None:
                                if is_dnf and driver_limit <= 0:
                                    continue
                                start_lap = cursor
                                end_lap = driver_limit if driver_limit > 0 else total_laps
                            elif start_lap is None:
                                start_lap = cursor
                            elif end_lap is None:
                                end_lap = driver_limit if driver_limit > 0 else start_lap

                            if start_lap <= 0 and end_lap <= 0:
                                continue
                            if start_lap <= 0:
                                start_lap = cursor
                            if end_lap <= 0:
                                end_lap = start_lap
                            if driver_limit > 0 and start_lap > driver_limit:
                                break
                            if start_lap < cursor:
                                start_lap = cursor
                            if driver_limit > 0:
                                end_lap = min(end_lap, driver_limit)
                            if end_lap < start_lap:
                                continue

                            if start_lap > cursor:
                                gap_end = start_lap - 1
                                normalized_ranges.append(
                                    (
                                        len(normalized_ranges) + 1,
                                        _resolved_compound("UNKNOWN", cursor, gap_end),
                                        cursor,
                                        gap_end,
                                    )
                                )
                            compound = _resolved_compound(stint.get("compound"), start_lap, end_lap)
                            normalized_ranges.append((stint_index, compound, start_lap, end_lap))
                            cursor = end_lap + 1

                        if driver_limit > 0 and cursor <= driver_limit:
                            if normalized_ranges:
                                last_idx, last_cmp, last_start, _ = normalized_ranges[-1]
                                normalized_ranges[-1] = (last_idx, last_cmp, last_start, driver_limit)
                            else:
                                normalized_ranges.append(
                                    (
                                        len(normalized_ranges) + 1,
                                        _resolved_compound("UNKNOWN", cursor, driver_limit),
                                        cursor,
                                        driver_limit,
                                    )
                                )

                for idx, compound, start_lap, end_lap in normalized_ranges:
                    stints.append(_compute_stint_metrics(idx, compound, start_lap, end_lap, lap_times))

                if not stints and driver_limit > 0:
                    stints = [
                        _compute_stint_metrics(
                            stint_index=1,
                            compound="UNKNOWN",
                            start_lap=1,
                            end_lap=driver_limit,
                            lap_times_by_lap=lap_times,
                        )
                    ]

                if is_dnf and len(stints) >= 2:
                    last = stints[-1]
                    prev = stints[-2]
                    if (
                        int(last.get("laps_count", 0) or 0) <= 1
                        and int(last.get("start_lap", 0) or 0) > 1
                        and int(prev.get("end_lap", 0) or 0) == int(last.get("start_lap", 0) or 0) - 1
                    ):
                        stints = stints[:-1]

                drivers.append(
                    {
                        "driver": driver_name,
                        "team": result.get("team") or (team_by_number.get(driver_number) if driver_number is not None else None),
                        "position": _safe_position(result.get("position")),
                        "status": result.get("status"),
                        "completed_laps": driver_limit if is_dnf else total_laps,
                        "stints": stints,
                    }
                )

            meaningful_compound_count = sum(
                1
                for driver in drivers
                for stint in driver.get("stints", [])
                if str(stint.get("compound", "")).strip().upper() not in {"", "UNKNOWN"}
            )
            if meaningful_compound_count == 0:
                payload = _empty_tyre_strategy_payload()
                TYRE_STRATEGY_CACHE[key] = payload
                return payload

            payload = {
                "season": season,
                "round": round_no,
                "race": round_payload.get("race"),
                "session": normalized_session,
                "total_laps": total_laps,
                "drivers": drivers,
                "source": "openf1",
                "notes": {
                    "compound": "Tyre compounds come from OpenF1 stint history.",
                    "metrics": "Fastest/slowest/average/consistency/degradation are computed from OpenF1 lap timings within each stint.",
                },
            }
            TYRE_STRATEGY_CACHE[key] = payload
            return payload
    except Exception:
        # Fallback below preserves availability when OpenF1 is unavailable.
        pass

    payload = _empty_tyre_strategy_payload()
    TYRE_STRATEGY_CACHE[key] = payload
    return payload


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return float(np.mean(values))


def _iqr(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    arr = np.array(values, dtype=float)
    q1 = float(np.percentile(arr, 25))
    q3 = float(np.percentile(arr, 75))
    return q3 - q1


def compute_pace_stats(lap_times: list[float]) -> dict:
    clean = [float(v) for v in lap_times if np.isfinite(v) and v > 0]
    if not clean:
        return {
            "laps_count": 0,
            "median_pace_s": None,
            "fastest_lap_s": None,
            "slowest_lap_s": None,
            "consistency_iqr_s": None,
            "pace_spread_s": None,
            "median_pace": None,
            "fastest_lap": None,
            "slowest_lap": None,
        }
    median = float(np.median(clean))
    fastest = min(clean)
    slowest = max(clean)
    iqr = _iqr(clean)
    spread = slowest - fastest
    return {
        "laps_count": len(clean),
        "median_pace_s": round(median, 3),
        "fastest_lap_s": round(fastest, 3),
        "slowest_lap_s": round(slowest, 3),
        "consistency_iqr_s": round(float(iqr), 3) if iqr is not None else None,
        "pace_spread_s": round(float(spread), 3),
        "median_pace": format_lap_time(median),
        "fastest_lap": format_lap_time(fastest),
        "slowest_lap": format_lap_time(slowest),
    }


def _driver_code_from_name(driver_name: str) -> str:
    parts = str(driver_name or "").strip().split()
    if not parts:
        return ""
    return parts[-1][:3].upper()


def _resolve_h2h_driver_name(requested: str | None, available: list[str]) -> str | None:
    if not available:
        return None
    if not requested:
        return available[0]
    req = str(requested).strip()
    if not req:
        return available[0]
    if req in available:
        return req
    req_norm = normalize_driver_label(req)
    for name in available:
        if normalize_driver_label(name) == req_norm:
            return name
    return None


def _summarize_sector_samples(samples: list[dict]) -> dict:
    speeds: list[float] = []
    throttles: list[float] = []
    brakes: list[float] = []
    for item in samples:
        try:
            speed = float(item.get("speed"))
        except (TypeError, ValueError):
            speed = None
        try:
            throttle = float(item.get("throttle"))
        except (TypeError, ValueError):
            throttle = None
        try:
            brake = float(item.get("brake"))
        except (TypeError, ValueError):
            brake = None
        if speed is not None and np.isfinite(speed):
            speeds.append(speed)
        if throttle is not None and np.isfinite(throttle):
            throttles.append(throttle)
        if brake is not None and np.isfinite(brake):
            brakes.append(brake)
    return {
        "avg_speed_kph": round(float(_mean_or_none(speeds)), 2) if speeds else None,
        "avg_throttle_pct": round(float(_mean_or_none(throttles)), 2) if throttles else None,
        "avg_brake_pct": round(float(_mean_or_none(brakes)), 2) if brakes else None,
    }


def _load_openf1_driver_h2h_laps(session_key: int, driver_number: int) -> list[dict]:
    raw_laps = [
        row
        for row in fetch_openf1("laps", session_key=session_key, timeout_s=6, max_attempts=2)
        if int(row.get("driver_number", 0) or 0) == int(driver_number)
    ]
    raw_car = fetch_openf1("car_data", session_key=session_key, driver_number=driver_number, timeout_s=8, max_attempts=1)

    car_samples: list[tuple[datetime, dict]] = []
    for row in raw_car:
        ts = parse_iso8601(row.get("date"))
        if ts is None:
            continue
        car_samples.append((ts, row))
    car_samples.sort(key=lambda item: item[0])

    laps: list[dict] = []
    for row in sorted(raw_laps, key=lambda item: int(item.get("lap_number", 0) or 0)):
        try:
            lap_no = int(row.get("lap_number", 0) or 0)
            lap_time_s = float(row.get("lap_duration"))
        except (TypeError, ValueError):
            continue
        if lap_no <= 0 or not np.isfinite(lap_time_s) or lap_time_s <= 0:
            continue

        try:
            s1 = float(row.get("duration_sector_1"))
        except (TypeError, ValueError):
            s1 = None
        try:
            s2 = float(row.get("duration_sector_2"))
        except (TypeError, ValueError):
            s2 = None
        try:
            s3 = float(row.get("duration_sector_3"))
        except (TypeError, ValueError):
            s3 = None

        sector_durations = [s1, s2, s3]
        has_valid_durations = all(val is not None and np.isfinite(val) and val > 0 for val in sector_durations)
        lap_start = parse_iso8601(row.get("date_start"))

        sectors = []
        if has_valid_durations and lap_start is not None:
            sector_start = lap_start
            for idx, duration in enumerate(sector_durations, start=1):
                sector_end = sector_start + timedelta(seconds=float(duration))
                samples = [
                    sample
                    for ts, sample in car_samples
                    if ts >= sector_start and ts < sector_end
                ]
                stats = _summarize_sector_samples(samples)
                sectors.append(
                    {
                        "sector": idx,
                        "time_s": round(float(duration), 3),
                        "time": format_lap_time(float(duration)),
                        **stats,
                    }
                )
                sector_start = sector_end
        else:
            for idx, duration in enumerate(sector_durations, start=1):
                valid = duration is not None and np.isfinite(duration) and duration > 0
                sectors.append(
                    {
                        "sector": idx,
                        "time_s": round(float(duration), 3) if valid else None,
                        "time": format_lap_time(float(duration)) if valid else None,
                        "avg_speed_kph": None,
                        "avg_throttle_pct": None,
                        "avg_brake_pct": None,
                    }
                )

        laps.append(
            {
                "lap": lap_no,
                "lap_time_s": round(lap_time_s, 3),
                "lap_time": format_lap_time(lap_time_s),
                "sectors": sectors,
            }
        )

    return laps


def _fallback_h2h_laps_from_jolpica(
    season: int,
    round_no: int,
    driver_a: str,
    driver_b: str,
    session_kind: str = "race",
) -> tuple[list[dict], list[dict]]:
    normalized_session = str(session_kind or "race").strip().lower()
    raw_results = load_official_results_rows(season, round_no, normalized_session)
    driver_id_to_name: dict[str, str] = {}
    for row in raw_results:
        driver = row.get("Driver", {})
        driver_id = driver.get("driverId")
        driver_name = map_driver(driver) if driver else None
        if driver_id and driver_name:
            driver_id_to_name[driver_id] = driver_name
    try:
        # Jolpica lap endpoint is race-oriented; sprint fallback may be sparse.
        lap_times_by_driver, _ = load_lap_times_by_driver(season, round_no, driver_id_to_name)
    except Exception:
        lap_times_by_driver = {}

    def _to_rows(driver_name: str) -> list[dict]:
        rows = []
        for lap_no, lap_time_s in sorted((lap_times_by_driver.get(driver_name) or {}).items()):
            rows.append(
                {
                    "lap": int(lap_no),
                    "lap_time_s": round(float(lap_time_s), 3),
                    "lap_time": format_lap_time(float(lap_time_s)),
                    "sectors": [
                        {"sector": 1, "time_s": None, "time": None, "avg_speed_kph": None, "avg_throttle_pct": None, "avg_brake_pct": None},
                        {"sector": 2, "time_s": None, "time": None, "avg_speed_kph": None, "avg_throttle_pct": None, "avg_brake_pct": None},
                        {"sector": 3, "time_s": None, "time": None, "avg_speed_kph": None, "avg_throttle_pct": None, "avg_brake_pct": None},
                    ],
                }
            )
        return rows

    return _to_rows(driver_a), _to_rows(driver_b)


def build_h2h_payload(
    season: int,
    round_no: int,
    driver_a: str | None,
    driver_b: str | None,
    session_kind: str = "race",
) -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    round_payload = get_round_payload(season, round_no)
    rows_source = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")
    rows = sorted(rows_source or [], key=lambda row: int(row.get("position", 999) or 999))
    if not rows:
        raw_rows = load_official_results_rows(season, round_no, normalized_session)
        parsed_rows = []
        for item in raw_rows:
            driver_info = item.get("Driver", {})
            parsed_rows.append(
                {
                    "position": int(item.get("position", 999) or 999),
                    "driver": map_driver(driver_info) if driver_info else None,
                    "team": normalize_constructor_name(season, item.get("Constructor", {}).get("name")),
                }
            )
        rows = sorted([r for r in parsed_rows if r.get("driver")], key=lambda row: int(row.get("position", 999) or 999))
    if not rows:
        rows = load_openf1_session_driver_rows(season, round_payload, normalized_session)

    available_drivers = [item.get("driver") for item in rows]
    available_drivers = [name for name in available_drivers if isinstance(name, str) and name.strip()]
    if len(available_drivers) < 2:
        raise HTTPException(status_code=404, detail="Not enough drivers available for H2H comparison.")

    resolved_a = _resolve_h2h_driver_name(driver_a, available_drivers)
    fallback_b = available_drivers[1] if len(available_drivers) > 1 else available_drivers[0]
    resolved_b = _resolve_h2h_driver_name(driver_b, available_drivers) or fallback_b

    if not resolved_a or not resolved_b:
        raise HTTPException(status_code=404, detail="Driver selection not found in this round.")
    if resolved_a == resolved_b:
        raise HTTPException(status_code=400, detail="Choose two different drivers for H2H.")

    cache_key = (season, round_no, normalized_session, resolved_a, resolved_b)
    cached = H2H_CACHE.get(cache_key)
    if cached is not None:
        return cached

    results_by_driver = {item.get("driver"): item for item in rows}
    team_a = (results_by_driver.get(resolved_a) or {}).get("team")
    team_b = (results_by_driver.get(resolved_b) or {}).get("team")

    laps_a: list[dict] = []
    laps_b: list[dict] = []
    source = "openf1"
    notes = []

    try:
        openf1_session_name = "Sprint" if normalized_session == "sprint" else "Race"
        openf1_session = pick_openf1_session(season, round_payload, openf1_session_name)
        if not openf1_session or not openf1_session.get("session_key"):
            raise RuntimeError("OpenF1 session key unavailable")
        session_key = int(openf1_session["session_key"])
        openf1_drivers = fetch_openf1("drivers", session_key=session_key)

        number_by_name: dict[str, int] = {}
        number_by_code: dict[str, int] = {}
        for item in openf1_drivers:
            raw_number = item.get("driver_number")
            try:
                number = int(raw_number)
            except (TypeError, ValueError):
                continue
            full_name = str(item.get("full_name", "")).strip()
            acronym = str(item.get("name_acronym", "")).strip().upper()
            if full_name:
                number_by_name[normalize_driver_label(full_name)] = number
            if acronym:
                number_by_code[acronym] = number

        def _resolve_driver_number(driver_name: str) -> int | None:
            normalized = normalize_driver_label(driver_name)
            if normalized in number_by_name:
                return number_by_name[normalized]
            code = _driver_code_from_name(driver_name)
            return number_by_code.get(code)

        number_a = _resolve_driver_number(resolved_a)
        number_b = _resolve_driver_number(resolved_b)
        if number_a is None or number_b is None:
            raise RuntimeError("Driver numbers unavailable in OpenF1")

        laps_a = _load_openf1_driver_h2h_laps(session_key, number_a)
        laps_b = _load_openf1_driver_h2h_laps(session_key, number_b)
        if not laps_a or not laps_b:
            raise RuntimeError("OpenF1 laps unavailable")
    except Exception:
        source = "jolpica"
        notes.append("OpenF1 sector data unavailable. Sector dominance speed/throttle/brake metrics are disabled for this request.")
        laps_a, laps_b = _fallback_h2h_laps_from_jolpica(season, round_no, resolved_a, resolved_b, normalized_session)

    lap_map_a = {int(item["lap"]): item for item in laps_a if item.get("lap") is not None}
    lap_map_b = {int(item["lap"]): item for item in laps_b if item.get("lap") is not None}
    common_laps = sorted(set(lap_map_a.keys()) & set(lap_map_b.keys()))
    lap_rows = []
    for lap_no in common_laps:
        time_a = lap_map_a[lap_no].get("lap_time_s")
        time_b = lap_map_b[lap_no].get("lap_time_s")
        delta = None
        if isinstance(time_a, (int, float)) and isinstance(time_b, (int, float)):
            delta = float(time_a) - float(time_b)
        lap_rows.append(
            {
                "lap": lap_no,
                "driver_a_lap_s": round(float(time_a), 3) if isinstance(time_a, (int, float)) else None,
                "driver_b_lap_s": round(float(time_b), 3) if isinstance(time_b, (int, float)) else None,
                "driver_a_lap": format_lap_time(float(time_a)) if isinstance(time_a, (int, float)) else None,
                "driver_b_lap": format_lap_time(float(time_b)) if isinstance(time_b, (int, float)) else None,
                "delta_s": round(delta, 3) if delta is not None else None,
            }
        )

    lap_times_a = [float(item["lap_time_s"]) for item in laps_a if isinstance(item.get("lap_time_s"), (int, float))]
    lap_times_b = [float(item["lap_time_s"]) for item in laps_b if isinstance(item.get("lap_time_s"), (int, float))]

    def _fastest_slowest(laps: list[dict]) -> dict:
        timed = [item for item in laps if isinstance(item.get("lap_time_s"), (int, float))]
        if not timed:
            return {"fastest": None, "slowest": None}
        fastest = min(timed, key=lambda item: float(item["lap_time_s"]))
        slowest = max(timed, key=lambda item: float(item["lap_time_s"]))
        return {
            "fastest": {
                "lap": int(fastest["lap"]),
                "lap_time_s": round(float(fastest["lap_time_s"]), 3),
                "lap_time": format_lap_time(float(fastest["lap_time_s"])),
            },
            "slowest": {
                "lap": int(slowest["lap"]),
                "lap_time_s": round(float(slowest["lap_time_s"]), 3),
                "lap_time": format_lap_time(float(slowest["lap_time_s"])),
            },
        }

    extremes_a = _fastest_slowest(laps_a)
    extremes_b = _fastest_slowest(laps_b)

    payload = {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "session": normalized_session,
        "source": source,
        "notes": notes,
        "drivers": {
            "driver_a": {"name": resolved_a, "team": team_a},
            "driver_b": {"name": resolved_b, "team": team_b},
        },
        "available_drivers": [
            {"name": name, "team": (results_by_driver.get(name) or {}).get("team")}
            for name in available_drivers
        ],
        "lap_times": {
            "common_laps": lap_rows,
            "driver_a_laps": laps_a,
            "driver_b_laps": laps_b,
            "pace": {
                "driver_a": compute_pace_stats(lap_times_a),
                "driver_b": compute_pace_stats(lap_times_b),
            },
        },
        "track_dominance": {
            "driver_a": {
                "fastest_lap": extremes_a["fastest"],
                "slowest_lap": extremes_a["slowest"],
                "laps": laps_a,
            },
            "driver_b": {
                "fastest_lap": extremes_b["fastest"],
                "slowest_lap": extremes_b["slowest"],
                "laps": laps_b,
            },
            "sectors_available": source == "openf1",
        },
    }
    H2H_CACHE[cache_key] = payload
    return payload


def build_round_telemetry_catalog(season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    cache_key = (season, round_no, normalized_session)
    cached = TELEMETRY_CATALOG_CACHE.get(cache_key)
    if cached is not None:
        return cached

    round_payload = get_round_payload(season, round_no)
    rows_source = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")
    rows = sorted(rows_source or [], key=lambda row: int(row.get("position", 999) or 999))

    if not rows:
        raw_rows = load_official_results_rows(season, round_no, normalized_session)
        for item in raw_rows:
            driver_info = item.get("Driver", {})
            driver_name = map_driver(driver_info) if driver_info else None
            if not driver_name:
                continue
            rows.append(
                {
                    "position": int(item.get("position", 999) or 999),
                    "driver": driver_name,
                    "team": normalize_constructor_name(season, item.get("Constructor", {}).get("name")),
                }
            )
        rows.sort(key=lambda row: int(row.get("position", 999) or 999))
    if not rows:
        rows = load_openf1_session_driver_rows(season, round_payload, normalized_session)

    raw_results = load_official_results_rows(season, round_no, normalized_session)
    lap_times_by_driver: dict[str, dict[int, float]] = {}
    max_lap_seen = 0
    source = "unavailable"
    row_driver_count = sum(1 for row in rows if row.get("driver"))
    if normalized_session == "race":
        driver_id_to_name: dict[str, str] = {}
        for row in raw_results:
            driver = row.get("Driver", {})
            driver_id = driver.get("driverId")
            driver_name = map_driver(driver) if driver else None
            if driver_id and driver_name:
                driver_id_to_name[driver_id] = driver_name
        if driver_id_to_name:
            try:
                lap_times_by_driver, max_lap_seen = load_lap_times_by_driver(season, round_no, driver_id_to_name)
                if lap_times_by_driver:
                    source = "jolpica"
            except Exception:
                lap_times_by_driver, max_lap_seen = {}, 0

    current_match_count = _count_lap_time_matches(rows, lap_times_by_driver) if lap_times_by_driver else 0
    should_try_openf1 = not lap_times_by_driver
    if normalized_session == "race" and row_driver_count and current_match_count < row_driver_count:
        should_try_openf1 = True

    if should_try_openf1:
        try:
            openf1_lap_times_by_driver, openf1_max_lap_seen = load_openf1_lap_times_by_driver(
                season,
                round_payload,
                normalized_session,
            )
            openf1_match_count = _count_lap_time_matches(rows, openf1_lap_times_by_driver) if openf1_lap_times_by_driver else 0
            if openf1_lap_times_by_driver and (not lap_times_by_driver or openf1_match_count > current_match_count):
                lap_times_by_driver = openf1_lap_times_by_driver
                max_lap_seen = openf1_max_lap_seen
                source = "openf1"
        except Exception:
            if not lap_times_by_driver:
                lap_times_by_driver, max_lap_seen = {}, 0

    expected_total_laps = expected_lap_count(season, round_no, raw_results)
    fallback_laps = 24 if normalized_session == "sprint" else 57
    total_laps = max(1, expected_total_laps or max_lap_seen or fallback_laps)
    if not lap_times_by_driver:
        if rows:
            payload = {
                "season": season,
                "round": round_no,
                "race": round_payload.get("race"),
                "session": normalized_session,
                "source": "unavailable",
                "total_laps": total_laps,
                "drivers": [
                    {
                        "driver": row.get("driver"),
                        "team": row.get("team"),
                        "position": int(row.get("position", idx + 1) or (idx + 1)),
                        "laps": [],
                        "fastest_lap": None,
                    }
                    for idx, row in enumerate(rows)
                    if row.get("driver")
                ],
            }
            TELEMETRY_CATALOG_CACHE[cache_key] = payload
            return payload
        payload = {
            "season": season,
            "round": round_no,
            "race": round_payload.get("race"),
            "session": normalized_session,
            "source": "unavailable",
            "total_laps": 0,
            "drivers": [],
        }
        TELEMETRY_CATALOG_CACHE[cache_key] = payload
        return payload

    normalized_lap_times_by_driver = _build_normalized_lap_times_lookup(lap_times_by_driver)
    drivers: list[dict] = []
    for row in rows:
        driver_name = row.get("driver")
        if not driver_name:
            continue
        lap_times = lap_times_by_driver.get(driver_name) or normalized_lap_times_by_driver.get(
            normalize_driver_label(driver_name),
            {},
        )
        available_laps = sorted(int(lap_no) for lap_no in lap_times.keys() if int(lap_no) > 0)

        fastest_lap = None
        if lap_times:
            fastest_lap_no, fastest_time_s = min(lap_times.items(), key=lambda item: float(item[1]))
            fastest_lap = {
                "lap": int(fastest_lap_no),
                "lap_time_s": round(float(fastest_time_s), 3),
                "lap_time": format_lap_time(float(fastest_time_s)),
            }
        drivers.append(
            {
                "driver": driver_name,
                "team": row.get("team"),
                "position": int(row.get("position", len(drivers) + 1) or (len(drivers) + 1)),
                "laps": available_laps,
                "fastest_lap": fastest_lap,
            }
        )

    payload = {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "session": normalized_session,
        "source": source,
        "total_laps": total_laps,
        "drivers": drivers,
    }
    TELEMETRY_CATALOG_CACHE[cache_key] = payload
    return payload


def _build_normalized_lap_times_lookup(lap_times_by_driver: dict[str, dict[int, float]]) -> dict[str, dict[int, float]]:
    normalized_lookup: dict[str, dict[int, float]] = {}
    for driver_name, laps in (lap_times_by_driver or {}).items():
        normalized_name = normalize_driver_label(driver_name)
        if not normalized_name:
            continue
        bucket = normalized_lookup.setdefault(normalized_name, {})
        for lap_no, lap_time_s in (laps or {}).items():
            try:
                lap_no_int = int(lap_no)
                lap_time_value = float(lap_time_s)
            except (TypeError, ValueError):
                continue
            if lap_no_int <= 0 or lap_time_value <= 0:
                continue
            existing = bucket.get(lap_no_int)
            if existing is None or lap_time_value < existing:
                bucket[lap_no_int] = lap_time_value
    return normalized_lookup


def _count_lap_time_matches(rows: list[dict], lap_times_by_driver: dict[str, dict[int, float]]) -> int:
    if not rows or not lap_times_by_driver:
        return 0
    normalized_lookup = _build_normalized_lap_times_lookup(lap_times_by_driver)
    matches = 0
    for row in rows:
        driver_name = row.get("driver")
        if not driver_name:
            continue
        lap_times = lap_times_by_driver.get(driver_name) or normalized_lookup.get(normalize_driver_label(driver_name))
        if lap_times:
            matches += 1
    return matches


def _build_synthetic_telemetry_trace(
    season: int,
    round_no: int,
    driver_name: str,
    driver_position: int,
    lap_no: int,
    total_laps: int,
) -> list[dict]:
    seed = (
        (season * 1000003)
        + (round_no * 10007)
        + (lap_no * 101)
        + sum((idx + 1) * ord(ch) for idx, ch in enumerate(driver_name))
    )
    rng = np.random.default_rng(seed)
    samples_count = 180
    pace_factor = max(0.78, 1.12 - (max(1, driver_position) * 0.011))
    degradation = 1.0 + ((lap_no / max(total_laps, 1)) * 0.02)

    def _is_drs_zone(progress_value: float) -> bool:
        return (
            (0.10 < progress_value < 0.18)
            or (0.44 < progress_value < 0.54)
            or (0.79 < progress_value < 0.88)
        )

    rows: list[dict] = []
    for sample_idx in range(samples_count):
        progress = sample_idx / max(samples_count - 1, 1)
        corner_phase = abs(np.sin((progress * np.pi * 2 * 6.0) + (driver_position * 0.13)))
        hairpin_phase = abs(np.sin((progress * np.pi * 2 * 2.35) + (lap_no * 0.07)))
        corner_intensity = min(1.0, (0.72 * corner_phase) + (0.42 * hairpin_phase))
        straight_factor = max(0.0, 1.0 - corner_intensity)

        brake = (corner_intensity * 86.0) + rng.normal(0, 4.5)
        brake = max(0.0, min(100.0, brake * (1.02 / pace_factor)))

        throttle = (34.0 + (straight_factor * 74.0) - (corner_intensity * 16.0)) + rng.normal(0, 3.2)
        throttle = max(0.0, min(100.0, throttle))

        speed = (
            90.0
            + (straight_factor * 210.0)
            + (throttle * 0.72)
            - (brake * 0.93)
            + (pace_factor * 12.0)
            - ((degradation - 1.0) * 95.0)
        )
        speed += rng.normal(0, 3.0)

        drs_zone = _is_drs_zone(progress)
        drs = int(drs_zone and throttle > 82.0 and brake < 12.0 and speed > 235.0)
        if drs:
            speed += 16.0 + rng.normal(0, 2.0)

        speed = max(70.0, min(370.0, speed))
        if speed < 95:
            gear = 1
        elif speed < 125:
            gear = 2
        elif speed < 155:
            gear = 3
        elif speed < 185:
            gear = 4
        elif speed < 215:
            gear = 5
        elif speed < 248:
            gear = 6
        elif speed < 286:
            gear = 7
        else:
            gear = 8
        rpm = 6100.0 + (gear * 930.0) + (throttle * 39.0) + (drs * 240.0) + rng.normal(0, 140.0)
        rpm = int(round(max(6000.0, min(15200.0, rpm))))

        rows.append(
            {
                "sample": sample_idx + 1,
                "distance_pct": round(progress * 100.0, 2),
                "speed": round(float(speed), 2),
                "gear": gear,
                "throttle": round(float(throttle), 2),
                "brake": round(float(brake), 2),
                "rpm": rpm,
                "drs": drs,
            }
        )

    return rows


def _to_float_or_none(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(parsed):
        return None
    return parsed


def _normalize_drs_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"on", "true", "1", "open", "enabled", "active"}:
            return 1
        if text in {"off", "false", "0", "closed", "disabled", "inactive"}:
            return 0
        parsed = _to_float_or_none(text)
        if parsed is None:
            return 0
        return 1 if parsed > 0 else 0
    parsed = _to_float_or_none(value)
    if parsed is None:
        return 0
    # OpenF1 can expose multiple non-zero DRS states; treat any non-zero as active.
    return 1 if parsed > 0 else 0


def _downsample_rows(rows: list[dict], max_points: int = 220) -> list[dict]:
    if len(rows) <= max_points:
        return rows
    keep_indices = np.linspace(0, len(rows) - 1, num=max_points, dtype=int)
    return [rows[int(i)] for i in keep_indices]


def _build_openf1_telemetry_trace(
    season: int,
    round_payload: dict,
    session_kind: str,
    driver_name: str,
    requested_lap: str,
) -> dict | None:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    openf1_session_name = "Sprint" if normalized_session == "sprint" else "Race"
    openf1_session = pick_openf1_session(season, round_payload, openf1_session_name)
    if not openf1_session or not openf1_session.get("session_key"):
        return None

    session_key = int(openf1_session["session_key"])
    openf1_drivers = fetch_openf1("drivers", session_key=session_key)
    number_by_name: dict[str, int] = {}
    number_by_code: dict[str, int] = {}
    team_by_number: dict[int, str] = {}
    for item in openf1_drivers:
        raw_number = item.get("driver_number")
        try:
            number = int(raw_number)
        except (TypeError, ValueError):
            continue
        full_name = str(item.get("full_name", "")).strip()
        acronym = str(item.get("name_acronym", "")).strip().upper()
        if full_name:
            number_by_name[normalize_driver_label(full_name)] = number
        if acronym:
            number_by_code[acronym] = number
        team_name = str(item.get("team_name", "")).strip()
        if team_name:
            team_by_number[number] = team_name

    normalized_driver = normalize_driver_label(driver_name)
    driver_number = number_by_name.get(normalized_driver)
    if driver_number is None:
        code = _driver_code_from_name(driver_name)
        driver_number = number_by_code.get(code)
    if driver_number is None:
        return None

    raw_laps = fetch_openf1(
        "laps",
        session_key=session_key,
        driver_number=driver_number,
        timeout_s=6,
        max_attempts=2,
    )
    lap_rows: list[dict] = []
    for row in raw_laps:
        try:
            lap_no = int(row.get("lap_number", 0) or 0)
        except (TypeError, ValueError):
            continue
        if lap_no <= 0:
            continue
        lap_rows.append(
            {
                "lap_number": lap_no,
                "lap_duration": _to_float_or_none(row.get("lap_duration")),
                "date_start": parse_iso8601(row.get("date_start")),
            }
        )
    if not lap_rows:
        return None
    lap_rows.sort(key=lambda item: item["lap_number"])
    available_laps = sorted({int(item["lap_number"]) for item in lap_rows})
    if not available_laps:
        return None

    timed_laps = [item for item in lap_rows if isinstance(item.get("lap_duration"), (int, float)) and item["lap_duration"] > 0]
    fastest_lap = None
    if timed_laps:
        fastest = min(timed_laps, key=lambda item: float(item["lap_duration"]))
        fastest_lap = {
            "lap": int(fastest["lap_number"]),
            "lap_time_s": round(float(fastest["lap_duration"]), 3),
            "lap_time": format_lap_time(float(fastest["lap_duration"])),
        }

    requested_lap_norm = str(requested_lap or "").strip().lower()
    if requested_lap_norm in {"fastest", "fast"}:
        selected_lap = int(fastest_lap["lap"]) if fastest_lap else available_laps[0]
    else:
        try:
            selected_lap = int(requested_lap)
        except (TypeError, ValueError):
            return None
        if selected_lap not in available_laps:
            return None

    selected_row = next((item for item in lap_rows if int(item["lap_number"]) == selected_lap), None)
    if selected_row is None:
        return None

    lap_start = selected_row.get("date_start")
    lap_end = None
    lap_duration = selected_row.get("lap_duration")
    if lap_start is not None and isinstance(lap_duration, (int, float)) and lap_duration > 0:
        lap_end = lap_start + timedelta(seconds=float(lap_duration))
    elif lap_start is not None:
        later_rows = [
            item for item in lap_rows
            if int(item["lap_number"]) > selected_lap and item.get("date_start") is not None
        ]
        if later_rows:
            lap_end = min(item["date_start"] for item in later_rows)
        else:
            lap_end = lap_start + timedelta(seconds=120)

    car_query: dict[str, str | int] = {
        "session_key": session_key,
        "driver_number": driver_number,
    }
    if lap_start is not None:
        car_query["date>"] = lap_start.isoformat()
    if lap_end is not None:
        car_query["date<"] = lap_end.isoformat()

    raw_car = fetch_openf1("car_data", timeout_s=6, max_attempts=2, **car_query)
    car_samples: list[tuple[datetime, dict]] = []
    for row in raw_car:
        ts = parse_iso8601(row.get("date"))
        if ts is None:
            continue
        car_samples.append((ts, row))
    car_samples.sort(key=lambda item: item[0])
    if not car_samples:
        return None

    if lap_start is not None and lap_end is not None:
        scoped = [row for ts, row in car_samples if ts >= lap_start and ts < lap_end]
    elif lap_start is not None:
        scoped = [row for ts, row in car_samples if ts >= lap_start]
    else:
        scoped = [row for _, row in car_samples]
    if not scoped:
        return None

    scoped = _downsample_rows(scoped, max_points=220)

    prev_speed = 0.0
    prev_gear = 1
    prev_throttle = 0.0
    prev_brake = 0.0
    prev_rpm = 6000
    rows: list[dict] = []
    for idx, row in enumerate(scoped):
        speed = _to_float_or_none(row.get("speed"))
        if speed is None:
            speed = prev_speed
        speed = max(0.0, min(400.0, float(speed)))
        prev_speed = speed

        raw_gear = _to_float_or_none(row.get("n_gear"))
        if raw_gear is None:
            raw_gear = _to_float_or_none(row.get("gear"))
        if raw_gear is None:
            gear = prev_gear
        else:
            gear = int(round(max(1.0, min(8.0, raw_gear))))
        prev_gear = gear

        throttle = _to_float_or_none(row.get("throttle"))
        if throttle is None:
            throttle = prev_throttle
        throttle = max(0.0, min(100.0, float(throttle)))
        prev_throttle = throttle

        brake = _to_float_or_none(row.get("brake"))
        if brake is None:
            brake = prev_brake
        brake = max(0.0, min(100.0, float(brake)))
        prev_brake = brake

        rpm_raw = _to_float_or_none(row.get("rpm"))
        if rpm_raw is None:
            rpm = prev_rpm
        else:
            rpm = int(round(max(0.0, min(20000.0, rpm_raw))))
        prev_rpm = rpm

        drs = _normalize_drs_value(row.get("drs"))
        progress = idx / max(len(scoped) - 1, 1)
        rows.append(
            {
                "sample": idx + 1,
                "distance_pct": round(progress * 100.0, 2),
                "speed": round(speed, 2),
                "gear": gear,
                "throttle": round(throttle, 2),
                "brake": round(brake, 2),
                "rpm": rpm,
                "drs": drs,
            }
        )

    if not rows:
        return None

    return {
        "source": "openf1",
        "session_key": session_key,
        "driver_number": driver_number,
        "team": team_by_number.get(driver_number),
        "selected_lap": selected_lap,
        "available_laps": available_laps,
        "fastest_lap": fastest_lap,
        "samples": rows,
    }


def load_official_results_rows(season: int, round_no: int, session_kind: str = "race") -> list[dict]:
    normalized_session = str(session_kind or "race").strip().lower()
    endpoint = f"{season}/{round_no}/results.json"
    if normalized_session == "sprint":
        endpoint = f"{season}/{round_no}/sprint.json"
    # Try fast path first, then resilient shared fetch fallback.
    try:
        payload = fetch_positions_fast(endpoint)
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        if normalized_session == "sprint":
            return (races[0] if races else {}).get("SprintResults", [])
        return (races[0] if races else {}).get("Results", [])
    except Exception:
        try:
            payload = fetch(endpoint)
            races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            if normalized_session == "sprint":
                return (races[0] if races else {}).get("SprintResults", [])
            return (races[0] if races else {}).get("Results", [])
        except Exception:
            return []


def fetch_positions_fast(path: str, **query: str | int) -> dict:
    qs = f"?{urlencode(query)}" if query else ""
    url = f"{BASE}/{path}{qs}"
    attempts = 0
    while True:
        attempts += 1
        try:
            with urlopen(url, timeout=12) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code == 429 and attempts < 4:
                time.sleep(0.25 * attempts)
                continue
            raise
        except URLError:
            if attempts < 4:
                time.sleep(0.25 * attempts)
                continue
            raise


def fetch_openf1(
    path: str,
    *,
    timeout_s: float = 18,
    max_attempts: int = 4,
    **query: str | int,
) -> list[dict]:
    qs = f"?{urlencode(query)}" if query else ""
    url = f"{OPENF1_BASE}/{path}{qs}"
    attempts = 0
    while True:
        attempts += 1
        try:
            with urlopen(url, timeout=timeout_s) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, list) else []
        except HTTPError as exc:
            if exc.code == 429 and attempts < max_attempts:
                time.sleep(0.35 * attempts)
                continue
            raise
        except URLError:
            if attempts < max_attempts:
                time.sleep(0.35 * attempts)
                continue
            raise
        except TimeoutError:
            if attempts < max_attempts:
                time.sleep(0.35 * attempts)
                continue
            raise


def parse_iso8601(text: str | None) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def normalize_driver_label(name: str | None) -> str:
    raw = unicodedata.normalize("NFD", str(name or ""))
    no_marks = "".join(ch for ch in raw if unicodedata.category(ch) != "Mn")
    return "".join(ch for ch in no_marks.lower().strip() if ch.isalnum() or ch.isspace())


def get_openf1_sessions(season: int, session_name: str) -> list[dict]:
    normalized_name = str(session_name or "Race").strip() or "Race"
    key = (season, normalized_name.lower())
    cached = OPENF1_SESSIONS_CACHE.get(key)
    if cached is not None:
        return cached
    sessions = fetch_openf1("sessions", year=season, session_name=normalized_name, timeout_s=4, max_attempts=1)
    sessions = sorted(sessions, key=lambda item: str(item.get("date_start", "")))
    OPENF1_SESSIONS_CACHE[key] = sessions
    return sessions


def pick_openf1_session(season: int, round_payload: dict, session_name: str) -> dict | None:
    sessions = get_openf1_sessions(season, session_name)
    if not sessions:
        return None

    try:
        round_no = int(round_payload.get("round", 0))
    except (TypeError, ValueError):
        round_no = 0
    if round_no > 0 and round_no <= len(sessions):
        candidate = sessions[round_no - 1]
        race_date = str(round_payload.get("date", "")).strip()
        candidate_date = str(candidate.get("date_start", ""))[:10]
        if race_date and race_date == candidate_date:
            return candidate

    race_dt = parse_iso8601(f"{round_payload.get('date', '')}T00:00:00+00:00")
    if race_dt is None:
        return sessions[0]

    country = str(round_payload.get("track", {}).get("country", "")).strip().lower()

    def score(item: dict) -> tuple[int, float]:
        session_dt = parse_iso8601(item.get("date_start"))
        if session_dt is None:
            return (2, 999999999.0)
        day_diff = abs((session_dt.date() - race_dt.date()).days)
        same_country = 0 if str(item.get("country_name", "")).strip().lower() == country and country else 1
        return (same_country + day_diff, abs((session_dt - race_dt).total_seconds()))

    return min(sessions, key=score)


def load_openf1_session_driver_rows(
    season: int,
    round_payload: dict,
    session_kind: str = "race",
) -> list[dict]:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    openf1_session_name = "Sprint" if normalized_session == "sprint" else "Race"
    openf1_session = pick_openf1_session(season, round_payload, openf1_session_name)
    if not openf1_session or not openf1_session.get("session_key"):
        return []

    session_key = int(openf1_session["session_key"])
    openf1_drivers = fetch_openf1("drivers", session_key=session_key, timeout_s=6, max_attempts=2)
    rows = []
    for idx, item in enumerate(sorted(openf1_drivers, key=lambda row: (
        str(row.get("team_name", "")),
        str(row.get("full_name", "")),
        int(row.get("driver_number", 999) or 999),
    )), start=1):
        full_name = str(item.get("full_name", "")).strip()
        if not full_name:
            continue
        team_name = str(item.get("team_name", "")).strip()
        rows.append(
            {
                "position": idx,
                "driver": full_name.title() if full_name.isupper() else full_name,
                "team": team_name or None,
                "status": None,
                "grid": 0,
            }
        )
    return rows


def load_openf1_lap_times_by_driver(
    season: int,
    round_payload: dict,
    session_kind: str = "race",
) -> tuple[dict[str, dict[int, float]], int]:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    openf1_session_name = "Sprint" if normalized_session == "sprint" else "Race"
    openf1_session = pick_openf1_session(season, round_payload, openf1_session_name)
    if not openf1_session or not openf1_session.get("session_key"):
        return {}, 0

    session_key = int(openf1_session["session_key"])
    openf1_drivers = fetch_openf1("drivers", session_key=session_key, timeout_s=3, max_attempts=1)
    name_by_number: dict[int, str] = {}
    for item in openf1_drivers:
        try:
            driver_number = int(item.get("driver_number"))
        except (TypeError, ValueError):
            continue
        full_name = str(item.get("full_name", "")).strip()
        if full_name:
            name_by_number[driver_number] = full_name.title() if full_name.isupper() else full_name

    raw_laps = fetch_openf1("laps", session_key=session_key)
    lap_times_by_driver: dict[str, dict[int, float]] = {}
    max_lap_seen = 0
    for row in raw_laps:
        try:
            driver_number = int(row.get("driver_number"))
            lap_no = int(row.get("lap_number"))
            lap_time_s = float(row.get("lap_duration"))
        except (TypeError, ValueError):
            continue
        if lap_no <= 0 or lap_time_s <= 0:
            continue
        driver_name = name_by_number.get(driver_number)
        if not driver_name:
            continue
        max_lap_seen = max(max_lap_seen, lap_no)
        lap_times_by_driver.setdefault(driver_name, {})[lap_no] = lap_time_s

    return lap_times_by_driver, max_lap_seen


def _build_positions_from_lap_times(
    round_payload: dict,
    season: int,
    round_no: int,
    session_kind: str,
    session_rows: list[dict],
    driver_team_map: dict[str, str],
    lap_times_by_driver: dict[str, dict[int, float]],
    target_laps: int,
    source: str,
) -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"

    session_driver_names = {
        normalize_driver_name_key(row.get("driver")): row.get("driver")
        for row in session_rows or []
        if row.get("driver")
    }

    lap_timings_by_lap: dict[int, dict[str, float]] = {}
    for driver_name, laps_by_no in (lap_times_by_driver or {}).items():
        if not driver_name:
            continue
        for lap_no, lap_time_s in (laps_by_no or {}).items():
            try:
                lap_idx = int(lap_no)
                lap_time = float(lap_time_s)
            except (TypeError, ValueError):
                continue
            if lap_idx <= 0 or not np.isfinite(lap_time) or lap_time <= 0:
                continue
            lap_timings_by_lap.setdefault(lap_idx, {})[driver_name] = lap_time

    rows = reconstruct_positions_from_lap_times(lap_timings_by_lap, target_laps)
    rows = canonicalize_position_rows(rows, session_driver_names)
    if not rows:
        return _empty_positions_payload(round_payload, season, round_no, normalized_session)

    grid_order: list[tuple[str, int]] = []
    if normalized_session == "sprint":
        sprint_grid_rows = round_payload.get("sprint_qualifying") or []
        for row in sprint_grid_rows:
            driver_name = row.get("driver")
            if not driver_name:
                continue
            try:
                grid = int(row.get("position", 999) or 999)
            except (TypeError, ValueError):
                grid = 999
            grid_order.append((driver_name, grid))
    else:
        for row in session_rows or []:
            driver_name = row.get("driver")
            if not driver_name:
                continue
            try:
                grid = int(row.get("grid", 0) or 0)
            except (TypeError, ValueError):
                grid = 0
            if grid <= 0:
                try:
                    grid = int(row.get("position", 999) or 999)
                except (TypeError, ValueError):
                    grid = 999
            grid_order.append((driver_name, grid))
    grid_order.sort(key=lambda item: (item[1], item[0]))

    max_position = 0
    for row in rows:
        for driver_name, position in row.items():
            if driver_name == "lap" or not isinstance(position, int):
                continue
            max_position = max(max_position, position)

    if grid_order:
        lap0 = {"lap": 0}
        for idx, (driver_name, _) in enumerate(grid_order, start=1):
            lap0[driver_name] = idx
        rows = [lap0, *rows]
        max_position = max(max_position, len(grid_order))

    lap1 = rows[1] if len(rows) > 1 and rows[0].get("lap") == 0 else (rows[0] if rows else {})
    started_drivers = [
        driver_name
        for driver_name, pos in sorted(
            ((k, v) for k, v in lap1.items() if k != "lap" and isinstance(v, int)),
            key=lambda item: (item[1], item[0]),
        )
    ]
    finish_order = [r["driver"] for r in sorted(session_rows or [], key=lambda x: int(x.get("position", 999) or 999)) if r.get("driver")]
    drivers_in_finish_order: list[str] = []
    seen: set[str] = set()
    for name in started_drivers + finish_order:
        if name in seen:
            continue
        seen.add(name)
        drivers_in_finish_order.append(name)

    completed_laps_by_driver: dict[str, int] = {}
    for driver_name, laps_by_no in (lap_times_by_driver or {}).items():
        canonical_name = resolve_canonical_driver_name(driver_name, session_driver_names) or driver_name
        if not canonical_name:
            continue
        completed_laps = max((int(lap_no) for lap_no in (laps_by_no or {}).keys()), default=0)
        completed_laps_by_driver[canonical_name] = max(
            completed_laps,
            int(completed_laps_by_driver.get(canonical_name, 0) or 0),
        )

    return {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "session": normalized_session,
        "drivers": [
            {"driver": driver, "team": driver_team_map.get(driver)}
            for driver in drivers_in_finish_order
        ],
        "laps": rows,
        "max_position": max_position or len(drivers_in_finish_order),
        "dnf_drivers": [
            r["driver"]
            for r in session_rows or []
            if r.get("driver") and not (
                is_classified_finisher_status(r.get("status"))
                or (normalized_session == "sprint" and not str(r.get("status") or "").strip())
            )
        ],
        "completed_laps_by_driver": completed_laps_by_driver,
        "source": source,
    }


def warm_positions_for_season(season: int, rounds: list[int] | None = None) -> list[dict]:
    season_data = get_season_data(season)
    all_rounds = [int(r["round"]) for r in season_data.get("rounds", [])]
    target_rounds = rounds if rounds is not None else all_rounds
    warmed: list[dict] = []
    for round_no in target_rounds:
        if round_no not in all_rounds:
            continue
        start = time.perf_counter()
        source = "memory"
        key = (season, round_no, "race")
        existing = POSITIONS_CACHE.get(key)
        if existing is None or str(existing.get("source", "")).lower() == "synthetic":
            payload = build_round_positions(season, round_no)
            source = str(payload.get("source", "upstream")).lower()
            if source == "synthetic":
                # Avoid holding stale synthetic payloads during warmup.
                POSITIONS_CACHE.pop(key, None)
        elapsed_ms = (time.perf_counter() - start) * 1000
        warmed.append({
            "season": season,
            "round": round_no,
            "source": source,
            "elapsed_ms": round(elapsed_ms, 2),
        })
    return warmed


@app.on_event("startup")
def preload_warm_seasons() -> None:
    for season in parse_warm_seasons():
        try:
            get_season_data(season)
        except Exception:
            # Keep startup resilient even if one warm season fails.
            pass
    if os.getenv("WARM_POSITIONS_ON_STARTUP", "0").strip().lower() in {"1", "true", "yes"}:
        rounds = parse_warm_rounds()
        for season in parse_warm_position_seasons():
            try:
                warm_positions_for_season(season, rounds)
            except Exception:
                # Keep startup resilient even if one warm season fails.
                pass


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/warmup")
def warmup(seasons: str | None = Query(None)) -> dict:
    season_values: list[int] = []
    raw = seasons if seasons is not None else os.getenv("WARM_SEASONS", "2025")
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            season = int(token)
        except ValueError:
            continue
        if season in SUPPORTED_SEASONS:
            season_values.append(season)
    season_values = list(dict.fromkeys(season_values))

    warmed: list[dict[str, float | int]] = []
    for season in season_values:
        start = time.perf_counter()
        get_season_data(season)
        elapsed_ms = (time.perf_counter() - start) * 1000
        warmed.append({"season": season, "elapsed_ms": round(elapsed_ms, 2)})

    return {"status": "ok", "warmed": warmed, "cached_seasons": sorted(SEASON_CACHE.keys())}


@app.get("/api/warmup/positions")
def warmup_positions(
    seasons: str | None = Query(None),
    rounds: str | None = Query(None),
) -> dict:
    raw_seasons = seasons if seasons is not None else os.getenv("WARM_POSITION_SEASONS", os.getenv("WARM_SEASONS", "2025"))
    season_values: list[int] = []
    for token in raw_seasons.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            season = int(token)
        except ValueError:
            continue
        if season in SUPPORTED_SEASONS:
            season_values.append(season)
    season_values = list(dict.fromkeys(season_values))

    target_rounds: list[int] | None = None
    raw_rounds = rounds if rounds is not None else os.getenv("WARM_POSITION_ROUNDS", "").strip()
    if raw_rounds and raw_rounds.lower() != "all":
        parsed_rounds: list[int] = []
        for token in raw_rounds.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                round_no = int(token)
            except ValueError:
                continue
            if round_no > 0:
                parsed_rounds.append(round_no)
        target_rounds = list(dict.fromkeys(parsed_rounds))

    warmed: list[dict] = []
    for season in season_values:
        warmed.extend(warm_positions_for_season(season, target_rounds))

    return {
        "status": "ok",
        "warmed": warmed,
        "cached_position_keys": [f"{k[0]}-{k[1]}-{k[2]}" for k in sorted(POSITIONS_CACHE.keys())],
    }


@app.get("/api/casual/seasons")
def casual_seasons() -> dict:
    return {"seasons": SUPPORTED_SEASONS}


@app.get("/api/casual/overview")
def casual_overview(season: int = Query(2025)) -> dict:
    season_data = get_season_data(season)
    rounds = season_data.get("rounds", [])
    latest_round = rounds[-1] if rounds else None
    return {
        "season": season_data["season"],
        "driver_standings": season_data["driver_standings"],
        "constructor_standings": season_data["constructor_standings"],
        "power_unit_standings": season_data.get("power_unit_standings", []),
        "constructor_power_units": season_data.get("constructor_power_units", []),
        "points_progression": season_data["points_progression"],
        "progression_drivers": season_data["progression_drivers"],
        "constructor_points_progression": season_data["constructor_points_progression"],
        "progression_constructors": season_data["progression_constructors"],
        "power_unit_points_progression": season_data.get("power_unit_points_progression", []),
        "progression_power_units": season_data.get("progression_power_units", []),
        "rounds_count": len(rounds),
        "latest_round": latest_round["round"] if latest_round else None,
        "latest_track": latest_round["track"] if latest_round else None,
        "generated_from": season_data.get("generated_from"),
    }


@app.get("/api/casual/results/{round_no}")
def casual_results(round_no: int, season: int = Query(2025)) -> dict:
    season_data = get_season_data(season)
    for item in season_data.get("rounds", []):
        if item["round"] == round_no:
            payload = dict(item)
            payload["session_schedule"] = get_round_session_schedule(season, round_no)
            return payload
    raise HTTPException(status_code=404, detail=f"Round {round_no} not found for season {season_data['season']}")


@app.get("/api/casual/rounds")
def casual_rounds(season: int = Query(2025)) -> dict:
    season_data = get_season_data(season)
    rounds_payload = {int(item["round"]): item for item in season_data.get("rounds", []) if item.get("round") is not None}
    rounds_summary = []
    for item in season_data.get("rounds_summary", []):
        summary = dict(item)
        round_payload = rounds_payload.get(int(summary.get("round", 0) or 0), {})
        sprint_rows = round_payload.get("sprint") or []
        sprint_winner = None
        if sprint_rows:
            sprint_sorted = sorted(
                [row for row in sprint_rows if row.get("driver")],
                key=lambda row: int(row.get("position", 999) or 999),
            )
            if sprint_sorted:
                sprint_winner = sprint_sorted[0].get("driver")
        summary["sprint_winner"] = sprint_winner
        rounds_summary.append(summary)
    return {
        "season": season_data["season"],
        "rounds": rounds_summary,
        "count": len(rounds_summary),
    }


@app.get("/api/casual/session-schedule")
def casual_session_schedule(season: int = Query(2026)) -> dict:
    if season not in SUPPORTED_SEASONS:
        raise HTTPException(
            status_code=400,
            detail=f"Season {season} is not supported. Supported: {SUPPORTED_SEASONS[0]}-{SUPPORTED_SEASONS[-1]}",
        )
    rounds = get_season_session_schedule(season)
    return {"season": season, "rounds": rounds, "count": len(rounds)}


def get_round_payload(season: int, round_no: int) -> dict:
    season_data = get_season_data(season)
    for item in season_data.get("rounds", []):
        if item["round"] == round_no:
            return item
    raise HTTPException(status_code=404, detail=f"Round {round_no} not found for season {season_data['season']}")


def get_driver_result(season: int, round_no: int, driver: str) -> dict:
    round_payload = get_round_payload(season, round_no)
    for result in round_payload.get("results", []):
        if result["driver"] == driver:
            return result
    raise HTTPException(status_code=404, detail=f"Driver '{driver}' not found in round {round_no}")


def _qualifying_cutoffs(season: int, field_size: int) -> tuple[int, int]:
    q2_count = 16 if season >= 2026 else 15
    q2_count = min(max(0, q2_count), max(0, field_size))
    q3_count = min(10, q2_count)
    return q2_count, q3_count


def _estimate_form_adjustments(season: int, driver_names: list[str], form_bias: float) -> dict[str, float]:
    standings = get_season_data(season).get("driver_standings", [])
    rank_map: dict[str, int] = {}
    for idx, row in enumerate(standings, start=1):
        driver_name = str(row.get("driver") or "").strip()
        if driver_name:
            rank_map[driver_name] = idx

    total_ranked = max(len(rank_map), len(driver_names), 1)
    max_effect = 0.22 * max(0.0, min(1.0, float(form_bias)))
    adjustments: dict[str, float] = {}
    midpoint = (total_ranked - 1) / 2 if total_ranked > 1 else 0.0
    scale = max(midpoint, 1.0)

    for driver_name in driver_names:
        rank = rank_map.get(driver_name, total_ranked)
        centered = (midpoint - (rank - 1)) / scale
        adjustments[driver_name] = round(-max_effect * centered, 4)

    return adjustments


def _extract_best_comparable_quali_time(row: dict | None) -> float | None:
    if not isinstance(row, dict):
        return None
    for key in ("q3", "q2", "q1"):
        parsed = parse_lap_time_to_seconds(row.get(key))
        if parsed is not None:
            return parsed
    return None


def _build_recent_qualifying_context(season: int, round_no: int, driver_names: list[str]) -> dict[str, dict]:
    season_data = get_season_data(season)
    rounds = [item for item in season_data.get("rounds", []) if int(item.get("round", 0) or 0) < round_no]
    recent_rounds = rounds[-5:]
    field_size = max(len(driver_names), 1)
    driver_set = set(driver_names)
    context: dict[str, dict] = {
        driver_name: {
            "recent_avg_position": None,
            "recent_position_component_s": 0.0,
            "teammate_gap_s": None,
            "teammate_component_s": 0.0,
            "recent_rounds_used": 0,
        }
        for driver_name in driver_names
    }
    if not recent_rounds:
        return context

    positions_by_driver: dict[str, list[int]] = {driver_name: [] for driver_name in driver_names}
    teammate_gaps_by_driver: dict[str, list[float]] = {driver_name: [] for driver_name in driver_names}

    for prev_round in recent_rounds:
        results_rows = prev_round.get("results") or []
        team_by_driver = {
            str(row.get("driver")): str(row.get("team"))
            for row in results_rows
            if row.get("driver") and row.get("team")
        }
        quali_rows = prev_round.get("qualifying") or []
        quali_by_driver = {
            str(row.get("driver")): row
            for row in quali_rows
            if row.get("driver")
        }

        teams_to_drivers: dict[str, list[str]] = {}
        for driver_name, team_name in team_by_driver.items():
            if driver_name not in driver_set or not team_name:
                continue
            teams_to_drivers.setdefault(team_name, []).append(driver_name)

        for driver_name in driver_names:
            qualifying_row = quali_by_driver.get(driver_name)
            if qualifying_row:
                position_value = int(qualifying_row.get("position", 0) or 0)
                if position_value > 0:
                    positions_by_driver[driver_name].append(position_value)

        for team_drivers in teams_to_drivers.values():
            if len(team_drivers) != 2:
                continue
            first, second = team_drivers
            row_first = quali_by_driver.get(first)
            row_second = quali_by_driver.get(second)
            time_first = _extract_best_comparable_quali_time(row_first)
            time_second = _extract_best_comparable_quali_time(row_second)
            if time_first is None or time_second is None:
                continue
            teammate_gaps_by_driver[first].append(time_second - time_first)
            teammate_gaps_by_driver[second].append(time_first - time_second)

    for driver_name in driver_names:
        recent_positions = positions_by_driver.get(driver_name) or []
        if recent_positions:
            avg_pos = float(np.mean(np.array(recent_positions, dtype=float)))
            centered = ((field_size + 1) / 2) - avg_pos
            position_component = -max(-0.12, min(0.12, centered * 0.012))
            context[driver_name]["recent_avg_position"] = round(avg_pos, 2)
            context[driver_name]["recent_position_component_s"] = round(position_component, 4)
            context[driver_name]["recent_rounds_used"] = len(recent_positions)

        teammate_gaps = teammate_gaps_by_driver.get(driver_name) or []
        if teammate_gaps:
            teammate_gap = float(np.mean(np.array(teammate_gaps, dtype=float)))
            teammate_component = -max(-0.09, min(0.09, teammate_gap * 0.45))
            context[driver_name]["teammate_gap_s"] = round(teammate_gap, 4)
            context[driver_name]["teammate_component_s"] = round(teammate_component, 4)

    return context


def _driver_profile_label(row: dict) -> str:
    pole_probability = float(row.get("pole_probability") or 0.0)
    q3_probability = float(row.get("q3_probability") or 0.0)
    q1_risk = float(row.get("q1_elimination_probability") or 0.0)
    total_adjustment = float(row.get("model_adjustment_s") or 0.0)
    top_outcomes = row.get("top_outcomes") or []
    if pole_probability >= 0.25:
        return "Strong pole threat"
    if q3_probability >= 0.8 and total_adjustment < -0.02:
        return "Quiet front-row danger"
    if q3_probability >= 0.45 and q3_probability <= 0.65:
        return "Q3 bubble"
    if q1_risk >= 0.3:
        return "Fragile floor"
    if top_outcomes and any(int(item.get("position", 99)) > 6 for item in top_outcomes[:2]):
        return "Volatile upside"
    return "Steady baseline"


def _safe_quantile(values: list[float], quantile: float, fallback: float = 0.0) -> float:
    clean = [float(v) for v in values if np.isfinite(v)]
    if not clean:
        return fallback
    return float(np.quantile(np.array(clean, dtype=float), quantile))


def _estimate_qualifying_reference_times(qualifying_rows: list[dict], results_rows: list[dict]) -> dict[str, dict]:
    known_q1: list[float] = []
    known_q2: list[float] = []
    known_q3: list[float] = []
    q2_deltas: list[float] = []
    q3_deltas: list[float] = []

    for row in qualifying_rows:
        q1 = parse_lap_time_to_seconds(row.get("q1"))
        q2 = parse_lap_time_to_seconds(row.get("q2"))
        q3 = parse_lap_time_to_seconds(row.get("q3"))
        if q1 is not None:
            known_q1.append(q1)
        if q2 is not None:
            known_q2.append(q2)
        if q3 is not None:
            known_q3.append(q3)
        if q1 is not None and q2 is not None:
            q2_deltas.append(q2 - q1)
        if q2 is not None and q3 is not None:
            q3_deltas.append(q3 - q2)

    median_q2_delta = _safe_quantile(q2_deltas, 0.5, fallback=-0.28)
    median_q3_delta = _safe_quantile(q3_deltas, 0.5, fallback=-0.17)
    q1_floor = _safe_quantile(known_q1, 0.9, fallback=80.0)
    q1_fast = _safe_quantile(known_q1, 0.1, fallback=q1_floor - 1.0)
    grid_by_driver = {
        str(row.get("driver")): int(row.get("grid", 0) or 0)
        for row in results_rows
        if row.get("driver")
    }
    finish_by_driver = {
        str(row.get("driver")): int(row.get("position", 0) or 0)
        for row in results_rows
        if row.get("driver")
    }

    field_size = max(len(qualifying_rows), len(results_rows), 20)
    references: dict[str, dict] = {}

    for row in qualifying_rows:
        driver_name = str(row.get("driver") or "").strip()
        if not driver_name:
            continue
        q1 = parse_lap_time_to_seconds(row.get("q1"))
        q2 = parse_lap_time_to_seconds(row.get("q2"))
        q3 = parse_lap_time_to_seconds(row.get("q3"))

        actual_position = int(row.get("position", 0) or 0)
        reference_slot = actual_position or grid_by_driver.get(driver_name) or finish_by_driver.get(driver_name) or field_size
        fallback_q1 = q1_floor + (max(reference_slot - 1, 0) * 0.11)

        est_q1 = q1
        if est_q1 is None and q2 is not None:
            est_q1 = q2 - median_q2_delta
        if est_q1 is None and q3 is not None:
            est_q1 = q3 - median_q3_delta - median_q2_delta
        if est_q1 is None:
            est_q1 = fallback_q1

        est_q2 = q2
        if est_q2 is None:
            est_q2 = est_q1 + median_q2_delta

        est_q3 = q3
        if est_q3 is None:
            est_q3 = est_q2 + median_q3_delta

        volatility_seed = [value for value in (q1, q2, q3) if value is not None]
        driver_std = float(np.std(np.array(volatility_seed, dtype=float))) if len(volatility_seed) >= 2 else 0.0
        pace_span = max(est_q1 - q1_fast, 0.0)
        sigma_base = 0.08 + min(0.22, pace_span * 0.055)
        sigma = min(0.42, max(0.075, sigma_base + (driver_std * 0.35)))

        references[driver_name] = {
            "driver": driver_name,
            "actual_position": actual_position if actual_position > 0 else None,
            "q1_s": round(float(est_q1), 3),
            "q2_s": round(float(est_q2), 3),
            "q3_s": round(float(est_q3), 3),
            "q1_actual": row.get("q1"),
            "q2_actual": row.get("q2"),
            "q3_actual": row.get("q3"),
            "sigma_s": round(float(sigma), 4),
        }

    return {
        "drivers": references,
        "median_q2_delta": round(float(median_q2_delta), 4),
        "median_q3_delta": round(float(median_q3_delta), 4),
    }


def build_qualifying_simulator(season: int, round_no: int, simulations: int, chaos: float, form_bias: float) -> dict:
    rounded_simulations = max(400, min(int(simulations), 20000))
    rounded_chaos = max(0.0, min(float(chaos), 1.0))
    rounded_form_bias = max(0.0, min(float(form_bias), 1.0))
    cache_key = (
        season,
        round_no,
        rounded_simulations,
        int(round(rounded_chaos * 1000)),
        int(round(rounded_form_bias * 1000)),
    )
    cached = QUALI_SIM_CACHE.get(cache_key)
    if cached:
        return cached

    round_payload = get_round_payload(season, round_no)
    qualifying_rows = sorted(round_payload.get("qualifying") or [], key=lambda row: int(row.get("position", 999) or 999))
    results_rows = sorted(round_payload.get("results") or [], key=lambda row: int(row.get("position", 999) or 999))

    if not qualifying_rows:
        raise HTTPException(
            status_code=404,
            detail="Qualifying data is not available for this round yet.",
        )

    team_by_driver = {
        str(row.get("driver")): str(row.get("team"))
        for row in results_rows
        if row.get("driver") and row.get("team")
    }
    references_payload = _estimate_qualifying_reference_times(qualifying_rows, results_rows)
    references = references_payload["drivers"]
    if not references:
        raise HTTPException(status_code=404, detail="Unable to build qualifying references for this round.")

    driver_names = [str(row.get("driver")) for row in qualifying_rows if str(row.get("driver") or "").strip()]
    field_size = len(driver_names)
    q2_count, q3_count = _qualifying_cutoffs(season, field_size)
    history_context = _build_recent_qualifying_context(season, round_no, driver_names)
    form_adjustments = _estimate_form_adjustments(season, driver_names, rounded_form_bias)
    rng = np.random.default_rng(
        seed=(
            season * 100_000
            + round_no * 1_000
            + rounded_simulations
            + int(round(rounded_chaos * 100))
            + int(round(rounded_form_bias * 100))
        )
    )

    attempts_by_session = {"q1": 3, "q2": 3, "q3": 2}
    chaos_multiplier = 1.0 + (rounded_chaos * 1.45)
    incident_risk = 0.014 + (rounded_chaos * 0.052)
    miracle_risk = 0.01 + (rounded_chaos * 0.012)
    session_time_totals = {"q1": 0.0, "q2": 0.0, "q3": 0.0}
    position_counts = {driver_name: [0] * field_size for driver_name in driver_names}
    pole_counts = {driver_name: 0 for driver_name in driver_names}
    front_row_counts = {driver_name: 0 for driver_name in driver_names}
    q3_counts = {driver_name: 0 for driver_name in driver_names}
    q2_elimination_counts = {driver_name: 0 for driver_name in driver_names}
    q1_elimination_counts = {driver_name: 0 for driver_name in driver_names}

    def simulate_session(active_drivers: list[str], session_key: str) -> list[tuple[str, float]]:
        ranked: list[tuple[str, float]] = []
        base_attempts = attempts_by_session[session_key]
        for driver_name in active_drivers:
            reference = references[driver_name]
            history_adjustment = float(history_context.get(driver_name, {}).get("recent_position_component_s", 0.0) or 0.0)
            history_adjustment += float(history_context.get(driver_name, {}).get("teammate_component_s", 0.0) or 0.0)
            raw_baseline = float(reference[f"{session_key}_s"])
            weighted_baseline = raw_baseline + history_adjustment + float(form_adjustments.get(driver_name, 0.0))
            mean_time = (raw_baseline * 0.68) + (weighted_baseline * 0.32)
            sigma = max(0.06, float(reference["sigma_s"]) * chaos_multiplier)
            best_lap = None
            for _ in range(base_attempts):
                lap = float(rng.normal(loc=mean_time, scale=sigma))
                if rng.random() < incident_risk:
                    lap += float(rng.uniform(0.18, 0.85)) * (0.8 + rounded_chaos)
                elif rng.random() < miracle_risk:
                    lap -= float(rng.uniform(0.02, 0.11))
                best_lap = lap if best_lap is None else min(best_lap, lap)
            ranked.append((driver_name, best_lap if best_lap is not None else mean_time))
        ranked.sort(key=lambda item: (item[1], item[0]))
        return ranked

    for _ in range(rounded_simulations):
        q1_ranked = simulate_session(driver_names, "q1")
        for _, lap_time in q1_ranked:
            session_time_totals["q1"] += float(lap_time)
        q2_drivers = [driver_name for driver_name, _ in q1_ranked[:q2_count]]
        for driver_name, _ in q1_ranked[q2_count:]:
            q1_elimination_counts[driver_name] += 1

        q2_ranked = simulate_session(q2_drivers, "q2")
        for _, lap_time in q2_ranked:
            session_time_totals["q2"] += float(lap_time)
        q3_drivers = [driver_name for driver_name, _ in q2_ranked[:q3_count]]
        for driver_name in q3_drivers:
            q3_counts[driver_name] += 1
        for driver_name, _ in q2_ranked[q3_count:]:
            q2_elimination_counts[driver_name] += 1

        q3_ranked = simulate_session(q3_drivers, "q3")
        for _, lap_time in q3_ranked:
            session_time_totals["q3"] += float(lap_time)

        final_order = [driver_name for driver_name, _ in q3_ranked]
        final_order.extend([driver_name for driver_name, _ in q2_ranked[q3_count:]])
        final_order.extend([driver_name for driver_name, _ in q1_ranked[q2_count:]])

        for position_idx, driver_name in enumerate(final_order, start=1):
            position_counts[driver_name][position_idx - 1] += 1
            if position_idx == 1:
                pole_counts[driver_name] += 1
            if position_idx <= 2:
                front_row_counts[driver_name] += 1

    expected_grid_rows: list[dict] = []
    heatmap_rows: list[dict] = []

    for driver_name in driver_names:
        counts = position_counts[driver_name]
        probabilities = [round(count / rounded_simulations, 4) for count in counts]
        expected_position = sum((idx + 1) * prob for idx, prob in enumerate(probabilities))
        pole_probability = pole_counts[driver_name] / rounded_simulations
        front_row_probability = front_row_counts[driver_name] / rounded_simulations
        q3_probability = q3_counts[driver_name] / rounded_simulations
        q1_risk = q1_elimination_counts[driver_name] / rounded_simulations
        q2_risk = q2_elimination_counts[driver_name] / rounded_simulations
        reference = references[driver_name]
        history = history_context.get(driver_name, {})
        model_adjustment = float(history.get("recent_position_component_s", 0.0) or 0.0)
        model_adjustment += float(history.get("teammate_component_s", 0.0) or 0.0)
        model_adjustment += float(form_adjustments.get(driver_name, 0.0) or 0.0)
        sorted_probabilities = sorted(
            (
                {"position": idx + 1, "probability": round(prob, 4)}
                for idx, prob in enumerate(probabilities)
                if prob > 0
            ),
            key=lambda item: (-float(item["probability"]), int(item["position"])),
        )
        expected_grid_rows.append(
            {
                "driver": driver_name,
                "team": team_by_driver.get(driver_name),
                "actual_position": reference.get("actual_position"),
                "expected_position": round(expected_position, 2),
                "pole_probability": round(pole_probability, 4),
                "front_row_probability": round(front_row_probability, 4),
                "q3_probability": round(q3_probability, 4),
                "q2_elimination_probability": round(q2_risk, 4),
                "q1_elimination_probability": round(q1_risk, 4),
                "baseline_q1": format_lap_time(reference.get("q1_s")),
                "baseline_q2": format_lap_time(reference.get("q2_s")),
                "baseline_q3": format_lap_time(reference.get("q3_s")),
                "baseline_q1_s": reference.get("q1_s"),
                "baseline_q2_s": reference.get("q2_s"),
                "baseline_q3_s": reference.get("q3_s"),
                "form_adjustment_s": round(float(form_adjustments.get(driver_name, 0.0)), 4),
                "recent_avg_position": history.get("recent_avg_position"),
                "recent_rounds_used": history.get("recent_rounds_used"),
                "recent_position_component_s": round(float(history.get("recent_position_component_s", 0.0) or 0.0), 4),
                "teammate_gap_s": history.get("teammate_gap_s"),
                "teammate_component_s": round(float(history.get("teammate_component_s", 0.0) or 0.0), 4),
                "model_adjustment_s": round(float(model_adjustment), 4),
                "top_outcomes": sorted_probabilities[:4],
            }
        )
        heatmap_rows.append(
            {
                "driver": driver_name,
                "team": team_by_driver.get(driver_name),
                "actual_position": reference.get("actual_position"),
                "probabilities": probabilities,
            }
        )

    expected_grid_rows.sort(
        key=lambda row: (
            float(row.get("expected_position") or 999),
            -(float(row.get("pole_probability") or 0.0)),
            row.get("driver") or "",
        )
    )
    expected_rank_by_driver = {
        str(row.get("driver")): idx + 1
        for idx, row in enumerate(expected_grid_rows)
    }
    comparison_rows = []
    for row in expected_grid_rows:
        row["profile_label"] = _driver_profile_label(row)
        actual_position = row.get("actual_position")
        expected_rank = expected_rank_by_driver.get(str(row.get("driver")), None)
        delta = None
        if isinstance(actual_position, int) and expected_rank is not None:
            delta = actual_position - expected_rank
        comparison_rows.append(
            {
                "driver": row.get("driver"),
                "team": row.get("team"),
                "expected_rank": expected_rank,
                "actual_position": actual_position,
                "delta_positions": delta,
                "expected_position": row.get("expected_position"),
                "pole_probability": row.get("pole_probability"),
            }
        )

    comparison_rows.sort(
        key=lambda item: (
            abs(int(item["delta_positions"])) if isinstance(item.get("delta_positions"), int) else -1,
            -(int(item["delta_positions"]) if isinstance(item.get("delta_positions"), int) else -999),
        ),
        reverse=True,
    )

    driver_details = {}
    for row in expected_grid_rows:
        actual_position = row.get("actual_position")
        expected_rank = expected_rank_by_driver.get(str(row.get("driver")))
        delta = actual_position - expected_rank if isinstance(actual_position, int) and expected_rank is not None else None
        explanation_bits = []
        model_adj = float(row.get("model_adjustment_s") or 0.0)
        if model_adj < -0.01:
            explanation_bits.append("Model gives this driver a small pace boost from recent form and teammate-relative trend.")
        elif model_adj > 0.01:
            explanation_bits.append("Model applies a small caution penalty from recent form and teammate-relative trend.")
        else:
            explanation_bits.append("Model keeps this driver close to the raw round pace baseline.")
        if float(row.get("pole_probability") or 0.0) >= 0.2:
            explanation_bits.append("Strong pole probability suggests a realistic front-row threat.")
        elif float(row.get("q1_elimination_probability") or 0.0) >= 0.25:
            explanation_bits.append("High Q1 elimination risk means the floor is still fragile.")
        driver_details[str(row.get("driver"))] = {
            "driver": row.get("driver"),
            "team": row.get("team"),
            "profile_label": row.get("profile_label"),
            "expected_rank": expected_rank,
            "actual_position": actual_position,
            "delta_positions": delta,
            "expected_position": row.get("expected_position"),
            "baseline": {
                "q1": row.get("baseline_q1"),
                "q2": row.get("baseline_q2"),
                "q3": row.get("baseline_q3"),
            },
            "adjustments": {
                "recent_form_s": row.get("recent_position_component_s"),
                "teammate_s": row.get("teammate_component_s"),
                "season_form_s": row.get("form_adjustment_s"),
                "total_s": row.get("model_adjustment_s"),
            },
            "recent_context": {
                "avg_position": row.get("recent_avg_position"),
                "rounds_used": row.get("recent_rounds_used"),
                "teammate_gap_s": row.get("teammate_gap_s"),
            },
            "probabilities": {
                "pole": row.get("pole_probability"),
                "front_row": row.get("front_row_probability"),
                "q3": row.get("q3_probability"),
                "q2_elimination": row.get("q2_elimination_probability"),
                "q1_elimination": row.get("q1_elimination_probability"),
            },
            "top_outcomes": row.get("top_outcomes"),
            "explanations": explanation_bits,
        }

    def _top_driver(metric: str) -> dict | None:
        if not expected_grid_rows:
            return None
        return max(
            expected_grid_rows,
            key=lambda row: (float(row.get(metric) or 0.0), -(float(row.get("expected_position") or 999))),
        )

    q2_bubble = min(
        expected_grid_rows,
        key=lambda row: abs(float(row.get("q3_probability") or 0.0) - 0.5),
    ) if expected_grid_rows else None

    payload = {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "track": round_payload.get("track"),
        "simulations": rounded_simulations,
        "field_size": field_size,
        "cutoffs": {
            "q2": q2_count,
            "q3": q3_count,
        },
        "inputs": {
            "chaos": round(rounded_chaos, 3),
            "form_bias": round(rounded_form_bias, 3),
        },
        "transparency": {
            "weights": {
                "raw_round_pace": 0.68,
                "recent_quali_trend_and_teammate_context": 0.32,
                "season_form_bias_slider_cap_s": 0.22,
            },
            "attempts_by_session": attempts_by_session,
            "chaos": {
                "variance_multiplier": round(chaos_multiplier, 3),
                "incident_risk": round(incident_risk, 3),
                "miracle_lap_risk": round(miracle_risk, 3),
            },
        },
        "assumptions": {
            "model": "Session-by-session Monte Carlo using round-specific qualifying references, recent qualifying trend, teammate-relative context, and optional season-form bias.",
            "notes": [
                "Reference means come from actual Q1/Q2/Q3 times when available, with missing sessions inferred from median session deltas.",
                "Recent qualifying trend and teammate-relative pace slightly reshape the baseline before randomness is applied.",
                "Form bias nudges season leaders slightly forward without overpowering the round-specific pace picture.",
                "Chaos increases variance and the chance of a compromised or exceptional lap.",
            ],
            "session_deltas": {
                "q2_minus_q1_s": references_payload["median_q2_delta"],
                "q3_minus_q2_s": references_payload["median_q3_delta"],
            },
        },
        "cards": {
            "pole_favorite": _top_driver("pole_probability"),
            "front_row_favorite": _top_driver("front_row_probability"),
            "bubble_driver": q2_bubble,
        },
        "reference_grid": [
            {
                "position": int(row.get("position", 0) or 0),
                "driver": row.get("driver"),
                "q1": row.get("q1"),
                "q2": row.get("q2"),
                "q3": row.get("q3"),
            }
            for row in qualifying_rows
        ],
        "expected_grid": expected_grid_rows,
        "comparison": comparison_rows,
        "driver_details": driver_details,
        "heatmap": heatmap_rows,
        "session_average_best_lap_s": {
            "q1": round(session_time_totals["q1"] / max(rounded_simulations * field_size, 1), 3),
            "q2": round(session_time_totals["q2"] / max(rounded_simulations * q2_count, 1), 3),
            "q3": round(session_time_totals["q3"] / max(rounded_simulations * q3_count, 1), 3),
        },
    }
    QUALI_SIM_CACHE[cache_key] = payload
    return payload


def _fallback_positions(round_payload: dict, season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    session_rows = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")
    results = sorted(session_rows or [], key=lambda r: int(r.get("position", 999)))
    drivers = [row["driver"] for row in results]
    if not drivers:
        return {
            "season": season,
            "round": round_no,
            "race": round_payload.get("race"),
            "session": normalized_session,
            "drivers": [],
            "laps": [],
        }

    start_positions = {}
    if normalized_session == "sprint":
        sprint_qualifying = round_payload.get("sprint_qualifying") or []
        sq_by_driver = {row.get("driver"): int(row.get("position", 0) or 0) for row in sprint_qualifying if row.get("driver")}
        for row in results:
            sq_grid = int(sq_by_driver.get(row["driver"], 0) or 0)
            start_positions[row["driver"]] = max(1, sq_grid or int(row.get("position", 0) or 1))
    else:
        start_positions = {row["driver"]: max(1, int(row.get("grid", row["position"]))) for row in results}
    finish_positions = {row["driver"]: max(1, int(row["position"])) for row in results}
    total_laps = expected_lap_count(season, round_no, load_official_results_rows(season, round_no, normalized_session)) or (24 if normalized_session == "sprint" else 57)
    rows: list[dict] = []

    for lap in range(1, total_laps + 1):
        progress = (lap - 1) / max(total_laps - 1, 1)
        scores = []
        for idx, driver in enumerate(drivers):
            start_pos = start_positions[driver]
            finish_pos = finish_positions[driver]
            trend = start_pos + ((finish_pos - start_pos) * progress)
            wiggle = np.sin((lap + idx * 3) / 4.2) * (1.8 * (1.0 - progress))
            score = trend + wiggle
            scores.append((driver, score))
        scores.sort(key=lambda item: (item[1], item[0]))
        row = {"lap": lap}
        for pos, (driver, _) in enumerate(scores, start=1):
            row[driver] = pos
        rows.append(row)

    return {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "session": normalized_session,
        "drivers": [
            {"driver": d, "team": next((r["team"] for r in results if r["driver"] == d), None)}
            for d in drivers
        ],
        "laps": rows,
        "dnf_drivers": [
            r["driver"]
            for r in results
            if not is_classified_finisher_status(r.get("status"))
        ],
        "source": "synthetic",
    }


def _empty_positions_payload(round_payload: dict, season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    return {
        "season": season,
        "round": round_no,
        "race": round_payload.get("race"),
        "session": normalized_session,
        "drivers": [],
        "laps": [],
        "dnf_drivers": [],
        "completed_laps_by_driver": {},
        "source": "unavailable",
    }


def build_round_positions(season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    key = (season, round_no, normalized_session)
    round_payload = get_round_payload(season, round_no)
    race = round_payload.get("race")
    session_rows = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")

    cached = POSITIONS_CACHE.get(key)
    if cached and str(cached.get("source", "")).lower() != "synthetic":
        repaired_cached = repair_cached_positions_payload(cached, round_payload, normalized_session)
        POSITIONS_CACHE[key] = repaired_cached
        return repaired_cached
    disk = load_positions_from_disk(season, round_no, normalized_session)
    if disk is not None and str(disk.get("source", "")).lower() != "synthetic":
        repaired_disk = repair_cached_positions_payload(disk, round_payload, normalized_session)
        POSITIONS_CACHE[key] = repaired_disk
        if repaired_disk != disk:
            save_positions_to_disk(repaired_disk)
        return repaired_disk

    try:
        raw_results = load_official_results_rows(season, round_no, normalized_session)
        target_laps = expected_lap_count(season, round_no, raw_results)

        driver_id_to_name: dict[str, str] = {}
        driver_team_map: dict[str, str] = {}
        session_driver_names = {
            normalize_driver_name_key(row.get("driver")): row.get("driver")
            for row in session_rows or []
            if row.get("driver")
        }
        for row in raw_results:
            driver = row.get("Driver", {})
            driver_id = driver.get("driverId")
            driver_name = map_driver(driver) if driver else None
            session_name = resolve_canonical_driver_name(driver_name, session_driver_names)
            if session_name:
                driver_name = session_name
            if driver_id and driver_name:
                driver_id_to_name[driver_id] = driver_name
            if driver_name:
                driver_team_map[driver_name] = normalize_constructor_name(season, row.get("Constructor", {}).get("name"))

        for result in session_rows or []:
            driver_team_map.setdefault(result["driver"], result.get("team"))

        if normalized_session == "sprint":
            openf1_lap_times, openf1_max_lap = load_openf1_lap_times_by_driver(season, round_payload, normalized_session)
            total_laps = target_laps or openf1_max_lap
            if not openf1_lap_times or total_laps <= 0:
                return _empty_positions_payload(round_payload, season, round_no, normalized_session)
            payload = _build_positions_from_lap_times(
                round_payload=round_payload,
                season=season,
                round_no=round_no,
                session_kind=normalized_session,
                session_rows=session_rows or [],
                driver_team_map=driver_team_map,
                lap_times_by_driver=openf1_lap_times,
                target_laps=total_laps,
                source="openf1",
            )
            POSITIONS_CACHE[key] = payload
            save_positions_to_disk(payload)
            return payload

        first_page = fetch_positions_fast(f"{season}/{round_no}/laps.json", limit=100, offset=0)
        first_mr = first_page.get("MRData", {})
        try:
            total_rows = int(first_mr.get("total", 0))
            limit_rows = max(1, int(first_mr.get("limit", 100)))
        except (TypeError, ValueError):
            total_rows = 0
            limit_rows = 100

        all_pages = [first_page]
        for offset in range(limit_rows, total_rows, limit_rows):
            all_pages.append(fetch_positions_fast(f"{season}/{round_no}/laps.json", limit=limit_rows, offset=offset))

        lap_map: dict[int, dict] = {}
        lap_timing_seconds: dict[int, dict[str, float]] = {}
        for page in all_pages:
            page_races = page.get("MRData", {}).get("RaceTable", {}).get("Races", [])
            page_payload = page_races[0] if page_races else {}
            for lap in page_payload.get("Laps", []):
                try:
                    lap_no = int(lap.get("number", 0))
                except (TypeError, ValueError):
                    continue
                if lap_no <= 0:
                    continue
                lap_row = lap_map.setdefault(lap_no, {"lap": lap_no})
                timing_row = lap_timing_seconds.setdefault(lap_no, {})
                for timing in lap.get("Timings", []):
                    driver_name = driver_id_to_name.get(timing.get("driverId", ""))
                    if not driver_name:
                        continue
                    lap_time_s = parse_lap_time_to_seconds(timing.get("time"))
                    if lap_time_s is not None:
                        timing_row[driver_name] = lap_time_s
                    try:
                        position = int(timing.get("position"))
                    except (TypeError, ValueError):
                        continue
                    lap_row[driver_name] = position

        laps = [lap_map[n] for n in sorted(lap_map.keys())]
        if not laps:
            openf1_lap_times, openf1_max_lap = load_openf1_lap_times_by_driver(season, round_payload, normalized_session)
            total_laps = target_laps or openf1_max_lap
            if not openf1_lap_times or total_laps <= 0:
                return _empty_positions_payload(round_payload, season, round_no, normalized_session)
            payload = _build_positions_from_lap_times(
                round_payload=round_payload,
                season=season,
                round_no=round_no,
                session_kind=normalized_session,
                session_rows=session_rows or [],
                driver_team_map=driver_team_map,
                lap_times_by_driver=openf1_lap_times,
                target_laps=total_laps,
                source="openf1",
            )
            POSITIONS_CACHE[key] = payload
            save_positions_to_disk(payload)
            return payload

        if target_laps > 0:
            normalized = []
            previous_row: dict | None = None
            for lap_no in range(1, target_laps + 1):
                source = lap_map.get(lap_no)
                if source is not None:
                    row = {"lap": lap_no}
                    for key, value in source.items():
                        if key != "lap":
                            row[key] = value
                    previous_row = row
                else:
                    row = {"lap": lap_no}
                    if previous_row is not None:
                        for key, value in previous_row.items():
                            if key != "lap":
                                row[key] = value
                    previous_row = row
                normalized.append(row)
            laps = normalized

        rows: list[dict] = []
        max_position = 0
        parsed_any_positions = False
        for lap in laps:
            row = {"lap": int(lap["lap"])}
            for driver_name, position in lap.items():
                if driver_name == "lap":
                    continue
                if not isinstance(position, int):
                    continue
                parsed_any_positions = True
                row[driver_name] = position
                max_position = max(max_position, position)
            rows.append(row)

        if not parsed_any_positions and target_laps > 0:
            rows = reconstruct_positions_from_lap_times(lap_timing_seconds, target_laps)
            for row in rows:
                for driver_name, position in row.items():
                    if driver_name == "lap" or not isinstance(position, int):
                        continue
                    max_position = max(max_position, position)

        rows = canonicalize_position_rows(rows, session_driver_names)

        grid_order = []
        for r in session_rows or []:
            grid = int(r.get("grid", 0) or 0)
            if grid <= 0:
                grid = int(r.get("position", 999) or 999)
            grid_order.append((r["driver"], grid))
        grid_order.sort(key=lambda item: (item[1], item[0]))
        if grid_order:
            lap0 = {"lap": 0}
            for idx, (driver_name, _) in enumerate(grid_order, start=1):
                lap0[driver_name] = idx
            rows = [lap0, *rows]
            max_position = max(max_position, len(grid_order))

        lap1 = rows[0] if rows else {}
        started_drivers = [
            driver_name
            for driver_name, pos in sorted(
                ((k, v) for k, v in lap1.items() if k != "lap" and isinstance(v, int)),
                key=lambda item: (item[1], item[0]),
            )
        ]
        finish_order = [r["driver"] for r in sorted(session_rows or [], key=lambda x: int(x["position"]))]
        drivers_in_finish_order = []
        seen = set()
        for name in started_drivers + finish_order:
            if name in seen:
                continue
            seen.add(name)
            drivers_in_finish_order.append(name)
        payload = {
            "season": season,
            "round": round_no,
            "race": race,
            "session": normalized_session,
            "drivers": [
                {"driver": driver, "team": driver_team_map.get(driver)}
                for driver in drivers_in_finish_order
            ],
            "laps": rows,
            "max_position": max_position or len(drivers_in_finish_order),
            "dnf_drivers": [
                r["driver"]
                for r in session_rows or []
                if not is_classified_finisher_status(r.get("status"))
            ],
            "completed_laps_by_driver": {
                driver_name: max(
                    (int(lap_no) for lap_no, lap_values in lap_timing_seconds.items() if driver_name in (lap_values or {})),
                    default=0,
                )
                for driver_name in driver_team_map.keys()
            },
            "source": "jolpica",
        }
        POSITIONS_CACHE[key] = payload
        save_positions_to_disk(payload)
        return payload
    except Exception:
        return _empty_positions_payload(round_payload, season, round_no, normalized_session)


def build_engineering_telemetry(round_no: int, driver_result: dict) -> dict:
    seed = (round_no * 1000) + sum(ord(c) for c in driver_result["driver"])
    rng = np.random.default_rng(seed)
    lap_len = 70
    position = max(1, int(driver_result["position"]))

    # Better race positions tend to produce stronger traces.
    base_speed = 295 - position * 1.8 + rng.normal(0, 1.2)
    speed = [base_speed + np.sin(i / 6.0) * 18 + rng.normal(0, 1.8) for i in range(lap_len)]
    throttle = [88 + np.sin(i / 8.5) * 11 - (position * 0.22) + rng.normal(0, 2.2) for i in range(lap_len)]
    brake = [max(0.0, 22 - np.sin(i / 4.3) * 24 + rng.normal(0, 2.0) + position * 0.28) for i in range(lap_len)]

    speed = [max(120.0, min(360.0, x)) for x in speed]
    throttle = [max(0.0, min(100.0, x)) for x in throttle]
    brake = [max(0.0, min(100.0, x)) for x in brake]

    smoothed = {
        "speed": moving_average(speed, 4),
        "throttle": moving_average(throttle, 4),
        "brake": moving_average(brake, 4),
    }

    telemetry_summary = {
        "max_speed": round(max(speed), 2),
        "avg_speed": round(float(np.mean(speed)), 2),
        "drs_usage_estimate_pct": round(sum(1 for x in throttle if x > 95) / len(throttle) * 100, 2),
    }
    return {"smoothed": smoothed, "summary": telemetry_summary}


@app.post("/api/engineering/driver-analysis")
def engineering_driver_analysis(payload: EngineeringDriverInput) -> dict:
    round_payload = get_round_payload(payload.season, payload.round_no)
    driver_result = get_driver_result(payload.season, payload.round_no, payload.driver)

    return {
        "season": payload.season,
        "round": payload.round_no,
        "race": round_payload["race"],
        "driver": payload.driver,
        "team": driver_result["team"],
        "race_result": {
            "position": driver_result["position"],
            "points": driver_result["points"],
            "status": driver_result["status"],
            "grid": driver_result["grid"],
        },
        "source": "unavailable",
        "telemetry": None,
        "lap_prediction": None,
        "strategy": None,
        "network": None,
        "explanations": {
            "telemetry": "No data",
            "lap_prediction": "No data",
            "strategy": "No data",
            "network": "No data",
        },
    }


@app.get("/api/engineering/positions/{round_no}")
def engineering_positions(
    round_no: int,
    season: int = Query(2025),
    session: str = Query("race"),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    return build_round_positions(season, round_no, session)


@app.get("/api/engineering/tyre-strategy/{round_no}")
def engineering_tyre_strategy(
    round_no: int,
    season: int = Query(2025),
    session: str = Query("race"),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    round_payload = get_round_payload(season, round_no)
    return build_tyre_strategy(round_payload, season, round_no, session)


@app.get("/api/engineering/h2h/{round_no}")
def engineering_h2h(
    round_no: int,
    season: int = Query(2025),
    driver_a: str | None = Query(None),
    driver_b: str | None = Query(None),
    session: str = Query("race"),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    return build_h2h_payload(season, round_no, driver_a, driver_b, session)


@app.get("/api/engineering/telemetry/{round_no}")
def engineering_round_telemetry(
    round_no: int,
    season: int = Query(2025),
    session: str = Query("race"),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    return build_round_telemetry_catalog(season, round_no, session)


@app.get("/api/engineering/telemetry/{round_no}/trace")
def engineering_round_telemetry_trace(
    round_no: int,
    season: int = Query(2025),
    driver: str = Query(""),
    lap: str = Query("fastest"),
    session: str = Query("race"),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025

    catalog = build_round_telemetry_catalog(season, round_no, session)
    available_drivers = [item.get("driver") for item in catalog.get("drivers", []) if item.get("driver")]
    resolved_driver = _resolve_h2h_driver_name(driver, available_drivers)
    if not resolved_driver:
        raise HTTPException(status_code=404, detail="Driver not found for telemetry selection.")

    driver_entry = next(
        (item for item in catalog.get("drivers", []) if item.get("driver") == resolved_driver),
        None,
    )
    if not driver_entry:
        raise HTTPException(status_code=404, detail="Telemetry driver payload unavailable.")

    available_laps = [int(x) for x in driver_entry.get("laps", []) if isinstance(x, int) and int(x) > 0]

    round_payload = get_round_payload(season, round_no)
    requested_lap = str(lap or "").strip().lower()
    if requested_lap not in {"fastest", "fast"}:
        try:
            int(lap)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Lap must be a number or 'fastest'.")

    openf1_trace: dict | None = None
    notes: list[str] = []
    try:
        openf1_trace = _build_openf1_telemetry_trace(
            season=season,
            round_payload=round_payload,
            session_kind=session,
            driver_name=resolved_driver,
            requested_lap=lap,
        )
    except Exception:
        openf1_trace = None

    if openf1_trace and openf1_trace.get("samples"):
        samples = openf1_trace["samples"]
        selected_lap = int(openf1_trace.get("selected_lap") or 0)
        response_available_laps = [
            int(x) for x in (openf1_trace.get("available_laps") or []) if isinstance(x, int) and int(x) > 0
        ]
        fastest_lap_payload = openf1_trace.get("fastest_lap")
        source = "openf1"
    else:
        return {
            "season": season,
            "round": round_no,
            "race": catalog.get("race"),
            "session": catalog.get("session"),
            "source": "unavailable",
            "notes": [],
            "driver": {
                "name": resolved_driver,
                "team": driver_entry.get("team"),
                "position": driver_entry.get("position"),
            },
            "lap": {
                "requested": lap,
                "selected": None,
                "fastest_lap": driver_entry.get("fastest_lap"),
                "available_laps": available_laps,
            },
            "stats": {
                "speed": {"min": None, "max": None, "avg": None},
                "gear": {"min": None, "max": None, "avg": None},
                "throttle": {"min": None, "max": None, "avg": None},
                "brake": {"min": None, "max": None, "avg": None},
                "rpm": {"min": None, "max": None, "avg": None},
                "drs": {"min": None, "max": None, "avg": None},
            },
            "samples": [],
        }

    def _metric_stats(key: str) -> dict:
        values = [float(item.get(key)) for item in samples if isinstance(item.get(key), (int, float))]
        if not values:
            return {"min": None, "max": None, "avg": None}
        return {
            "min": round(float(min(values)), 3),
            "max": round(float(max(values)), 3),
            "avg": round(float(np.mean(values)), 3),
        }

    return {
        "season": season,
        "round": round_no,
        "race": catalog.get("race"),
        "session": catalog.get("session"),
        "source": source,
        "notes": notes,
        "driver": {
            "name": resolved_driver,
            "team": openf1_trace.get("team") if openf1_trace and openf1_trace.get("team") else driver_entry.get("team"),
            "position": driver_entry.get("position"),
        },
        "lap": {
            "requested": lap,
            "selected": selected_lap,
            "fastest_lap": fastest_lap_payload,
            "available_laps": response_available_laps,
        },
        "stats": {
            "speed": _metric_stats("speed"),
            "gear": _metric_stats("gear"),
            "throttle": _metric_stats("throttle"),
            "brake": _metric_stats("brake"),
            "rpm": _metric_stats("rpm"),
            "drs": _metric_stats("drs"),
        },
        "samples": samples,
    }


@app.get("/api/engineering/quali-simulator/{round_no}")
def engineering_quali_simulator(
    round_no: int,
    season: int = Query(2025),
    simulations: int = Query(5000, ge=400, le=20000),
    chaos: float = Query(0.0, ge=0.0, le=1.0),
    form_bias: float = Query(0.0, ge=0.0, le=1.0),
) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    return build_qualifying_simulator(season, round_no, simulations, chaos, form_bias)


@app.post("/api/engineering/telemetry/analyze")
def telemetry_analyze(payload: TelemetryPayload) -> dict:
    return {
        "source": "unavailable",
        "smoothed": None,
        "summary": None,
    }


@app.post("/api/engineering/lap/predict")
def predict_lap_time(features: LapPredictionInput) -> dict:
    return {
        "source": "unavailable",
        "predicted_lap_time_s": None,
        "input": features.model_dump(),
        "model": None,
    }


@app.post("/api/engineering/strategy/simulate")
def strategy_simulation(payload: StrategyInput) -> dict:
    return {
        "source": "unavailable",
        "best_pit_lap": None,
        "best_avg_total_time_s": None,
        "candidates": [],
    }


@app.post("/api/engineering/network/simulate")
def network_simulation(payload: NetworkInput) -> dict:
    return {
        "source": "unavailable",
        "sent": payload.packets,
        "received": None,
        "loss_pct": None,
        "avg_latency_ms": None,
        "jitter_ms": None,
        "p95_latency_ms": None,
        "strategy_decision_risk": None,
    }


@app.websocket("/ws/network-stream")
async def network_stream(socket: WebSocket) -> None:
    await socket.accept()
    try:
        while True:
            packet = {
                "latency_ms": round(max(0.0, random.gauss(55, 12)), 2),
                "jitter_ms": round(max(0.0, random.gauss(9, 3)), 2),
                "loss": random.random() < 0.02,
                "queue_depth": random.randint(2, 12),
            }
            await socket.send_json(packet)
            await asyncio.sleep(0.75)
    except Exception:
        await socket.close()
