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
POSITIONS_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "positions_cache"
POSITIONS_CACHE_VERSION = 5


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
    raw_laps = fetch_openf1("laps", session_key=session_key, driver_number=driver_number)
    raw_car = fetch_openf1("car_data", session_key=session_key, driver_number=driver_number)

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

    raw_results = load_official_results_rows(season, round_no, normalized_session)
    lap_times_by_driver: dict[str, dict[int, float]] = {}
    max_lap_seen = 0
    source = "unavailable"
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
        if not lap_times_by_driver:
            try:
                lap_times_by_driver, max_lap_seen = load_openf1_lap_times_by_driver(season, round_payload, normalized_session)
                if lap_times_by_driver:
                    source = "openf1"
            except Exception:
                lap_times_by_driver, max_lap_seen = {}, 0

    expected_total_laps = expected_lap_count(season, round_no, raw_results)
    fallback_laps = 24 if normalized_session == "sprint" else 57
    total_laps = max(1, expected_total_laps or max_lap_seen or fallback_laps)
    if not lap_times_by_driver:
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

    drivers: list[dict] = []
    for row in rows:
        driver_name = row.get("driver")
        if not driver_name:
            continue
        lap_times = lap_times_by_driver.get(driver_name, {})
        available_laps = sorted(int(lap_no) for lap_no in lap_times.keys() if int(lap_no) > 0)
        if not available_laps:
            continue

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

    raw_laps = fetch_openf1("laps", session_key=session_key, driver_number=driver_number)
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

    raw_car = fetch_openf1("car_data", session_key=session_key, driver_number=driver_number)
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


def fetch_openf1(path: str, **query: str | int) -> list[dict]:
    qs = f"?{urlencode(query)}" if query else ""
    url = f"{OPENF1_BASE}/{path}{qs}"
    attempts = 0
    while True:
        attempts += 1
        try:
            with urlopen(url, timeout=18) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data if isinstance(data, list) else []
        except HTTPError as exc:
            if exc.code == 429 and attempts < 4:
                time.sleep(0.35 * attempts)
                continue
            raise
        except URLError:
            if attempts < 4:
                time.sleep(0.35 * attempts)
                continue
            raise
        except TimeoutError:
            if attempts < 4:
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
    sessions = fetch_openf1("sessions", year=season, session_name=normalized_name)
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
    openf1_drivers = fetch_openf1("drivers", session_key=session_key)
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
        "points_progression": season_data["points_progression"],
        "progression_drivers": season_data["progression_drivers"],
        "constructor_points_progression": season_data["constructor_points_progression"],
        "progression_constructors": season_data["progression_constructors"],
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
    return {
        "season": season_data["season"],
        "rounds": season_data.get("rounds_summary", []),
        "count": len(season_data.get("rounds_summary", [])),
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
        "source": "unavailable",
    }


def build_round_positions(season: int, round_no: int, session_kind: str = "race") -> dict:
    normalized_session = str(session_kind or "race").strip().lower()
    if normalized_session not in {"race", "sprint"}:
        normalized_session = "race"
    key = (season, round_no, normalized_session)
    cached = POSITIONS_CACHE.get(key)
    if cached and str(cached.get("source", "")).lower() != "synthetic":
        return cached
    disk = load_positions_from_disk(season, round_no, normalized_session)
    if disk is not None and str(disk.get("source", "")).lower() != "synthetic":
        POSITIONS_CACHE[key] = disk
        return disk

    round_payload = get_round_payload(season, round_no)
    race = round_payload.get("race")
    session_rows = round_payload.get("sprint") if normalized_session == "sprint" else round_payload.get("results")

    try:
        raw_results = load_official_results_rows(season, round_no, normalized_session)
        target_laps = expected_lap_count(season, round_no, raw_results)

        driver_id_to_name: dict[str, str] = {}
        driver_team_map: dict[str, str] = {}
        for row in raw_results:
            driver = row.get("Driver", {})
            driver_id = driver.get("driverId")
            driver_name = map_driver(driver) if driver else None
            if driver_id and driver_name:
                driver_id_to_name[driver_id] = driver_name
            if driver_name:
                driver_team_map[driver_name] = normalize_constructor_name(season, row.get("Constructor", {}).get("name"))

        for result in session_rows or []:
            driver_team_map.setdefault(result["driver"], result.get("team"))

        if normalized_session == "sprint":
            return _empty_positions_payload(round_payload, season, round_no, normalized_session)

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
                for timing in lap.get("Timings", []):
                    driver_name = driver_id_to_name.get(timing.get("driverId", ""))
                    if not driver_name:
                        continue
                    try:
                        position = int(timing.get("position"))
                    except (TypeError, ValueError):
                        continue
                    lap_row[driver_name] = position

        laps = [lap_map[n] for n in sorted(lap_map.keys())]
        if not laps:
            return _empty_positions_payload(round_payload, season, round_no, normalized_session)

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
        for lap in laps:
            row = {"lap": int(lap["lap"])}
            for driver_name, position in lap.items():
                if driver_name == "lap":
                    continue
                if not isinstance(position, int):
                    continue
                row[driver_name] = position
                max_position = max(max_position, position)
            rows.append(row)

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
    if not available_laps:
        raise HTTPException(status_code=404, detail="No laps available for selected telemetry driver.")

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
