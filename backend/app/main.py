from __future__ import annotations

import asyncio
import json
import os
import random
import time
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

from .season_data import BASE, SUPPORTED_SEASONS, fetch, load_or_build_season, map_driver, normalize_constructor_name

app = FastAPI(title="F1 Dual-Mode Analytics API", version="0.1.0")

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
POSITIONS_CACHE: dict[tuple[int, int], dict] = {}
POSITIONS_CACHE_DIR = Path(__file__).resolve().parents[1] / "data" / "positions_cache"
POSITIONS_CACHE_VERSION = 4


def get_season_data(season: int) -> dict:
    if season not in SUPPORTED_SEASONS:
        raise HTTPException(
            status_code=400,
            detail=f"Season {season} is not supported. Supported: {SUPPORTED_SEASONS[0]}-{SUPPORTED_SEASONS[-1]}",
        )
    if season not in SEASON_CACHE:
        try:
            SEASON_CACHE[season] = load_or_build_season(season)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Failed to load season {season}: {exc}") from exc
    return SEASON_CACHE[season]


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


def positions_cache_file(season: int, round_no: int) -> Path:
    return POSITIONS_CACHE_DIR / f"{season}_{round_no}.json"


def load_positions_from_disk(season: int, round_no: int) -> dict | None:
    target = positions_cache_file(season, round_no)
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
    if int(payload.get("cache_version", 0)) != POSITIONS_CACHE_VERSION:
        return None
    return payload


def save_positions_to_disk(payload: dict) -> None:
    season = payload.get("season")
    round_no = payload.get("round")
    if not isinstance(season, int) or not isinstance(round_no, int):
        return
    POSITIONS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    target = positions_cache_file(season, round_no)
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


def load_official_results_rows(season: int, round_no: int) -> list[dict]:
    # Try fast path first, then resilient shared fetch fallback.
    try:
        payload = fetch_positions_fast(f"{season}/{round_no}/results.json")
        races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
        return (races[0] if races else {}).get("Results", [])
    except Exception:
        try:
            payload = fetch(f"{season}/{round_no}/results.json")
            races = payload.get("MRData", {}).get("RaceTable", {}).get("Races", [])
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
        key = (season, round_no)
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
        "cached_position_keys": [f"{k[0]}-{k[1]}" for k in sorted(POSITIONS_CACHE.keys())],
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
        "constructor_standings": season_data["constructor_standings"][:10],
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
            return item
    raise HTTPException(status_code=404, detail=f"Round {round_no} not found for season {season_data['season']}")


@app.get("/api/casual/rounds")
def casual_rounds(season: int = Query(2025)) -> dict:
    season_data = get_season_data(season)
    return {
        "season": season_data["season"],
        "rounds": season_data.get("rounds_summary", []),
        "count": len(season_data.get("rounds_summary", [])),
    }


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


def _fallback_positions(round_payload: dict, season: int, round_no: int) -> dict:
    results = sorted(round_payload.get("results", []), key=lambda r: int(r.get("position", 999)))
    drivers = [row["driver"] for row in results]
    if not drivers:
        return {"season": season, "round": round_no, "race": round_payload.get("race"), "drivers": [], "laps": []}

    start_positions = {row["driver"]: max(1, int(row.get("grid", row["position"]))) for row in results}
    finish_positions = {row["driver"]: max(1, int(row["position"])) for row in results}
    total_laps = expected_lap_count(season, round_no, load_official_results_rows(season, round_no)) or 57
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
        "drivers": [
            {"driver": d, "team": next((r["team"] for r in results if r["driver"] == d), None)}
            for d in drivers
        ],
        "laps": rows,
        "dnf_drivers": [
            r["driver"]
            for r in round_payload.get("results", [])
            if not is_classified_finisher_status(r.get("status"))
        ],
        "source": "synthetic",
    }


def build_round_positions(season: int, round_no: int) -> dict:
    key = (season, round_no)
    cached = POSITIONS_CACHE.get(key)
    if cached and str(cached.get("source", "")).lower() != "synthetic":
        return cached
    disk = load_positions_from_disk(season, round_no)
    if disk is not None and str(disk.get("source", "")).lower() != "synthetic":
        POSITIONS_CACHE[key] = disk
        return disk

    round_payload = get_round_payload(season, round_no)
    race = round_payload.get("race")

    try:
        raw_results = load_official_results_rows(season, round_no)
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

        for result in round_payload.get("results", []):
            driver_team_map.setdefault(result["driver"], result.get("team"))

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
            fallback = _fallback_positions(round_payload, season, round_no)
            POSITIONS_CACHE[key] = fallback
            return fallback

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

        if not rows:
            fallback = _fallback_positions(round_payload, season, round_no)
            POSITIONS_CACHE[key] = fallback
            return fallback

        grid_order = []
        for r in round_payload.get("results", []):
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
        finish_order = [r["driver"] for r in sorted(round_payload.get("results", []), key=lambda x: int(x["position"]))]
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
            "drivers": [
                {"driver": driver, "team": driver_team_map.get(driver)}
                for driver in drivers_in_finish_order
            ],
            "laps": rows,
            "max_position": max_position or len(drivers_in_finish_order),
            "dnf_drivers": [
                r["driver"]
                for r in round_payload.get("results", [])
                if not is_classified_finisher_status(r.get("status"))
            ],
            "source": "jolpica",
        }
        POSITIONS_CACHE[key] = payload
        save_positions_to_disk(payload)
        return payload
    except Exception:
        fallback = _fallback_positions(round_payload, season, round_no)
        POSITIONS_CACHE[key] = fallback
        # Do not persist synthetic fallback; allow future rebuild attempts.
        return fallback


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

    telemetry = build_engineering_telemetry(payload.round_no, driver_result)

    tyre_by_position = "SOFT" if int(driver_result["position"]) <= 6 else "MEDIUM"
    lap_pred = predict_lap_time(
        LapPredictionInput(
            tyre_compound=tyre_by_position,
            sector1=30.7 + int(driver_result["position"]) * 0.04,
            sector2=36.9 + int(driver_result["position"]) * 0.05,
            sector3=24.6 + int(driver_result["position"]) * 0.03,
            tyre_age_laps=8 + int(driver_result["position"]) // 2,
            track_temp_c=33.0 + (payload.round_no % 5),
        )
    )

    strategy = strategy_simulation(
        StrategyInput(
            total_laps=57,
            pit_window_start=13 + (payload.round_no % 3),
            pit_window_end=31 + (payload.round_no % 2),
            simulations=500,
        )
    )

    network = network_simulation(
        NetworkInput(
            packets=160,
            base_latency_ms=42 + int(driver_result["position"]) * 0.8 + (payload.round_no % 4),
            jitter_ms=8 + int(driver_result["position"]) * 0.25,
            packet_loss_rate=min(0.12, 0.02 + int(driver_result["position"]) * 0.002),
            bandwidth_mbps=max(2.0, 4.8 - int(driver_result["position"]) * 0.1),
        )
    )

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
        "telemetry": telemetry,
        "lap_prediction": lap_pred,
        "strategy": strategy,
        "network": network,
        "explanations": {
            "telemetry": "Speed, throttle, and brake traces are estimated per driver and round. Better finishing position generally maps to stronger pace and smoother braking.",
            "lap_prediction": "Predicted lap time is inferred from synthetic sector profile, tyre choice, tyre age, and track temperature calibrated by race result context.",
            "strategy": "Monte Carlo simulation sweeps pit windows to estimate total race time distribution and recommend the most competitive pit lap.",
            "network": "Telemetry link quality model estimates latency, jitter, and packet loss; higher delay and loss increase pit-wall decision risk.",
        },
    }


@app.get("/api/engineering/positions/{round_no}")
def engineering_positions(round_no: int, season: int = Query(2025)) -> dict:
    if season not in SUPPORTED_SEASONS:
        season = 2025
    return build_round_positions(season, round_no)


@app.post("/api/engineering/telemetry/analyze")
def telemetry_analyze(payload: TelemetryPayload) -> dict:
    speed_smooth = moving_average(payload.speed, payload.window)
    throttle_smooth = moving_average(payload.throttle, payload.window)
    brake_smooth = moving_average(payload.brake, payload.window)

    drs_usage_pct = 0.0
    if payload.throttle:
        drs_usage_pct = sum(1 for x in payload.throttle if x > 95) / len(payload.throttle) * 100

    return {
        "smoothed": {
            "speed": speed_smooth,
            "throttle": throttle_smooth,
            "brake": brake_smooth,
        },
        "summary": {
            "max_speed": max(payload.speed) if payload.speed else 0,
            "avg_speed": float(np.mean(payload.speed)) if payload.speed else 0,
            "drs_usage_estimate_pct": round(drs_usage_pct, 2),
        },
    }


@app.post("/api/engineering/lap/predict")
def predict_lap_time(features: LapPredictionInput) -> dict:
    compound_map = {"SOFT": 0, "MEDIUM": 1, "HARD": 2}
    x = np.array([
        [
            compound_map[features.tyre_compound],
            features.sector1,
            features.sector2,
            features.sector3,
            features.tyre_age_laps,
            features.track_temp_c,
        ]
    ])
    pred = model.predict(x)[0]

    return {
        "predicted_lap_time_s": round(float(pred), 3),
        "input": features.model_dump(),
        "model": "LinearRegression-v1",
    }


@app.post("/api/engineering/strategy/simulate")
def strategy_simulation(payload: StrategyInput) -> dict:
    rng = np.random.default_rng(22)
    outcomes = []

    for _ in range(payload.simulations):
        pit_lap = int(rng.integers(payload.pit_window_start, payload.pit_window_end + 1))
        base = 5400.0
        tyre_deg = pit_lap * 0.12 + (payload.total_laps - pit_lap) * 0.08
        traffic = rng.normal(0, 2.5)
        pit_delta = rng.normal(20.5, 1.2)
        total = base + tyre_deg + pit_delta + traffic
        outcomes.append((pit_lap, total))

    by_lap: dict[int, list[float]] = {}
    for lap, time_s in outcomes:
        by_lap.setdefault(lap, []).append(time_s)

    avg_by_lap = [{"pit_lap": lap, "avg_total_time_s": float(np.mean(times))} for lap, times in by_lap.items()]
    avg_by_lap.sort(key=lambda x: x["avg_total_time_s"])
    best = avg_by_lap[0]

    return {
        "best_pit_lap": best["pit_lap"],
        "best_avg_total_time_s": round(best["avg_total_time_s"], 3),
        "candidates": avg_by_lap[:10],
    }


@app.post("/api/engineering/network/simulate")
def network_simulation(payload: NetworkInput) -> dict:
    rng = np.random.default_rng(5)
    latencies = []
    lost = 0

    for _ in range(payload.packets):
        if rng.random() < payload.packet_loss_rate:
            lost += 1
            continue
        sample = max(0.0, rng.normal(payload.base_latency_ms, payload.jitter_ms))
        bandwidth_penalty = 15.0 / max(payload.bandwidth_mbps, 0.1)
        latencies.append(sample + bandwidth_penalty)

    received = len(latencies)
    jitter = float(np.std(latencies)) if latencies else 0.0
    avg = float(np.mean(latencies)) if latencies else 0.0
    p95 = float(np.percentile(latencies, 95)) if latencies else 0.0

    strategy_risk = "LOW"
    if avg > 90 or (lost / max(payload.packets, 1)) > 0.08:
        strategy_risk = "HIGH"
    elif avg > 65 or (lost / max(payload.packets, 1)) > 0.04:
        strategy_risk = "MEDIUM"

    return {
        "sent": payload.packets,
        "received": received,
        "loss_pct": round((lost / max(payload.packets, 1)) * 100, 2),
        "avg_latency_ms": round(avg, 2),
        "jitter_ms": round(jitter, 2),
        "p95_latency_ms": round(p95, 2),
        "strategy_decision_risk": strategy_risk,
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
