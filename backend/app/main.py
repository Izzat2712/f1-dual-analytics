from __future__ import annotations

import asyncio
import os
import random
from typing import Literal

import numpy as np
from fastapi import FastAPI, HTTPException, Query, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sklearn.linear_model import LinearRegression

from .season_data import SUPPORTED_SEASONS, load_or_build_season

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


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
