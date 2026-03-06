from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

BASE = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
SUPPORTED_SEASONS = list(range(2021, 2027))
_LAST_FETCH_TS = 0.0

PRESEASON_2026_DRIVER_STANDINGS = [
    {"driver": "Alexander Albon", "team": "Williams"},
    {"driver": "Fernando Alonso", "team": "Aston Martin"},
    {"driver": "Kimi Antonelli", "team": "Mercedes"},
    {"driver": "Oliver Bearman", "team": "Haas F1 Team"},
    {"driver": "Gabriel Bortoleto", "team": "Audi"},
    {"driver": "Valtteri Bottas", "team": "Cadillac"},
    {"driver": "Franco Colapinto", "team": "Alpine F1 Team"},
    {"driver": "Pierre Gasly", "team": "Alpine F1 Team"},
    {"driver": "Isack Hadjar", "team": "Red Bull"},
    {"driver": "Lewis Hamilton", "team": "Ferrari"},
    {"driver": "Nico Hulkenberg", "team": "Audi"},
    {"driver": "Liam Lawson", "team": "RB F1 Team"},
    {"driver": "Charles Leclerc", "team": "Ferrari"},
    {"driver": "Arvid Lindblad", "team": "RB F1 Team"},
    {"driver": "Lando Norris", "team": "McLaren"},
    {"driver": "Esteban Ocon", "team": "Haas F1 Team"},
    {"driver": "Sergio Perez", "team": "Cadillac"},
    {"driver": "Oscar Piastri", "team": "McLaren"},
    {"driver": "George Russell", "team": "Mercedes"},
    {"driver": "Carlos Sainz", "team": "Williams"},
    {"driver": "Lance Stroll", "team": "Aston Martin"},
    {"driver": "Max Verstappen", "team": "Red Bull"},
]

PRESEASON_2026_CONSTRUCTOR_STANDINGS = [
    "McLaren",
    "Ferrari",
    "Mercedes",
    "Red Bull",
    "Aston Martin",
    "Williams",
    "RB F1 Team",
    "Haas F1 Team",
    "Audi",
    "Alpine F1 Team",
    "Cadillac",
]


def fetch(path: str, **query: str | int) -> dict:
    global _LAST_FETCH_TS
    qs = f"?{urlencode(query)}" if query else ""
    url = f"{BASE}/{path}{qs}"
    attempts = 0
    while True:
        attempts += 1
        try:
            now = time.time()
            elapsed = now - _LAST_FETCH_TS
            if elapsed < 0.22:
                time.sleep(0.22 - elapsed)
            with urlopen(url, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
            _LAST_FETCH_TS = time.time()
            return payload
        except HTTPError as exc:
            if exc.code == 429 and attempts < 12:
                time.sleep(min(25.0, 2.0 * attempts))
                continue
            raise
        except URLError:
            if attempts < 12:
                time.sleep(min(25.0, 2.0 * attempts))
                continue
            raise


def map_driver(driver: dict) -> str:
    return f"{driver['givenName']} {driver['familyName']}"


def normalize_constructor_name(season: int, constructor_name: str | None) -> str | None:
    if not constructor_name:
        return constructor_name
    normalized = str(constructor_name).strip()
    lowered = normalized.lower()
    if season >= 2026 and constructor_name in {"Sauber", "Kick Sauber"}:
        return "Audi"
    if "cadillac" in lowered:
        return "Cadillac"
    return normalized


def season_file(season: int) -> Path:
    return DATA_DIR / f"season_{season}.json"


def extract_round_summary(season: int, race: dict, quali: dict | None, sprint: dict | None) -> dict:
    race_results = race.get("Results", [])
    race_winner = race_results[0] if race_results else {}
    fastest_lap = None
    for item in race_results:
        if item.get("FastestLap", {}).get("rank") == "1":
            fastest_lap = item
            break

    quali_results = (quali or {}).get("QualifyingResults", [])
    sprint_results = (sprint or {}).get("SprintResults", [])

    return {
        "round": int(race["round"]),
        "race_name": race["raceName"],
        "date": race.get("date"),
        "circuit": race.get("Circuit", {}).get("circuitName"),
        "country": race.get("Circuit", {}).get("Location", {}).get("country"),
        "winner": map_driver(race_winner.get("Driver", {})) if race_winner else None,
        "winner_team": normalize_constructor_name(season, race_winner.get("Constructor", {}).get("name")),
        "pole": map_driver(quali_results[0]["Driver"]) if quali_results else None,
        "pole_time": quali_results[0].get("Q3") if quali_results else None,
        "fastest_lap_driver": map_driver(fastest_lap.get("Driver", {})) if fastest_lap else None,
        "fastest_lap_time": (fastest_lap or {}).get("FastestLap", {}).get("Time", {}).get("time"),
        "had_sprint": len(sprint_results) > 0,
    }


def validate_dataset(data: dict) -> None:
    standings = {row["driver"]: float(row["points"]) for row in data.get("driver_standings", [])}
    points_progression = data.get("points_progression", [])
    # Pre-season snapshots can exist with only schedule data and no standings yet.
    if not standings:
        return
    if not points_progression:
        raise RuntimeError("Season dataset has no points progression.")

    final_row = points_progression[-1]
    mismatched = [
        name for name, pts in standings.items()
        if abs(float(final_row.get(name, 0.0)) - pts) > 1e-6
    ]
    if mismatched:
        raise RuntimeError("Season dataset is inconsistent (progression != standings).")


def latest_driver_team_map(data: dict) -> dict[str, str]:
    latest_team: dict[str, str] = {}
    for round_payload in data.get("rounds", []):
        for section in ("results", "sprint", "qualifying", "sprint_qualifying"):
            for row in round_payload.get(section, []):
                driver = row.get("driver")
                team = row.get("team")
                if driver and team:
                    latest_team[driver] = team
    return latest_team


def align_driver_standings_teams(data: dict) -> bool:
    latest_team = latest_driver_team_map(data)
    changed = False
    for row in data.get("driver_standings", []):
        driver = row.get("driver")
        if not driver:
            continue
        mapped = latest_team.get(driver)
        if mapped and row.get("team") != mapped:
            row["team"] = mapped
            changed = True
    return changed


def compute_podium_counts_from_rounds(data: dict) -> tuple[dict[str, int], dict[str, int]]:
    driver_podiums: dict[str, int] = {}
    constructor_podiums: dict[str, int] = {}
    for round_payload in data.get("rounds", []):
        for row in round_payload.get("results", []):
            try:
                position = int(row.get("position"))
            except (TypeError, ValueError):
                continue
            if position < 1 or position > 3:
                continue
            driver = row.get("driver")
            team = row.get("team")
            if driver:
                driver_podiums[driver] = driver_podiums.get(driver, 0) + 1
            if team:
                constructor_podiums[team] = constructor_podiums.get(team, 0) + 1
    return driver_podiums, constructor_podiums


def ensure_standings_podiums(data: dict) -> bool:
    driver_podiums, constructor_podiums = compute_podium_counts_from_rounds(data)
    changed = False

    for row in data.get("driver_standings", []):
        driver = row.get("driver")
        podiums = int(driver_podiums.get(driver, 0))
        if row.get("podiums") != podiums:
            row["podiums"] = podiums
            changed = True

    for row in data.get("constructor_standings", []):
        team = row.get("team")
        podiums = int(constructor_podiums.get(team, 0))
        if row.get("podiums") != podiums:
            row["podiums"] = podiums
            changed = True

    return changed


def ensure_preseason_standings(data: dict) -> bool:
    season = int(data.get("season", 0) or 0)
    if season != 2026:
        return False
    if data.get("driver_standings") and data.get("constructor_standings"):
        return False

    changed = False
    if not data.get("driver_standings"):
        data["driver_standings"] = [
            {
                "position": idx,
                "driver": item["driver"],
                "team": item["team"],
                "points": 0.0,
                "wins": 0,
                "podiums": 0,
            }
            for idx, item in enumerate(PRESEASON_2026_DRIVER_STANDINGS, start=1)
        ]
        changed = True

    if not data.get("constructor_standings"):
        data["constructor_standings"] = [
            {
                "position": idx,
                "team": team,
                "points": 0.0,
                "wins": 0,
                "podiums": 0,
            }
            for idx, team in enumerate(PRESEASON_2026_CONSTRUCTOR_STANDINGS, start=1)
        ]
        changed = True

    if not data.get("progression_drivers"):
        data["progression_drivers"] = [item["driver"] for item in PRESEASON_2026_DRIVER_STANDINGS]
        changed = True

    if not data.get("progression_constructors"):
        data["progression_constructors"] = PRESEASON_2026_CONSTRUCTOR_STANDINGS.copy()
        changed = True

    for row in data.get("points_progression", []):
        for driver in data.get("progression_drivers", []):
            row.setdefault(driver, 0.0)

    for row in data.get("constructor_points_progression", []):
        for team in data.get("progression_constructors", []):
            row.setdefault(team, 0.0)

    return changed


def build_season_dataset(season: int) -> dict:
    schedule_raw = fetch(f"{season}.json", limit=100)
    races = schedule_raw["MRData"]["RaceTable"]["Races"]

    driver_standings_raw = fetch(f"{season}/driverStandings.json")
    constructor_standings_raw = fetch(f"{season}/constructorStandings.json")

    final_driver_lists = driver_standings_raw["MRData"]["StandingsTable"]["StandingsLists"]
    final_constructor_lists = constructor_standings_raw["MRData"]["StandingsTable"]["StandingsLists"]

    final_driver_standings = final_driver_lists[-1]["DriverStandings"] if final_driver_lists else []
    final_constructor_standings = (
        final_constructor_lists[-1]["ConstructorStandings"] if final_constructor_lists else []
    )

    progression_drivers = [map_driver(item["Driver"]) for item in final_driver_standings]
    progression_constructors = [
        normalize_constructor_name(season, item["Constructor"]["name"]) for item in final_constructor_standings
    ]

    rounds: list[dict] = []
    points_progression: list[dict] = []
    constructor_points_progression: list[dict] = []
    cumulative_driver_points = {name: 0.0 for name in progression_drivers}
    cumulative_constructor_points = {name: 0.0 for name in progression_constructors}

    for race in races:
        round_no = int(race["round"])

        race_details = fetch(f"{season}/{round_no}/results.json")
        race_races = race_details["MRData"]["RaceTable"]["Races"]
        race_payload = race_races[0] if race_races else race

        quali_details = fetch(f"{season}/{round_no}/qualifying.json")
        quali_races = quali_details["MRData"]["RaceTable"]["Races"]
        quali_payload = quali_races[0] if quali_races else None

        sprint_payload = None
        if season >= 2021 and "Sprint" in race:
            sprint_details = fetch(f"{season}/{round_no}/sprint.json")
            sprint_races = sprint_details["MRData"]["RaceTable"]["Races"]
            sprint_payload = sprint_races[0] if sprint_races else None

        round_summary = extract_round_summary(season, race_payload, quali_payload, sprint_payload)

        results = []
        for res in race_payload.get("Results", []):
            time_value = res.get("Time", {}).get("time") or f"+{res.get('Time', {}).get('millis', '-')}"
            driver_name = map_driver(res["Driver"])
            constructor_name = normalize_constructor_name(season, res["Constructor"]["name"])
            race_points = float(res["points"])
            results.append(
                {
                    "position": int(res["position"]),
                    "driver": driver_name,
                    "team": constructor_name,
                    "time": time_value,
                    "points": race_points,
                    "grid": int(res["grid"]),
                    "status": res["status"],
                }
            )
            if driver_name in cumulative_driver_points:
                cumulative_driver_points[driver_name] += race_points
            if constructor_name in cumulative_constructor_points:
                cumulative_constructor_points[constructor_name] += race_points

        qualifying = []
        if quali_payload:
            for q in quali_payload.get("QualifyingResults", []):
                qualifying.append(
                    {
                        "position": int(q["position"]),
                        "driver": map_driver(q["Driver"]),
                        "q1": q.get("Q1"),
                        "q2": q.get("Q2"),
                        "q3": q.get("Q3"),
                    }
                )

        sprint = []
        sprint_qualifying = []
        if sprint_payload:
            sprint_rows = sprint_payload.get("SprintResults", [])
            for s in sprint_rows:
                driver_name = map_driver(s["Driver"])
                constructor_name = normalize_constructor_name(season, s["Constructor"]["name"])
                sprint_points = float(s["points"])
                sprint.append(
                    {
                        "position": int(s["position"]),
                        "driver": driver_name,
                        "team": constructor_name,
                        "points": sprint_points,
                        "time": s.get("Time", {}).get("time"),
                    }
                )
                if driver_name in cumulative_driver_points:
                    cumulative_driver_points[driver_name] += sprint_points
                if constructor_name in cumulative_constructor_points:
                    cumulative_constructor_points[constructor_name] += sprint_points
            # Jolpica doesn't expose a separate sprint-qualifying endpoint.
            # Derive sprint-qualifying order from sprint starting grid.
            sprint_qualifying = sorted(
                [
                    {
                        "position": int(s.get("grid", 0)),
                        "driver": map_driver(s["Driver"]),
                        "team": normalize_constructor_name(season, s["Constructor"]["name"]),
                        "sq1": None,
                        "sq2": None,
                        "sq3": None,
                    }
                    for s in sprint_rows
                ],
                key=lambda x: x["position"],
            )

        rounds.append(
            {
                "round": round_no,
                "race": race_payload["raceName"],
                "date": race_payload.get("date"),
                "track": {
                    "name": race_payload["Circuit"]["circuitName"],
                    "country": race_payload["Circuit"]["Location"]["country"],
                    "locality": race_payload["Circuit"]["Location"]["locality"],
                },
                "results": results,
                "sprint_qualifying": sprint_qualifying,
                "sprint": sprint,
                "qualifying": qualifying,
                "practice": {
                    "fp1_best": "Data unavailable from Ergast endpoint",
                    "fp2_best": "Data unavailable from Ergast endpoint",
                    "fp3_best": "Data unavailable from Ergast endpoint",
                },
                "summary": round_summary,
            }
        )

        row = {"round": round_no}
        for name in progression_drivers:
            row[name] = round(cumulative_driver_points.get(name, 0.0), 1)
        points_progression.append(row)

        constructor_row = {"round": round_no}
        for name in progression_constructors:
            constructor_row[name] = round(cumulative_constructor_points.get(name, 0.0), 1)
        constructor_points_progression.append(constructor_row)

    data = {
        "season": season,
        "generated_from": "https://api.jolpi.ca/ergast/f1",
        "driver_standings": [
            {
                "position": int(item["position"]),
                "driver": map_driver(item["Driver"]),
                "points": float(item["points"]),
                "wins": int(item["wins"]),
                "team": normalize_constructor_name(season, item["Constructors"][0]["name"]),
            }
            for item in final_driver_standings
        ],
        "constructor_standings": [
            {
                "position": int(item["position"]),
                "team": normalize_constructor_name(season, item["Constructor"]["name"]),
                "points": float(item["points"]),
                "wins": int(item["wins"]),
            }
            for item in final_constructor_standings
        ],
        "progression_drivers": progression_drivers,
        "points_progression": points_progression,
        "progression_constructors": progression_constructors,
        "constructor_points_progression": constructor_points_progression,
        "rounds_summary": [r["summary"] for r in rounds],
        "rounds": rounds,
    }
    ensure_preseason_standings(data)
    align_driver_standings_teams(data)
    ensure_standings_podiums(data)
    validate_dataset(data)
    return data


def load_or_build_season(season: int, force_refresh: bool = False) -> dict:
    if season not in SUPPORTED_SEASONS:
        raise RuntimeError(f"Season {season} is not supported. Supported: {SUPPORTED_SEASONS[0]}-{SUPPORTED_SEASONS[-1]}")

    target = season_file(season)
    if target.exists() and not force_refresh:
        data = json.loads(target.read_text(encoding="utf-8"))
        changed = ensure_preseason_standings(data)
        changed = align_driver_standings_teams(data) or changed
        changed = ensure_standings_podiums(data) or changed
        validate_dataset(data)
        if changed:
            target.write_text(json.dumps(data, indent=2), encoding="utf-8")
        return data

    data = build_season_dataset(season)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data
