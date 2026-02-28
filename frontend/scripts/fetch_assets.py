from __future__ import annotations

import json
import shutil
import time
import unicodedata
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT.parents[0] / "backend" / "data"
ASSETS = ROOT / "public" / "assets"
DRIVERS_DIR = ASSETS / "drivers"
TEAMS_DIR = ASSETS / "teams"

TEAM_URLS = {
    "McLaren": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/mclaren.png",
    "Red Bull": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/red-bull-racing.png",
    "Ferrari": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/ferrari.png",
    "Mercedes": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/mercedes.png",
    "Aston Martin": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/aston-martin.png",
    "Williams": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/williams.png",
    "RB F1 Team": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/rb.png",
    "Haas F1 Team": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/haas-f1-team.png",
    "Sauber": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/kick-sauber.png",
    "Alpine F1 Team": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2025/alpine.png",
    "Alfa Romeo": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2023/alfa-romeo.png",
    "AlphaTauri": "https://media.formula1.com/image/upload/f_auto,q_auto/content/dam/fom-website/teams/2023/alphatauri.png",
}

DRIVER_SLUGS = {
    "alexander albon": "albon",
    "andrea kimi antonelli": "antonelli",
    "antonio giovinazzi": "giovinazzi",
    "carlos sainz": "sainz",
    "charles leclerc": "leclerc",
    "daniel ricciardo": "ricciardo",
    "esteban ocon": "ocon",
    "fernando alonso": "alonso",
    "franco colapinto": "colapinto",
    "gabriel bortoleto": "bortoleto",
    "george russell": "russell",
    "guanyu zhou": "zhou",
    "isack hadjar": "hadjar",
    "jack doohan": "doohan",
    "kevin magnussen": "magnussen",
    "kimi raikkonen": "raikkonen",
    "lance stroll": "stroll",
    "lando norris": "norris",
    "lewis hamilton": "hamilton",
    "liam lawson": "lawson",
    "logan sargeant": "sargeant",
    "max verstappen": "verstappen",
    "mick schumacher": "schumacher",
    "nicholas latifi": "latifi",
    "nico hulkenberg": "hulkenberg",
    "nikita mazepin": "mazepin",
    "nyck de vries": "devries",
    "oliver bearman": "bearman",
    "oscar piastri": "piastri",
    "pierre gasly": "gasly",
    "robert kubica": "kubica",
    "sebastian vettel": "vettel",
    "sergio perez": "perez",
    "valtteri bottas": "bottas",
    "yuki tsunoda": "tsunoda",
}

MANUAL_DRIVER_URLS = {
    "robert kubica": "https://upload.wikimedia.org/wikipedia/commons/thumb/7/7b/Robert_Kubica_at_Monza_2023.jpg/330px-Robert_Kubica_at_Monza_2023.jpg",
}


def slugify_team(name: str) -> str:
    return (
        name.lower()
        .replace(" ", "-")
        .replace(".", "")
        .replace("'", "")
        .replace("&", "and")
    )


def normalize_name(value: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", value) if unicodedata.category(c) != "Mn"
    ).lower().strip()


def fetch_bytes(url: str, attempts: int = 2) -> bytes | None:
    for i in range(attempts):
        try:
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as r:
                return r.read()
        except (HTTPError, URLError):
            time.sleep(0.6 * (i + 1))
    return None


def read_drivers_and_teams() -> tuple[dict[int, set[str]], dict[int, set[str]], dict[int, set[int]]]:
    drivers_by_season: dict[int, set[str]] = {}
    teams_by_season: dict[int, set[str]] = {}
    rounds_by_season: dict[int, set[int]] = {}
    for season in range(2021, 2026):
        path = DATA_ROOT / f"season_{season}.json"
        if not path.exists():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        season_drivers: set[str] = set()
        season_teams: set[str] = set()
        season_rounds: set[int] = set()

        for d in data.get("driver_standings", []):
            season_drivers.add(normalize_name(d["driver"]))
            season_teams.add(d["team"])
        for c in data.get("constructor_standings", []):
            season_teams.add(c["team"])

        for r in data.get("rounds", []):
            try:
                round_no = int(r.get("round"))
            except (TypeError, ValueError):
                continue
            season_rounds.add(round_no)

            for entry in r.get("results", []):
                if entry.get("driver"):
                    season_drivers.add(normalize_name(entry["driver"]))
                if entry.get("team"):
                    season_teams.add(entry["team"])
            for entry in r.get("qualifying", []):
                if entry.get("driver"):
                    season_drivers.add(normalize_name(entry["driver"]))
                if entry.get("team"):
                    season_teams.add(entry["team"])
            for entry in r.get("sprint", []):
                if entry.get("driver"):
                    season_drivers.add(normalize_name(entry["driver"]))
                if entry.get("team"):
                    season_teams.add(entry["team"])
            for entry in r.get("sprint_qualifying", []):
                if entry.get("driver"):
                    season_drivers.add(normalize_name(entry["driver"]))
                if entry.get("team"):
                    season_teams.add(entry["team"])

        drivers_by_season[season] = season_drivers
        teams_by_season[season] = season_teams
        rounds_by_season[season] = season_rounds
    return drivers_by_season, teams_by_season, rounds_by_season


def write_placeholders() -> None:
    (ASSETS / "driver-placeholder.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">'
        '<rect width="64" height="64" rx="32" fill="#101317"/>'
        '<text x="50%" y="54%" text-anchor="middle" font-size="22" fill="#fff" font-family="Arial">D</text>'
        "</svg>",
        encoding="utf-8",
    )
    (ASSETS / "team-placeholder.svg").write_text(
        '<svg xmlns="http://www.w3.org/2000/svg" width="64" height="64">'
        '<rect width="64" height="64" rx="32" fill="#2b3440"/>'
        '<text x="50%" y="54%" text-anchor="middle" font-size="22" fill="#fff" font-family="Arial">T</text>'
        "</svg>",
        encoding="utf-8",
    )


def main() -> None:
    DRIVERS_DIR.mkdir(parents=True, exist_ok=True)
    TEAMS_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS.mkdir(parents=True, exist_ok=True)
    write_placeholders()

    drivers_by_season, teams_by_season, rounds_by_season = read_drivers_and_teams()

    driver_ok = 0
    for season, drivers in sorted(drivers_by_season.items()):
        season_dir = DRIVERS_DIR / str(season)
        season_dir.mkdir(parents=True, exist_ok=True)

        for name in sorted(drivers):
            slug = DRIVER_SLUGS.get(name)
            if not slug:
                continue
            target = season_dir / f"{slug}.png"

            url = (
                "https://media.formula1.com/image/upload/"
                f"f_png,c_limit,w_160,q_auto/content/dam/fom-website/drivers/{season}Drivers/{slug}"
            )
            data = fetch_bytes(url)
            if not data:
                for alt in [2025, 2024, 2023, 2022, 2021]:
                    if alt == season:
                        continue
                    alt_url = (
                        "https://media.formula1.com/image/upload/"
                        f"f_png,c_limit,w_160,q_auto/content/dam/fom-website/drivers/{alt}Drivers/{slug}"
                    )
                    data = fetch_bytes(alt_url)
                    if data:
                        break

            if not data and name in MANUAL_DRIVER_URLS:
                data = fetch_bytes(MANUAL_DRIVER_URLS[name], attempts=2)

            if data:
                target.write_bytes(data)
                # Keep a flat fallback copy too.
                (DRIVERS_DIR / f"{slug}.png").write_bytes(data)
                for round_no in sorted(rounds_by_season.get(season, set())):
                    round_dir = season_dir / str(round_no)
                    round_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(target, round_dir / f"{slug}.png")
                driver_ok += 1

    team_ok = 0
    for season, season_teams in sorted(teams_by_season.items()):
        season_team_dir = TEAMS_DIR / str(season)
        season_team_dir.mkdir(parents=True, exist_ok=True)
        for team in sorted(season_teams):
            url = TEAM_URLS.get(team)
            if not url:
                continue
            slug = slugify_team(team)
            target = TEAMS_DIR / f"{slug}.png"
            season_target = season_team_dir / f"{slug}.png"
            data = fetch_bytes(url)
            if data:
                target.write_bytes(data)
                season_target.write_bytes(data)
                for round_no in sorted(rounds_by_season.get(season, set())):
                    round_dir = season_team_dir / str(round_no)
                    round_dir.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(season_target, round_dir / f"{slug}.png")
                team_ok += 1

    print(f"Downloaded drivers: {driver_ok}")
    print(f"Downloaded teams: {team_ok}")


if __name__ == "__main__":
    main()
