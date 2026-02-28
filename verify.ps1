$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $root "backend"
$frontendDir = Join-Path $root "frontend"
$venvPython = Join-Path $backendDir ".venv\Scripts\python.exe"

if (-not (Test-Path $venvPython)) {
  py -3 -m venv (Join-Path $backendDir ".venv")
}

& $venvPython -m pip install -r (Join-Path $backendDir "requirements.txt") | Out-Null
& $venvPython (Join-Path $backendDir "scripts\build_season_dataset.py") --season 2025 | Out-Null

Write-Host "Running backend API smoke checks..."
Push-Location $backendDir
@'
from fastapi.testclient import TestClient
from app.main import app

c = TestClient(app)

assert c.get("/api/health").status_code == 200
assert c.get("/api/casual/seasons").status_code == 200
assert c.get("/api/casual/overview?season=2025").status_code == 200
assert c.get("/api/casual/rounds?season=2025").status_code == 200
assert c.get("/api/casual/results/7?season=2025").status_code == 200
assert c.post("/api/engineering/telemetry/analyze", json={
    "speed": [200, 210, 220],
    "throttle": [80, 95, 99],
    "brake": [0, 2, 10],
    "window": 2
}).status_code == 200
assert c.post("/api/engineering/lap/predict", json={
    "tyre_compound": "SOFT",
    "sector1": 30.8,
    "sector2": 37.1,
    "sector3": 24.7,
    "tyre_age_laps": 10,
    "track_temp_c": 35
}).status_code == 200
assert c.post("/api/engineering/strategy/simulate", json={
    "total_laps": 57,
    "pit_window_start": 14,
    "pit_window_end": 32,
    "simulations": 200
}).status_code == 200
assert c.post("/api/engineering/network/simulate", json={
    "packets": 120,
    "base_latency_ms": 45,
    "jitter_ms": 10,
    "packet_loss_rate": 0.03,
    "bandwidth_mbps": 4
}).status_code == 200

round_payload = c.get("/api/casual/results/1?season=2025").json()
first_driver = round_payload["results"][0]["driver"]
assert c.post("/api/engineering/driver-analysis", json={
    "season": 2025,
    "round_no": 1,
    "driver": first_driver
}).status_code == 200

print("backend smoke checks passed")
'@ | & $venvPython -
if ($LASTEXITCODE -ne 0) { throw "Backend smoke checks failed" }
Pop-Location

Write-Host "Building frontend..."
Push-Location $frontendDir
if (-not (Test-Path (Join-Path $frontendDir "node_modules"))) {
  npm install | Out-Null
}
npm run build | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Frontend build failed" }
Pop-Location

Write-Host "Frontend build passed"
Write-Host "All checks passed"
