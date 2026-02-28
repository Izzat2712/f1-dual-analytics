param(
  [int]$BackendPort = 8000,
  [int]$FrontendPort = 5173
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $root "backend"
$frontendDir = Join-Path $root "frontend"
$venvPython = Join-Path $backendDir ".venv\Scripts\python.exe"

Write-Host "Preparing backend environment..."
if (-not (Test-Path $venvPython)) {
  py -3 -m venv (Join-Path $backendDir ".venv")
}

& $venvPython -m pip install -r (Join-Path $backendDir "requirements.txt")

Write-Host "Refreshing 2025 season dataset..."
& $venvPython (Join-Path $backendDir "scripts\build_season_dataset.py") --season 2025

Write-Host "Preparing frontend dependencies..."
if (-not (Test-Path (Join-Path $frontendDir "node_modules"))) {
  Push-Location $frontendDir
  npm install
  Pop-Location
}

Write-Host "Starting backend on http://localhost:$BackendPort ..."
$backendCmd = "cd `"$backendDir`"; & `"$venvPython`" -m uvicorn app.main:app --reload --port $BackendPort"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $backendCmd | Out-Null

Write-Host "Starting frontend on http://localhost:$FrontendPort ..."
$frontendCmd = "cd `"$frontendDir`"; `$env:VITE_API_BASE='http://localhost:$BackendPort'; npm run dev -- --port $FrontendPort"
Start-Process powershell -ArgumentList "-NoExit", "-Command", $frontendCmd | Out-Null

Write-Host ""
Write-Host "Website is launching in two terminals."
Write-Host "Frontend: http://localhost:$FrontendPort"
Write-Host "Backend docs: http://localhost:$BackendPort/docs"
