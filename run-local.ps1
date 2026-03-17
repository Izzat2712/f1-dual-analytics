param(
  [int]$BackendPort = 8000,
  [int]$FrontendPort = 5173
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$backendDir = Join-Path $root "backend"
$frontendDir = Join-Path $root "frontend"
$venvPython = Join-Path $backendDir ".venv\Scripts\python.exe"

function Stop-PortListeners {
  param(
    [int]$Port
  )

  $listeners = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique)
  foreach ($listenerPid in $listeners) {
    if ($listenerPid -and $listenerPid -ne $PID) {
      try {
        Stop-Process -Id $listenerPid -Force -ErrorAction Stop
        Write-Host "Stopped existing listener on port $Port (PID $listenerPid)."
      } catch {
        Write-Warning "Unable to stop PID $listenerPid on port ${Port}: $($_.Exception.Message)"
      }
    }
  }
}

function Stop-MatchingProcesses {
  param(
    [string]$CommandPattern,
    [string]$Label
  )

  $matches = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine -like $CommandPattern
  })
  foreach ($match in $matches) {
    if ($match.ProcessId -and $match.ProcessId -ne $PID) {
      try {
        Stop-Process -Id $match.ProcessId -Force -ErrorAction Stop
        Write-Host "Stopped existing $Label process (PID $($match.ProcessId))."
      } catch {
        Write-Warning "Unable to stop $Label PID $($match.ProcessId): $($_.Exception.Message)"
      }
    }
  }
}

function Wait-ForBackend {
  param(
    [int]$Port,
    [int]$TimeoutSeconds = 45
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      $response = Invoke-WebRequest -UseBasicParsing "http://localhost:$Port/api/health" -TimeoutSec 3
      if ($response.StatusCode -eq 200) {
        return $true
      }
    } catch {
      Start-Sleep -Milliseconds 800
    }
  }
  return $false
}

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

Stop-PortListeners -Port $BackendPort
Stop-PortListeners -Port $FrontendPort
Stop-MatchingProcesses -CommandPattern "*uvicorn app.main:app*" -Label "backend"
Stop-MatchingProcesses -CommandPattern "*npm run dev*--port $FrontendPort*" -Label "frontend shell"
Stop-MatchingProcesses -CommandPattern "*vite*--port $FrontendPort*" -Label "frontend"

Write-Host "Starting backend on http://localhost:$BackendPort ..."
$backendCmd = "cd `"$backendDir`"; & `"$venvPython`" -m uvicorn app.main:app --host 127.0.0.1 --port $BackendPort"
Start-Process powershell -ArgumentList "-NoProfile", "-NoExit", "-Command", $backendCmd | Out-Null

Write-Host "Waiting for backend health check..."
if (-not (Wait-ForBackend -Port $BackendPort)) {
  throw "Backend did not become ready on port $BackendPort."
}

Write-Host "Starting frontend on http://localhost:$FrontendPort ..."
$frontendCmd = "cd `"$frontendDir`"; `$env:VITE_API_BASE='http://localhost:$BackendPort'; npm run dev -- --host 127.0.0.1 --port $FrontendPort"
Start-Process powershell -ArgumentList "-NoProfile", "-NoExit", "-Command", $frontendCmd | Out-Null

Write-Host ""
Write-Host "Website is launching in two terminals."
Write-Host "Frontend: http://localhost:$FrontendPort"
Write-Host "Backend docs: http://localhost:$BackendPort/docs"
