# Dual-Mode Formula 1 Analytics Platform

Fan-facing race intelligence and engineering-grade simulation in one full-stack app.

## Overview

This project delivers two experiences in a single Formula 1 analytics platform:

- `Casual Mode`: standings, round results, qualifying, sprint weekends, and progression charts.
- `Engineering Nerd Mode`: telemetry analytics, lap prediction, strategy what-if simulation, network delay modeling, and race engineer guidance.

It supports season switching for `2021-2026` with dynamic round selection and locally cached datasets/assets.

## Core Features

### Casual Mode

- Driver standings and constructor standings
- Round-aware race results
- Qualifying + sprint qualifying + sprint result (when applicable)
- Driver points progression and constructor points progression
- Full season table (`stats for every round`)
- Track ribbon with circuit/country/locality/date
- Season + round dropdown selectors

### Engineering Nerd Mode

- Driver-specific telemetry traces (speed, throttle, brake)
- Driver vs teammate delta analysis
- Sector time decomposition
- Lap time prediction engine
- Strategy simulation with pit-window what-if controls
- Communication network simulation (latency/jitter/loss)
- Race engineer radio recommendation block
- Season-aware + round-aware driver portraits

## Tech Stack

- `Frontend`: React, Vite, Recharts
- `Backend`: FastAPI, NumPy, scikit-learn
- `Data Source`: Jolpica/Ergast API
- `Storage`: JSON season snapshots in `backend/data/`
- `Infra`: Docker Compose support

## Project Structure

```text
f1-dual/
  backend/                 FastAPI app + dataset builder
  frontend/                React UI + local asset fetcher
  docs/                    Architecture notes
  docker-compose.yml
  run-local.ps1
  verify.ps1
```

## Quick Start (Windows)

From the project root:

```powershell
.\run-local.ps1
```

This launches:

- Frontend: `http://localhost:5173`
- Backend docs: `http://localhost:8000/docs`

## Manual Local Setup

### Backend

```powershell
cd backend
py -3 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

## Docker

```bash
docker compose up --build
```



- Supported seasons: `2021-2026`
- Sprint qualifying list is derived from sprint starting grid due to source API limitations.
- Driver standings team is aligned to each driver's latest team appearance in that season.
