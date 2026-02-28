# System Architecture

## Overview

The platform is split into two primary layers:

- React frontend for visualization and user mode control.
- FastAPI backend for analytics, simulation, and prediction services.

## Casual Mode Data Flow

1. Frontend requests `/api/casual/overview` and `/api/casual/results/{round}`.
2. Backend returns race weekend and standings payloads.
3. Frontend renders tables and progression charts.

## Engineering Mode Data Flow

1. Telemetry arrays are posted to `/api/engineering/telemetry/analyze`.
2. Backend applies moving-average filtering and returns smoothed channels.
3. Lap features are posted to `/api/engineering/lap/predict`.
4. Strategy and network payloads are run via simulation endpoints.
5. Frontend visualizes outputs through line/bar charts and KPI cards.

## Future Integrations

- FastF1 session ingestion
- Ergast historical API sync jobs
- PostgreSQL persistence for sessions and model outputs
- WebSocket stream ingestion to support live pit wall simulation
