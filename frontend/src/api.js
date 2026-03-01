const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

export async function getSeasons() {
  const res = await fetch(`${API_BASE}/api/casual/seasons`);
  return res.json();
}

export async function getCasualOverview(season) {
  const res = await fetch(`${API_BASE}/api/casual/overview?season=${season}`);
  return res.json();
}

export async function getRoundResults(roundNo, season) {
  const res = await fetch(`${API_BASE}/api/casual/results/${roundNo}?season=${season}`);
  return res.json();
}

export async function getRoundsSummary(season) {
  const res = await fetch(`${API_BASE}/api/casual/rounds?season=${season}`);
  return res.json();
}

export async function getEngineeringDriverAnalysis(payload) {
  const res = await fetch(`${API_BASE}/api/engineering/driver-analysis`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function analyzeTelemetry(payload) {
  const res = await fetch(`${API_BASE}/api/engineering/telemetry/analyze`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function predictLap(payload) {
  const res = await fetch(`${API_BASE}/api/engineering/lap/predict`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function simulateStrategy(payload) {
  const res = await fetch(`${API_BASE}/api/engineering/strategy/simulate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function simulateNetwork(payload) {
  const res = await fetch(`${API_BASE}/api/engineering/network/simulate`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return res.json();
}

export async function getEngineeringPositions(roundNo, season) {
  const res = await fetch(`${API_BASE}/api/engineering/positions/${roundNo}?season=${season}`);
  return res.json();
}
