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

export async function getSessionSchedule(season) {
  const res = await fetch(`${API_BASE}/api/casual/session-schedule?season=${season}`);
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

export async function getEngineeringPositions(roundNo, season, session = "race") {
  const res = await fetch(
    `${API_BASE}/api/engineering/positions/${roundNo}?season=${season}&session=${encodeURIComponent(session)}`
  );
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // Ignore body parse failures; keep generic message.
    }
    throw new Error(`Failed to load positions (${res.status})${detail}`);
  }
  return res.json();
}

export async function getEngineeringTyreStrategy(roundNo, season, session = "race") {
  const res = await fetch(`${API_BASE}/api/engineering/tyre-strategy/${roundNo}?season=${season}&session=${encodeURIComponent(session)}`);
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // Ignore body parse failures; keep generic message.
    }
    throw new Error(`Failed to load tyre strategy (${res.status})${detail}`);
  }
  return res.json();
}

export async function getEngineeringH2H(roundNo, season, driverA, driverB, session = "race") {
  const query = new URLSearchParams({
    season: String(season),
    driver_a: String(driverA || ""),
    driver_b: String(driverB || ""),
    session: String(session || "race"),
  });
  const res = await fetch(`${API_BASE}/api/engineering/h2h/${roundNo}?${query.toString()}`);
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // Ignore body parse failures; keep generic message.
    }
    throw new Error(`Failed to load H2H (${res.status})${detail}`);
  }
  return res.json();
}

export async function getEngineeringTelemetryCatalog(roundNo, season, session = "race") {
  const res = await fetch(
    `${API_BASE}/api/engineering/telemetry/${roundNo}?season=${season}&session=${encodeURIComponent(session)}`
  );
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // Ignore body parse failures; keep generic message.
    }
    throw new Error(`Failed to load telemetry catalog (${res.status})${detail}`);
  }
  return res.json();
}

export async function getEngineeringTelemetryTrace(roundNo, season, driver, lap = "fastest", session = "race") {
  const query = new URLSearchParams({
    season: String(season),
    driver: String(driver || ""),
    lap: String(lap || "fastest"),
    session: String(session || "race"),
  });
  const res = await fetch(`${API_BASE}/api/engineering/telemetry/${roundNo}/trace?${query.toString()}`);
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // Ignore body parse failures; keep generic message.
    }
    throw new Error(`Failed to load telemetry trace (${res.status})${detail}`);
  }
  return res.json();
}
