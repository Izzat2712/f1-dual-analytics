import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Line,
  LineChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Bar,
  BarChart,
} from "recharts";
import {
  getSeasons,
  getCasualOverview,
  getRoundResults,
  getRoundsSummary,
  getSessionSchedule,
  getEngineeringDriverAnalysis,
  getEngineeringPositions,
  getEngineeringTyreStrategy,
  getEngineeringH2H,
  getEngineeringTelemetryCatalog,
  getEngineeringTelemetryTrace,
  simulateNetwork,
  simulateStrategy,
} from "./api";
import { Analytics } from "@vercel/analytics/react";
import "./styles.css";

const TEAM_COLORS = {
  "McLaren": "#FF8700",
  "Red Bull": "#3671C6",
  "Ferrari": "#E8002D",
  // Use turquoise (secondary) for visibility over white background.
  "Mercedes": "#00A19B",
  "Aston Martin": "#00665E",
  "Williams": "#00A0DD",
  // Team primary is white in 2025; use navy secondary for chart visibility.
  "RB F1 Team": "#1634CB",
  // Team primary is white in 2025; use red secondary for chart visibility.
  "Haas F1 Team": "#E6002B",
  "Sauber": "#00E701",
  // Alpine is two-tone pink/blue; use electric blue for clarity.
  "Alpine F1 Team": "#0090FF",
};

const TEAM_SLUGS = {
  "McLaren": "mclaren",
  "Red Bull": "red-bull",
  "Ferrari": "ferrari",
  "Mercedes": "mercedes",
  "Aston Martin": "aston-martin",
  "Williams": "williams",
  "RB F1 Team": "rb-f1-team",
  "Haas F1 Team": "haas-f1-team",
  "Sauber": "sauber",
  "Alpine F1 Team": "alpine-f1-team",
  "Alfa Romeo": "alfa-romeo",
  "AlphaTauri": "alphatauri",
};

const TEAM_COLOR_ALIASES = {
  "stake f1 team kick sauber": "Sauber",
  "kick sauber": "Sauber",
  "visa cash app rb": "RB F1 Team",
  "rb": "RB F1 Team",
  "alphatauri": "AlphaTauri",
};

const TYRE_COMPOUND_COLORS = {
  SOFT: "#ef4444",
  MEDIUM: "#facc15",
  HARD: "#e5e7eb",
  INTERMEDIATE: "#22c55e",
  WET: "#3b82f6",
  UNKNOWN: "#64748b",
};

const TELEMETRY_CARDS = [
  { key: "speed", title: "Speed Trace", dataKey: "speed", color: "#ef4444", unit: "km/h" },
  { key: "gear", title: "Gear Shifts", dataKey: "gear", color: "#f59e0b", unit: "" },
  { key: "throttle", title: "Throttle Input", dataKey: "throttle", color: "#22c55e", unit: "%" },
  { key: "brake", title: "Brake Input", dataKey: "brake", color: "#60a5fa", unit: "%" },
  { key: "rpm", title: "RPM", dataKey: "rpm", color: "#a78bfa", unit: "rpm" },
];

const DRIVER_PHOTO_SLUGS = {
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
};

function normalizeName(name) {
  return String(name || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function slugifyTeamName(teamName) {
  const direct = TEAM_SLUGS[teamName];
  if (direct) {
    return direct;
  }
  return String(teamName || "")
    .toLowerCase()
    .replace(/\s+/g, "-")
    .replace(/\./g, "")
    .replace(/'/g, "")
    .replace(/&/g, "and");
}

function getDriverPhotoCandidates(driverName, season, roundNo) {
  const slug = DRIVER_PHOTO_SLUGS[normalizeName(driverName)];
  if (!slug) {
    return ["/assets/driver-placeholder.svg"];
  }
  return [
    `/assets/drivers/${season}/${roundNo}/${slug}.png`,
    `/assets/drivers/${season}/${slug}.png`,
    `/assets/drivers/${slug}.png`,
    "/assets/driver-placeholder.svg",
  ];
}

function getTeamLogoCandidates(teamName, season, roundNo) {
  const slug = slugifyTeamName(teamName);
  if (!slug) {
    return ["/assets/team-placeholder.svg"];
  }
  return [
    `/assets/teams/${season}/${roundNo}/${slug}.png`,
    `/assets/teams/${season}/${slug}.png`,
    `/assets/teams/${slug}.png`,
    "/assets/team-placeholder.svg",
  ];
}

function DriverAvatar({ driverName, season, roundNo }) {
  const candidates = getDriverPhotoCandidates(driverName, season, roundNo);
  return (
    <img
      className="avatar-img"
      src={candidates[0]}
      data-candidates={JSON.stringify(candidates)}
      data-idx="0"
      alt={`${driverName} portrait`}
      onError={(e) => {
        const img = e.currentTarget;
        const all = JSON.parse(img.dataset.candidates || "[]");
        const idx = Number(img.dataset.idx || "0");
        const next = all[idx + 1];
        if (next) {
          img.src = next;
          img.dataset.idx = String(idx + 1);
          return;
        }
        img.style.display = "none";
      }}
    />
  );
}

function TeamLogo({ teamName, season, roundNo }) {
  const candidates = getTeamLogoCandidates(teamName, season, roundNo);
  return (
    <img
      className="team-logo"
      src={candidates[0]}
      data-candidates={JSON.stringify(candidates)}
      data-idx="0"
      alt={`${teamName} logo`}
      onError={(e) => {
        const img = e.currentTarget;
        const all = JSON.parse(img.dataset.candidates || "[]");
        const idx = Number(img.dataset.idx || "0");
        const next = all[idx + 1];
        if (next) {
          img.src = next;
          img.dataset.idx = String(idx + 1);
          return;
        }
        img.style.display = "none";
      }}
    />
  );
}

function DriverPortrait({ driverName, season, roundNo }) {
  const candidates = getDriverPhotoCandidates(driverName, season, roundNo);
  return (
    <img
      className="driver-portrait-img"
      src={candidates[0]}
      data-candidates={JSON.stringify(candidates)}
      data-idx="0"
      alt={`${driverName} portrait`}
      onError={(e) => {
        const img = e.currentTarget;
        const all = JSON.parse(img.dataset.candidates || "[]");
        const idx = Number(img.dataset.idx || "0");
        const next = all[idx + 1];
        if (next) {
          img.src = next;
          img.dataset.idx = String(idx + 1);
          return;
        }
        img.style.display = "none";
      }}
    />
  );
}

function colorForDriver(driverName, driverTeamMap, index) {
  const teamName = driverTeamMap[driverName];
  if (teamName && TEAM_COLORS[teamName]) {
    return TEAM_COLORS[teamName];
  }
  const fallbackHue = (index * 47) % 360;
  return `hsl(${fallbackHue} 70% 42%)`;
}

function colorForTeam(teamName, fallback = "#d7263d") {
  const raw = String(teamName || "").trim();
  if (!raw) return fallback;
  if (TEAM_COLORS[raw]) return TEAM_COLORS[raw];

  const normalized = normalizeName(raw);
  const alias = TEAM_COLOR_ALIASES[normalized];
  if (alias && TEAM_COLORS[alias]) {
    return TEAM_COLORS[alias];
  }

  return fallback;
}

function driverCode(driverName) {
  const parts = String(driverName || "").trim().split(/\s+/);
  const seed = parts.length ? parts[parts.length - 1] : driverName;
  return String(seed || "").slice(0, 3).toUpperCase();
}

function formatLapTimeSeconds(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value <= 0) return "-";
  const mins = Math.floor(value / 60);
  const secs = value - (mins * 60);
  return `${mins}:${secs.toFixed(3).padStart(6, "0")}`;
}

function formatMetricSeconds(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value) || value < 0) return "-";
  return `${value.toFixed(3)}s`;
}

function formatSignedDelta(seconds) {
  const value = Number(seconds);
  if (!Number.isFinite(value)) return "-";
  if (Math.abs(value) < 0.0005) return "0.000s";
  const sign = value > 0 ? "+" : "-";
  return `${sign}${Math.abs(value).toFixed(3)}s`;
}

function isClassifiedFinisherStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  if (value === "finished") return true;
  if (value === "lapped") return true;
  if (value.startsWith("+")) return true;
  if (value.includes("lap") && /\d/.test(value)) return true;
  return false;
}

function mapRaceResultStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  if (
    value.includes("did not start")
    || value === "dns"
    || value.includes("withdrawn")
    || value.includes("did not qualify")
    || value === "dnq"
  ) {
    return "dns";
  }
  if (value.includes("excluded") || value === "dsq" || value.includes("disqualified")) {
    return "disqualification";
  }
  if (isClassifiedFinisherStatus(value)) {
    return "finished";
  }
  return "retired";
}

function colorForCompound(compound) {
  return TYRE_COMPOUND_COLORS[String(compound || "").toUpperCase()] || TYRE_COMPOUND_COLORS.UNKNOWN;
}

function formatCountdownParts(diffMs) {
  const totalSeconds = Math.max(0, Math.floor(diffMs / 1000));
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  return {
    days: String(days).padStart(2, "0"),
    hours: String(hours).padStart(2, "0"),
    minutes: String(minutes).padStart(2, "0"),
    seconds: String(seconds).padStart(2, "0"),
  };
}

function parseSessionTimestamp(startUtc) {
  const ts = Date.parse(String(startUtc || ""));
  return Number.isFinite(ts) ? ts : null;
}

const COUNTRY_TO_ISO2 = {
  "Australia": "AU",
  "China": "CN",
  "Japan": "JP",
  "Bahrain": "BH",
  "Saudi Arabia": "SA",
  "United States": "US",
  "USA": "US",
  "Italy": "IT",
  "Monaco": "MC",
  "Spain": "ES",
  "Canada": "CA",
  "Austria": "AT",
  "United Kingdom": "GB",
  "UK": "GB",
  "Belgium": "BE",
  "Hungary": "HU",
  "Netherlands": "NL",
  "Azerbaijan": "AZ",
  "Singapore": "SG",
  "Mexico": "MX",
  "Brazil": "BR",
  "Qatar": "QA",
  "Abu Dhabi": "AE",
  "UAE": "AE",
  "United Arab Emirates": "AE",
};

function isoToFlagEmoji(iso2) {
  const normalized = String(iso2 || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(normalized)) return "";
  const base = 127397;
  return String.fromCodePoint(...[...normalized].map((c) => base + c.charCodeAt(0)));
}

function countryToFlagEmoji(country) {
  const code = COUNTRY_TO_ISO2[String(country || "").trim()] || "";
  return isoToFlagEmoji(code);
}

function countryToIso2(country) {
  return COUNTRY_TO_ISO2[String(country || "").trim()] || "";
}

function formatLocalStart(ts) {
  if (!Number.isFinite(ts)) return "-";
  try {
    return new Intl.DateTimeFormat(undefined, {
      weekday: "short",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZoneName: "short",
    }).format(new Date(ts));
  } catch {
    return new Date(ts).toLocaleString();
  }
}

function NextSessionCountdown() {
  const [nowMs, setNowMs] = useState(() => Date.now());
  const [scheduleRounds, setScheduleRounds] = useState([]);

  useEffect(() => {
    const timer = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);

  useEffect(() => {
    getSessionSchedule(2026).then((payload) => {
      setScheduleRounds(Array.isArray(payload?.rounds) ? payload.rounds : []);
    }).catch(() => {
      setScheduleRounds([]);
    });
  }, []);

  const normalizedRounds = useMemo(() => {
    return (scheduleRounds || [])
      .map((round) => {
        const sessions = (round?.session_schedule || [])
          .map((entry) => {
            const ts = parseSessionTimestamp(entry?.start_utc);
            if (!Number.isFinite(ts)) return null;
            return {
              code: entry?.code || "",
              label: entry?.label || "Session",
              startUtc: entry?.start_utc,
              ts,
            };
          })
          .filter(Boolean)
          .sort((a, b) => a.ts - b.ts);
        return {
          round: Number(round?.round || 0),
          raceName: String(round?.race_name || "Grand Prix"),
          country: round?.track?.country || "",
          sessions,
        };
      })
      .filter((round) => round.sessions.length > 0)
      .sort((a, b) => a.round - b.round);
  }, [scheduleRounds]);

  const targetRound = useMemo(() => {
    return normalizedRounds.find((round) => {
      const raceSession = round.sessions.find((session) => session.code === "race") || round.sessions[round.sessions.length - 1];
      return raceSession && raceSession.ts > nowMs;
    }) || null;
  }, [normalizedRounds, nowMs]);

  const nextSession = useMemo(() => {
    if (!targetRound) return null;
    return targetRound.sessions.find((session) => session.ts > nowMs) || null;
  }, [targetRound, nowMs]);

  const iso2 = countryToIso2(targetRound?.country);
  const flag = countryToFlagEmoji(targetRound?.country);
  const flagImg = iso2 ? `https://flagcdn.com/w40/${iso2.toLowerCase()}.png` : "";
  const raceName = String(targetRound?.raceName || "Next Race");

  if (!normalizedRounds.length) {
    return (
      <div className="countdown-card">
        <div className="countdown-race">
          {flagImg ? <img className="countdown-flag-img" src={flagImg} alt={`${targetRound?.country || "Country"} flag`} loading="lazy" /> : <span className="countdown-flag">{flag || "🏁"}</span>}
          <span>{raceName}</span>
        </div>
        <div className="countdown-session">Schedule Unavailable</div>
        <div className="countdown-time">--d --h --m --s</div>
        <div className="countdown-local-start">Local start: -</div>
      </div>
    );
  }

  if (!nextSession) {
    return (
      <div className="countdown-card">
        <div className="countdown-race">
          {flagImg ? <img className="countdown-flag-img" src={flagImg} alt={`${targetRound?.country || "Country"} flag`} loading="lazy" /> : <span className="countdown-flag">{flag || "🏁"}</span>}
          <span>{raceName}</span>
        </div>
        <div className="countdown-session">Race Weekend Complete</div>
        <div className="countdown-time">00d 00h 00m 00s</div>
        <div className="countdown-local-start">Local start: -</div>
      </div>
    );
  }

  const diff = nextSession.ts - nowMs;
  const parts = formatCountdownParts(diff);

  return (
    <div className="countdown-card">
      <div className="countdown-race">
        {flagImg ? <img className="countdown-flag-img" src={flagImg} alt={`${targetRound?.country || "Country"} flag`} loading="lazy" /> : <span className="countdown-flag">{flag || "🏁"}</span>}
        <span>{raceName}</span>
      </div>
      <div className="countdown-session">{String(nextSession.label || "Session").toUpperCase()}</div>
      <div className="countdown-time">
        {parts.days}d {parts.hours}h {parts.minutes}m {parts.seconds}s
      </div>
      <div className="countdown-local-start">Local start: {formatLocalStart(nextSession.ts)}</div>
    </div>
  );
}

function ProgressionTooltip({ active, label, payload }) {
  if (!active || !payload?.length) return null;

  const sorted = payload
    .filter((item) => Number.isFinite(item?.value))
    .sort((a, b) => {
      if (b.value !== a.value) return b.value - a.value;
      return String(a.name).localeCompare(String(b.name));
    });
  const ranked = sorted.map((item, idx) => ({ ...item, rank: idx + 1 }));
  const half = Math.ceil(ranked.length / 2);
  const orderedForTwoCols = [];
  for (let i = 0; i < half; i += 1) {
    orderedForTwoCols.push(ranked[i]);
    if (i + half < ranked.length) {
      orderedForTwoCols.push(ranked[i + half]);
    }
  }

  return (
    <div style={{ background: "#f3f5f8", border: "1px solid #b9c2cd", padding: "0.45rem 0.55rem", minWidth: 380 }}>
      <div style={{ marginBottom: "0.3rem", fontWeight: 700 }}>{label}</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 14px" }}>
        {orderedForTwoCols.map((item) => (
          <div key={item.name} style={{ color: item.color || "#111", lineHeight: 1.2, fontSize: "0.82rem", whiteSpace: "nowrap" }}>
            {item.rank}. {item.name} : {item.value}
          </div>
        ))}
      </div>
    </div>
  );
}

function ConstructorProgressionTooltip({ active, label, payload }) {
  if (!active || !payload?.length) return null;

  const sorted = payload
    .filter((item) => Number.isFinite(item?.value))
    .sort((a, b) => {
      if (b.value !== a.value) return b.value - a.value;
      return String(a.name).localeCompare(String(b.name));
    });
  const ranked = sorted.map((item, idx) => ({ ...item, rank: idx + 1 }));
  const half = Math.ceil(ranked.length / 2);
  const orderedForTwoCols = [];
  for (let i = 0; i < half; i += 1) {
    orderedForTwoCols.push(ranked[i]);
    if (i + half < ranked.length) {
      orderedForTwoCols.push(ranked[i + half]);
    }
  }

  return (
    <div style={{ background: "#f3f5f8", border: "1px solid #b9c2cd", padding: "0.45rem 0.55rem", minWidth: 340 }}>
      <div style={{ marginBottom: "0.3rem", fontWeight: 700 }}>{label}</div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "2px 14px" }}>
        {orderedForTwoCols.map((item) => (
          <div key={item.name} style={{ color: item.color || "#111", lineHeight: 1.2, fontSize: "0.82rem", whiteSpace: "nowrap" }}>
            {item.rank}. {item.name} : {item.value}
          </div>
        ))}
      </div>
    </div>
  );
}

function PositionTooltip({
  active,
  label,
  lapRowsByLap,
  driverColors,
  allDrivers,
  dnfDriversSet,
  lastSeenLapByDriver,
  lastSeenPositionByDriver,
}) {
  if (!active || !Number.isFinite(Number(label))) {
    return null;
  }
  const lap = Number(label);
  const row = lapRowsByLap[lap];
  if (!row) {
    return null;
  }

  const items = (allDrivers || []).map((driver) => {
    const value = row[driver];
    if (Number.isFinite(value)) {
      return { driver, rankValue: Number(value), display: `P${value}` };
    }
    const lastSeen = lastSeenLapByDriver?.[driver] ?? -1;
    const lastPos = lastSeenPositionByDriver?.[driver];
    if (dnfDriversSet?.has(driver) && lap > lastSeen) {
      return { driver, rankValue: 999, display: "(DNF)" };
    }
    if (Number.isFinite(lastPos)) {
      return { driver, rankValue: Number(lastPos), display: `P${lastPos}` };
    }
    return { driver, rankValue: 1000, display: "-" };
  });

  const ordered = items.sort((a, b) => {
    if (a.rankValue !== b.rankValue) return a.rankValue - b.rankValue;
    return a.driver.localeCompare(b.driver);
  });

  return (
    <div className="position-tooltip">
      <div className="position-tooltip-title">Lap {lap}</div>
      <div className="position-tooltip-list">
        {ordered.map((item) => (
          <div key={item.driver} style={{ color: driverColors[item.driver] || "#d4d9e1" }}>
            {driverCode(item.driver)} : {item.display}
          </div>
        ))}
      </div>
    </div>
  );
}

function InfoHint({ label, content }) {
  return (
    <button type="button" className="info-icon-btn" aria-label={label}>
      i
      <span className="info-tooltip">{content}</span>
    </button>
  );
}

function CasualPanel({ overview, race, roundsSummary, roundNo }) {
  const noDataText = "No data yet for this season/round.";
  const progressionDrivers = overview?.progression_drivers || [];
  const progressionConstructors = overview?.progression_constructors || [];
  const isSprintWeekend = (race?.sprint_qualifying?.length || 0) > 0;
  const [sprintSessionTab, setSprintSessionTab] = useState("results");
  const [raceSessionTab, setRaceSessionTab] = useState("results");
  const driverTeamMap = useMemo(() => {
    const map = {};
    for (const item of overview?.driver_standings || []) {
      map[item.driver] = item.team;
    }
    return map;
  }, [overview]);

  useEffect(() => {
    setSprintSessionTab("results");
    setRaceSessionTab("results");
  }, [roundNo, race?.race]);

  return (
    <div className="casual-layout">
      <div className={`top-grid ${isSprintWeekend ? "sprint-weekend" : ""}`}>
        <div className="card top-card driver-card">
        <h3>Driver Standings (All Drivers)</h3>
        <div className="table-scroll card-table-wrap">
          <table>
            <thead>
              <tr><th>Pos</th><th></th><th>Driver</th><th>Team</th><th>Wins</th><th>Pts</th></tr>
            </thead>
            <tbody>
              {overview?.driver_standings?.length
                ? overview.driver_standings.map((d) => (
                  <tr key={d.position}>
                    <td>{d.position}</td>
                    <td className="avatar-cell"><DriverAvatar driverName={d.driver} season={overview?.season || 2025} roundNo={roundNo} /></td>
                    <td>{d.driver}</td>
                    <td>{d.team}</td>
                    <td>{d.wins}</td>
                    <td>{d.points}</td>
                  </tr>
                ))
                : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
            </tbody>
          </table>
        </div>
        </div>

        <div className="card top-card constructor-card">
        <h3>Constructor Standings</h3>
        <div className="table-scroll card-table-wrap">
          <table>
          <thead>
            <tr><th>Pos</th><th></th><th>Team</th><th>Pts</th></tr>
          </thead>
          <tbody>
            {overview?.constructor_standings?.length
              ? overview.constructor_standings.map((c) => (
                <tr key={c.position}>
                  <td>{c.position}</td>
                  <td className="logo-cell">
                    <TeamLogo teamName={c.team} season={overview?.season || 2025} roundNo={roundNo} />
                  </td>
                  <td>{c.team}</td>
                  <td>{c.points}</td>
                </tr>
              ))
              : <tr><td colSpan={4} className="small">{noDataText}</td></tr>}
          </tbody>
          </table>
        </div>
        </div>

        {isSprintWeekend ? (
          <>
            <div className="card top-card session-standings-card qualifying-stack-card">
              <div className="session-card-head">
                <h3>Sprint Session</h3>
                <div className="session-tabbar">
                  <button
                    className={sprintSessionTab === "results" ? "active" : ""}
                    onClick={() => setSprintSessionTab("results")}
                  >
                    Sprint Result
                  </button>
                  <button
                    className={sprintSessionTab === "qualifying" ? "active" : ""}
                    onClick={() => setSprintSessionTab("qualifying")}
                  >
                    Sprint Qualifying
                  </button>
                </div>
              </div>

              <div className="table-scroll card-table-wrap">
                {sprintSessionTab === "results" ? (
                  <table>
                    <thead>
                      <tr><th>Pos</th><th></th><th>Driver</th><th>Team</th><th>Time</th><th>Pts</th></tr>
                    </thead>
                    <tbody>
                      {race?.sprint?.length
                        ? race.sprint.map((s) => (
                          <tr key={`${s.position}-${s.driver}`}>
                            <td>{s.position}</td>
                            <td className="avatar-cell"><DriverAvatar driverName={s.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                            <td>{s.driver}</td>
                            <td>{s.team}</td>
                            <td>{s.time || "-"}</td>
                            <td>{s.points}</td>
                          </tr>
                        ))
                        : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
                    </tbody>
                  </table>
                ) : (
                  <table>
                    <thead><tr><th>Pos</th><th></th><th>Driver</th><th>Team</th></tr></thead>
                    <tbody>
                      {race?.sprint_qualifying?.length
                        ? race.sprint_qualifying.map((sq) => (
                          <tr key={`${sq.position}-${sq.driver}`}>
                            <td>{sq.position}</td>
                            <td className="avatar-cell"><DriverAvatar driverName={sq.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                            <td>{sq.driver}</td>
                            <td>{sq.team}</td>
                          </tr>
                        ))
                        : <tr><td colSpan={4} className="small">{noDataText}</td></tr>}
                    </tbody>
                  </table>
                )}
              </div>
            </div>

            <div className="card top-card session-standings-card qualifying-stack-card">
              <div className="session-card-head">
                <div>
                  <h3>Race Session</h3>
                  <div className="small">{race?.race}</div>
                </div>
                <div className="session-tabbar">
                  <button
                    className={raceSessionTab === "results" ? "active" : ""}
                    onClick={() => setRaceSessionTab("results")}
                  >
                    Round Result
                  </button>
                  <button
                    className={raceSessionTab === "qualifying" ? "active" : ""}
                    onClick={() => setRaceSessionTab("qualifying")}
                  >
                    Qualifying
                  </button>
                </div>
              </div>

              <div className="table-scroll card-table-wrap">
                {raceSessionTab === "results" ? (
                  <table>
                    <thead>
                      <tr><th>Pos</th><th></th><th>Driver</th><th>Team</th><th>Time</th><th>Pts</th></tr>
                    </thead>
                    <tbody>
                      {race?.results?.length
                        ? race.results.map((r) => (
                          <tr key={r.position}>
                            <td>{r.position}</td>
                            <td className="avatar-cell"><DriverAvatar driverName={r.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                            <td>{r.driver}</td>
                            <td>{r.team}</td>
                            <td>{r.time}</td>
                            <td>{r.points}</td>
                          </tr>
                        ))
                        : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
                    </tbody>
                  </table>
                ) : (
                  <table>
                    <thead><tr><th>Pos</th><th></th><th>Driver</th><th>Q1</th><th>Q2</th><th>Q3</th></tr></thead>
                    <tbody>
                      {race?.qualifying?.length
                        ? race.qualifying.map((q) => (
                          <tr key={q.position}>
                            <td>{q.position}</td>
                            <td className="avatar-cell"><DriverAvatar driverName={q.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                            <td>{q.driver}</td>
                            <td>{q.q1 || "-"}</td>
                            <td>{q.q2 || "-"}</td>
                            <td>{q.q3 || "-"}</td>
                          </tr>
                        ))
                        : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </>
        ) : (
          <>
            <div className="card top-card qualifying-stack-card">
              <h3>Qualifying Results</h3>
              <div className="table-scroll card-table-wrap">
                <table>
                  <thead><tr><th>Pos</th><th></th><th>Driver</th><th>Q1</th><th>Q2</th><th>Q3</th></tr></thead>
                  <tbody>
                    {race?.qualifying?.length
                      ? race.qualifying.map((q) => (
                        <tr key={q.position}>
                          <td>{q.position}</td>
                          <td className="avatar-cell"><DriverAvatar driverName={q.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                          <td>{q.driver}</td>
                          <td>{q.q1 || "-"}</td>
                          <td>{q.q2 || "-"}</td>
                          <td>{q.q3 || "-"}</td>
                        </tr>
                      ))
                      : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
                  </tbody>
                </table>
              </div>
            </div>

            <div className="card top-card">
              <h3>Round Results</h3>
              <div className="small">{race?.race}</div>
              <div className="table-scroll card-table-wrap">
                <table>
                  <thead>
                    <tr><th>Pos</th><th></th><th>Driver</th><th>Team</th><th>Time</th><th>Pts</th></tr>
                  </thead>
                  <tbody>
                    {race?.results?.length
                      ? race.results.map((r) => (
                        <tr key={r.position}>
                          <td>{r.position}</td>
                          <td className="avatar-cell"><DriverAvatar driverName={r.driver} season={race?.season || overview?.season || 2025} roundNo={roundNo} /></td>
                          <td>{r.driver}</td>
                          <td>{r.team}</td>
                          <td>{r.time}</td>
                          <td>{r.points}</td>
                        </tr>
                      ))
                      : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}
      </div>

      <div className="card progression-card">
        <h3>All Drivers Points Progression ({overview?.season || 2025})</h3>
        <div className="small">Every driver line is shown. Hover to compare round-by-round points.</div>
        <ResponsiveContainer width="100%" height={500}>
          <LineChart data={overview?.points_progression || []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="round" />
            <YAxis domain={[0, 450]} />
            <Tooltip content={<ProgressionTooltip />} />
            {progressionDrivers.map((driver, idx) => (
              <Line
                key={driver}
                type="monotone"
                dataKey={driver}
                stroke={colorForDriver(driver, driverTeamMap, idx)}
                strokeWidth={1.8}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="card progression-card">
        <h3>Constructor Points Progression ({overview?.season || 2025})</h3>
        <div className="small">Constructors ranked by cumulative points each round.</div>
        <ResponsiveContainer width="100%" height={420}>
          <LineChart data={overview?.constructor_points_progression || []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="round" />
            <YAxis />
            <Tooltip content={<ConstructorProgressionTooltip />} />
            {progressionConstructors.map((team, idx) => (
              <Line
                key={team}
                type="monotone"
                dataKey={team}
                stroke={TEAM_COLORS[team] || `hsl(${(idx * 47) % 360} 70% 42%)`}
                strokeWidth={2}
                dot={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      <div className="card rounds-card">
        <h3>{overview?.season || 2025} Stats For Every Round</h3>
        <div className="table-scroll">
          <table>
            <thead>
              <tr><th>Rnd</th><th>Race</th><th>Winner</th><th>Pole</th><th>Fastest Lap</th><th>Sprint</th></tr>
            </thead>
            <tbody>
              {(roundsSummary || []).length
                ? roundsSummary.map((r) => (
                  <tr key={r.round}>
                    <td>{r.round}</td>
                    <td>{r.race_name}</td>
                    <td>{r.winner || "-"}</td>
                    <td>{r.pole || "-"}</td>
                    <td>{r.fastest_lap_driver || "-"}</td>
                    <td>{r.had_sprint ? "Yes" : "No"}</td>
                  </tr>
                ))
                : <tr><td colSpan={6} className="small">{noDataText}</td></tr>}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

function EngineeringPanel({ roundNo, race }) {
  const [engineeringTab, setEngineeringTab] = useState("positions");
  const [positionsViewTab, setPositionsViewTab] = useState("lap_by_lap");
  const [positionsSession, setPositionsSession] = useState("race");
  const [driver, setDriver] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const [teammateAnalysis, setTeammateAnalysis] = useState(null);
  const [positionsData, setPositionsData] = useState(null);
  const [positionsLoading, setPositionsLoading] = useState(false);
  const [positionsError, setPositionsError] = useState("");
  const [selectedPositionDrivers, setSelectedPositionDrivers] = useState([]);
  const [tyreStrategyData, setTyreStrategyData] = useState(null);
  const [tyreStrategyLoading, setTyreStrategyLoading] = useState(false);
  const [tyreStrategyError, setTyreStrategyError] = useState("");
  const [hoveredTyreStint, setHoveredTyreStint] = useState(null);
  const [selectedTyreStint, setSelectedTyreStint] = useState(null);
  const [tyreStrategySession, setTyreStrategySession] = useState("race");
  const [h2hTab, setH2hTab] = useState("lap_times");
  const [h2hSession, setH2hSession] = useState("race");
  const [h2hDriverA, setH2hDriverA] = useState("");
  const [h2hDriverB, setH2hDriverB] = useState("");
  const [h2hData, setH2hData] = useState(null);
  const [h2hLoading, setH2hLoading] = useState(false);
  const [h2hError, setH2hError] = useState("");
  const [selectedH2HLap, setSelectedH2HLap] = useState(null);
  const [trackLapSelectionA, setTrackLapSelectionA] = useState("fastest");
  const [trackLapSelectionB, setTrackLapSelectionB] = useState("fastest");
  const [selectedSectorIndex, setSelectedSectorIndex] = useState(1);
  const [telemetrySession, setTelemetrySession] = useState("race");
  const [telemetryCatalog, setTelemetryCatalog] = useState(null);
  const [telemetryLoading, setTelemetryLoading] = useState(false);
  const [telemetryError, setTelemetryError] = useState("");
  const [telemetrySelections, setTelemetrySelections] = useState({});
  const [telemetryTraces, setTelemetryTraces] = useState({});
  const [telemetryCardLoading, setTelemetryCardLoading] = useState({});
  const telemetryFetchSignatureRef = useRef({});
  const [networkInput, setNetworkInput] = useState({
    packets: 180,
    base_latency_ms: 50,
    jitter_ms: 10,
    packet_loss_rate: 0.03,
    bandwidth_mbps: 3.5,
  });
  const [networkWhatIf, setNetworkWhatIf] = useState(null);
  const [strategyInput, setStrategyInput] = useState({
    total_laps: 57,
    pit_window_start: 14,
    pit_window_end: 32,
    simulations: 600,
  });
  const [strategyWhatIf, setStrategyWhatIf] = useState(null);

  const drivers = useMemo(() => race?.results?.map((r) => r.driver) || [], [race]);
  const isSprintWeekend = useMemo(
    () => Boolean((race?.sprint_qualifying?.length || 0) > 0 || (race?.sprint?.length || 0) > 0),
    [race]
  );
  const teammate = useMemo(() => {
    if (!analysis || !race?.results) return null;
    const match = race.results.find((r) => r.team === analysis.team && r.driver !== analysis.driver);
    return match?.driver || null;
  }, [analysis, race]);

  const sectorBreakdown = useMemo(() => {
    if (!analysis?.lap_prediction?.predicted_lap_time_s) return [];
    const lap = Number(analysis.lap_prediction.predicted_lap_time_s);
    const s1 = lap * 0.33;
    const s2 = lap * 0.4;
    const s3 = lap * 0.27;
    return [
      { sector: "S1", time_s: Number(s1.toFixed(3)) },
      { sector: "S2", time_s: Number(s2.toFixed(3)) },
      { sector: "S3", time_s: Number(s3.toFixed(3)) },
    ];
  }, [analysis]);

  const sectorBarColor = useMemo(
    () => colorForTeam(analysis?.team, "#d7263d"),
    [analysis]
  );

  useEffect(() => {
    if (!drivers.length) {
      setDriver("");
      return;
    }
    if (!driver || !drivers.includes(driver)) {
      setDriver(drivers[0]);
    }
  }, [drivers, driver]);

  useEffect(() => {
    if (drivers.length < 2) {
      setH2hDriverA(drivers[0] || "");
      setH2hDriverB("");
      return;
    }
    if (!h2hDriverA || !drivers.includes(h2hDriverA)) {
      setH2hDriverA(drivers[0]);
      return;
    }
    if (!h2hDriverB || !drivers.includes(h2hDriverB) || h2hDriverB === h2hDriverA) {
      const fallback = drivers.find((name) => name !== h2hDriverA) || drivers[1] || "";
      setH2hDriverB(fallback);
    }
  }, [drivers, h2hDriverA, h2hDriverB]);

  useEffect(() => {
    if (!driver || !roundNo) return;
    getEngineeringDriverAnalysis({ season: race?.season || 2025, round_no: roundNo, driver }).then(setAnalysis);
  }, [roundNo, driver, race]);

  useEffect(() => {
    if (!teammate || !roundNo) {
      setTeammateAnalysis(null);
      return;
    }
    getEngineeringDriverAnalysis({ season: race?.season || 2025, round_no: roundNo, driver: teammate }).then(setTeammateAnalysis);
  }, [roundNo, teammate, race]);

  useEffect(() => {
    simulateNetwork(networkInput).then(setNetworkWhatIf);
  }, [networkInput]);

  useEffect(() => {
    simulateStrategy(strategyInput).then(setStrategyWhatIf);
  }, [strategyInput]);

  useEffect(() => {
    if (!roundNo) return;
    const season = race?.season || 2025;
    setPositionsLoading(true);
    setPositionsError("");
    getEngineeringPositions(roundNo, season, positionsSession)
      .then((data) => {
        setPositionsData(data);
        const allDrivers = (data?.drivers || []).map((d) => d.driver);
        setSelectedPositionDrivers(allDrivers);
      })
      .catch(() => {
        setPositionsError("Unable to load lap-by-lap positions.");
        setPositionsData(null);
      })
      .finally(() => setPositionsLoading(false));
  }, [roundNo, race?.season, positionsSession]);

  useEffect(() => {
    if (!roundNo) return;
    const season = race?.season || 2025;
    setTyreStrategyLoading(true);
    setTyreStrategyError("");
    setHoveredTyreStint(null);
    setSelectedTyreStint(null);
    getEngineeringTyreStrategy(roundNo, season, tyreStrategySession)
      .then((data) => {
        setTyreStrategyData(data);
      })
      .catch(() => {
        setTyreStrategyError("Unable to load tyre strategy.");
        setTyreStrategyData(null);
      })
      .finally(() => setTyreStrategyLoading(false));
  }, [roundNo, race?.season, tyreStrategySession]);

  useEffect(() => {
    setTyreStrategySession("race");
  }, [roundNo, race?.season]);

  useEffect(() => {
    if (!roundNo || !h2hDriverA || !h2hDriverB || h2hDriverA === h2hDriverB) {
      return;
    }
    const season = race?.season || 2025;
    setH2hLoading(true);
    setH2hError("");
    getEngineeringH2H(roundNo, season, h2hDriverA, h2hDriverB, h2hSession)
      .then((data) => {
        setH2hData(data);
        setTrackLapSelectionA("fastest");
        setTrackLapSelectionB("fastest");
        setSelectedSectorIndex(1);
        const firstLap = data?.lap_times?.common_laps?.[0]?.lap;
        setSelectedH2HLap(Number.isFinite(Number(firstLap)) ? Number(firstLap) : null);
      })
      .catch((error) => {
        setH2hData(null);
        setH2hError(error?.message || "Unable to load H2H comparison.");
      })
      .finally(() => setH2hLoading(false));
  }, [roundNo, race?.season, h2hDriverA, h2hDriverB, h2hSession]);

  useEffect(() => {
    setH2hTab("lap_times");
  }, [roundNo, race?.season]);

  useEffect(() => {
    setH2hSession("race");
  }, [roundNo, race?.season]);

  useEffect(() => {
    setPositionsViewTab("lap_by_lap");
  }, [roundNo, race?.season]);

  useEffect(() => {
    setPositionsSession("race");
  }, [roundNo, race?.season]);

  useEffect(() => {
    if (!roundNo) return;
    const season = race?.season || 2025;
    setTelemetryLoading(true);
    setTelemetryError("");
    getEngineeringTelemetryCatalog(roundNo, season, telemetrySession)
      .then((data) => {
        setTelemetryCatalog(data);
      })
      .catch((error) => {
        setTelemetryCatalog(null);
        setTelemetryError(error?.message || "Unable to load telemetry catalog.");
      })
      .finally(() => setTelemetryLoading(false));
  }, [roundNo, race?.season, telemetrySession]);

  useEffect(() => {
    const drivers = (telemetryCatalog?.drivers || []).map((item) => item?.driver).filter(Boolean);
    if (!drivers.length) {
      setTelemetrySelections({});
      return;
    }
    setTelemetrySelections((current) => {
      const next = {};
      TELEMETRY_CARDS.forEach((card) => {
        const existing = current?.[card.key] || {};
        const chosenDriver = drivers.includes(existing.driver) ? existing.driver : "";
        next[card.key] = {
          driver: chosenDriver,
          lap: existing.lap || "fastest",
        };
      });
      return next;
    });
  }, [telemetryCatalog]);

  useEffect(() => {
    const season = race?.season || 2025;
    const selections = TELEMETRY_CARDS
      .map((card) => ({ cardKey: card.key, selection: telemetrySelections?.[card.key] }))
      .filter((item) => item.selection?.driver);
    const selectedCardKeys = new Set(selections.map((item) => item.cardKey));
    const inactiveCardKeys = TELEMETRY_CARDS
      .map((card) => card.key)
      .filter((cardKey) => !selectedCardKeys.has(cardKey));

    if (inactiveCardKeys.length) {
      setTelemetryTraces((current) => {
        if (!current) return {};
        const next = { ...current };
        inactiveCardKeys.forEach((cardKey) => {
          delete next[cardKey];
        });
        return next;
      });
      setTelemetryCardLoading((current) => {
        const next = { ...(current || {}) };
        inactiveCardKeys.forEach((cardKey) => {
          next[cardKey] = false;
        });
        return next;
      });
    }

    if (!roundNo || !selections.length) {
      setTelemetryTraces({});
      setTelemetryCardLoading({});
      telemetryFetchSignatureRef.current = {};
      return;
    }

    const previousSignatures = telemetryFetchSignatureRef.current || {};
    const pending = selections.filter(({ cardKey, selection }) => {
      const signature = `${season}|${roundNo}|${telemetrySession}|${selection.driver}|${selection.lap || "fastest"}`;
      const changed = previousSignatures[cardKey] !== signature;
      if (changed) {
        previousSignatures[cardKey] = signature;
      }
      return changed;
    });
    telemetryFetchSignatureRef.current = previousSignatures;
    if (!pending.length) {
      return;
    }

    const loadingMap = {};
    pending.forEach(({ cardKey }) => {
      loadingMap[cardKey] = true;
    });
    setTelemetryCardLoading((current) => ({ ...current, ...loadingMap }));

    let cancelled = false;
    Promise.all(
      pending.map(async ({ cardKey, selection }) => {
        try {
          const payload = await getEngineeringTelemetryTrace(
            roundNo,
            season,
            selection.driver,
            selection.lap || "fastest",
            telemetrySession
          );
          return { cardKey, payload, error: "" };
        } catch (error) {
          return { cardKey, payload: null, error: error?.message || "Unable to load trace." };
        }
      })
    ).then((results) => {
      if (cancelled) return;
      setTelemetryTraces((current) => {
        const next = { ...(current || {}) };
        results.forEach((item) => {
          next[item.cardKey] = item;
        });
        return next;
      });
      const doneMap = {};
      results.forEach((item) => {
        doneMap[item.cardKey] = false;
      });
      setTelemetryCardLoading((current) => ({ ...current, ...doneMap }));
    });

    return () => {
      cancelled = true;
    };
  }, [roundNo, race?.season, telemetrySession, telemetrySelections]);

  useEffect(() => {
    setTelemetrySession("race");
    setTelemetryTraces({});
    setTelemetryCardLoading({});
    telemetryFetchSignatureRef.current = {};
  }, [roundNo, race?.season]);

  const telemetryGraph = (analysis?.telemetry?.smoothed?.speed || []).map((s, i) => ({
    idx: i,
    speed: s,
    throttle: analysis?.telemetry?.smoothed?.throttle?.[i] || 0,
    brake: analysis?.telemetry?.smoothed?.brake?.[i] || 0,
  }));

  const telemetryDelta = analysis && teammateAnalysis
    ? {
        speed: Number((analysis.telemetry.summary.avg_speed - teammateAnalysis.telemetry.summary.avg_speed).toFixed(2)),
        drs: Number((analysis.telemetry.summary.drs_usage_estimate_pct - teammateAnalysis.telemetry.summary.drs_usage_estimate_pct).toFixed(2)),
        lap: Number((analysis.lap_prediction.predicted_lap_time_s - teammateAnalysis.lap_prediction.predicted_lap_time_s).toFixed(3)),
      }
    : null;

  const engineerRadio = useMemo(() => {
    if (!analysis) return null;

    const risk = networkWhatIf?.strategy_decision_risk || analysis?.network?.strategy_decision_risk || "LOW";
    const bestPitLap = strategyWhatIf?.best_pit_lap || analysis?.strategy?.best_pit_lap;
    const lapDelta = telemetryDelta?.lap ?? 0;

    if (risk === "HIGH") {
      return {
        call: "Hold current strategy. Telemetry link unstable, avoid aggressive reactive calls.",
        confidence: "MEDIUM",
      };
    }

    if (bestPitLap <= strategyInput.pit_window_start + 1 && lapDelta < 0) {
      return {
        call: `Box at window open (lap ${bestPitLap}). Undercut favored and pace delta is positive.`,
        confidence: "HIGH",
      };
    }

    if (bestPitLap >= strategyInput.pit_window_end - 1) {
      return {
        call: `Extend stint to lap ${bestPitLap}. Overcut profile currently stronger.`,
        confidence: "MEDIUM",
      };
    }

    if (lapDelta > 0.2) {
      return {
        call: `Manage tyres and traffic, target pit lap ${bestPitLap}. Teammate pace currently stronger.`,
        confidence: "MEDIUM",
      };
    }

    return {
      call: `Target pit lap ${bestPitLap}. Maintain push mode with current tyre management profile.`,
      confidence: "HIGH",
    };
  }, [analysis, networkWhatIf, strategyWhatIf, strategyInput, telemetryDelta]);

  const positionDriverTeamMap = useMemo(() => {
    const map = {};
    for (const result of race?.results || []) {
      map[result.driver] = result.team;
    }
    for (const item of positionsData?.drivers || []) {
      if (item.driver && item.team) {
        map[item.driver] = item.team;
      }
    }
    return map;
  }, [race, positionsData]);

  const positionDrivers = useMemo(
    () => (positionsData?.drivers || []).map((d) => d.driver),
    [positionsData]
  );

  const positionDriverColors = useMemo(() => {
    const map = {};
    for (const [idx, name] of positionDrivers.entries()) {
      map[name] = colorForDriver(name, positionDriverTeamMap, idx);
    }
    return map;
  }, [positionDrivers, positionDriverTeamMap]);

  const visiblePositionDrivers = useMemo(() => {
    if (!selectedPositionDrivers.length) return [];
    const selected = new Set(selectedPositionDrivers);
    return positionDrivers.filter((name) => selected.has(name));
  }, [positionDrivers, selectedPositionDrivers]);

  const lapRowsByLap = useMemo(() => {
    const map = {};
    for (const row of positionsData?.laps || []) {
      map[Number(row.lap)] = row;
    }
    return map;
  }, [positionsData]);

  const maxPositionValue = useMemo(() => {
    if (positionsData?.max_position) return positionsData.max_position;
    return Math.max(20, positionDrivers.length || 20);
  }, [positionsData, positionDrivers]);

  const positionTicks = useMemo(() => {
    const ticks = [1];
    for (let p = 5; p <= maxPositionValue; p += 5) {
      ticks.push(p);
    }
    if (maxPositionValue > 1 && !ticks.includes(maxPositionValue)) {
      ticks.push(maxPositionValue);
    }
    return ticks;
  }, [maxPositionValue]);

  const maxLapValue = useMemo(() => {
    const laps = positionsData?.laps || [];
    if (!laps.length) return 0;
    return laps.reduce((acc, row) => Math.max(acc, Number(row.lap) || 0), 0);
  }, [positionsData]);

  const minLapValue = useMemo(() => {
    const laps = positionsData?.laps || [];
    if (!laps.length) return 0;
    return laps.reduce((acc, row) => Math.min(acc, Number(row.lap) || 0), Number(laps[0].lap) || 0);
  }, [positionsData]);

  const dnfDriversSet = useMemo(() => new Set(positionsData?.dnf_drivers || []), [positionsData]);

  const lastSeenLapByDriver = useMemo(() => {
    const last = {};
    for (const row of positionsData?.laps || []) {
      const lap = Number(row.lap);
      for (const driver of positionDrivers) {
        if (Number.isFinite(row[driver])) {
          last[driver] = lap;
        }
      }
    }
    return last;
  }, [positionsData, positionDrivers]);

  const lastSeenPositionByDriver = useMemo(() => {
    const lastPos = {};
    for (const row of positionsData?.laps || []) {
      for (const driver of positionDrivers) {
        if (Number.isFinite(row[driver])) {
          lastPos[driver] = Number(row[driver]);
        }
      }
    }
    return lastPos;
  }, [positionsData, positionDrivers]);

  const dashedDriversSet = useMemo(() => {
    const teamToDrivers = new Map();
    for (const driverName of positionDrivers) {
      const team = positionDriverTeamMap[driverName];
      if (!team) continue;
      if (!teamToDrivers.has(team)) {
        teamToDrivers.set(team, []);
      }
      teamToDrivers.get(team).push(driverName);
    }

    const set = new Set();
    for (const driversInTeam of teamToDrivers.values()) {
      const ordered = [...driversInTeam].sort((a, b) => a.localeCompare(b));
      // Pick one stable representative per team for dashed style.
      const pick = ordered[ordered.length - 1];
      if (pick) {
        set.add(pick);
      }
    }
    return set;
  }, [positionDrivers, positionDriverTeamMap]);

  const positionsSummaryRows = useMemo(() => {
    const rowsSource = positionsSession === "sprint" ? race?.sprint : race?.results;
    const rows = Array.isArray(rowsSource) ? [...rowsSource] : [];
    const sprintGridByDriver = {};
    if (positionsSession === "sprint") {
      for (const row of race?.sprint_qualifying || []) {
        if (row?.driver) {
          sprintGridByDriver[row.driver] = Number(row.position);
        }
      }
    }
    rows.sort((a, b) => Number(a?.position || 999) - Number(b?.position || 999));
    return rows.map((row) => {
      const finishPosition = Number(row?.position);
      const startPosition = positionsSession === "sprint"
        ? Number(sprintGridByDriver[row?.driver])
        : Number(row?.grid);
      const hasPositions = Number.isFinite(finishPosition) && Number.isFinite(startPosition);
      const delta = hasPositions ? (startPosition - finishPosition) : null;
      let changeText = "-";
      if (hasPositions) {
        if (delta > 0) changeText = `\u2191 +${delta}`;
        else if (delta < 0) changeText = `\u2193 -${Math.abs(delta)}`;
        else changeText = `\u2192 0`;
      }
      return {
        ...row,
        finishPosition: Number.isFinite(finishPosition) ? finishPosition : null,
        startPosition: Number.isFinite(startPosition) ? startPosition : null,
        changeText,
        statusLabel: mapRaceResultStatus(
          positionsSession === "sprint" && !row?.status ? "finished" : row?.status
        ),
      };
    });
  }, [race, positionsSession]);

  const h2hDriverInfoA = h2hData?.drivers?.driver_a || null;
  const h2hDriverInfoB = h2hData?.drivers?.driver_b || null;
  const h2hCommonLaps = h2hData?.lap_times?.common_laps || [];
  const h2hLapsA = h2hData?.track_dominance?.driver_a?.laps || [];
  const h2hLapsB = h2hData?.track_dominance?.driver_b?.laps || [];
  const h2hPaceA = h2hData?.lap_times?.pace?.driver_a || null;
  const h2hPaceB = h2hData?.lap_times?.pace?.driver_b || null;
  const sectorsAvailable = Boolean(h2hData?.track_dominance?.sectors_available);

  const h2hColorA = colorForTeam(h2hDriverInfoA?.team, "#e11d48");
  const h2hColorB = colorForTeam(h2hDriverInfoB?.team, "#2563eb");

  const h2hLapChartData = useMemo(
    () => h2hCommonLaps.map((item) => ({
      lap: Number(item.lap),
      driverA: Number(item.driver_a_lap_s),
      driverB: Number(item.driver_b_lap_s),
      delta: Number(item.delta_s),
    })),
    [h2hCommonLaps]
  );

  const h2hLapByNumber = useMemo(() => {
    const map = {};
    for (const row of h2hCommonLaps) {
      map[Number(row.lap)] = row;
    }
    return map;
  }, [h2hCommonLaps]);

  const selectedH2HRow = selectedH2HLap != null ? h2hLapByNumber[selectedH2HLap] : null;

  const trackLapMapA = useMemo(() => {
    const map = {};
    for (const row of h2hLapsA) {
      map[Number(row.lap)] = row;
    }
    return map;
  }, [h2hLapsA]);

  const trackLapMapB = useMemo(() => {
    const map = {};
    for (const row of h2hLapsB) {
      map[Number(row.lap)] = row;
    }
    return map;
  }, [h2hLapsB]);

  const fastestLapA = h2hData?.track_dominance?.driver_a?.fastest_lap || null;
  const slowestLapA = h2hData?.track_dominance?.driver_a?.slowest_lap || null;
  const fastestLapB = h2hData?.track_dominance?.driver_b?.fastest_lap || null;
  const slowestLapB = h2hData?.track_dominance?.driver_b?.slowest_lap || null;

  const resolveTrackLap = (selection, lapsMap, fastestLap, slowestLap) => {
    if (selection === "fastest") return lapsMap[Number(fastestLap?.lap)] || null;
    if (selection === "slowest") return lapsMap[Number(slowestLap?.lap)] || null;
    return lapsMap[Number(selection)] || null;
  };

  const selectedTrackLapA = resolveTrackLap(trackLapSelectionA, trackLapMapA, fastestLapA, slowestLapA);
  const selectedTrackLapB = resolveTrackLap(trackLapSelectionB, trackLapMapB, fastestLapB, slowestLapB);

  const sectorRows = useMemo(() => {
    const sectorsA = selectedTrackLapA?.sectors || [];
    const sectorsB = selectedTrackLapB?.sectors || [];
    const rows = [1, 2, 3].map((sector) => {
      const a = sectorsA.find((item) => Number(item?.sector) === sector) || {};
      const b = sectorsB.find((item) => Number(item?.sector) === sector) || {};
      const timeA = Number(a?.time_s);
      const timeB = Number(b?.time_s);
      const hasTimes = Number.isFinite(timeA) && Number.isFinite(timeB);
      const delta = hasTimes ? timeA - timeB : null;
      let winner = "none";
      if (hasTimes) {
        if (Math.abs(delta) < 0.0005) winner = "tie";
        else winner = delta < 0 ? "driver_a" : "driver_b";
      }
      return {
        sector,
        driverA: a,
        driverB: b,
        delta,
        winner,
      };
    });
    return rows;
  }, [selectedTrackLapA, selectedTrackLapB]);

  const selectedSectorRow = sectorRows.find((row) => row.sector === selectedSectorIndex) || sectorRows[0] || null;

  const telemetryDrivers = useMemo(() => telemetryCatalog?.drivers || [], [telemetryCatalog]);
  const telemetryDriverNames = useMemo(
    () => telemetryDrivers.map((item) => item?.driver).filter(Boolean),
    [telemetryDrivers]
  );
  const telemetryDriverTeamMap = useMemo(() => {
    const map = {};
    for (const result of race?.results || []) {
      if (result?.driver && result?.team) {
        map[result.driver] = result.team;
      }
    }
    telemetryDrivers.forEach((item) => {
      if (item?.driver && item?.team) {
        map[item.driver] = item.team;
      }
    });
    return map;
  }, [race, telemetryDrivers]);
  const telemetryDriverColors = useMemo(() => {
    const map = {};
    telemetryDriverNames.forEach((name, idx) => {
      map[name] = colorForDriver(name, telemetryDriverTeamMap, idx);
    });
    return map;
  }, [telemetryDriverNames, telemetryDriverTeamMap]);
  const telemetryDriverByName = useMemo(() => {
    const map = {};
    telemetryDrivers.forEach((item) => {
      if (item?.driver) {
        map[item.driver] = item;
      }
    });
    return map;
  }, [telemetryDrivers]);

  const telemetryLapOptionsForDriver = (driverName) => {
    const row = telemetryDriverByName[driverName];
    const laps = Array.isArray(row?.laps) ? row.laps : [];
    const fastestLapNo = Number(row?.fastest_lap?.lap);
    const fastestLabel = Number.isFinite(fastestLapNo)
      ? `Fastest Lap (L${fastestLapNo})`
      : "Fastest Lap";
    const options = [{ value: "fastest", label: fastestLabel }];
    laps.forEach((lapNo) => {
      options.push({ value: String(lapNo), label: `Lap ${lapNo}` });
    });
    return options;
  };

  const updateTelemetrySelection = (cardKey, patch) => {
    setTelemetrySelections((current) => {
      const nextCard = { ...(current?.[cardKey] || {}), ...patch };
      return { ...current, [cardKey]: nextCard };
    });
  };

  const togglePositionDriver = (driverName) => {
    setSelectedPositionDrivers((current) => {
      if (current.includes(driverName)) {
        return current.filter((item) => item !== driverName);
      }
      return [...current, driverName];
    });
  };

  const selectAllPositionDrivers = () => setSelectedPositionDrivers(positionDrivers);
  const clearAllPositionDrivers = () => setSelectedPositionDrivers([]);

  const renderPositionsTab = () => (
    <div className="grid">
      <div className="card wide-card">
        <div className="positions-head">
          <div>
            <h3>Positions</h3>
            <div className="small">Switch between lap trace and final race/sprint summary.</div>
            {isSprintWeekend ? (
              <div className="session-tabbar" style={{ marginTop: "0.45rem" }}>
                <button
                  type="button"
                  className={positionsSession === "race" ? "active" : ""}
                  onClick={() => setPositionsSession("race")}
                >
                  Race
                </button>
                <button
                  type="button"
                  className={positionsSession === "sprint" ? "active" : ""}
                  onClick={() => setPositionsSession("sprint")}
                >
                  Sprint
                </button>
              </div>
            ) : null}
            <div className="session-tabbar" style={{ marginTop: "0.45rem" }}>
              <button
                type="button"
                className={positionsViewTab === "lap_by_lap" ? "active" : ""}
                onClick={() => setPositionsViewTab("lap_by_lap")}
              >
                Lap-by-lap
              </button>
              <button
                type="button"
                className={positionsViewTab === "summary" ? "active" : ""}
                onClick={() => setPositionsViewTab("summary")}
              >
                Summary
              </button>
            </div>
          </div>
          {positionsViewTab === "lap_by_lap" ? (
            <details className="position-selector">
              <summary>{visiblePositionDrivers.length ? `${visiblePositionDrivers.length} Drivers` : "No Drivers"}</summary>
              <div className="position-selector-menu">
                <div className="position-selector-actions">
                  <button type="button" onClick={selectAllPositionDrivers}>All</button>
                  <button type="button" onClick={clearAllPositionDrivers}>None</button>
                </div>
                <div className="position-selector-list">
                  {positionDrivers.map((name) => {
                    const checked = selectedPositionDrivers.includes(name);
                    return (
                      <label key={name}>
                        <input
                          type="checkbox"
                          checked={checked}
                          onChange={() => togglePositionDriver(name)}
                        />
                        <span style={{ color: positionDriverColors[name] || "#1d2c3f" }}>{driverCode(name)}</span>
                      </label>
                    );
                  })}
                </div>
              </div>
            </details>
          ) : null}
        </div>

        {positionsViewTab === "lap_by_lap" ? (
          <>
            {positionsLoading ? <div className="small">Loading lap positions...</div> : null}
            {positionsError ? <div className="small">{positionsError}</div> : null}
            {!positionsLoading && !positionsError ? (
              <div style={{ width: "100%", height: "680px" }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={positionsData?.laps || []}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis
                      dataKey="lap"
                      type="number"
                      allowDecimals={false}
                      domain={[minLapValue, maxLapValue || 1]}
                      tickCount={Math.min(14, Math.max(6, Math.floor((maxLapValue || 1) / 4)))}
                    />
                    <YAxis
                      type="number"
                      reversed
                      domain={[0.5, maxPositionValue + 0.5]}
                      ticks={positionTicks}
                      allowDecimals={false}
                      tickFormatter={(val) => `P${val}`}
                    />
                    <Tooltip
                      content={
                        <PositionTooltip
                          lapRowsByLap={lapRowsByLap}
                          driverColors={positionDriverColors}
                          allDrivers={positionDrivers}
                          dnfDriversSet={dnfDriversSet}
                          lastSeenLapByDriver={lastSeenLapByDriver}
                          lastSeenPositionByDriver={lastSeenPositionByDriver}
                        />
                      }
                    />
                    {visiblePositionDrivers.map((name) => (
                      <Line
                        key={name}
                        type="monotone"
                        dataKey={name}
                        stroke={positionDriverColors[name] || "#334155"}
                        strokeWidth={2}
                        strokeDasharray={dashedDriversSet.has(name) ? "6 4" : undefined}
                        dot={false}
                        connectNulls={false}
                        isAnimationActive={false}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              </div>
            ) : null}

            <div className="position-driver-legend">
              <span style={{ color: "#5e6773" }}>QUALIFYING RESULTS:</span>
              {visiblePositionDrivers.map((name) => (
                <span key={name} style={{ color: positionDriverColors[name] || "#111" }}>
                  {dashedDriversSet.has(name) ? `${driverCode(name)} (dashed)` : driverCode(name)}
                </span>
              ))}
            </div>
          </>
        ) : (
          <div className="table-scroll card-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Pos</th>
                  <th>Driver</th>
                  <th>Team</th>
                  <th>Time</th>
                  <th>Pts</th>
                  <th>Start</th>
                  <th>Change</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {positionsSummaryRows.length ? positionsSummaryRows.map((row) => (
                  <tr key={`${row.driver}-${row.position}`}>
                    <td>{row.finishPosition ?? "-"}</td>
                    <td style={{ color: colorForTeam(row.team, "#dce7ff") }}>{row.driver || "-"}</td>
                    <td>{row.team || "-"}</td>
                    <td>{row.time || "-"}</td>
                    <td>{Number.isFinite(Number(row.points)) ? Number(row.points).toFixed(1) : "-"}</td>
                    <td>{row.startPosition ?? "-"}</td>
                    <td>{row.changeText}</td>
                    <td>
                      <span className={`status-pill status-${row.statusLabel}`}>{row.statusLabel}</span>
                    </td>
                  </tr>
                )) : (
                  <tr>
                    <td colSpan={8} className="small">No {positionsSession} results available.</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );

  const renderTyreStrategyTab = () => {
    const totalLaps = Math.max(1, Number(tyreStrategyData?.total_laps || 0));
    const selected = selectedTyreStint?.stint || null;

    return (
      <div className="grid">
        <div className="card wide-card">
          <div className="positions-head">
            <div>
              <h3>Tyre Strategy</h3>
              <div className="small">Stint lengths are proportional to lap count. Hover for quick info, click for full detail.</div>
              {isSprintWeekend ? (
                <div className="session-tabbar" style={{ marginTop: "0.4rem" }}>
                  <button
                    type="button"
                    className={tyreStrategySession === "race" ? "active" : ""}
                    onClick={() => setTyreStrategySession("race")}
                  >
                    Race
                  </button>
                  <button
                    type="button"
                    className={tyreStrategySession === "sprint" ? "active" : ""}
                    onClick={() => setTyreStrategySession("sprint")}
                  >
                    Sprint
                  </button>
                </div>
              ) : null}
            </div>
            <div className="tyre-legend">
              {["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"].map((compound) => (
                <span key={compound}>
                  <i style={{ background: colorForCompound(compound) }} />
                  {compound}
                </span>
              ))}
            </div>
          </div>

          {tyreStrategyLoading ? <div className="small">Loading tyre strategy...</div> : null}
          {tyreStrategyError ? <div className="small">{tyreStrategyError}</div> : null}

          {!tyreStrategyLoading && !tyreStrategyError ? (
            <div className="tyre-grid">
              {(tyreStrategyData?.drivers || []).map((driverRow) => (
                <div className="tyre-row" key={driverRow.driver}>
                  <div className="tyre-driver-label">P{driverRow.position} {driverCode(driverRow.driver)}</div>
                  <div className="tyre-row-track">
                    {(driverRow.stints || []).map((stint) => {
                      const widthPct = Math.max(0.8, (Number(stint.laps_count || 0) / totalLaps) * 100);
                      const isActive = selectedTyreStint
                        && selectedTyreStint.driver === driverRow.driver
                        && selectedTyreStint.stint?.stint_index === stint.stint_index;
                      const isHovered = hoveredTyreStint
                        && hoveredTyreStint.driver === driverRow.driver
                        && hoveredTyreStint.stint?.stint_index === stint.stint_index;
                      return (
                        <button
                          key={`${driverRow.driver}-${stint.stint_index}`}
                          type="button"
                          className={`tyre-stint-segment ${isActive ? "active" : ""}`}
                          style={{ width: `${widthPct}%`, background: colorForCompound(stint.compound) }}
                          title={`${stint.compound} (L${stint.start_lap}-L${stint.end_lap}, ${stint.laps_count} laps)`}
                          onMouseEnter={() => setHoveredTyreStint({ driver: driverRow.driver, position: driverRow.position, team: driverRow.team, stint })}
                          onMouseLeave={() => setHoveredTyreStint(null)}
                          onClick={() => setSelectedTyreStint({ driver: driverRow.driver, position: driverRow.position, team: driverRow.team, stint })}
                        >
                          <span>{stint.laps_count}</span>
                          {isHovered ? (
                            <div className="tyre-stint-tooltip">
                              {stint.compound} | L{stint.start_lap} to L{stint.end_lap} | {stint.laps_count} laps
                            </div>
                          ) : null}
                        </button>
                      );
                    })}
                  </div>
                </div>
              ))}
            </div>
          ) : null}

          <p className="small">{tyreStrategyData?.notes?.compound || ""}</p>
        </div>

        {selected ? (
          <div className="tyre-stint-popup-backdrop" onClick={() => setSelectedTyreStint(null)}>
            <div className="tyre-stint-popup-card" onClick={(e) => e.stopPropagation()}>
              <button
                type="button"
                className="tyre-stint-popup-close"
                aria-label="Close stint detail"
                onClick={() => setSelectedTyreStint(null)}
              >
                X
              </button>
              <h3>Selected Stint Detail</h3>
              <div className="tyre-stint-detail">
                <p className="small"><strong>Driver:</strong> P{selectedTyreStint.position} {selectedTyreStint.driver}</p>
                <p className="small"><strong>Team:</strong> {selectedTyreStint.team || "-"}</p>
                <p className="small"><strong>Tyre compound:</strong> {selected.compound}</p>
                <p className="small"><strong>Stint range:</strong> L{selected.start_lap} to L{selected.end_lap}</p>
                <p className="small"><strong>Stint length:</strong> {selected.laps_count} laps</p>
                <p className="small"><strong>Fastest lap:</strong> {selected.fastest_lap || "-"}</p>
                <p className="small"><strong>Slowest lap:</strong> {selected.slowest_lap || "-"}</p>
                <p className="small"><strong>Average lap:</strong> {selected.avg_lap || "-"}</p>
                <p className="small"><strong>Consistency (std dev):</strong> {formatMetricSeconds(selected.consistency_s)}</p>
                <p className="small"><strong>Degradation:</strong> {Number.isFinite(Number(selected.degradation_s_per_lap)) ? `${Number(selected.degradation_s_per_lap).toFixed(4)} s/lap` : "-"}</p>
                <p className="small"><strong>Timed laps in stint:</strong> {selected.laps_timed ?? 0}</p>
                <p className="small">{tyreStrategyData?.notes?.metrics || ""}</p>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    );
  };

  const renderH2HTab = () => {
    const trackDelta = Number(selectedTrackLapA?.lap_time_s) - Number(selectedTrackLapB?.lap_time_s);
    const trackDeltaWinner = !Number.isFinite(trackDelta)
      ? "-"
      : (trackDelta < 0 ? h2hDriverInfoA?.name : (trackDelta > 0 ? h2hDriverInfoB?.name : "Tie"));

    const lapOptionsA = [
      { value: "fastest", label: `Fastest (${fastestLapA?.lap ? `L${fastestLapA.lap}` : "-"})` },
      { value: "slowest", label: `Slowest (${slowestLapA?.lap ? `L${slowestLapA.lap}` : "-"})` },
      ...h2hLapsA.map((item) => ({ value: String(item.lap), label: `Lap ${item.lap}` })),
    ];
    const lapOptionsB = [
      { value: "fastest", label: `Fastest (${fastestLapB?.lap ? `L${fastestLapB.lap}` : "-"})` },
      { value: "slowest", label: `Slowest (${slowestLapB?.lap ? `L${slowestLapB.lap}` : "-"})` },
      ...h2hLapsB.map((item) => ({ value: String(item.lap), label: `Lap ${item.lap}` })),
    ];

    return (
      <div className="grid">
        <div className="card wide-card">
          <div className="positions-head">
            <div>
              <h3>Head to Head</h3>
              <div className="small">Compare two drivers lap-by-lap and by sector dominance.</div>
              {isSprintWeekend ? (
                <div className="session-tabbar" style={{ marginTop: "0.45rem" }}>
                  <button type="button" className={h2hSession === "race" ? "active" : ""} onClick={() => setH2hSession("race")}>Race</button>
                  <button type="button" className={h2hSession === "sprint" ? "active" : ""} onClick={() => setH2hSession("sprint")}>Sprint</button>
                </div>
              ) : null}
              <div className="session-tabbar" style={{ marginTop: "0.45rem" }}>
                <button type="button" className={h2hTab === "lap_times" ? "active" : ""} onClick={() => setH2hTab("lap_times")}>Lap Times</button>
                <button type="button" className={h2hTab === "track_dominance" ? "active" : ""} onClick={() => setH2hTab("track_dominance")}>Track Dominance</button>
              </div>
            </div>
          </div>

          <div className="form-row" style={{ marginBottom: "0.5rem" }}>
            <span className="small">Driver A</span>
            <select value={h2hDriverA} onChange={(e) => setH2hDriverA(e.target.value)}>
              {drivers.map((name) => <option key={`h2h-a-${name}`} value={name}>{name}</option>)}
            </select>
            {h2hTab === "track_dominance" ? (
              <>
                <span className="small">Lap A</span>
                <select value={trackLapSelectionA} onChange={(e) => setTrackLapSelectionA(e.target.value)}>
                  {lapOptionsA.map((option) => <option key={`a-${option.value}`} value={option.value}>{option.label}</option>)}
                </select>
              </>
            ) : null}
            <span className="small">Driver B</span>
            <select value={h2hDriverB} onChange={(e) => setH2hDriverB(e.target.value)}>
              {drivers.map((name) => <option key={`h2h-b-${name}`} value={name}>{name}</option>)}
            </select>
            {h2hTab === "lap_times" ? (
              <>
                <span className="small">Lap</span>
                <select
                  value={selectedH2HLap ?? ""}
                  onChange={(e) => {
                    const lap = Number(e.target.value);
                    setSelectedH2HLap(Number.isFinite(lap) ? lap : null);
                  }}
                >
                  {h2hCommonLaps.map((row) => (
                    <option key={`lap-select-${row.lap}`} value={Number(row.lap)}>
                      Lap {row.lap}
                    </option>
                  ))}
                </select>
              </>
            ) : null}
            {h2hTab === "track_dominance" ? (
              <>
                <span className="small">Lap B</span>
                <select value={trackLapSelectionB} onChange={(e) => setTrackLapSelectionB(e.target.value)}>
                  {lapOptionsB.map((option) => <option key={`b-${option.value}`} value={option.value}>{option.label}</option>)}
                </select>
              </>
            ) : null}
          </div>

          {h2hLoading ? <div className="small">Loading H2H...</div> : null}
          {h2hError ? <div className="small">{h2hError}</div> : null}
          {h2hData?.notes?.length ? <div className="small">{h2hData.notes.join(" ")}</div> : null}

          {!h2hLoading && !h2hError && h2hTab === "lap_times" ? (
            <div className="h2h-section">
              {selectedH2HRow ? (
                <div className="h2h-selected-lap-top card">
                  <h3>Lap {selectedH2HRow.lap}</h3>
                  <p className="small" style={{ color: h2hColorA }}>
                    {h2hDriverInfoA?.name || "Driver A"}: {selectedH2HRow.driver_a_lap || "-"}
                  </p>
                  <p className="small" style={{ color: h2hColorB }}>
                    {h2hDriverInfoB?.name || "Driver B"}: {selectedH2HRow.driver_b_lap || "-"}
                  </p>
                  <p className="small">
                    Delta ({h2hDriverInfoA?.name || "A"} - {h2hDriverInfoB?.name || "B"}): <strong>{formatSignedDelta(selectedH2HRow.delta_s)}</strong>
                  </p>
                </div>
              ) : null}

              <div className="h2h-chart-wrap">
                <ResponsiveContainer width="100%" height={340}>
                  <LineChart
                    data={h2hLapChartData}
                    onClick={(state) => {
                      const lap = Number(state?.activeLabel);
                      if (Number.isFinite(lap)) setSelectedH2HLap(lap);
                    }}
                  >
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="lap" />
                    <YAxis domain={["auto", "auto"]} />
                    <Tooltip
                      content={(
                        <H2HLapTimesTooltip
                          driverAName={h2hDriverInfoA?.name}
                          driverBName={h2hDriverInfoB?.name}
                          colorA={h2hColorA}
                          colorB={h2hColorB}
                        />
                      )}
                    />
                    <Line type="monotone" dataKey="driverA" stroke={h2hColorA} strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="driverB" stroke={h2hColorB} strokeWidth={2} dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <div className="h2h-pace-grid">
                <div className="card">
                  <div className="card-title-row">
                    <h3 style={{ color: h2hColorA }}>{h2hDriverInfoA?.name || "Driver A"} Pace</h3>
                    <InfoHint
                      label="About pace metrics"
                      content={(
                        <>
                          <strong>Median pace:</strong> middle lap time.
                          <br />
                          <strong>Consistency (IQR):</strong> spread of the middle 50% laps (lower = steadier).
                          <br />
                          <strong>Pace spread:</strong> slowest lap minus fastest lap (lower = tighter).
                        </>
                      )}
                    />
                  </div>
                  <p className="small">Median pace: <strong>{h2hPaceA?.median_pace || "-"}</strong></p>
                  <p className="small">Fastest lap: <strong>{h2hPaceA?.fastest_lap || "-"}</strong></p>
                  <p className="small">Slowest lap: <strong>{h2hPaceA?.slowest_lap || "-"}</strong></p>
                  <p className="small">Consistency (IQR): <strong>{formatMetricSeconds(h2hPaceA?.consistency_iqr_s)}</strong></p>
                  <p className="small">Pace spread: <strong>{formatMetricSeconds(h2hPaceA?.pace_spread_s)}</strong></p>
                </div>
                <div className="card">
                  <div className="card-title-row">
                    <h3 style={{ color: h2hColorB }}>{h2hDriverInfoB?.name || "Driver B"} Pace</h3>
                    <InfoHint
                      label="About pace metrics"
                      content={(
                        <>
                          <strong>Median pace:</strong> middle lap time.
                          <br />
                          <strong>Consistency (IQR):</strong> spread of the middle 50% laps (lower = steadier).
                          <br />
                          <strong>Pace spread:</strong> slowest lap minus fastest lap (lower = tighter).
                        </>
                      )}
                    />
                  </div>
                  <p className="small">Median pace: <strong>{h2hPaceB?.median_pace || "-"}</strong></p>
                  <p className="small">Fastest lap: <strong>{h2hPaceB?.fastest_lap || "-"}</strong></p>
                  <p className="small">Slowest lap: <strong>{h2hPaceB?.slowest_lap || "-"}</strong></p>
                  <p className="small">Consistency (IQR): <strong>{formatMetricSeconds(h2hPaceB?.consistency_iqr_s)}</strong></p>
                  <p className="small">Pace spread: <strong>{formatMetricSeconds(h2hPaceB?.pace_spread_s)}</strong></p>
                </div>
              </div>
            </div>
          ) : null}

          {!h2hLoading && !h2hError && h2hTab === "track_dominance" ? (
            <div className="h2h-section">
              <p className="small">
                Lap delta ({h2hDriverInfoA?.name || "A"} - {h2hDriverInfoB?.name || "B"}): <strong>{formatSignedDelta(trackDelta)}</strong>
                {" "}in favor of <strong>{trackDeltaWinner}</strong>
              </p>

              <div className="h2h-track-layout">
                {sectorRows.map((row) => (
                  <button
                    key={`sector-${row.sector}`}
                    type="button"
                    className={`h2h-sector-segment ${selectedSectorIndex === row.sector ? "active" : ""}`}
                    style={{
                      background: row.winner === "driver_a" ? h2hColorA : (row.winner === "driver_b" ? h2hColorB : "rgba(148, 163, 184, 0.4)"),
                    }}
                    onClick={() => setSelectedSectorIndex(row.sector)}
                  >
                    S{row.sector}
                  </button>
                ))}
              </div>

              {selectedSectorRow ? (
                <div className="h2h-sector-detail card">
                  <h3>Sector {selectedSectorRow.sector} Dominance</h3>
                  <p className="small">
                    Faster: <strong>{selectedSectorRow.winner === "driver_a"
                      ? (h2hDriverInfoA?.name || "Driver A")
                      : (selectedSectorRow.winner === "driver_b" ? (h2hDriverInfoB?.name || "Driver B") : "Tie / N/A")}</strong>
                  </p>
                  <p className="small">
                    Delta ({h2hDriverInfoA?.name || "A"} - {h2hDriverInfoB?.name || "B"}): <strong>{formatSignedDelta(selectedSectorRow.delta)}</strong>
                  </p>
                  <p className="small" style={{ color: h2hColorA }}>
                    {h2hDriverInfoA?.name || "Driver A"} | Time: {selectedSectorRow.driverA?.time || "-"} | Avg speed: {Number.isFinite(Number(selectedSectorRow.driverA?.avg_speed_kph)) ? `${Number(selectedSectorRow.driverA?.avg_speed_kph).toFixed(2)} km/h` : "-"} | Avg throttle: {Number.isFinite(Number(selectedSectorRow.driverA?.avg_throttle_pct)) ? `${Number(selectedSectorRow.driverA?.avg_throttle_pct).toFixed(2)}%` : "-"} | Avg brake: {Number.isFinite(Number(selectedSectorRow.driverA?.avg_brake_pct)) ? `${Number(selectedSectorRow.driverA?.avg_brake_pct).toFixed(2)}%` : "-"}
                  </p>
                  <p className="small" style={{ color: h2hColorB }}>
                    {h2hDriverInfoB?.name || "Driver B"} | Time: {selectedSectorRow.driverB?.time || "-"} | Avg speed: {Number.isFinite(Number(selectedSectorRow.driverB?.avg_speed_kph)) ? `${Number(selectedSectorRow.driverB?.avg_speed_kph).toFixed(2)} km/h` : "-"} | Avg throttle: {Number.isFinite(Number(selectedSectorRow.driverB?.avg_throttle_pct)) ? `${Number(selectedSectorRow.driverB?.avg_throttle_pct).toFixed(2)}%` : "-"} | Avg brake: {Number.isFinite(Number(selectedSectorRow.driverB?.avg_brake_pct)) ? `${Number(selectedSectorRow.driverB?.avg_brake_pct).toFixed(2)}%` : "-"}
                  </p>
                  {!sectorsAvailable ? (
                    <p className="small">Detailed sector telemetry is unavailable for this round/source.</p>
                  ) : null}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    );
  };

  const renderTelemetryTab = () => (
    <div className="grid telemetry-grid">
      <div className="card wide-card">
        <div className="positions-head">
          <div>
            <h3>Telemetry</h3>
            <div className="small">Each card can target a different driver and lap. Fastest lap is available for every driver.</div>
            <div className="telemetry-warning">
              Warning: load one telemetry card at a time to reduce buffering, delay, and chart load failures. First-time load might be slow.
            </div>
            {isSprintWeekend ? (
              <div className="session-tabbar" style={{ marginTop: "0.45rem" }}>
                <button type="button" className={telemetrySession === "race" ? "active" : ""} onClick={() => setTelemetrySession("race")}>Race</button>
                <button type="button" className={telemetrySession === "sprint" ? "active" : ""} onClick={() => setTelemetrySession("sprint")}>Sprint</button>
              </div>
            ) : null}
          </div>
        </div>
        {telemetryLoading ? <div className="small">Loading telemetry catalog...</div> : null}
        {telemetryError ? <div className="small">{telemetryError}</div> : null}
        {!telemetryLoading && !telemetryError && !telemetryDriverNames.length ? (
          <div className="small">No telemetry drivers available for this round/session.</div>
        ) : null}
      </div>

      {!telemetryLoading && !telemetryError ? TELEMETRY_CARDS.map((card, idx) => {
        const selection = telemetrySelections?.[card.key] || {};
        const driverName = selection.driver || "";
        const lapValue = selection.lap || "fastest";
        const lapOptions = driverName ? telemetryLapOptionsForDriver(driverName) : [];
        const traceState = telemetryTraces?.[card.key];
        const traceSamples = traceState?.payload?.samples || [];
        const stats = traceState?.payload?.stats?.[card.dataKey] || {};
        const selectedLap = traceState?.payload?.lap?.selected;
        const isCardLoading = Boolean(telemetryCardLoading?.[card.key]);
        const selectedDriverColor = telemetryDriverColors[driverName] || card.color;
        const sourceRaw = String(traceState?.payload?.source || "").toLowerCase();
        const sourceLabel = sourceRaw === "openf1" ? "OPENF1" : (sourceRaw === "synthetic" ? "SYNTHETIC" : "N/A");
        const sourceClass = sourceRaw === "openf1" ? "source-openf1" : (sourceRaw === "synthetic" ? "source-synthetic" : "source-na");
        const centerClass = TELEMETRY_CARDS.length === 5 && idx >= 3
          ? (idx === 3 ? "telemetry-card-center-left" : "telemetry-card-center-right")
          : "";

        return (
          <div key={card.key} className={`card telemetry-card ${centerClass}`}>
            <div className="card-title-row">
              <h3>{card.title}</h3>
              <span className={`telemetry-source-pill ${sourceClass}`}>{sourceLabel}</span>
            </div>
            <div className="form-row telemetry-card-controls">
              <span className="small">Driver</span>
              <select
                value={driverName}
                onChange={(e) => updateTelemetrySelection(card.key, { driver: e.target.value, lap: "fastest" })}
              >
                <option value="">Choose driver</option>
                {telemetryDriverNames.map((name) => <option key={`${card.key}-${name}`} value={name}>{name}</option>)}
              </select>
              <span className="small">Lap</span>
              <select
                value={lapValue}
                disabled={!driverName}
                onChange={(e) => updateTelemetrySelection(card.key, { lap: e.target.value })}
              >
                {!driverName ? <option value="fastest">Choose driver first</option> : null}
                {lapOptions.map((opt) => <option key={`${card.key}-${driverName}-${opt.value}`} value={opt.value}>{opt.label}</option>)}
              </select>
            </div>

            {traceState?.error ? <div className="small">{traceState.error}</div> : null}
            {!traceState && telemetryDriverNames.length && driverName ? <div className="small">Loading trace...</div> : null}
            {!driverName ? <div className="small">Choose driver to load telemetry.</div> : null}

            <div className="telemetry-chart-wrap" style={{ width: "100%", height: 230, marginTop: "0.45rem" }}>
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={traceSamples}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="distance_pct" tickFormatter={(value) => `${Math.round(Number(value))}%`} />
                  <YAxis
                    domain={
                      card.dataKey === "gear" ? [1, 8]
                        : card.dataKey === "drs" ? [0, 1]
                        : card.dataKey === "throttle" || card.dataKey === "brake" ? [0, 100]
                        : ["auto", "auto"]
                    }
                    allowDecimals={card.dataKey !== "gear" && card.dataKey !== "drs"}
                  />
                  <Tooltip
                    formatter={(value) => {
                      if (card.dataKey === "drs") {
                        return Number(value) === 1 ? "ON" : "OFF";
                      }
                      if (card.dataKey === "gear") {
                        return `G${value}`;
                      }
                      if (card.unit) {
                        return `${value} ${card.unit}`;
                      }
                      return value;
                    }}
                    labelFormatter={(value) => `Distance ${Math.round(Number(value))}%`}
                  />
                  <Line
                    type="monotone"
                    dataKey={card.dataKey}
                    stroke={selectedDriverColor}
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={false}
                  />
                </LineChart>
              </ResponsiveContainer>
              {isCardLoading ? (
                <div className="telemetry-loading-overlay">
                  <span>Loading telemetry...</span>
                </div>
              ) : null}
            </div>
            <p className="small">Lap: {Number.isFinite(Number(selectedLap)) ? `L${selectedLap}` : "-"}</p>
            <p className="small">Min: {Number.isFinite(Number(stats?.min)) ? Number(stats.min).toFixed(2) : "-"} | Max: {Number.isFinite(Number(stats?.max)) ? Number(stats.max).toFixed(2) : "-"} | Avg: {Number.isFinite(Number(stats?.avg)) ? Number(stats.avg).toFixed(2) : "-"}</p>
          </div>
        );
      }) : null}
    </div>
  );

  const renderWhatIfTab = () => (
    <div className="grid">
      <div className="card">
        <h3>Engineering View Controls</h3>
        <div className="form-row">
          <span className="small">Round: {roundNo}</span>
          <span className="small">Driver:</span>
          <select value={driver} onChange={(e) => setDriver(e.target.value)}>
            {drivers.map((d) => <option key={d} value={d}>{d}</option>)}
          </select>
          <span className="small">Team: {analysis?.team || "-"}</span>
          <span className="small">Finish Pos: {analysis?.race_result?.position || "-"}</span>
          <span className="small">Teammate: {teammate || "-"}</span>
        </div>
        <div className="driver-portrait-wrap">
          {driver ? <DriverPortrait driverName={driver} season={race?.season || 2025} roundNo={roundNo} /> : null}
        </div>
      </div>

      <div className="card info-card">
        <div className="card-title-row">
          <h3>Lap Time Prediction Engine</h3>
          <InfoHint
            label="About lap time prediction engine"
            content="Uses a synthetic LinearRegression model with tyre compound, synthetic sector profile, tyre age, and track temperature. Output is an estimated lap time, not direct live telemetry fitting."
          />
        </div>
        <div className="kpi">{analysis?.lap_prediction ? formatLapTimeSeconds(analysis.lap_prediction.predicted_lap_time_s) : "..."}</div>
        <p className="small">Model: {analysis?.lap_prediction?.model || "-"}</p>
        <p className="small">{analysis?.explanations?.lap_prediction || "-"}</p>
      </div>

      <div className="card" style={{ minHeight: 300 }}>
        <h3>Telemetry Signal Analysis</h3>
        <ResponsiveContainer width="100%" height={235}>
          <LineChart data={telemetryGraph}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="idx" />
            <YAxis />
            <Tooltip />
            <Line dataKey="speed" stroke="#d7263d" dot={false} />
            <Line dataKey="throttle" stroke="#1f7a8c" dot={false} />
            <Line dataKey="brake" stroke="#111" dot={false} />
          </LineChart>
        </ResponsiveContainer>
        <p className="small">Max speed: {analysis?.telemetry?.summary?.max_speed ?? "-"} km/h</p>
        <p className="small">DRS estimate: {analysis?.telemetry?.summary?.drs_usage_estimate_pct ?? "-"}%</p>
        <p className="small">{analysis?.explanations?.telemetry || "-"}</p>
      </div>

      <div className="card">
        <h3>Driver vs Teammate Delta</h3>
        <p className="small">Avg speed delta: <strong>{telemetryDelta ? `${telemetryDelta.speed} km/h` : "-"}</strong></p>
        <p className="small">DRS usage delta: <strong>{telemetryDelta ? `${telemetryDelta.drs}%` : "-"}</strong></p>
        <p className="small">Predicted lap delta: <strong>{telemetryDelta ? `${telemetryDelta.lap}s` : "-"}</strong></p>
        <p className="small">Negative lap delta means selected driver is faster than teammate.</p>
      </div>

      <div className="card info-card" style={{ minHeight: 280 }}>
        <div className="card-title-row">
          <h3>Sector Time Decomposition</h3>
          <InfoHint
            label="About sector time decomposition"
            content="This chart splits predicted lap time into synthetic S1/S2/S3 percentages (33/40/27). It is for pace profiling, not measured sector timing."
          />
        </div>
        <ResponsiveContainer width="100%" height={210}>
          <BarChart data={sectorBreakdown}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="sector" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="time_s" fill={sectorBarColor} />
          </BarChart>
        </ResponsiveContainer>
        <p className="small">Synthetic split of predicted lap time into S1/S2/S3 for quick pace profiling.</p>
      </div>

      <div className="card info-card">
        <div className="card-title-row">
          <h3>Race Engineer Radio</h3>
          <InfoHint
            label="About race engineer radio"
            content="This is an autogenerated call built from network risk, best pit-lap recommendation, and teammate lap delta. It is a rules-based advisory, not live team radio."
          />
        </div>
        <p className="small"><strong>Call:</strong> {engineerRadio?.call || "-"}</p>
        <p className="small"><strong>Confidence:</strong> {engineerRadio?.confidence || "-"}</p>
      </div>

      <div className="card info-card" style={{ minHeight: 300 }}>
        <div className="card-title-row">
          <h3>Strategy Simulation (What-If)</h3>
          <InfoHint
            label="About strategy simulation"
            content="Runs Monte Carlo simulations across the selected pit window. Total race time estimate combines base race time, tyre degradation trend, pit-stop delta, and traffic noise."
          />
        </div>
        <div className="form-row">
          <span className="small">Pit start</span>
          <input type="number" min="1" max="55" value={strategyInput.pit_window_start} onChange={(e) => setStrategyInput((s) => ({ ...s, pit_window_start: Number(e.target.value) || 1 }))} />
          <span className="small">Pit end</span>
          <input type="number" min="2" max="57" value={strategyInput.pit_window_end} onChange={(e) => setStrategyInput((s) => ({ ...s, pit_window_end: Number(e.target.value) || 2 }))} />
        </div>
        <ResponsiveContainer width="100%" height={210}>
          <BarChart data={strategyWhatIf?.candidates || analysis?.strategy?.candidates || []}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="pit_lap" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="avg_total_time_s" fill="#1f7a8c" />
          </BarChart>
        </ResponsiveContainer>
        <p className="small">Best pit lap: {strategyWhatIf?.best_pit_lap ?? analysis?.strategy?.best_pit_lap ?? "-"}</p>
        <p className="small">{analysis?.explanations?.strategy || "-"}</p>
      </div>

      <div className="card info-card" style={{ minHeight: 300 }}>
        <div className="card-title-row">
          <h3>Communication Network Simulation (What-If)</h3>
          <InfoHint
            label="About network simulation"
            content="Simulates telemetry packet delivery with latency, jitter, packet loss, and bandwidth penalty. Higher average delay or loss increases strategy decision risk."
          />
        </div>
        <div className="form-row">
          <span className="small">Latency</span>
          <input type="number" min="1" max="200" value={networkInput.base_latency_ms} onChange={(e) => setNetworkInput((s) => ({ ...s, base_latency_ms: Number(e.target.value) || 1 }))} />
          <span className="small">Jitter</span>
          <input type="number" min="0" max="100" value={networkInput.jitter_ms} onChange={(e) => setNetworkInput((s) => ({ ...s, jitter_ms: Number(e.target.value) || 0 }))} />
          <span className="small">Loss</span>
          <input type="number" min="0" max="0.5" step="0.005" value={networkInput.packet_loss_rate} onChange={(e) => setNetworkInput((s) => ({ ...s, packet_loss_rate: Number(e.target.value) || 0 }))} />
        </div>
        <p className="small">Avg Latency: <strong>{analysis?.network?.avg_latency_ms ?? "-"} ms</strong></p>
        <p className="small">Jitter: <strong>{analysis?.network?.jitter_ms ?? "-"} ms</strong></p>
        <p className="small">Packet Loss: <strong>{analysis?.network?.loss_pct ?? "-"}%</strong></p>
        <p className="small">Decision Risk: <strong>{analysis?.network?.strategy_decision_risk ?? "-"}</strong></p>
        <hr />
        <p className="small">What-if Latency: <strong>{networkWhatIf?.avg_latency_ms ?? "-"} ms</strong></p>
        <p className="small">What-if Loss: <strong>{networkWhatIf?.loss_pct ?? "-"}%</strong></p>
        <p className="small">What-if Risk: <strong>{networkWhatIf?.strategy_decision_risk ?? "-"}</strong></p>
        <p className="small">{analysis?.explanations?.network || "-"}</p>
      </div>
    </div>
  );

  const renderDiagnosticsTab = () => (
    <div className="grid">
      <div className="card wide-card">
        <h3>Coming Soon</h3>
        <div className="small">More features are coming soon. Stay tuned.</div>
      </div>
    </div>
  );

  return (
    <div className="engineering-panel-wrap">
      <div className="engineering-tabbar">
        <button className={engineeringTab === "positions" ? "active" : ""} onClick={() => setEngineeringTab("positions")}>Positions</button>
        <button className={engineeringTab === "tyre_strategy" ? "active" : ""} onClick={() => setEngineeringTab("tyre_strategy")}>Tyre Strategy</button>
        <button className={engineeringTab === "h2h" ? "active" : ""} onClick={() => setEngineeringTab("h2h")}>H2H</button>
        <button className={engineeringTab === "telemetry" ? "active" : ""} onClick={() => setEngineeringTab("telemetry")}>Telemetry</button>
        <button className={engineeringTab === "whatif" ? "active" : ""} onClick={() => setEngineeringTab("whatif")}>Race Engineer</button>
        <button className={engineeringTab === "diagnostics" ? "active" : ""} onClick={() => setEngineeringTab("diagnostics")}>COMING SOON</button>
      </div>
      {engineeringTab === "positions" ? renderPositionsTab() : null}
      {engineeringTab === "tyre_strategy" ? renderTyreStrategyTab() : null}
      {engineeringTab === "h2h" ? renderH2HTab() : null}
      {engineeringTab === "telemetry" ? renderTelemetryTab() : null}
      {engineeringTab === "whatif" ? renderWhatIfTab() : null}
      {engineeringTab === "diagnostics" ? renderDiagnosticsTab() : null}
    </div>
  );
}

function H2HLapTimesTooltip({ active, label, payload, driverAName, driverBName, colorA, colorB }) {
  if (!active || !payload?.length) return null;
  const valueA = payload.find((item) => item?.dataKey === "driverA")?.value;
  const valueB = payload.find((item) => item?.dataKey === "driverB")?.value;

  return (
    <div className="position-tooltip">
      <div className="position-tooltip-title">Lap {label}</div>
      <div className="position-tooltip-list">
        <div style={{ color: colorA || "#d4d9e1" }}>
          {driverAName || "Driver A"}: {formatLapTimeSeconds(valueA)}
        </div>
        <div style={{ color: colorB || "#d4d9e1" }}>
          {driverBName || "Driver B"}: {formatLapTimeSeconds(valueB)}
        </div>
      </div>
    </div>
  );
}

function GithubIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        fill="currentColor"
        d="M12 .5a12 12 0 0 0-3.79 23.39c.6.11.82-.26.82-.58v-2.16c-3.35.73-4.06-1.42-4.06-1.42-.55-1.4-1.35-1.77-1.35-1.77-1.1-.76.08-.75.08-.75 1.22.08 1.86 1.26 1.86 1.26 1.08 1.86 2.84 1.32 3.53 1.01.11-.79.42-1.32.76-1.62-2.67-.3-5.48-1.34-5.48-5.95 0-1.31.47-2.38 1.25-3.22-.12-.3-.54-1.53.12-3.2 0 0 1.01-.32 3.3 1.23a11.4 11.4 0 0 1 6 0c2.28-1.55 3.29-1.23 3.29-1.23.66 1.67.24 2.9.12 3.2.78.84 1.25 1.91 1.25 3.22 0 4.62-2.81 5.64-5.49 5.94.43.37.82 1.1.82 2.22v3.29c0 .32.22.69.82.58A12 12 0 0 0 12 .5Z"
      />
    </svg>
  );
}

function LinkedinIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path
        fill="currentColor"
        d="M4.98 3.5C4.98 4.88 3.86 6 2.49 6S0 4.88 0 3.5 1.12 1 2.49 1s2.49 1.12 2.49 2.5ZM.5 8h4V23h-4V8Zm7 0h3.83v2.05h.05c.53-1.01 1.84-2.08 3.79-2.08 4.05 0 4.8 2.67 4.8 6.14V23h-4v-7.84c0-1.87-.03-4.28-2.61-4.28-2.61 0-3.01 2.04-3.01 4.15V23h-4V8Z"
      />
    </svg>
  );
}

export default function App() {
  const [mode, setMode] = useState("casual");
  const [season, setSeason] = useState(2025);
  const [seasons, setSeasons] = useState([]);
  const [roundNo, setRoundNo] = useState(1);
  const [overview, setOverview] = useState(null);
  const [race, setRace] = useState(null);
  const [roundsSummary, setRoundsSummary] = useState([]);
  const roundOptions = useMemo(
    () => (roundsSummary || []).map((r) => ({ value: Number(r.round), label: `Round ${r.round} - ${r.race_name}` })),
    [roundsSummary]
  );

  useEffect(() => {
    getSeasons().then((data) => setSeasons(data?.seasons || []));
  }, []);

  useEffect(() => {
    getCasualOverview(season).then((data) => {
      setOverview(data);
      if (data?.rounds_count && data.rounds_count >= 1) {
        setRoundNo(1);
      }
    });
    getRoundsSummary(season).then((payload) => setRoundsSummary(payload?.rounds || []));
  }, [season]);

  useEffect(() => {
    getRoundResults(roundNo, season).then((payload) => setRace({ ...payload, season }));
  }, [roundNo, season]);

  return (
    <div className="page">
      <section className="hero">
        <div className="hero-main">
          <div className="hero-left">
            <h1>Dual-Mode Formula 1 Analytics Platform</h1>

            <div className="track-ribbon">
              <span className="track-chip">{race?.track?.name || "Track loading..."}</span>
              <span className="track-chip">{race?.track?.country || "-"}</span>
              <span className="track-chip">{race?.track?.locality || "-"}</span>
              <span className="track-chip">Date {race?.date || "-"}</span>
            </div>

            <div className="mode-switch">
              <button className={mode === "casual" ? "active" : ""} onClick={() => setMode("casual")}>Casual Mode</button>
              <button className={mode === "engineering" ? "active" : ""} onClick={() => setMode("engineering")}>Nerd Mode</button>
              <div className="form-row">
                <span className="small">Season</span>
                <select value={season} onChange={(e) => setSeason(Number(e.target.value) || 2025)}>
                  {seasons.map((s) => (
                    <option key={s} value={s}>{s}</option>
                  ))}
                </select>
              </div>
              <div className="form-row">
                <span className="small">Round</span>
                <select
                  value={roundNo}
                  onChange={(e) => setRoundNo(Number(e.target.value) || 1)}
                >
                  {roundOptions.length
                    ? roundOptions.map((opt) => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))
                    : <option value={roundNo}>Round {roundNo}</option>}
                </select>
              </div>
            </div>
            <div className="hero-note-row">
              <div className="hero-warning-note">
                Warning: first-time load might be slow. If buggy, please refresh the page.
              </div>
              <div className="hero-social-links">
                <a
                  className="hero-social-link"
                  href="https://github.com/Izzat2712"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="Izzat GitHub profile"
                  title="GitHub"
                >
                  <GithubIcon />
                </a>
                <a
                  className="hero-social-link"
                  href="https://www.linkedin.com/in/izzat-zakhir-673b99321"
                  target="_blank"
                  rel="noopener noreferrer"
                  aria-label="Izzat LinkedIn profile"
                  title="LinkedIn"
                >
                  <LinkedinIcon />
                </a>
              </div>
            </div>
          </div>
          <NextSessionCountdown />
        </div>
      </section>

      {mode === "casual"
        ? <CasualPanel overview={overview} race={race} roundsSummary={roundsSummary} roundNo={roundNo} />
        : <EngineeringPanel roundNo={roundNo} race={race} />}
      <Analytics />
    </div>
  );
}
