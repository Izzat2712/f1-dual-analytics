import React, { useEffect, useMemo, useState } from "react";
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
  getEngineeringDriverAnalysis,
  getEngineeringPositions,
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
  const [engineeringTab, setEngineeringTab] = useState("whatif");
  const [driver, setDriver] = useState("");
  const [analysis, setAnalysis] = useState(null);
  const [teammateAnalysis, setTeammateAnalysis] = useState(null);
  const [positionsData, setPositionsData] = useState(null);
  const [positionsLoading, setPositionsLoading] = useState(false);
  const [positionsError, setPositionsError] = useState("");
  const [selectedPositionDrivers, setSelectedPositionDrivers] = useState([]);
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
    getEngineeringPositions(roundNo, season)
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
            <h3>Lap-by-Lap Position Changes</h3>
            <div className="small">Track driver positions throughout the race. Use selector to filter.</div>
          </div>
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
        </div>

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
      </div>
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
        <button className={engineeringTab === "whatif" ? "active" : ""} onClick={() => setEngineeringTab("whatif")}>Race Engineer</button>
        <button className={engineeringTab === "positions" ? "active" : ""} onClick={() => setEngineeringTab("positions")}>Lap-by-Lap</button>
        <button className={engineeringTab === "diagnostics" ? "active" : ""} onClick={() => setEngineeringTab("diagnostics")}>COMING SOON</button>
      </div>
      {engineeringTab === "whatif" ? renderWhatIfTab() : null}
      {engineeringTab === "positions" ? renderPositionsTab() : null}
      {engineeringTab === "diagnostics" ? renderDiagnosticsTab() : null}
    </div>
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
      </section>

      {mode === "casual"
        ? <CasualPanel overview={overview} race={race} roundsSummary={roundsSummary} roundNo={roundNo} />
        : <EngineeringPanel roundNo={roundNo} race={race} />}
      <Analytics />
    </div>
  );
}
