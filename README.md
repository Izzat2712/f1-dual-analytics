# Dual-Mode Formula 1 Analytics Platform

Formula 1 analytics website with two user experiences in one product:

- `Casual Mode` for fans
- `Nerd Mode` for engineering-focused analysis

Supports season and round navigation across `2021-2026`.

## Website Features

### Global

- Season selector and round selector
- Track ribbon (circuit, country, locality, race date)
- Next-session countdown card
- Team/driver visual assets
- Responsive layout for desktop and mobile

### Casual Mode

- Driver standings table
- Constructor standings table
- Qualifying results
- Round race results
- Full-season round summary table
- Driver points progression chart
- Constructor points progression chart
- Sprint-weekend handling where available

### Nerd Mode

- `Positions` tab
  - Lap-by-lap position traces
  - Race/sprint summary view
  - Driver filtering and legends

- `Tyre Strategy` tab
  - Stint visualization by driver
  - Compound coloring
  - Per-stint detail popup

- `H2H` tab
  - Driver vs driver lap-time comparison
  - Track dominance view and sector breakdown
  - Race/sprint session switching

- `Telemetry` tab
  - Per-card driver and lap selectors
  - OpenF1/synthetic source labels
  - Cards for speed trace, gear shifts, throttle input, brake input, and RPM
  - Per-card loading states and telemetry warnings

- `Race Engineer` tab
  - Driver analysis
  - Lap prediction
  - Strategy what-if simulation
  - Network simulation
  - Engineer radio recommendation

### Data Notes

- Primary race result/lap context: Jolpica/Ergast
- Telemetry source: OpenF1 when available, otherwise synthetic fallback
- Sprint qualifying is derived from sprint grid when direct source data is unavailable
