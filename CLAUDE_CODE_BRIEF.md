# MLB HR Props Pipeline — Claude Code Project Brief
> Paste this at the start of every Claude Code session to give full context.

---

## Project Overview
Daily MLB home run props dashboard and scoring pipeline.

- **Live URL:** https://dm-2026.github.io/mlb-props/
- **Repo:** github.com/dm-2026/mlb-props
- **Local path:** C:\mlb-props
- **Branch:** gh-pages (served via GitHub Pages)

---

## File Structure
```
C:\mlb-props\
├── pipeline.py          # Main data pipeline + scoring engine
├── deploy.py            # Deployment script (pushes to gh-pages)
├── index.html           # Dashboard frontend
├── data/
│   └── data.json        # Pipeline output, consumed by index.html
├── og-image.png         # Open Graph image for link previews ("LET$ GET THE BREAD" poster)
└── .github/
    └── workflows/
        └── daily.yml    # GitHub Actions — runs 9 AM and 12:30 PM ET daily
```

---

## Daily Workflow
1. Run `pipeline.py` at ~8–9 AM ET (generates data.json)
2. Run `pipeline.py` again at ~11–11:30 AM ET (lineups confirmed)
3. Run `python deploy.py --skip-pipeline` to deploy without re-running pipeline
4. GitHub Actions also auto-runs at **9 AM ET** (0 13 * * *) and **12:30 PM ET** (30 16 * * *)

---

## Pipeline Architecture (pipeline.py)

### Data Sources
- **Statcast:** pybaseball (cached, 20s timeout on all API calls)
- **Lineups/probables:** MLB Stats API (`statsapi.mlb.com/api`)
- **Weather:** RotoWire scraper (`rotowire.com/baseball/weather.php`)
- **Handedness:** MLB Stats API roster endpoint with `?hydrate=person` + individual people API fallback

### Scoring Model — Component Weights
| Component | Weight |
|-----------|--------|
| EV + Barrel | 25% |
| Pitcher Vulnerability | 20% |
| Pitch Collision | 20% |
| Park Factor | 13% |
| Platoon | 12% |
| Weather | 5% |
| L14D Form | 5% |

### Tier Thresholds
- **PRIME:** score ≥ 55 AND factors_aligning ≥ 3
- **HIGH:** score ≥ 46 AND factors_aligning ≥ 2
- **MED:** score ≥ 30
- **LOW:** score ≥ 20
- **FADE:** score < 20

### Factors Aligning (max 5)
A factor "aligns" when:
- `pitcher_vuln > 0.4`
- `collision > 0.3`
- `park_score > 0.6`
- `weather_mult > 1.05`
- `form > 0.4`

### Dominant Signal Star ★
Shown on card when any single component exceeds threshold:
- `pitcher_vuln > 0.80`
- `collision > 0.70`
- `ev_barrel > 0.75`
- `park > 0.85`
- `form > 0.75`

### Pitcher Quality Multiplier (uses `_overall_hr9`)
| HR/9 Range | Multiplier |
|------------|-----------|
| < 0.6 | 0.85x |
| 0.6–0.9 | 0.93x |
| 0.9–1.4 | 1.00x |
| 1.4–1.8 | 1.05x |
| > 1.8 | 1.10x |

### 2025/2026 Data Blending
- Under 100 BF in 2026 → 100% 2025 data
- 100–200 BF → linear blend
- Over 200 BF → 100% 2026 data

---

## Critical Business Rules

### 🚨 Hard Rules (NEVER change without explicit instruction)
1. **0% barrel rate = automatic FADE** — regardless of any other factors. This is non-negotiable.
2. Player names sometimes come in approximated (e.g. "Wyatt Longford", "Alec Bulerson") — resolve silently, never flag as errors.

### Pitcher Vulnerability (HR% per PA)
- Uses `HR% = HRs / PA outcomes` (not HR/9 — this was a deliberate fix)
- Minimum 20 PA outcomes required before showing HR%
- `pa_events = group_vs[group_vs["events"].notna()]` — counts actual PA outcomes only
- HR Dist % = share of pitcher's total HRs by that pitch type (shown in matrix)
- Vuln scoring uses `hr_pct` internally normalized to 8%

### Overall HR/9
- Calculated as `_overall_hr9` across full dataset
- Shown in meta row of each batter card

### Handedness Resolution
1. Roster API fetch done first to build `hand_lookup` dict (both int and str keys)
2. Boxscore used for batting order ONLY, not handedness
3. Individual people API fallback per player if not in lookup
4. Switch hitters (S): bats L vs RHP, bats R vs LHP

### PITCH_NAMES Display Dict
```python
PITCH_NAMES = {
  'FC': 'Cutter', 'FS': 'Splitter',  # key fixes
  'FF': 'Four-seam', 'SI': 'Sinker', 'CH': 'Changeup',
  'SL': 'Slider', 'CU': 'Curveball', 'SW': 'Sweeper',
  'KC': 'Knuckle curve', 'ST': 'Sweeper'
}
```

---

## Hardcoded Fallback Data

### Seed Batters (used when pybaseball returns thin data < 3 pitch types or < 50 pitches)
- Matt Olson (621566)
- Kyle Schwarber (656941)
- Yordan Alvarez (670541)
- Aaron Judge (592450)
- Pete Alonso (624413)
- Shohei Ohtani (660271)
- Freddie Freeman (518692)
- Vladimir Guerrero Jr. (665489)

### FORCE_HARDCODED Dict
Currently: `{}` (empty — all batters using live 2026 data)

### ELITE_FADE List (auto-faded regardless of matchup)
- Yoshinobu Yamamoto (641154)
- Tarik Skubal (669923)
- Corbin Burnes (592789)
- Paul Skenes (694973)
- Jacob deGrom (572971)

---

## Dashboard Features (index.html)

### Layout
- **Header:** Logo + date + data freshness badge
- **Summary row:** Games / Prime / High / Auto-fades counts
- **Ticker tape:** Scrolling slate insights, PRIME player names, wind/park alerts. Hover to pause.
- **Game chips:** Compact, clickable, expand inline. Sort by Score or Pitch Collision per game.
- **Filter buttons:** All · Prime · High · Med · Low · My List (count badge)
- **Ranked targets list:** Batter cards with expand/collapse
- **Auto-fade list:** Bottom section

### Batter Card Contents
**Badges row:** Park factor · Wind · Temperature · Rain · Platoon · L14D HR streak

**Pitch matrix columns:**
- Pitch · Usage · Batter SLG · HR Dist (% of pitcher's HRs) · Collision (combined bar) · Avg LA

**Meta row:**
- Exit velo · Barrel% · Recent · 2026 PA · Park factor · HR mult · Pitcher HR/9

**Insight box:** Auto-generated text summary of top factors

### Special Features

**Laser Props ⚡**
Gold bolt next to batter name when ALL THREE:
- EV ≥ 91 mph AND
- Barrel% ≥ 10% AND
- Pitcher HR/9 ≥ 0.85

**Wrigley Wind Alert**
- CHC home + wind OUT ≥ 15 mph → pulsing green banner
- CHC home + wind IN ≥ 15 mph → red banner
- Wind IN check uses `startswith("WIND IN")` to prevent false matches

**No HR Signal Badge** (game-level, shown on chip)
Checks: park ≤ 90, wind IN ≥ 10mph, temp < 50°F, rain > 40%, either pitcher HR/9 < 0.85
- 3+ factors → 🚫 "No HR Beta" (red)
- 2 factors → ⚠ "HR Suppressed" (orange)

**My List**
- localStorage with gold ✓ checkbox on each card
- Auto-clears daily (keyed by `TODAY_STR`)
- Notes textarea at bottom of My List view, also daily auto-clear
- Keys: `mlb_mylist`, `mlb_notes`

### CSS Variables / Theme
Dark theme. Key variables:
- `--prime`, `--high`, `--med`, `--fade` for tier colors
- `--surface`, `--surface2` for card backgrounds
- `--text`, `--text2`, `--text3` for text hierarchy
- `--mono` for monospace font

---

## GitHub Actions (daily.yml)

```yaml
# Runs at 9 AM ET (0 13 * * *) and 12:30 PM ET (30 16 * * *)
# Manual trigger via workflow_dispatch
# Pybaseball cache preserved between runs via actions/cache@v4
# Push uses ${{ secrets.GITHUB_TOKEN }}
# Repo Settings → Actions → General → Workflow permissions → Read and write REQUIRED
```

Pipeline runs ~3–5 min with warm cache, ~28 min cold.

---

## Deploy Notes
- GitHub Pages serves from `gh-pages` branch
- `DEPLOY_INCLUDES` in deploy.py: `index.html`, `data/data.json`, `og-image.png`
- Standard deploy command: `python deploy.py --skip-pipeline`
- Full rebuild command: `python deploy.py` (runs pipeline first)

---

## Known Issues / History of Fixes
These were all previously broken and fixed — don't revert:

1. **PA calculation** — uses `pa_events = group_vs[group_vs["events"].notna()]` (fixes overcounting)
2. **HR% per pitch type** — uses HR/PA not HR/9 per pitch (more accurate for short samples)
3. **Handedness** — roster API first, boxscore for order only, people API fallback, switch hitter logic
4. **FC→Cutter, FS→Splitter** — display names fixed in PITCH_NAMES dict
5. **Wind IN false matches** — uses `startswith("WIND IN")` not just `"IN" in wind`
6. **Pitcher arsenal IP proxy** — uses per-pitch-type pitch count as denominator, not total arsenal
7. **batSide lookup** — overhauled to use MLB Stats API roster endpoint with `hydrate=person`
8. **0% barrel hard fade** — `barrel_pct or 5.0` was swallowing zero; now early-returns FADE explicitly
9. **Data blending weights** — `_weight` column was computed but never applied; fixed via proportional row sampling
10. **BF count** — was using `len(df["batter"].unique())` (unique IDs); now uses `events.notna().sum()` (actual PA outcomes)
11. **Wind IN regex bug** — lazy quantifier clipped `"blowing in"` to `"blowing"`; fixed with post-process detection
12. **`run_factor` unused** — now contributes 15% of collision score alongside SLG (85%)
13. **Lineup confirmed flag** — `get_roster_batters()` returns `(batters, confirmed: bool)`; cards show "Projected" badge when lineup not yet posted
14. **Recent form** — upgraded from raw HR count to HR/PA rate over L14D (falls back to count if < 10 PA)

---

## Odds API
- Daily cache: `odds_cache.json`

---

## Owner Preferences & Style Guide
- **Value over chalk** — avoid heavy juice legs
- **Trim weak legs** rather than forcing them into parlays
- **0% barrel = hard fade**, no exceptions
- Prefer concise, data-driven output
- Dashboard should feel like a pro tool, not a fantasy app
- Dark theme, monospace fonts for data, clean card layout

---

## How to Start a Session
When opening Claude Code in `C:\mlb-props`, say:

> "Read pipeline.py, deploy.py, index.html, data/data.json, and .github/workflows/daily.yml. I've pasted a project brief above — use that as your primary context for how the scoring model, pipeline, and dashboard work. Ask me what we're working on today."
