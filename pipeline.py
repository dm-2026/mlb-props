"""
MLB HR Props Pipeline
Fetches today's slate, scores each batter via pitch-type collision model,
writes data/data.json for the dashboard to consume.

Run: python pipeline.py
Then: python deploy.py  (pushes to GitHub Pages)
"""

import json
import os
import sys
import math
import re
import requests
import datetime
import logging
from pathlib import Path
from bs4 import BeautifulSoup

# ── Optional pybaseball import (graceful fallback to hardcoded data) ──────────
try:
    import pybaseball as pb
    pb.cache.enable()
    PYBASEBALL_AVAILABLE = True
except ImportError:
    PYBASEBALL_AVAILABLE = False
    print("[WARNING] pybaseball not installed. Using hardcoded seed data.")
    print("          Install with: pip install pybaseball")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
OUTPUT_FILE = DATA_DIR / "data.json"

TODAY = datetime.date.today()
SEASON_START_2026 = datetime.date(2026, 3, 27)
SEASON_START_2025 = datetime.date(2025, 3, 20)

# ── Scoring weights (from brief) ──────────────────────────────────────────────
WEIGHTS = {
    "ev_barrel":        0.20,  # reduced from 0.25 to fund collision bump
    "pitcher_vuln":     0.20,
    "pitch_collision":  0.25,  # bumped from 0.20 — HR Dist now wired in; strongest signal
    "park_factor":      0.13,
    "platoon":          0.12,
    "weather":          0.05,
    "recent_form":      0.05,
}

# ── Park factors (HR index, 100 = neutral) ────────────────────────────────────
# Source: Statcast park factors 2024-2025 average
PARK_FACTORS = {
    "LAD": {"overall": 132, "lhb": 128, "rhb": 136, "suppress": False, "dome": False, "lat": 34.0739, "lon": -118.2400},
    "CIN": {"overall": 118, "lhb": 115, "rhb": 121, "suppress": False, "dome": False, "lat": 39.0974, "lon": -84.5067},
    "ATL": {"overall": 114, "lhb": 118, "rhb": 110, "suppress": False, "dome": False, "lat": 33.8908, "lon": -84.4677},
    "PHI": {"overall": 111, "lhb": 122, "rhb": 102, "suppress": False, "dome": False, "lat": 39.9056, "lon": -75.1665},
    "TOR": {"overall": 108, "lhb": 105, "rhb": 111, "suppress": False, "dome": True,  "lat": 43.6414, "lon": -79.3894},
    "COL": {"overall": 125, "lhb": 122, "rhb": 128, "suppress": False, "dome": False, "lat": 39.7559, "lon": -104.9942},
    "TEX": {"overall": 103, "lhb": 101, "rhb": 105, "suppress": False, "dome": True,  "lat": 32.7512, "lon": -97.0832},
    "HOU": {"overall": 102, "lhb": 100, "rhb": 104, "suppress": False, "dome": False, "lat": 29.7573, "lon": -95.3555},
    "NYY": {"overall": 101, "lhb": 108, "rhb": 95,  "suppress": False, "dome": False, "lat": 40.8296, "lon": -73.9262},
    "NYM": {"overall": 100, "lhb": 98,  "rhb": 102, "suppress": False, "dome": False, "lat": 40.7571, "lon": -73.8458},
    "BOS": {"overall": 99,  "lhb": 103, "rhb": 96,  "suppress": False, "dome": False, "lat": 42.3467, "lon": -71.0972},
    "BAL": {"overall": 103, "lhb": 101, "rhb": 105, "suppress": False, "dome": False, "lat": 39.2838, "lon": -76.6218},
    "MIN": {"overall": 101, "lhb": 100, "rhb": 102, "suppress": False, "dome": False, "lat": 44.9817, "lon": -93.2778},
    "CHC": {"overall": 102, "lhb": 100, "rhb": 104, "suppress": False, "dome": False, "lat": 41.9484, "lon": -87.6553},
    "CHW": {"overall": 98,  "lhb": 96,  "rhb": 100, "suppress": False, "dome": False, "lat": 41.8300, "lon": -87.6338},
    "DET": {"overall": 98,  "lhb": 97,  "rhb": 99,  "suppress": False, "dome": False, "lat": 42.3390, "lon": -83.0485},
    "KC":  {"overall": 96,  "lhb": 95,  "rhb": 97,  "suppress": False, "dome": False, "lat": 39.0517, "lon": -94.4803},
    "MIL": {"overall": 97,  "lhb": 96,  "rhb": 98,  "suppress": False, "dome": False, "lat": 43.0280, "lon": -87.9712},
    "PIT": {"overall": 96,  "lhb": 95,  "rhb": 97,  "suppress": False, "dome": False, "lat": 40.4469, "lon": -80.0058},
    "STL": {"overall": 97,  "lhb": 96,  "rhb": 98,  "suppress": False, "dome": False, "lat": 38.6226, "lon": -90.1928},
    "ARI": {"overall": 102, "lhb": 100, "rhb": 104, "suppress": False, "dome": True,  "lat": 33.4453, "lon": -112.0667},
    "CLE": {"overall": 92,  "lhb": 91,  "rhb": 93,  "suppress": True,  "dome": False, "lat": 41.4962, "lon": -81.6852},
    "SF":  {"overall": 79,  "lhb": 82,  "rhb": 77,  "suppress": True,  "dome": False, "lat": 37.7786, "lon": -122.3893},
    "SD":  {"overall": 83,  "lhb": 85,  "rhb": 81,  "suppress": True,  "dome": False, "lat": 32.7076, "lon": -117.1570},
    "SEA": {"overall": 84,  "lhb": 86,  "rhb": 82,  "suppress": True,  "dome": False, "lat": 47.5914, "lon": -122.3325},
    "OAK": {"overall": 95,  "lhb": 94,  "rhb": 96,  "suppress": False, "dome": False, "lat": 37.7516, "lon": -122.2005},
    "TB":  {"overall": 97,  "lhb": 96,  "rhb": 98,  "suppress": False, "dome": True,  "lat": 27.7683, "lon": -82.6534},
    "LAA": {"overall": 96,  "lhb": 95,  "rhb": 97,  "suppress": False, "dome": False, "lat": 33.8003, "lon": -117.8827},
    "MIA": {"overall": 95,  "lhb": 94,  "rhb": 96,  "suppress": False, "dome": True,  "lat": 25.7781, "lon": -80.2197},
    "WSH": {"overall": 99,  "lhb": 98,  "rhb": 100, "suppress": False, "dome": False, "lat": 38.8730, "lon": -77.0074},
}

# Elite pitcher auto-fade list (mlbam IDs + names)
ELITE_FADE = {
    663776: "Garrett Crochet",
    641154: "Yoshinobu Yamamoto",
    669923: "Tarik Skubal",
    592789: "Corbin Burnes",
    694973: "Paul Skenes",
    # 572971: "Jacob deGrom",  # removed — inactive/retired, won't appear as probable
}

# Players whose 2025 data is unreliable (injury/down season) — use hardcoded seed instead
# Add entries as: {player_id: "Player Name"}
FORCE_HARDCODED = {}

# Module-level cache for pitcher HR/9 lookups during the no-HR signal check
_HR9_CACHE: dict = {}

# ── MLB Stats API helpers ─────────────────────────────────────────────────────

def get_todays_schedule():
    """Returns list of games for today from MLB Stats API."""
    date_str = TODAY.strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}&hydrate=probablePitcher,team,venue,weather,lineups"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        log.error(f"Schedule fetch failed: {e}")
        return []

    games = []
    for date_block in data.get("dates", []):
        for g in date_block.get("games", []):
            try:
                game = {
                    "game_pk": g["gamePk"],
                    "game_date": g.get("gameDate", ""),
                    "status": g.get("status", {}).get("detailedState", ""),
                    "venue_name": g.get("venue", {}).get("name", ""),
                    "away_team": g["teams"]["away"]["team"]["abbreviation"],
                    "home_team": g["teams"]["home"]["team"]["abbreviation"],
                    "away_probable": extract_pitcher(g["teams"]["away"]),
                    "home_probable": extract_pitcher(g["teams"]["home"]),
                    "weather": g.get("weather", {}),
                }
                games.append(game)
            except (KeyError, TypeError):
                continue
    log.info(f"Found {len(games)} games for {date_str}")
    return games


def extract_pitcher(team_data):
    """Safely extract probable pitcher info from team block."""
    pp = team_data.get("probablePitcher")
    if not pp:
        return None
    return {
        "id": pp.get("id"),
        "name": pp.get("fullName", "TBD"),
        "throws": pp.get("pitchHand", {}).get("code", "R"),
    }


def get_roster_batters(team_abbrev, game_pk):
    """
    Fetch active roster for a specific team by abbreviation.
    Always looks up by team abbreviation to avoid home/away cross-contamination.
    Gets batting handedness from the roster API (reliable) — boxscore doesn't include it.
    Uses boxscore batting order if confirmed lineup is available.
    """
    # Always fetch roster first to get reliable handedness data
    hand_lookup = {}  # player_id -> bats (L/R/S)
    try:
        url2 = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={TODAY.year}"
        r2 = requests.get(url2, timeout=20)
        teams = r2.json().get("teams", [])
        team_id = next(
            (t["id"] for t in teams if t.get("abbreviation") == team_abbrev), None
        )
        if team_id:
            # Use hydrate=person to get full person details including batSide
            roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active&hydrate=person"
            r3 = requests.get(roster_url, timeout=20)
            for p in r3.json().get("roster", []):
                pid = p["person"]["id"]
                # batSide comes back under person when hydrated
                hand = p.get("person", {}).get("batSide", {}).get("code")
                if not hand:
                    hand = p.get("person", {}).get("bats", {}).get("code", "R")
                hand_lookup[int(pid)] = hand or "R"
                hand_lookup[str(pid)] = hand or "R"
            log.info(f"  Hand lookup built for {team_abbrev}: {len(hand_lookup)//2} players")
    except Exception as e:
        log.warning(f"  Hand lookup failed for {team_abbrev}: {e}")

    # Try confirmed lineup from boxscore for batting order
    try:
        url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            data = r.json()
            for side in ["home", "away"]:
                team_data = data.get("teams", {}).get(side, {})
                abbrev = team_data.get("team", {}).get("abbreviation", "")
                if abbrev != team_abbrev:
                    continue
                batters_order = team_data.get("battingOrder", [])
                if batters_order:
                    players = team_data.get("players", {})
                    result = []
                    for pid in batters_order:
                        p = players.get(f"ID{pid}", {})
                        info = p.get("person", {})
                        pos = p.get("position", {})
                        # Try hand_lookup first, then fall back to people API
                        bats = hand_lookup.get(int(pid), hand_lookup.get(str(pid)))
                        if not bats:
                            try:
                                pr = requests.get(f"https://statsapi.mlb.com/api/v1/people/{pid}", timeout=20)
                                bats = pr.json().get("people", [{}])[0].get("batSide", {}).get("code", "R")
                            except Exception:
                                bats = "R"
                        result.append({
                            "id": pid,
                            "name": info.get("fullName", ""),
                            "bats": bats,
                            "position": pos.get("abbreviation", ""),
                        })
                    if result:
                        log.info(f"  Confirmed lineup for {team_abbrev}: {len(result)} batters")
                        return result, True   # confirmed lineup
    except Exception:
        pass

    # Fallback: return full roster from hand_lookup
    if hand_lookup:
        try:
            url2 = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={TODAY.year}"
            r2 = requests.get(url2, timeout=20)
            teams = r2.json().get("teams", [])
            team_id = next(
                (t["id"] for t in teams if t.get("abbreviation") == team_abbrev), None
            )
            if team_id:
                roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
                r3 = requests.get(roster_url, timeout=20)
                batters = []
                for p in r3.json().get("roster", []):
                    pos = p.get("position", {}).get("abbreviation", "")
                    if pos not in ("SP", "RP", "P"):
                        pid = p["person"]["id"]
                        batters.append({
                            "id": pid,
                            "name": p["person"]["fullName"],
                            "bats": hand_lookup.get(pid, "R"),
                            "position": pos,
                        })
                log.info(f"  Active roster for {team_abbrev}: {len(batters)} batters (projected)")
                return batters[:13], False   # projected — lineup not yet confirmed
        except Exception as e:
            log.error(f"Roster fetch failed for {team_abbrev}: {e}")
    return [], False


# ── Weather — RotoWire scraper ────────────────────────────────────────────────

# RotoWire already translates wind direction per stadium (e.g. "blowing out",
# "blowing in", "left to right") so we don't need a compass bearing lookup table.

WIND_MULTIPLIERS = {
    "blowing out to left":   1.12,  # best for HR — pull hitters benefit most
    "blowing out to right":  1.06,
    "blowing out to center": 1.08,
    "blowing out":           1.10,
    "blowing in":            0.90,
    "left to right":         0.99,
    "right to left":         0.99,
}

CITY_TO_TEAM = {
    "pittsburgh": "PIT",   "cleveland": "CLE",    "boston": "BOS",
    "new york city": "NYY","new york": "NYM",      "chicago": "CHC",
    "washington": "WSH",   "philadelphia": "PHI",  "baltimore": "BAL",
    "atlanta": "ATL",      "miami": "MIA",         "detroit": "DET",
    "minneapolis": "MIN",  "kansas city": "KC",    "st. louis": "STL",
    "denver": "COL",       "houston": "HOU",       "arlington": "TEX",
    "dallas": "TEX",       "seattle": "SEA",       "los angeles": "LAD",
    "anaheim": "LAA",      "san francisco": "SF",  "san diego": "SD",
    "phoenix": "ARI",      "oakland": "OAK",       "cincinnati": "CIN",
    "milwaukee": "MIL",    "toronto": "TOR",       "tampa": "TB",
    "sacramento": "OAK",   "minneapolis": "MIN",
}

DOME_TEAMS = {"TOR", "TB", "TEX", "ARI", "MIA", "SEA"}

_SENTENCE_RE = re.compile(
    r"(\d+)°\s*F\s+with\s+a\s+(\d+)%\s+chance\s+of\s+[\w\s]+?\s+and\s+"
    r"(\d+)\s+MPH\s+wind\s+([\w\s\-]+?)\s+in\s+([\w\s.,]+?)\s+at",
    re.IGNORECASE,
)


def get_weather_rotowire():
    """
    Scrapes RotoWire's MLB weather page (plain HTML, no JS required).
    Returns dict keyed by home team abbreviation:
      {temp_f, wind_mph, wind_dir, rain_pct, rain_risk,
       hr_multiplier, wind_label, dome, description}
    """
    url = "https://www.rotowire.com/baseball/weather.php"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        full_text = soup.get_text(" ", strip=True)
    except Exception as e:
        log.error(f"RotoWire weather fetch failed: {e}")
        return {}

    results = {}

    for m in _SENTENCE_RE.finditer(full_text):
        temp_f    = int(m.group(1))
        rain_pct  = int(m.group(2))
        wind_mph  = int(m.group(3))
        wind_desc = m.group(4).strip().lower()
        city_raw  = m.group(5).strip().lower()

        # ── Regex lazy-match fix ──────────────────────────────────────────────
        # The lazy quantifier on wind_desc stops at the FIRST " in " it finds.
        # "blowing in in Chicago" → wind_desc="blowing", city_raw="in chicago"
        # "blowing in from left field in Chicago" → wind_desc="blowing", city_raw="in from left field in chicago"
        # Detect this: if wind_desc is bare "blowing" and city_raw starts with "in ",
        # the actual direction was "blowing in [...]" — recover it.
        if wind_desc == "blowing" and city_raw.startswith("in "):
            wind_desc = "blowing in"
            city_raw = city_raw[3:]  # strip the leading "in "

        # Match city string to team abbreviation
        team = None
        # Try longest match first to avoid "new york" matching before "new york city"
        for city_key in sorted(CITY_TO_TEAM, key=len, reverse=True):
            if city_key in city_raw:
                team = CITY_TO_TEAM[city_key]
                break
        if not team or team in results:  # keep first match per team
            continue

        # Wind direction → HR multiplier
        hr_wind_mult = 1.0
        wind_label = f"{wind_mph} mph"
        for phrase, mult in WIND_MULTIPLIERS.items():
            if phrase in wind_desc:
                hr_wind_mult = mult
                if mult > 1.02:
                    wind_label = f"Wind OUT {wind_mph} mph"
                elif mult < 0.95:
                    wind_label = f"Wind IN {wind_mph} mph"
                else:
                    wind_label = f"Wind cross {wind_mph} mph"
                break

        # Low wind — minimal impact regardless of direction
        if wind_mph < 6:
            hr_wind_mult = 1.0
            wind_label = f"{wind_mph} mph (calm)"

        # Temperature → HR multiplier
        if temp_f < 50:   temp_mult = 0.88
        elif temp_f < 60: temp_mult = 0.94
        elif temp_f > 85: temp_mult = 1.06
        elif temp_f > 75: temp_mult = 1.03
        else:             temp_mult = 1.0

        # Rain risk tier
        if rain_pct >= 70:   rain_risk = "LIKELY_DELAY"
        elif rain_pct >= 40: rain_risk = "POSSIBLE_DELAY"
        else:                rain_risk = "LOW"

        results[team] = {
            "temp_f":        temp_f,
            "wind_mph":      wind_mph,
            "wind_dir":      wind_desc,
            "rain_pct":      rain_pct,
            "rain_chance":   round(rain_pct / 100, 2),  # keep compat with rest of pipeline
            "rain_risk":     rain_risk,
            "hr_multiplier": round(max(hr_wind_mult * temp_mult, 0.80), 3),  # floor at 0.80
            "wind_label":    wind_label,
            "dome":          False,
        }

    log.info(f"RotoWire weather: {len(results)} outdoor parks parsed")

    # Inject dome entries (weather neutral)
    for t in DOME_TEAMS:
        results.setdefault(t, {
            "temp_f": 72, "wind_mph": 0, "wind_dir": "dome",
            "rain_pct": 0, "rain_chance": 0, "rain_risk": "LOW",
            "hr_multiplier": 1.0, "wind_label": "Dome", "dome": True,
        })

    return results



# ── Pybaseball data layer ─────────────────────────────────────────────────────

def get_pitcher_arsenal(pitcher_id, pitcher_name, batter_hand="R"):
    """
    Returns dict: {pitch_type: {usage_pct, hr_pct, barrel_rate_allowed, avg_ev_allowed}}
    Filtered to pitches thrown to batter_hand (L or R).
    HR/9 is calculated per pitch type using that pitch type's own IP proxy,
    not the total arsenal IP proxy (fixes inflation bug).
    2025 is used as the primary dataset; 2026 is only blended in after 100+ BF.
    """
    if not PYBASEBALL_AVAILABLE:
        return get_hardcoded_pitcher_data(pitcher_id)

    try:
        import pandas as pd

        start_2025 = SEASON_START_2025.strftime("%Y-%m-%d")
        end_2025 = "2025-10-05"
        df25 = pb.statcast_pitcher(start_2025, end_2025, pitcher_id)

        start_2026 = SEASON_START_2026.strftime("%Y-%m-%d")
        end_2026 = TODAY.strftime("%Y-%m-%d")
        df26 = pb.statcast_pitcher(start_2026, end_2026, pitcher_id)

        # Tighter blending — only start mixing in 2026 after 100 batters faced
        # BF = actual PA outcomes (events.notna()), not unique batter count
        bf_2026 = int(df26["events"].notna().sum()) if len(df26) > 0 else 0
        if bf_2026 < 100:
            # Use 2025 only — 2026 sample too small to be meaningful
            df_all = df25.copy() if not df25.empty else pd.DataFrame()
            log.info(f"  {pitcher_name}: {bf_2026} BF in 2026 — using 2025 data only")
        elif bf_2026 >= 200:
            # 2026 sample large enough — use exclusively
            df_all = df26.copy()
            log.info(f"  {pitcher_name}: {bf_2026} BF in 2026 — using 2026 data only")
        else:
            # Linear blend: 100 BF = 0% 2026 contribution, 200 BF = 100% 2026
            # Subsample df25 proportionally so row counts reflect the weight ratio
            w26 = min((bf_2026 - 100) / 100, 1.0)
            w25 = 1.0 - w26
            n25_target = int(len(df26) * (w25 / w26)) if w26 > 0 else len(df25)
            df25_sampled = df25.sample(n=min(n25_target, len(df25)), random_state=42) if len(df25) > 0 else df25
            df_all = pd.concat([df25_sampled, df26])
            log.info(f"  {pitcher_name}: blending 2025 ({round(w25*100)}%, {len(df25_sampled)} pitches) + 2026 ({round(w26*100)}%, {len(df26)} pitches)")

        if df_all.empty:
            log.warning(f"No Statcast data for pitcher {pitcher_name} ({pitcher_id})")
            return {}

        total_pitches = len(df_all)

        # Filter to this batter handedness
        df_vs = df_all[df_all["stand"] == batter_hand] if "stand" in df_all.columns else df_all
        if len(df_vs) < 50:
            log.info(f"  {pitcher_name} vs {batter_hand}HB: small sample ({len(df_vs)}), using full dataset")
            df_vs = df_all

        arsenal = {}
        for pt, group_all in df_all.groupby("pitch_type"):
            if pt is None or str(pt) == "nan" or pt == "PO":
                continue

            # Usage from full dataset (all batters, all hands)
            usage_pct = round(len(group_all) / total_pitches * 100, 1)
            if usage_pct < 2:
                continue

            # HR/9 from hand-specific group for this pitch type
            group_vs = df_vs[df_vs["pitch_type"] == pt]
            if len(group_vs) == 0:
                continue

            hr_events = group_vs[group_vs["events"] == "home_run"]
            hr_count_pt = len(hr_events)

            # HR% per PA — only count PAs that ended on this pitch type
            # Filter to rows where an actual plate appearance outcome occurred
            pa_events = group_vs[group_vs["events"].notna() & (group_vs["events"] != "")]
            pa_on_pitch = len(pa_events)
            hr_pct = (hr_count_pt / pa_on_pitch * 100) if pa_on_pitch >= 20 else None

            # Barrel rate allowed — use launch_speed_angle==6 (pybaseball doesn't return "barrel" column)
            if "launch_speed_angle" in group_vs.columns:
                barrels = (group_vs["launch_speed_angle"] == 6).sum()
            elif "barrel" in group_vs.columns:
                barrels = group_vs["barrel"].sum()
            else:
                barrels = 0
            barrel_rate = barrels / len(group_vs) if len(group_vs) > 0 else 0
            avg_ev = group_vs["launch_speed"].mean() if "launch_speed" in group_vs.columns else None

            # ── Pitcher K / whiff metrics per pitch type ──────────────────────
            swing_desc = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
                          "hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
            if "description" in group_vs.columns:
                swings_p = group_vs[group_vs["description"].isin(swing_desc)]
                whiffs_p = group_vs[group_vs["description"].isin({"swinging_strike", "swinging_strike_blocked"})]
                pitcher_whiff_rate = round(len(whiffs_p) / len(swings_p) * 100, 1) if len(swings_p) >= 10 else None
            else:
                pitcher_whiff_rate = None

            # Pitcher K% on this pitch = Ks ending on this pitch / PA ending on this pitch
            pitcher_k_rate_pt = round(len(hr_events[hr_events["events"] == "strikeout"]) / pa_on_pitch * 100, 1) if pa_on_pitch >= 10 else None
            # (reuse pa_events filter already computed above)
            k_pa_pt = pa_events[pa_events["events"] == "strikeout"] if pa_on_pitch > 0 else group_vs.iloc[0:0]
            pitcher_k_rate_pt = round(len(k_pa_pt) / pa_on_pitch * 100, 1) if pa_on_pitch >= 10 else None

            arsenal[str(pt)] = {
                "usage_pct": usage_pct,
                "hr_pct": round(hr_pct, 1) if hr_pct is not None else None,
                "hr_count": hr_count_pt,
                "barrel_rate_allowed": round(barrel_rate * 100, 1),
                "avg_ev_allowed": round(avg_ev, 1) if avg_ev and not math.isnan(avg_ev) else None,
                "pitch_count": len(group_vs),
                "batter_hand": batter_hand,
                "whiff_rate": pitcher_whiff_rate,   # % of swings that miss on this pitch
                "k_rate": pitcher_k_rate_pt,         # % of PAs ending in K on this pitch
            }

        # ── HR distribution % — share of total HRs by pitch type ─────────────
        # e.g. "70% of pitcher's HRs came on four-seam" — displayed in matrix
        # Uses hand-specific HRs (vs LHB or RHB depending on batter)
        total_hrs_vs = sum(v.get("hr_count", 0) for k, v in arsenal.items() if not k.startswith("_"))
        for pt in arsenal:
            if pt.startswith("_"):
                continue
            hr_ct = arsenal[pt].get("hr_count", 0)
            arsenal[pt]["hr_dist_pct"] = round(hr_ct / total_hrs_vs * 100) if total_hrs_vs > 0 else None

        # Total HRs allowed to this batter hand — used to supplement platoon score
        arsenal["_hrs_vs_hand"] = total_hrs_vs

        # Overall pitcher K% and whiff rate vs this batter hand
        if "description" in df_vs.columns:
            swing_desc_p = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
                            "hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
            p_swings = df_vs[df_vs["description"].isin(swing_desc_p)]
            p_whiffs = df_vs[df_vs["description"].isin({"swinging_strike", "swinging_strike_blocked"})]
            overall_pitcher_whiff = round(len(p_whiffs) / len(p_swings) * 100, 1) if len(p_swings) >= 20 else None
            p_pa = df_vs[df_vs["events"].notna() & (df_vs["events"] != "")]
            p_ks = p_pa[p_pa["events"] == "strikeout"]
            overall_pitcher_k_rate = round(len(p_ks) / len(p_pa) * 100, 1) if len(p_pa) >= 20 else None
        else:
            overall_pitcher_whiff = None
            overall_pitcher_k_rate = None

        arsenal["_pitcher_k_rate"] = overall_pitcher_k_rate    # K% vs this batter hand
        arsenal["_pitcher_whiff_rate"] = overall_pitcher_whiff  # SwStr% vs this batter hand

        log.info(f"  Pitcher {pitcher_name} vs {batter_hand}HB: {len(arsenal)} pitch types, {total_hrs_vs} HR vs {batter_hand}HB, K%={overall_pitcher_k_rate}")

        # Overall pitcher HR/9 — calculated across full dataset (all hands, all pitches)
        # This is the accurate traditional HR/9 for the meta row display
        total_ip_proxy = len(df_all) / 12
        total_hrs = len(df_all[df_all["events"] == "home_run"]) if "events" in df_all.columns else 0
        overall_hr9 = round((total_hrs / total_ip_proxy * 9), 2) if total_ip_proxy > 0 else 0.0
        arsenal["_overall_hr9"] = overall_hr9

        return arsenal

    except Exception as e:
        log.error(f"Arsenal fetch failed for {pitcher_name}: {e}")
        return get_hardcoded_pitcher_data(pitcher_id)


def get_batter_pitch_stats(batter_id, batter_name, batter_hand):
    """
    Returns dict: {pitch_type: {slg, hr_count, xbh_count, run_factor, avg_launch_angle, avg_ev}}
    """
    if not PYBASEBALL_AVAILABLE:
        return get_hardcoded_batter_data(batter_id)

    try:
        start_2025 = SEASON_START_2025.strftime("%Y-%m-%d")
        end_2025 = "2025-10-05"
        df25 = pb.statcast_batter(start_2025, end_2025, batter_id)

        start_2026 = SEASON_START_2026.strftime("%Y-%m-%d")
        end_2026 = TODAY.strftime("%Y-%m-%d")
        df26 = pb.statcast_batter(start_2026, end_2026, batter_id)

        pa_2026 = df26["at_bat_number"].nunique() if len(df26) > 0 else 0
        w26 = min(pa_2026 / 200, 1.0)
        w25 = 1.0 - w26

        import pandas as pd
        if w26 >= 1.0 or len(df25) == 0:
            df = df26.copy() if len(df26) > 0 else df25
        elif len(df26) == 0:
            df = df25
        else:
            # Subsample df25 proportionally so its row weight matches w25
            n25_target = int(len(df26) * (w25 / w26)) if w26 > 0 else len(df25)
            df25_sampled = df25.sample(n=min(n25_target, len(df25)), random_state=42)
            df = pd.concat([df25_sampled, df26])

        if df.empty:
            return {}

        # Global stats
        contact = df[df["launch_speed"].notna()]
        avg_ev = contact["launch_speed"].mean() if len(contact) > 0 else None
        # Barrel classification: launch_speed_angle == 6 (Statcast standard)
        # NOTE: pybaseball does not return a "barrel" column — LSA code 6 is the correct source
        if "launch_speed_angle" in df.columns:
            barrels = (df["launch_speed_angle"] == 6).sum()
        elif "barrel" in df.columns:
            barrels = df["barrel"].sum()  # legacy fallback
        else:
            barrels = 0
        barrel_pct = barrels / len(contact) * 100 if len(contact) > 0 else 0
        contact_events = len(contact)  # passed to score_batter to gate the barrel hard fade

        # L14D form — HR/PA rate (more precise than raw count)
        two_weeks_ago = (TODAY - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
        df_recent = df26[df26["game_date"] >= two_weeks_ago] if len(df26) > 0 else df25[df25["game_date"] >= two_weeks_ago]
        hr_recent = len(df_recent[df_recent["events"] == "home_run"])
        pa_recent = int(df_recent["events"].notna().sum())
        # Rate only meaningful with 10+ PA — avoids 1 HR in 1 PA = 100% noise
        hr_rate_14d = round(hr_recent / pa_recent, 3) if pa_recent >= 10 else None

        pitch_stats = {}
        for pt, group in df.groupby("pitch_type"):
            if pt is None or str(pt) == "nan" or pt == "PO":
                continue
            contact_group = group[group["launch_speed"].notna()]
            hrs = group[group["events"] == "home_run"]
            xbh = group[group["events"].isin(["double", "triple", "home_run"])]

            slg_denom = group[group["events"].notna()].shape[0]
            slg_num = (len(hrs) * 4 +
                       len(group[group["events"] == "triple"]) * 3 +
                       len(group[group["events"] == "double"]) * 2 +
                       len(group[group["events"] == "single"]))
            slg = slg_num / slg_denom if slg_denom > 0 else 0

            avg_la = contact_group["launch_angle"].mean() if len(contact_group) > 0 else None

            # ── K / whiff metrics per pitch type ─────────────────────────────
            # Whiff rate = swinging strikes / total swings (swinging_strike / all swing descriptions)
            swing_desc = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
                          "hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
            if "description" in group.columns:
                swings = group[group["description"].isin(swing_desc)]
                whiffs = group[group["description"].isin({"swinging_strike", "swinging_strike_blocked"})]
                whiff_rate = round(len(whiffs) / len(swings) * 100, 1) if len(swings) >= 10 else None
                # Chase rate = swings on pitches outside zone / pitches outside zone
                # zone col: 11-14 = outside zone in pybaseball
                if "zone" in group.columns:
                    outside = group[group["zone"].isin([11, 12, 13, 14])]
                    chases = outside[outside["description"].isin(swing_desc)]
                    chase_rate_pt = round(len(chases) / len(outside) * 100, 1) if len(outside) >= 10 else None
                else:
                    chase_rate_pt = None
            else:
                whiff_rate = None
                chase_rate_pt = None

            # K rate on this pitch type = strikeouts ending on this pitch / PA ending on this pitch
            pa_events_pt = group[group["events"].notna() & (group["events"] != "")]
            k_events_pt = pa_events_pt[pa_events_pt["events"] == "strikeout"]
            k_rate_pt = round(len(k_events_pt) / len(pa_events_pt) * 100, 1) if len(pa_events_pt) >= 10 else None

            pitch_stats[str(pt)] = {
                "hr_count": len(hrs),
                "xbh_count": len(xbh),
                "run_factor": len(hrs) + len(xbh),
                "slg": round(slg, 3),
                "avg_launch_angle": round(avg_la, 1) if avg_la and not math.isnan(avg_la) else None,
                "sample_pitches": len(group),
                "whiff_rate": whiff_rate,       # % of swings that miss — key K signal
                "k_rate": k_rate_pt,             # % of PAs ending in K on this pitch
                "chase_rate": chase_rate_pt,     # % of outside pitches swung at
            }

        # ── Global K / whiff stats ────────────────────────────────────────────
        if "description" in df.columns:
            swing_desc_g = {"swinging_strike", "swinging_strike_blocked", "foul", "foul_tip",
                            "hit_into_play", "hit_into_play_no_out", "hit_into_play_score"}
            all_swings = df[df["description"].isin(swing_desc_g)]
            all_whiffs = df[df["description"].isin({"swinging_strike", "swinging_strike_blocked"})]
            overall_whiff_rate = round(len(all_whiffs) / len(all_swings) * 100, 1) if len(all_swings) >= 20 else None
            # Overall K rate = strikeouts / total PA outcomes
            pa_all = df[df["events"].notna() & (df["events"] != "")]
            k_all = pa_all[pa_all["events"] == "strikeout"]
            overall_k_rate = round(len(k_all) / len(pa_all) * 100, 1) if len(pa_all) >= 20 else None
            # Chase rate overall
            if "zone" in df.columns:
                outside_all = df[df["zone"].isin([11, 12, 13, 14])]
                chase_all = outside_all[outside_all["description"].isin(swing_desc_g)]
                overall_chase_rate = round(len(chase_all) / len(outside_all) * 100, 1) if len(outside_all) >= 20 else None
            else:
                overall_chase_rate = None
        else:
            overall_whiff_rate = None
            overall_k_rate = None
            overall_chase_rate = None

        # Attach global stats to result
        pitch_stats["_meta"] = {
            "avg_ev": round(avg_ev, 1) if avg_ev and not math.isnan(avg_ev) else None,
            "barrel_pct": round(barrel_pct, 1),
            "contact_events": contact_events,
            "hr_recent_14d": hr_recent,
            "hr_rate_14d": hr_rate_14d,
            "pa_recent_14d": pa_recent,
            "pa_2026": pa_2026,
            "w26": round(w26, 2),
            "k_rate": overall_k_rate,           # batter's overall K% — primary K signal
            "whiff_rate": overall_whiff_rate,   # overall whiff rate — swing-and-miss tendency
            "chase_rate": overall_chase_rate,   # chase rate — expands zone = more Ks
        }

        # Force hardcoded seed for players whose 2025 data is unreliable
        # (injury years, down seasons) — edit FORCE_HARDCODED at module level
        if batter_id in FORCE_HARDCODED:
            hardcoded = get_hardcoded_batter_data(batter_id)
            if hardcoded:
                log.info(f"  {batter_name}: using hardcoded seed (injury/down 2025 season)")
                hardcoded["_meta"]["hr_recent_14d"] = hr_recent
                hardcoded["_meta"]["hr_rate_14d"] = hr_rate_14d
                hardcoded["_meta"]["pa_recent_14d"] = pa_recent
                hardcoded["_meta"]["pa_2026"] = pa_2026
                return hardcoded

        # If live data is too thin (< 50 total pitches seen), supplement with
        # hardcoded seed data for known elite players — preserves L14D from live
        real_pitch_types = len(pitch_stats) - 1  # exclude _meta
        if real_pitch_types < 3 or len(df) < 50:
            hardcoded = get_hardcoded_batter_data(batter_id)
            if hardcoded and batter_id in [670541, 592450, 624413, 660271, 518692, 665489, 621566, 656941]:
                log.info(f"  {batter_name}: thin live data ({real_pitch_types} pitch types), using hardcoded seed")
                # Keep live L14D and PA count but use hardcoded pitch stats
                hardcoded["_meta"]["hr_recent_14d"] = hr_recent
                hardcoded["_meta"]["hr_rate_14d"] = hr_rate_14d
                hardcoded["_meta"]["pa_recent_14d"] = pa_recent
                hardcoded["_meta"]["pa_2026"] = pa_2026
                return hardcoded

        log.info(f"  Batter {batter_name}: {real_pitch_types} pitch types, {pa_2026} PA in 2026")
        return pitch_stats

    except Exception as e:
        log.error(f"Batter fetch failed for {batter_name}: {e}")
        return get_hardcoded_batter_data(batter_id)


# ── Scoring model ─────────────────────────────────────────────────────────────

def score_batter(batter, pitcher, arsenal, batter_stats, park_factor, weather):
    """
    Returns (score_0_100, component_scores, top_insight, tier)
    """
    meta = batter_stats.get("_meta", {})
    avg_ev = meta.get("avg_ev") or 87.0
    barrel_pct = meta.get("barrel_pct")
    contact_events = meta.get("contact_events", 100)  # default 100 for hardcoded seed data
    # Hard rule: 0% barrel rate = automatic FADE — but only with 30+ contact events
    # (below that, the sample is too thin to trust a 0%; use 5.0 default instead)
    if barrel_pct == 0 and contact_events >= 30:
        return 0, {k: 0 for k in WEIGHTS}, "0% barrel rate — hard fade", "FADE", []
    barrel_pct = barrel_pct or 5.0
    hr_recent = meta.get("hr_recent_14d") or 0

    # 1. EV + Barrel (25%) — normalize: EV 80-100, barrel 0-25%
    ev_score = min(max((avg_ev - 80) / 20, 0), 1)
    barrel_score = min(barrel_pct / 20, 1)
    ev_barrel_score = (ev_score * 0.6 + barrel_score * 0.4)

    PITCH_NAMES = {
        "FF":"Four-seam", "SI":"Sinker", "FC":"Cutter", "CH":"Changeup",
        "SL":"Slider", "CU":"Curveball", "SW":"Sweeper", "FS":"Splitter",
        "ST":"Sweeper", "KC":"Knuckle curve", "KN":"Knuckleball", "CS":"Slow curve",
    }

    # 2. Pitcher vulnerability (20%) — weighted by usage
    pitcher_vuln_score = 0.0
    pitcher_insights = []
    for pt, pa in arsenal.items():
        if pt.startswith("_"):
            continue
        usage = pa.get("usage_pct", 0) / 100
        hr_pct = pa.get("hr_pct") or 0
        vuln = min(hr_pct / 8.0, 1.0)
        pitcher_vuln_score += usage * vuln
        if usage > 0.20 and vuln > 0.5:
            pt_name = PITCH_NAMES.get(str(pt), str(pt))
            pitcher_insights.append(
                f"throws {round(usage*100)}% {pt_name} — {hr_pct}% HR rate allowed"
            )
    pitcher_vuln_score = min(pitcher_vuln_score, 1.0)

    # 3. Pitch collision (25%) — usage x HR distribution x batter damage
    # All three signals must align: pitcher throws it often (usage),
    # pitcher gives up HRs on it specifically (hr_dist_pct), batter crushes it (slg + hr_norm).
    # hr_dist_pct normalized to [0,1] using 50% as the elite ceiling.
    collision_score = 0.0
    collision_insights = []
    for pt, pa in arsenal.items():
        if pt.startswith("_"):
            continue
        bs = batter_stats.get(pt, {})
        if not bs:
            continue
        usage = pa.get("usage_pct", 0) / 100
        hr_dist = pa.get("hr_dist_pct")
        slg = bs.get("slg", 0)
        sample = bs.get("sample_pitches", 0)
        if sample < 20:
            continue
        slg_norm = min(slg / 0.900, 1.0)
        hr_norm = min(bs.get("hr_count", 0) / 8, 1.0)
        batter_damage = slg_norm * 0.80 + hr_norm * 0.20

        if hr_dist is not None:
            hr_dist_norm = min(hr_dist / 50.0, 1.0)
            pt_score = usage * hr_dist_norm * batter_damage
        else:
            # hr_dist unavailable (hardcoded seed / thin sample) — two-way fallback
            pt_score = usage * batter_damage * 0.70
        collision_score += pt_score
        if usage > 0.25 and slg > 0.500:
            pt_name = PITCH_NAMES.get(str(pt), str(pt))
            hr_dist_str = f", {hr_dist}% of pitcher HRs" if hr_dist is not None else ""
            collision_insights.append(
                f"{pt_name} ({round(usage*100)}% usage{hr_dist_str}): {slg:.3f} SLG"
            )
    collision_score = min(collision_score, 1.0)

    # 4. Park factor (15%)
    batter_hand = batter.get("bats", "R")
    pf_key = "lhb" if batter_hand == "L" else "rhb"
    pf = park_factor.get(pf_key, park_factor.get("overall", 100))
    # Normalize: 75=0, 100=0.5, 135=1.0
    park_score = min(max((pf - 75) / 60, 0), 1.0)

    # 5. Platoon (12%) — blended: traditional hand advantage + HRs allowed to this hand
    # HRs allowed to this hand can override the matchup — 18+ HRs vs RHB beats a same-hand "disadvantage"
    p_throws = pitcher.get("throws", "R")
    hrs_vs_hand = arsenal.get("_hrs_vs_hand", None)

    # Base platoon: advantage if batter hand ≠ pitcher hand
    platoon_base = 0.65 if batter_hand != p_throws else 0.35

    if hrs_vs_hand is not None:
        # HR volume signal: 0 HRs = 0.0, 10 HRs = 0.50, 20+ HRs = 1.0
        hr_vol_score = min(hrs_vs_hand / 20.0, 1.0)
        # Blend: 60% traditional platoon, 40% HR volume vs this hand
        # If pitcher has given up 18 HRs to RHBs, that 40% weight pushes score up
        # regardless of whether batter is same or opposite hand
        platoon_score = platoon_base * 0.60 + hr_vol_score * 0.40
    else:
        platoon_score = platoon_base

    # 6. Weather (5%)
    weather_score = min(max((weather.get("hr_multiplier", 1.0) - 0.85) / 0.30, 0), 1.0)

    # 7. Recent form (5%) — HR/PA rate is more precise than raw count
    # 10% HR/PA over 14 days = max score (elite hot streak)
    # Falls back to count-based if PA sample is too small (< 10 PA)
    hr_rate = meta.get("hr_rate_14d")
    if hr_rate is not None:
        form_score = min(hr_rate / 0.10, 1.0)
    else:
        form_score = min(hr_recent / 4, 1.0)

    # Weighted total
    raw = (
        WEIGHTS["ev_barrel"]       * ev_barrel_score +
        WEIGHTS["pitcher_vuln"]    * pitcher_vuln_score +
        WEIGHTS["pitch_collision"] * collision_score +
        WEIGHTS["park_factor"]     * park_score +
        WEIGHTS["platoon"]         * platoon_score +
        WEIGHTS["weather"]         * weather_score +
        WEIGHTS["recent_form"]     * form_score
    )

    score = round(raw * 100)

    # ── Pitcher quality multiplier ────────────────────────────────────────────
    # Use overall HR/9 from _overall_hr9 key (accurate, full dataset)
    # Falls back to weighted hr_pct if not available
    overall_hr9 = arsenal.get("_overall_hr9")
    if overall_hr9 is not None:
        if overall_hr9 < 0.6:
            pitcher_mult = 0.85
        elif overall_hr9 < 0.9:
            pitcher_mult = 0.93
        elif overall_hr9 < 1.4:
            pitcher_mult = 1.00
        elif overall_hr9 < 1.8:
            pitcher_mult = 1.05
        else:
            pitcher_mult = 1.10
    else:
        # Fallback: weighted avg HR% across arsenal
        total_usage = sum(pa.get("usage_pct", 0) for pt, pa in arsenal.items() if not pt.startswith("_"))
        if total_usage > 0:
            weighted_hr_pct = sum(
                pa.get("hr_pct", 0) * pa.get("usage_pct", 0)
                for pt, pa in arsenal.items() if not pt.startswith("_")
            ) / total_usage
            # HR% thresholds: <2% elite, 2-3.5% neutral, >5% vulnerable
            if weighted_hr_pct < 2.0:
                pitcher_mult = 0.85
            elif weighted_hr_pct < 3.0:
                pitcher_mult = 0.93
            elif weighted_hr_pct < 4.5:
                pitcher_mult = 1.00
            elif weighted_hr_pct < 6.0:
                pitcher_mult = 1.05
            else:
                pitcher_mult = 1.10
        else:
            pitcher_mult = 1.0

    score = round(score * pitcher_mult)
    # ─────────────────────────────────────────────────────────────────────────

    components = {
        "ev_barrel":       round(ev_barrel_score * 100),
        "pitcher_vuln":    round(pitcher_vuln_score * 100),
        "pitch_collision": round(collision_score * 100),
        "park_factor":     round(park_score * 100),
        "platoon":         round(platoon_score * 100),
        "weather":         round(weather_score * 100),
        "recent_form":     round(form_score * 100),
    }

    # Build insight string
    all_insights = collision_insights + pitcher_insights
    top_insight = all_insights[0] if all_insights else "Insufficient pitch-type sample for collision signal."

    # Dominant signal — any single component above 80 gets flagged with a star
    dominant_signals = []
    if pitcher_vuln_score > 0.80:  dominant_signals.append("pitcher_vuln")
    if collision_score > 0.70:     dominant_signals.append("pitch_collision")
    if ev_barrel_score > 0.75:     dominant_signals.append("ev_barrel")
    if park_score > 0.85:          dominant_signals.append("park_factor")
    if form_score > 0.75:          dominant_signals.append("recent_form")

    # Tier
    factors_aligning = sum([
        pitcher_vuln_score > 0.4,
        collision_score > 0.3,
        park_score > 0.6,
        weather.get("hr_multiplier", 1.0) > 1.05,
        form_score > 0.4,
    ])
    if score >= 55 and factors_aligning >= 3:
        tier = "PRIME"
    elif score >= 46 and factors_aligning >= 2:
        tier = "HIGH"
    elif score >= 30:
        tier = "MED"
    elif score >= 20:
        tier = "LOW"
    else:
        tier = "FADE"

    return score, components, top_insight, tier, dominant_signals


# ── K prop scoring model ──────────────────────────────────────────────────────

K_WEIGHTS = {
    "batter_k_rate":    0.30,  # batter's actual K% — most stable
    "batter_whiff":     0.20,  # batter's whiff rate — swing-and-miss tendency
    "pitcher_k_rate":   0.25,  # pitcher's actual K% vs this hand
    "k_collision":      0.15,  # pitch-type K collision — batter whiff × pitcher whiff on same pitch
    "platoon":          0.10,  # same-hand = more Ks
}

def score_batter_k(batter, pitcher, arsenal, batter_stats):
    """
    Returns (score_0_100, components, insight, tier_05, tier_15)
    Uses real Statcast K%, whiff rate, and pitch-type collision.
    Falls back to proxy signals only when live data unavailable.
    HIGH = genuine K prop edge (~top 15%), not just "everyone strikes out sometimes"
    """
    meta = batter_stats.get("_meta", {})
    PITCH_NAMES_K = {
        "FF":"Four-seam", "SI":"Sinker", "FC":"Cutter", "CH":"Changeup",
        "SL":"Slider", "CU":"Curveball", "SW":"Sweeper", "FS":"Splitter",
        "ST":"Sweeper", "KC":"Knuckle curve", "KN":"Knuckleball",
    }

    # ── 1. Batter K rate (30%) ────────────────────────────────────────────────
    # Use real K% if available; fall back to barrel/EV proxy only if not
    batter_k_pct = meta.get("k_rate")       # actual K% from Statcast
    batter_whiff_pct = meta.get("whiff_rate")  # overall whiff rate

    if batter_k_pct is not None:
        # Normalize: 10% K = 0.0 (elite contact), 25% = 0.50 (average), 45%+ = 1.0 (high K)
        # Ceiling raised to 45% so extreme whiffers don't auto-max
        batter_k_score = max(0.0, min(1.0, (batter_k_pct - 10.0) / 35.0))
    else:
        # Fallback proxy: low barrel + low EV = more Ks
        barrel_pct = meta.get("barrel_pct") or 5.0
        avg_ev = meta.get("avg_ev") or 87.0
        contact_quality = (barrel_pct * 1.2 + (avg_ev - 80) * 0.8)
        batter_k_score = max(0.05, min(0.75, 1.0 - contact_quality / 36.0))
        batter_k_pct = None

    # ── 2. Batter whiff rate (20%) ────────────────────────────────────────────
    if batter_whiff_pct is not None:
        # Normalize: 15% = 0.0 (great contact), 28% = 0.50 (average), 45%+ = 1.0 (big whiffer)
        batter_whiff_score = max(0.0, min(1.0, (batter_whiff_pct - 15.0) / 30.0))
    else:
        batter_whiff_score = batter_k_score * 0.8

    # ── 3. Pitcher K rate vs this hand (25%) ─────────────────────────────────
    pitcher_k_pct = arsenal.get("_pitcher_k_rate")
    pitcher_whiff_pct = arsenal.get("_pitcher_whiff_rate")

    # Gate: only trust pitcher K% if we have enough PA sample
    # Small sample (Connelly Early 17 PA) produces extreme/meaningless values
    total_pitcher_pa = sum(
        pa.get("pitch_count", 0) for pt, pa in arsenal.items() if not pt.startswith("_")
    )
    pitcher_sample_ok = total_pitcher_pa >= 80  # ~20+ IP equivalent

    if pitcher_k_pct is not None and pitcher_sample_ok:
        # Normalize: 12% = 0.0 (contact), 22% = 0.50 (average), 38%+ = 1.0 (elite K)
        pitcher_k_score = max(0.0, min(1.0, (pitcher_k_pct - 12.0) / 26.0))
    else:
        # Fallback: use HR/9 inversely — more reliable with small samples
        overall_hr9 = arsenal.get("_overall_hr9") or 1.2
        pitcher_k_score = max(0.10, min(0.80, (1.6 - overall_hr9) / 1.4))
        pitcher_k_pct = None  # flag as unavailable for display

    # ── 4. Pitch-type K collision (15%) ───────────────────────────────────────
    # The real money: pitcher's best K pitch × batter's whiff rate on that pitch
    # e.g. Pitcher throws sweeper 30% with 45% whiff rate, batter whiffs on sweepers 40%
    collision_score = 0.0
    best_collision = 0.0
    best_collision_insight = None

    for pt, pa in arsenal.items():
        if pt.startswith("_"):
            continue
        bs = batter_stats.get(pt, {})
        if not bs:
            continue
        usage = pa.get("usage_pct", 0) / 100
        p_whiff = pa.get("whiff_rate")       # pitcher's whiff rate on this pitch
        b_whiff = bs.get("whiff_rate")        # batter's whiff rate on this pitch
        b_k_rate_pt = bs.get("k_rate")        # batter's K rate on this pitch
        sample = bs.get("sample_pitches", 0)

        if sample < 15:
            continue

        # Score this pitch: usage × avg of pitcher and batter whiff signals
        if p_whiff is not None and b_whiff is not None:
            # Both real — normalize: 15% = low, 30% = avg, 50%+ = elite
            p_w_norm = max(0, min(1.0, (p_whiff - 15.0) / 35.0))
            b_w_norm = max(0, min(1.0, (b_whiff - 15.0) / 35.0))
            pt_collision = usage * (p_w_norm * 0.6 + b_w_norm * 0.4)
        elif b_k_rate_pt is not None:
            # Use batter K rate on this pitch as proxy
            pt_collision = usage * max(0, min(1.0, (b_k_rate_pt - 10.0) / 25.0)) * 0.7
        else:
            continue

        collision_score += pt_collision
        if pt_collision > best_collision:
            best_collision = pt_collision
            pt_name = PITCH_NAMES_K.get(str(pt), str(pt))
            p_whiff_str = f"{p_whiff:.0f}% pitcher whiff" if p_whiff else ""
            b_whiff_str = f"{b_whiff:.0f}% batter whiff" if b_whiff else ""
            stats_str = " · ".join(filter(None, [p_whiff_str, b_whiff_str]))
            best_collision_insight = f"{pt_name} ({round(usage*100)}% usage): {stats_str}" if stats_str else f"{pt_name} ({round(usage*100)}% usage) — K collision"

    collision_score = min(collision_score, 1.0)

    # ── 5. Platoon (10%) ──────────────────────────────────────────────────────
    b_hand = batter.get("bats", "R")
    p_throws = pitcher.get("throws", "R")
    platoon_score = 0.65 if b_hand == p_throws else 0.40

    # ── Weighted total ────────────────────────────────────────────────────────
    raw = (
        K_WEIGHTS["batter_k_rate"]  * batter_k_score +
        K_WEIGHTS["batter_whiff"]   * batter_whiff_score +
        K_WEIGHTS["pitcher_k_rate"] * pitcher_k_score +
        K_WEIGHTS["k_collision"]    * collision_score +
        K_WEIGHTS["platoon"]        * platoon_score
    )
    score = round(raw * 100)

    components = {
        "batter_k_rate":  round(batter_k_score * 100),
        "batter_whiff":   round(batter_whiff_score * 100),
        "pitcher_k_rate": round(pitcher_k_score * 100),
        "k_collision":    round(collision_score * 100),
        "platoon":        round(platoon_score * 100),
    }

    # Build insight — prefer real collision, fall back to pitcher-level signal
    if best_collision_insight:
        insight = best_collision_insight
    elif pitcher_k_pct is not None:
        insight = f"Pitcher K% {pitcher_k_pct:.1f}% vs {b_hand}HB"
    else:
        overall_hr9 = arsenal.get("_overall_hr9") or 1.2
        insight = f"Pitcher HR/9 {overall_hr9:.2f} — {'miss-bat profile' if overall_hr9 < 0.9 else 'contact-oriented' if overall_hr9 > 1.4 else 'neutral'}"

    # ── Tiers ─────────────────────────────────────────────────────────────────
    # With real data, expected range: weak matchup 20-35, average 35-52, strong 52-68, elite 68+
    if score >= 65:
        tier_05 = "HIGH"
    elif score >= 48:
        tier_05 = "MED"
    elif score >= 32:
        tier_05 = "LOW"
    else:
        tier_05 = "FADE"

    # 1.5 Ks needs elite matchup on both sides
    if score >= 74:
        tier_15 = "HIGH"
    elif score >= 58:
        tier_15 = "MED"
    elif score >= 44:
        tier_15 = "LOW"
    else:
        tier_15 = "FADE"

    return score, components, insight, tier_05, tier_15


# ── Hardcoded seed data (used when pybaseball unavailable) ────────────────────
# Replace these with real fetched data once pipeline is running

def get_hardcoded_pitcher_data(pitcher_id):
    PITCHER_DATA = {
        # Logan Webb
        621111: {
            "SI": {"usage_pct": 34.0, "hr_pct": 0.41, "barrel_rate_allowed": 5.1, "avg_ev_allowed": 86.2, "pitch_count": 800},
            "SW": {"usage_pct": 27.0, "hr_pct": 0.22, "barrel_rate_allowed": 3.8, "avg_ev_allowed": 85.1, "pitch_count": 635},
            "CH": {"usage_pct": 24.0, "hr_pct": 0.18, "barrel_rate_allowed": 3.2, "avg_ev_allowed": 84.9, "pitch_count": 565},
            "FC": {"usage_pct": 8.0,  "hr_pct": 0.55, "barrel_rate_allowed": 6.2, "avg_ev_allowed": 87.1, "pitch_count": 188},
            "FF": {"usage_pct": 7.0,  "hr_pct": 0.88, "barrel_rate_allowed": 8.1, "avg_ev_allowed": 88.5, "pitch_count": 165},
        },
        # Corbin Burnes
        592789: {
            "SI": {"usage_pct": 30.0, "hr_pct": 0.28, "barrel_rate_allowed": 4.1, "avg_ev_allowed": 85.2, "pitch_count": 600},
            "SL": {"usage_pct": 25.0, "hr_pct": 0.20, "barrel_rate_allowed": 3.5, "avg_ev_allowed": 84.8, "pitch_count": 500},
            "FC": {"usage_pct": 22.0, "hr_pct": 0.35, "barrel_rate_allowed": 4.8, "avg_ev_allowed": 86.0, "pitch_count": 440},
            "CH": {"usage_pct": 15.0, "hr_pct": 0.15, "barrel_rate_allowed": 3.0, "avg_ev_allowed": 84.0, "pitch_count": 300},
            "FF": {"usage_pct": 8.0,  "hr_pct": 0.60, "barrel_rate_allowed": 6.5, "avg_ev_allowed": 87.5, "pitch_count": 160},
        },
        # Tarik Skubal
        669923: {
            "FF": {"usage_pct": 35.0, "hr_pct": 0.42, "barrel_rate_allowed": 5.0, "avg_ev_allowed": 86.0, "pitch_count": 700},
            "CH": {"usage_pct": 30.0, "hr_pct": 0.18, "barrel_rate_allowed": 3.2, "avg_ev_allowed": 84.5, "pitch_count": 600},
            "SL": {"usage_pct": 25.0, "hr_pct": 0.22, "barrel_rate_allowed": 3.8, "avg_ev_allowed": 85.0, "pitch_count": 500},
            "CU": {"usage_pct": 10.0, "hr_pct": 0.30, "barrel_rate_allowed": 4.0, "avg_ev_allowed": 85.5, "pitch_count": 200},
        },
    }
    return PITCHER_DATA.get(pitcher_id, {
        "FF": {"usage_pct": 45.0, "hr_pct": 1.20, "barrel_rate_allowed": 8.5, "avg_ev_allowed": 89.0, "pitch_count": 400},
        "SL": {"usage_pct": 30.0, "hr_pct": 0.80, "barrel_rate_allowed": 6.0, "avg_ev_allowed": 87.0, "pitch_count": 266},
        "CH": {"usage_pct": 25.0, "hr_pct": 0.60, "barrel_rate_allowed": 5.0, "avg_ev_allowed": 86.0, "pitch_count": 222},
    })


def get_hardcoded_batter_data(batter_id):
    BATTER_DATA = {
        # Matt Olson
        621566: {
            "SI": {"hr_count": 12, "xbh_count": 28, "run_factor": 40, "slg": 0.810, "avg_launch_angle": 24.1, "sample_pitches": 280},
            "FF": {"hr_count": 18, "xbh_count": 35, "run_factor": 53, "slg": 0.870, "avg_launch_angle": 22.8, "sample_pitches": 350},
            "CH": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.680, "avg_launch_angle": 20.1, "sample_pitches": 180},
            "SW": {"hr_count": 3,  "xbh_count": 8,  "run_factor": 11, "slg": 0.420, "avg_launch_angle": 12.5, "sample_pitches": 120},
            "FC": {"hr_count": 4,  "xbh_count": 10, "run_factor": 14, "slg": 0.560, "avg_launch_angle": 18.3, "sample_pitches": 110},
            "_meta": {"avg_ev": 94.1, "barrel_pct": 16.8, "hr_recent_14d": 2, "pa_2026": 48, "w26": 0.24},
        },
        # Kyle Schwarber
        656941: {
            "FF": {"hr_count": 22, "xbh_count": 38, "run_factor": 60, "slg": 0.950, "avg_launch_angle": 26.2, "sample_pitches": 400},
            "SI": {"hr_count": 8,  "xbh_count": 18, "run_factor": 26, "slg": 0.720, "avg_launch_angle": 22.0, "sample_pitches": 220},
            "SW": {"hr_count": 4,  "xbh_count": 12, "run_factor": 16, "slg": 0.510, "avg_launch_angle": 14.1, "sample_pitches": 180},
            "_meta": {"avg_ev": 91.8, "barrel_pct": 16.1, "hr_recent_14d": 1, "pa_2026": 55, "w26": 0.28},
        },
        # Yordan Alvarez
        670541: {
            "FF": {"hr_count": 20, "xbh_count": 38, "run_factor": 58, "slg": 0.920, "avg_launch_angle": 25.2, "sample_pitches": 380},
            "SI": {"hr_count": 10, "xbh_count": 22, "run_factor": 32, "slg": 0.820, "avg_launch_angle": 22.1, "sample_pitches": 260},
            "SL": {"hr_count": 8,  "xbh_count": 18, "run_factor": 26, "slg": 0.730, "avg_launch_angle": 20.3, "sample_pitches": 200},
            "CH": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.680, "avg_launch_angle": 19.1, "sample_pitches": 180},
            "FC": {"hr_count": 5,  "xbh_count": 12, "run_factor": 17, "slg": 0.640, "avg_launch_angle": 18.5, "sample_pitches": 150},
            "_meta": {"avg_ev": 95.4, "barrel_pct": 18.2, "hr_recent_14d": 2, "pa_2026": 40, "w26": 0.20},
        },
        # Aaron Judge
        592450: {
            "FF": {"hr_count": 28, "xbh_count": 45, "run_factor": 73, "slg": 0.980, "avg_launch_angle": 27.1, "sample_pitches": 420},
            "SL": {"hr_count": 10, "xbh_count": 22, "run_factor": 32, "slg": 0.720, "avg_launch_angle": 22.4, "sample_pitches": 280},
            "CH": {"hr_count": 8,  "xbh_count": 18, "run_factor": 26, "slg": 0.680, "avg_launch_angle": 20.8, "sample_pitches": 200},
            "SI": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.620, "avg_launch_angle": 19.2, "sample_pitches": 160},
            "_meta": {"avg_ev": 96.2, "barrel_pct": 22.1, "hr_recent_14d": 2, "pa_2026": 45, "w26": 0.23},
        },
        # Pete Alonso
        624413: {
            "FF": {"hr_count": 22, "xbh_count": 38, "run_factor": 60, "slg": 0.900, "avg_launch_angle": 26.4, "sample_pitches": 400},
            "SI": {"hr_count": 12, "xbh_count": 26, "run_factor": 38, "slg": 0.820, "avg_launch_angle": 23.1, "sample_pitches": 280},
            "CH": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.660, "avg_launch_angle": 20.5, "sample_pitches": 180},
            "SL": {"hr_count": 5,  "xbh_count": 12, "run_factor": 17, "slg": 0.580, "avg_launch_angle": 18.2, "sample_pitches": 160},
            "_meta": {"avg_ev": 93.8, "barrel_pct": 14.8, "hr_recent_14d": 1, "pa_2026": 42, "w26": 0.21},
        },
        # Shohei Ohtani
        660271: {
            "FF": {"hr_count": 24, "xbh_count": 40, "run_factor": 64, "slg": 0.940, "avg_launch_angle": 25.8, "sample_pitches": 400},
            "SL": {"hr_count": 12, "xbh_count": 24, "run_factor": 36, "slg": 0.780, "avg_launch_angle": 22.3, "sample_pitches": 280},
            "CH": {"hr_count": 8,  "xbh_count": 18, "run_factor": 26, "slg": 0.710, "avg_launch_angle": 20.1, "sample_pitches": 220},
            "SW": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.640, "avg_launch_angle": 18.8, "sample_pitches": 180},
            "_meta": {"avg_ev": 94.8, "barrel_pct": 19.4, "hr_recent_14d": 2, "pa_2026": 48, "w26": 0.24},
        },
        # Freddie Freeman
        518692: {
            "FF": {"hr_count": 16, "xbh_count": 34, "run_factor": 50, "slg": 0.820, "avg_launch_angle": 22.8, "sample_pitches": 340},
            "FS": {"hr_count": 10, "xbh_count": 22, "run_factor": 32, "slg": 0.850, "avg_launch_angle": 20.2, "sample_pitches": 260},
            "SL": {"hr_count": 6,  "xbh_count": 16, "run_factor": 22, "slg": 0.640, "avg_launch_angle": 18.5, "sample_pitches": 200},
            "CH": {"hr_count": 4,  "xbh_count": 12, "run_factor": 16, "slg": 0.560, "avg_launch_angle": 16.8, "sample_pitches": 160},
            "_meta": {"avg_ev": 92.1, "barrel_pct": 12.4, "hr_recent_14d": 2, "pa_2026": 33, "w26": 0.17},
        },
        # Vladimir Guerrero Jr.
        665489: {
            "FF": {"hr_count": 18, "xbh_count": 34, "run_factor": 52, "slg": 0.860, "avg_launch_angle": 24.2, "sample_pitches": 360},
            "SI": {"hr_count": 8,  "xbh_count": 18, "run_factor": 26, "slg": 0.720, "avg_launch_angle": 21.8, "sample_pitches": 240},
            "SL": {"hr_count": 6,  "xbh_count": 14, "run_factor": 20, "slg": 0.640, "avg_launch_angle": 19.4, "sample_pitches": 200},
            "CH": {"hr_count": 4,  "xbh_count": 10, "run_factor": 14, "slg": 0.580, "avg_launch_angle": 17.2, "sample_pitches": 160},
            "_meta": {"avg_ev": 92.8, "barrel_pct": 13.6, "hr_recent_14d": 1, "pa_2026": 44, "w26": 0.22},
        },
    }
    return BATTER_DATA.get(batter_id, {
        "FF": {"hr_count": 8, "xbh_count": 20, "run_factor": 28, "slg": 0.620, "avg_launch_angle": 18.0, "sample_pitches": 200},
        "SL": {"hr_count": 4, "xbh_count": 12, "run_factor": 16, "slg": 0.480, "avg_launch_angle": 15.0, "sample_pitches": 150},
        "CH": {"hr_count": 3, "xbh_count": 9,  "run_factor": 12, "slg": 0.420, "avg_launch_angle": 14.0, "sample_pitches": 120},
        "_meta": {"avg_ev": 88.5, "barrel_pct": 7.2, "hr_recent_14d": 0, "pa_2026": 40, "w26": 0.20},
    })


# ── Game Lines model ──────────────────────────────────────────────────────────

ODDS_API_KEY = "155b5429de19953f629634ef23a481d4"

# MLB team name → abbreviation mapping for Odds API
ODDS_TEAM_MAP = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS", "Chicago Cubs": "CHC", "Chicago White Sox": "CHW",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE", "Colorado Rockies": "COL",
    "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL", "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK", "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX", "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
    "Athletics": "OAK",
}

# MLB Stats API team ID → abbreviation
MLB_TEAM_ID_MAP = {}  # populated lazily

def get_mlb_odds():
    """
    Fetch today's MLB moneylines and totals from The Odds API.
    Returns dict keyed by frozenset of {away_abbrev, home_abbrev}:
      {away, home, away_ml, home_ml, total_line, total_over_odds, total_under_odds,
       away_implied, home_implied, bookmaker}
    """
    url = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,totals",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        games = r.json()
    except Exception as e:
        log.error(f"Odds API fetch failed: {e}")
        return {}

    result = {}
    for g in games:
        away_name = g.get("away_team", "")
        home_name = g.get("home_team", "")
        away_abbr = ODDS_TEAM_MAP.get(away_name)
        home_abbr = ODDS_TEAM_MAP.get(home_name)
        if not away_abbr or not home_abbr:
            continue

        away_ml = home_ml = None
        total_line = total_over_odds = total_under_odds = None
        bookmaker_used = None

        # Prefer DraftKings, then FanDuel, then first available
        bookmakers = g.get("bookmakers", [])
        priority = ["draftkings", "fanduel", "betmgm", "caesars"]
        def bk_rank(b): 
            k = b.get("key", "")
            return priority.index(k) if k in priority else 99
        bookmakers_sorted = sorted(bookmakers, key=bk_rank)

        for bk in bookmakers_sorted:
            for mkt in bk.get("markets", []):
                if mkt["key"] == "h2h" and away_ml is None:
                    for out in mkt.get("outcomes", []):
                        name = out.get("name", "")
                        price = out.get("price")
                        abbr = ODDS_TEAM_MAP.get(name)
                        if abbr == away_abbr:
                            away_ml = price
                        elif abbr == home_abbr:
                            home_ml = price
                    bookmaker_used = bk.get("title", bk.get("key"))
                if mkt["key"] == "totals" and total_line is None:
                    for out in mkt.get("outcomes", []):
                        pt = out.get("point")
                        price = out.get("price")
                        if out.get("name") == "Over":
                            total_line = pt
                            total_over_odds = price
                        elif out.get("name") == "Under":
                            total_under_odds = price

        if away_ml is None and home_ml is None:
            continue

        def ml_to_implied(ml):
            if ml is None: return None
            if ml > 0: return round(100 / (ml + 100) * 100, 1)
            else: return round(abs(ml) / (abs(ml) + 100) * 100, 1)

        key = f"{away_abbr}@{home_abbr}"
        result[key] = {
            "away": away_abbr, "home": home_abbr,
            "away_ml": away_ml, "home_ml": home_ml,
            "away_implied": ml_to_implied(away_ml),
            "home_implied": ml_to_implied(home_ml),
            "total_line": total_line,
            "total_over_odds": total_over_odds,
            "total_under_odds": total_under_odds,
            "bookmaker": bookmaker_used,
        }

    log.info(f"Odds API: {len(result)} MLB games with lines")
    return result


def get_team_season_stats():
    """
    Fetch team-level season stats from MLB Stats API.
    Returns dict keyed by team abbreviation:
      {runs_per_game, era, whip, hits_per_game, hr_per_game, ops}
    """
    season = TODAY.year
    url = f"https://statsapi.mlb.com/api/v1/teams/stats?season={season}&sportId=1&stats=season&group=pitching"
    url_hit = f"https://statsapi.mlb.com/api/v1/teams/stats?season={season}&sportId=1&stats=season&group=hitting"

    # Get team ID → abbrev mapping
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={season}", timeout=15)
        teams = r.json().get("teams", [])
        id_to_abbr = {t["id"]: t.get("abbreviation", "") for t in teams}
    except Exception as e:
        log.error(f"Team ID map failed: {e}")
        id_to_abbr = {}

    team_stats = {}

    # Pitching stats
    try:
        r = requests.get(url, timeout=15)
        for rec in r.json().get("stats", [{}])[0].get("splits", []):
            tid = rec.get("team", {}).get("id")
            abbr = id_to_abbr.get(tid, "")
            if not abbr: continue
            s = rec.get("stat", {})
            team_stats.setdefault(abbr, {})
            team_stats[abbr].update({
                "team_era":  float(s.get("era", 4.20) or 4.20),
                "team_whip": float(s.get("whip", 1.30) or 1.30),
                "team_k9":   float(s.get("strikeoutsPer9Inn", 8.5) or 8.5),
            })
    except Exception as e:
        log.error(f"Team pitching stats failed: {e}")

    # Hitting stats
    try:
        r = requests.get(url_hit, timeout=15)
        for rec in r.json().get("stats", [{}])[0].get("splits", []):
            tid = rec.get("team", {}).get("id")
            abbr = id_to_abbr.get(tid, "")
            if not abbr: continue
            s = rec.get("stat", {})
            gp = float(s.get("gamesPlayed", 1) or 1)
            # Gate: need at least 5 games for meaningful team offense stats
            if gp < 5:
                continue
            team_stats.setdefault(abbr, {})
            team_stats[abbr].update({
                "runs_per_game": round(float(s.get("runs", 0) or 0) / gp, 2),
                "ops":           float(s.get("ops", 0.720) or 0.720),
                "hr_per_game":   round(float(s.get("homeRuns", 0) or 0) / gp, 2),
                "games_played":  int(gp),
            })
    except Exception as e:
        log.error(f"Team hitting stats failed: {e}")

    log.info(f"Team season stats: {len(team_stats)} teams loaded")
    return team_stats


def get_pitcher_season_line(pitcher_id, pitcher_name):
    """
    Fetch a pitcher's 2026 season stats from MLB Stats API.
    2026 ONLY — no prior season fallback (stale data misleads the model).
    Returns None if fewer than 15 IP (too small to trust).
    """
    season = TODAY.year  # 2026
    url = f"https://statsapi.mlb.com/api/v1/people/{pitcher_id}/stats?stats=season&season={season}&group=pitching&sportId=1"
    try:
        r = requests.get(url, timeout=15)
        splits = r.json().get("stats", [{}])[0].get("splits", [])
        if not splits:
            log.info(f"  {pitcher_name}: no {season} stats — skipping (model will use league avg)")
            return None
        s = splits[0].get("stat", {})
        ip = float(s.get("inningsPitched", 0) or 0)

        era = float(s.get("era", 4.50) or 4.50)
        log.info(f"  {pitcher_name}: {season} ERA {era} over {ip} IP")
        return {
            "era":        era,
            "whip":       float(s.get("whip", 1.35) or 1.35),
            "ip":         ip,
            "k9":         float(s.get("strikeoutsPer9Inn", 8.0) or 8.0),
            "hr9":        float(s.get("homeRunsPer9", 0) or 0),
            "runs_per_9": round(float(s.get("earnedRuns", 0) or 0) / max(ip, 1) * 9, 2),
            "hits_per_9": float(s.get("hitsPer9Inn", 8.5) or 8.5),
            "bb9":        float(s.get("walksPer9Inn", 3.0) or 3.0),
            "games":      int(s.get("gamesStarted", s.get("gamesPitched", 1)) or 1),
            "season":     season,
        }
    except Exception as e:
        log.error(f"Pitcher season stats failed for {pitcher_name}: {e}")
        return None


def score_game_lines(game, away_pitcher, home_pitcher, away_stats, home_stats,
                     away_pitcher_line, home_pitcher_line, odds, park, weather):
    """
    Returns a game lines dict with:
      - projected_total: model's run total estimate
      - total_edge: over/under lean + edge % vs posted line
      - away_win_prob / home_win_prob: model win probabilities
      - ml_edge: which side has model edge and by how much vs implied odds
      - factors: list of key factors that drove the model
    """
    factors = []
    LEAGUE_AVG_RUNS_PER_GAME = 4.45  # 2024-2025 MLB average

    # ── Projected run total ───────────────────────────────────────────────────
    # Base: each team's expected runs = pitcher's runs allowed + opponent offense blend
    def team_run_expectation(pitcher_line, opp_team_stats):
        """Runs expected to score against this pitcher."""
        if pitcher_line:
            # Pitcher ERA → runs per game (ERA / 9 * ~6 IP average)
            avg_ip = min(pitcher_line.get("ip", 1) / max(pitcher_line.get("games", 1), 1), 7.0)
            avg_ip = max(avg_ip, 4.5)  # floor at 4.5 IP
            pitcher_runs = pitcher_line["era"] / 9 * avg_ip
            # Blend 70% pitcher ERA, 30% opponent offense
            opp_rpg = opp_team_stats.get("runs_per_game", LEAGUE_AVG_RUNS_PER_GAME)
            expected = pitcher_runs * 0.70 + opp_rpg * 0.30
        else:
            # No pitcher data — use league average + opponent offense
            opp_rpg = opp_team_stats.get("runs_per_game", LEAGUE_AVG_RUNS_PER_GAME)
            expected = (LEAGUE_AVG_RUNS_PER_GAME + opp_rpg) / 2

        return round(max(expected, 1.5), 2)

    away_runs = team_run_expectation(home_pitcher_line, away_stats)  # away bats vs home pitcher
    home_runs = team_run_expectation(away_pitcher_line, home_stats)  # home bats vs away pitcher
    base_total = round(away_runs + home_runs, 1)

    # ── Park factor adjustment ────────────────────────────────────────────────
    pf = park.get("overall", 100)
    pf_mult = pf / 100.0
    # Softer adjustment — park factor moves total by up to ±1.5 runs at extremes
    pf_adj = (pf_mult - 1.0) * 3.0  # COL 125 → +0.75, SF 79 → -0.63
    projected_total = round(base_total + pf_adj, 1)
    if abs(pf_adj) >= 0.3:
        direction = "boosting" if pf_adj > 0 else "suppressing"
        factors.append(f"Park {direction} total by {abs(round(pf_adj,1))} runs (PF {pf})")

    # ── Weather adjustment ────────────────────────────────────────────────────
    hr_mult = weather.get("hr_multiplier", 1.0)
    temp_f = weather.get("temp_f", 70)
    wind_label = weather.get("wind_label", "")
    dome = park.get("dome", False)

    if not dome:
        weather_adj = (hr_mult - 1.0) * 2.5  # wind out → +runs, wind in → -runs
        if temp_f < 50:
            weather_adj -= 0.4
            factors.append(f"Cold ({temp_f}°F) suppressing scoring")
        elif temp_f > 85:
            weather_adj += 0.2
        if abs(weather_adj) >= 0.2:
            projected_total = round(projected_total + weather_adj, 1)
            if "OUT" in wind_label.upper():
                factors.append(f"Wind out ({wind_label}) adding ~{abs(round(weather_adj,1))} runs")
            elif "IN" in wind_label.upper():
                factors.append(f"Wind in ({wind_label}) removing ~{abs(round(weather_adj,1))} runs")

    # ── Pitcher quality factors ───────────────────────────────────────────────
    for label, pl, side in [("Away", away_pitcher_line, "away"), ("Home", home_pitcher_line, "home")]:
        if not pl: continue
        if pl["era"] <= 2.80:
            factors.append(f"{label} pitcher (ERA {pl['era']:.2f}) — ace-level, suppressing total")
        elif pl["era"] >= 5.50:
            factors.append(f"{label} pitcher (ERA {pl['era']:.2f}) — vulnerable, boosting total")
        if pl.get("hr9", 0) >= 1.8:
            factors.append(f"{label} pitcher HR/9 {pl['hr9']:.2f} — HR-prone")
        if pl.get("k9", 0) >= 11.0:
            factors.append(f"{label} pitcher K/9 {pl['k9']:.1f} — high strikeout, suppresses scoring")

    # ── Total edge vs posted line ─────────────────────────────────────────────
    total_line = odds.get("total_line") if odds else None
    total_edge = None
    total_lean = None
    total_edge_pct = None

    if total_line:
        diff = projected_total - total_line
        total_lean = "OVER" if diff > 0 else "UNDER"
        # Convert run difference to edge confidence
        # 0.3 run diff = slight edge, 0.8 = moderate, 1.5+ = strong
        abs_diff = abs(diff)
        if abs_diff >= 1.5:
            confidence = "STRONG"
            edge_pct = min(72, 55 + abs_diff * 8)
        elif abs_diff >= 0.8:
            confidence = "MODERATE"
            edge_pct = min(65, 55 + abs_diff * 6)
        elif abs_diff >= 0.3:
            confidence = "SLIGHT"
            edge_pct = min(58, 52 + abs_diff * 5)
        else:
            confidence = "NONE"
            edge_pct = 50.0

        total_edge_pct = round(edge_pct, 1)
        total_edge = {
            "lean": total_lean,
            "confidence": confidence,
            "projected": projected_total,
            "line": total_line,
            "diff": round(diff, 1),
            "edge_pct": total_edge_pct,
            "over_odds": odds.get("total_over_odds"),
            "under_odds": odds.get("total_under_odds"),
        }
        factors.append(f"Model projects {projected_total} runs vs posted O/U {total_line} → {total_lean} lean ({confidence})")

    # ── Win probability model ─────────────────────────────────────────────────
    # Base from run expectation differential + home field advantage
    HOME_FIELD_BOOST = 0.54  # home teams win ~54% at neutral ERA
    run_diff = home_runs - away_runs  # positive = home team advantage

    # Convert run differential to win probability via log5-inspired sigmoid
    # +1 run advantage ≈ 60% win prob, +2 runs ≈ 70%, -1 run ≈ 40%
    import math as _math
    home_win_prob_raw = 1 / (1 + _math.exp(-run_diff * 0.45)) * 0.65 + HOME_FIELD_BOOST * 0.35
    home_win_prob = round(max(0.25, min(0.80, home_win_prob_raw)) * 100, 1)
    away_win_prob = round(100 - home_win_prob, 1)

    # ── Money line edge ───────────────────────────────────────────────────────
    ml_edge = None
    if odds and odds.get("away_implied") and odds.get("home_implied"):
        away_implied = odds["away_implied"]
        home_implied = odds["home_implied"]

        away_edge = round(away_win_prob - away_implied, 1)
        home_edge = round(home_win_prob - home_implied, 1)

        # Only flag edge if model disagrees by 5%+ (noise filter)
        if abs(away_edge) >= 5 or abs(home_edge) >= 5:
            if away_edge > home_edge:
                lean_side = "AWAY"
                lean_team = game.get("away_team", "")
                model_prob = away_win_prob
                implied_prob = away_implied
                edge_size = away_edge
                ml_odds = odds.get("away_ml")
            else:
                lean_side = "HOME"
                lean_team = game.get("home_team", "")
                model_prob = home_win_prob
                implied_prob = home_implied
                edge_size = home_edge
                ml_odds = odds.get("home_ml")

            ml_confidence = "STRONG" if abs(edge_size) >= 10 else "MODERATE" if abs(edge_size) >= 7 else "SLIGHT"
            ml_edge = {
                "lean": lean_side,
                "team": lean_team,
                "model_prob": model_prob,
                "implied_prob": implied_prob,
                "edge": edge_size,
                "confidence": ml_confidence,
                "ml_odds": ml_odds,
            }

    return {
        "game": f"{game.get('away_team')} @ {game.get('home_team')}",
        "away_team": game.get("away_team"),
        "home_team": game.get("home_team"),
        "venue": game.get("venue_name", ""),
        "away_pitcher": away_pitcher.get("name", "TBD") if away_pitcher else "TBD",
        "home_pitcher": home_pitcher.get("name", "TBD") if home_pitcher else "TBD",
        "away_pitcher_era": away_pitcher_line.get("era") if away_pitcher_line else None,
        "home_pitcher_era": home_pitcher_line.get("era") if home_pitcher_line else None,
        "away_pitcher_k9": away_pitcher_line.get("k9") if away_pitcher_line else None,
        "home_pitcher_k9": home_pitcher_line.get("k9") if home_pitcher_line else None,
        "away_pitcher_hr9": away_pitcher_line.get("hr9") if away_pitcher_line else None,
        "home_pitcher_hr9": home_pitcher_line.get("hr9") if home_pitcher_line else None,
        "away_pitcher_ip": away_pitcher_line.get("ip") if away_pitcher_line else None,
        "home_pitcher_ip": home_pitcher_line.get("ip") if home_pitcher_line else None,
        "projected_total": projected_total,
        "away_runs": away_runs,
        "home_runs": home_runs,
        "away_win_prob": away_win_prob,
        "home_win_prob": home_win_prob,
        "total_edge": total_edge,
        "ml_edge": ml_edge,
        "park_factor": pf,
        "park_dome": park.get("dome", False),
        "weather_label": weather.get("wind_label", ""),
        "weather_temp": weather.get("temp_f", 72),
        "hr_multiplier": weather.get("hr_multiplier", 1.0),
        "factors": factors[:5],  # top 5 most impactful
        "odds_available": odds is not None,
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run():
    log.info(f"=== MLB HR Props Pipeline — {TODAY} ===")

    # Fetch all weather in one shot from RotoWire
    log.info("Fetching weather from RotoWire…")
    weather_by_team = get_weather_rotowire()

    log.info("Fetching MLB odds…")
    odds_by_game = get_mlb_odds()

    log.info("Fetching team season stats…")
    team_season_stats = get_team_season_stats()

    games = get_todays_schedule()
    if not games:
        log.warning("No games found. Writing empty slate.")
        games = []

    output_games = []
    all_targets = []
    k_targets = []
    game_lines = []
    auto_fades = []

    _weather_fallback = {
        "temp_f": 70, "wind_mph": 0, "wind_dir": "unknown",
        "rain_pct": 0, "rain_chance": 0, "rain_risk": "LOW",
        "hr_multiplier": 1.0, "wind_label": "—", "dome": False,
    }

    for game in games:
        home = game["home_team"]
        away = game["away_team"]
        park = PARK_FACTORS.get(home, PARK_FACTORS.get("NYM"))  # fallback

        # Look up pre-parsed weather by home team; dome teams always get neutral
        if park.get("dome"):
            weather = {
                "temp_f": 72, "wind_mph": 0, "wind_dir": "dome",
                "rain_pct": 0, "rain_chance": 0, "rain_risk": "LOW",
                "hr_multiplier": 1.0, "wind_label": "Dome", "dome": True,
            }
        else:
            weather = weather_by_team.get(home, _weather_fallback)

        # ── No HR signal — count suppressive factors ─────────────────────────
        no_hr_factors = []
        pf = park.get("overall", 100)
        w = weather
        wl = w.get("wind_label", "")
        dome = park.get("dome", False)

        if pf <= 90:
            no_hr_factors.append(f"Suppressive park ({pf})")
        if not dome and wl.upper().startswith("WIND IN") and w.get("wind_mph", 0) >= 10:
            no_hr_factors.append(f"Wind IN {w.get('wind_mph')} mph")
        if not dome and w.get("temp_f", 70) < 50:
            no_hr_factors.append(f"{w.get('temp_f')}°F — cold suppression")
        if not dome and w.get("rain_chance", 0) > 0.65:
            # 65%+ means it's very likely actually raining during the game
            # (wet ball, reduced carry) — below this it's mainly postponement risk
            no_hr_factors.append(f"Rain {round(w.get('rain_chance',0)*100)}%")

        # Check both probable pitchers' overall HR/9
        for side_key in ["away_probable", "home_probable"]:
            p = game.get(side_key)
            if not p:
                continue
            pid = p.get("id")
            pname = p.get("name", "")
            if not pid:
                continue
            try:
                # Use module-level cache to avoid re-fetching across games
                cache_key = f"{pid}_R"
                if cache_key not in _HR9_CACHE:
                    a = get_pitcher_arsenal(pid, pname, "R")
                    _HR9_CACHE[cache_key] = a.get("_overall_hr9")
                hr9 = _HR9_CACHE.get(cache_key)
                if hr9 is not None and hr9 < 0.85:
                    no_hr_factors.append(f"{pname.split()[-1]} HR/9 {hr9}")
            except Exception:
                pass

        if len(no_hr_factors) >= 3:
            no_hr_signal = {"level": "red", "label": "No HR Beta", "factors": no_hr_factors}
        elif len(no_hr_factors) == 2:
            no_hr_signal = {"level": "orange", "label": "HR Suppressed", "factors": no_hr_factors}
        else:
            no_hr_signal = None

        game_entry = {
            "game_pk": game["game_pk"],
            "away_team": away,
            "home_team": home,
            "venue": game["venue_name"],
            "status": game["status"],
            "away_probable": game["away_probable"],
            "home_probable": game["home_probable"],
            "park_factor": park.get("overall", 100),
            "park_suppress": park.get("suppress", False),
            "park_dome": park.get("dome", False),
            "weather": weather,
            "no_hr_signal": no_hr_signal,
        }
        output_games.append(game_entry)

        # ── Game lines scoring ────────────────────────────────────────────────
        away_p = game.get("away_probable")
        home_p = game.get("home_probable")
        away_pl = get_pitcher_season_line(away_p["id"], away_p["name"]) if away_p else None
        home_pl = get_pitcher_season_line(home_p["id"], home_p["name"]) if home_p else None
        away_ts = team_season_stats.get(away, {})
        home_ts = team_season_stats.get(home, {})
        game_odds = odds_by_game.get(f"{away}@{home}") or odds_by_game.get(f"{home}@{away}")
        # Try alternate key formats
        if not game_odds:
            for k in odds_by_game:
                parts = k.split("@")
                if len(parts) == 2 and set(parts) == {away, home}:
                    game_odds = odds_by_game[k]
                    break

        gl = score_game_lines(
            game={"away_team": away, "home_team": home, "venue_name": game["venue_name"]},
            away_pitcher=away_p, home_pitcher=home_p,
            away_stats=away_ts, home_stats=home_ts,
            away_pitcher_line=away_pl, home_pitcher_line=home_pl,
            odds=game_odds, park=park, weather=weather,
        )
        game_lines.append(gl)

        # Score batters for each side
        for side in ["away", "home"]:
            pitching_side = "home" if side == "away" else "away"
            pitcher_info = game[f"{pitching_side}_probable"]
            batting_team = game[f"{side}_team"]

            if not pitcher_info:
                log.info(f"  No probable pitcher for {pitching_side} team, skipping batter scoring")
                continue

            pitcher_id = pitcher_info["id"]
            pitcher_name = pitcher_info["name"]

            # Check auto-fade
            if pitcher_id in ELITE_FADE:
                auto_fades.append({
                    "pitcher_name": ELITE_FADE[pitcher_id],
                    "reason": "Elite pitcher — auto-fade",
                    "game": f"{away} @ {home}",
                })
                log.info(f"  Auto-fade: {pitcher_name}")
                continue

            log.info(f"  Processing batters vs {pitcher_name} ({batting_team})")

            batters, lineup_confirmed = get_roster_batters(batting_team, game["game_pk"])
            # Cache arsenal per batter hand to avoid re-fetching for same handedness
            arsenal_cache = {}

            for batter in batters[:9]:  # top 9 in lineup
                b_id = batter["id"]
                b_name = batter["name"]
                b_hand = batter.get("bats", "R")

                # Switch hitters bat opposite to pitcher handedness
                if b_hand == "S":
                    b_hand = "L" if pitcher_info.get("throws", "R") == "R" else "R"
                    log.info(f"  Switch hitter {b_name} batting {b_hand} vs {pitcher_info.get('throws')}HP")

                # Fetch hand-specific arsenal (cached per hand)
                if b_hand not in arsenal_cache:
                    arsenal_cache[b_hand] = get_pitcher_arsenal(pitcher_id, pitcher_name, b_hand)
                arsenal = arsenal_cache[b_hand]

                batter_stats = get_batter_pitch_stats(b_id, b_name, b_hand)
                if not batter_stats:
                    continue

                score, components, insight, tier, dominant_signals = score_batter(
                    batter, pitcher_info, arsenal, batter_stats, park, weather
                )

                if tier != "FADE":
                    target = {
                        "batter_id": b_id,
                        "batter_name": b_name,
                        "batter_team": batting_team,
                        "batter_hand": b_hand,
                        "pitcher_name": pitcher_name,
                        "pitcher_throws": pitcher_info.get("throws", "R"),
                        "game": f"{away} @ {home}",
                        "venue": game["venue_name"],
                        "score": score,
                        "tier": tier,
                        "components": components,
                        "insight": insight,
                        "dominant_signals": dominant_signals,
                        "park_factor": park.get("lhb" if b_hand == "L" else "rhb", park.get("overall", 100)),
                        "park_suppress": park.get("suppress", False),
                        "weather_label": weather["wind_label"],
                        "weather_temp": weather["temp_f"],
                        "hr_multiplier": weather["hr_multiplier"],
                        "rain_chance": weather["rain_chance"],
                        "lineup_confirmed": lineup_confirmed,
                        "batter_meta": batter_stats.get("_meta", {}),
                        "pitcher_overall_hr9": arsenal.get("_overall_hr9", None),
                        "pitcher_hrs_vs_hand": arsenal.get("_hrs_vs_hand", None),
                        "pitch_matrix": {
                            pt: {**batter_stats.get(pt, {}), **{
                                "pitcher_usage": arsenal.get(pt, {}).get("usage_pct"),
                                "pitcher_hr_pct": arsenal.get(pt, {}).get("hr_pct"),
                                "pitcher_hr_dist": arsenal.get(pt, {}).get("hr_dist_pct"),
                            }}
                            for pt in arsenal if not pt.startswith("_") and pt in batter_stats
                        },
                    }
                    all_targets.append(target)

                # ── K prop scoring (independent of HR tier) ───────────────────
                k_score, k_components, k_insight, k_tier_05, k_tier_15 = score_batter_k(
                    batter, pitcher_info, arsenal, batter_stats
                )
                if k_tier_05 != "FADE":
                    k_targets.append({
                        "batter_id": b_id,
                        "batter_name": b_name,
                        "batter_team": batting_team,
                        "batter_hand": b_hand,
                        "pitcher_name": pitcher_name,
                        "pitcher_throws": pitcher_info.get("throws", "R"),
                        "game": f"{away} @ {home}",
                        "venue": game["venue_name"],
                        "score": k_score,
                        "tier_05": k_tier_05,
                        "tier_15": k_tier_15,
                        "components": k_components,
                        "insight": k_insight,
                        "lineup_confirmed": lineup_confirmed,
                        "batter_meta": batter_stats.get("_meta", {}),
                        "pitcher_overall_hr9": arsenal.get("_overall_hr9", None),
                    })

    # Auto-fades for suppressive park + bad weather
    for g in output_games:
        if g["park_suppress"] and g["weather"].get("hr_multiplier", 1.0) < 0.95:
            auto_fades.append({
                "pitcher_name": f"Any batter @ {g['home_team']}",
                "reason": f"Suppressive park + unfavorable weather",
                "game": f"{g['away_team']} @ {g['home_team']}",
            })
        if g["weather"].get("rain_chance", 0) > 0.40 and not g["park_dome"]:
            auto_fades.append({
                "pitcher_name": f"{g['away_team']} @ {g['home_team']}",
                "reason": f"Rain risk {round(g['weather']['rain_chance']*100)}% — postponement possible",
                "game": f"{g['away_team']} @ {g['home_team']}",
            })

    # Deduplicate — keep highest score per batter (prevents double-scoring from roster loop)
    seen_batters = {}
    for t in all_targets:
        key = t["batter_id"]
        if key not in seen_batters or t["score"] > seen_batters[key]["score"]:
            seen_batters[key] = t
    all_targets = list(seen_batters.values())

    # Sort — PRIME first, then HIGH, then MED, then by score descending within tier
    tier_order = {"PRIME": 0, "HIGH": 1, "MED": 2, "LOW": 3}
    all_targets.sort(key=lambda x: (tier_order.get(x["tier"], 3), -x["score"]))
    seen_fades = set()
    deduped_fades = []
    for f in auto_fades:
        key = f["pitcher_name"]
        if key not in seen_fades:
            seen_fades.add(key)
            deduped_fades.append(f)

    # ── Laser prop targets ────────────────────────────────────────────────────
    # Flags elite hard-contact profiles facing pitchers with high hard-hit rates
    # Designed for FanDuel Laser HR props (110+ MPH exit velo HRs)
    laser_targets = []
    for t in all_targets:
        meta = t.get("batter_meta", {})
        avg_ev = meta.get("avg_ev") or 0
        barrel_pct = meta.get("barrel_pct") or 0
        overall_hr9 = t.get("pitcher_overall_hr9") or 0

        # Criteria: elite EV + barrel profile facing a hittable pitcher
        is_laser = (
            avg_ev >= 91.0 and          # elite exit velocity
            barrel_pct >= 10.0 and      # elite barrel rate
            overall_hr9 >= 0.85 and     # pitcher is hittable
            t["tier"] in ("PRIME", "HIGH", "MED")
        )
        if is_laser:
            laser_targets.append({
                "batter_name": t["batter_name"],
                "batter_team": t["batter_team"],
                "pitcher_name": t["pitcher_name"],
                "game": t["game"],
                "venue": t["venue"],
                "avg_ev": avg_ev,
                "barrel_pct": barrel_pct,
                "pitcher_hr9": overall_hr9,
                "score": t["score"],
                "tier": t["tier"],
            })

    laser_targets.sort(key=lambda x: (-(x["avg_ev"] or 0) - (x["barrel_pct"] or 0)))
    log.info(f"  Laser targets: {len(laser_targets)}")

    # Deduplicate K targets — keep highest score per batter
    seen_k = {}
    for t in k_targets:
        key = t["batter_id"]
        if key not in seen_k or t["score"] > seen_k[key]["score"]:
            seen_k[key] = t
    k_targets = list(seen_k.values())
    k_targets.sort(key=lambda x: -x["score"])
    log.info(f"  K targets: {len(k_targets)}")

    # Build final output
    output = {
        "generated_at": datetime.datetime.now().isoformat(),
        "date": TODAY.isoformat(),
        "pybaseball_live": PYBASEBALL_AVAILABLE,
        "games": output_games,
        "targets": all_targets,
        "laser_targets": laser_targets,
        "k_targets": k_targets,
        "game_lines": game_lines,
        "auto_fades": deduped_fades,
        "summary": {
            "total_games": len(output_games),
            "prime_count": sum(1 for t in all_targets if t["tier"] == "PRIME"),
            "high_count": sum(1 for t in all_targets if t["tier"] == "HIGH"),
            "med_count": sum(1 for t in all_targets if t["tier"] == "MED"),
            "low_count": sum(1 for t in all_targets if t["tier"] == "LOW"),
            "fade_count": len(deduped_fades),
            "k_high_05": sum(1 for t in k_targets if t["tier_05"] == "HIGH"),
            "k_med_05":  sum(1 for t in k_targets if t["tier_05"] == "MED"),
            "k_high_15": sum(1 for t in k_targets if t["tier_15"] == "HIGH"),
        },
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    log.info(f"✓ Wrote {OUTPUT_FILE} — {len(all_targets)} targets, {len(deduped_fades)} fades")
    log.info(f"  PRIME: {output['summary']['prime_count']} | HIGH: {output['summary']['high_count']} | MED: {output['summary']['med_count']}")

    return output


if __name__ == "__main__":
    run()
