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
    "ev_barrel":        0.25,
    "pitcher_vuln":     0.20,
    "pitch_collision":  0.20,
    "park_factor":      0.15,
    "platoon":          0.10,
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
    572971: "Jacob deGrom",
}
# ── MLB Stats API helpers ─────────────────────────────────────────────────────

def get_todays_schedule():
    """Returns list of games for today from MLB Stats API."""
    date_str = TODAY.strftime("%Y-%m-%d")
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}&hydrate=probablePitcher,team,venue,weather,lineups"
    try:
        r = requests.get(url, timeout=10)
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
        r2 = requests.get(url2, timeout=10)
        teams = r2.json().get("teams", [])
        team_id = next(
            (t["id"] for t in teams if t.get("abbreviation") == team_abbrev), None
        )
        if team_id:
            roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
            r3 = requests.get(roster_url, timeout=10)
            for p in r3.json().get("roster", []):
                pid = p["person"]["id"]
                hand = p.get("person", {}).get("batSide", {}).get("code", "R")
                hand_lookup[pid] = hand
    except Exception as e:
        log.warning(f"  Hand lookup failed for {team_abbrev}: {e}")

    # Try confirmed lineup from boxscore for batting order
    try:
        url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
        r = requests.get(url, timeout=10)
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
                        # Use hand_lookup for reliable handedness — boxscore doesn't have it
                        bats = hand_lookup.get(pid, "R")
                        result.append({
                            "id": pid,
                            "name": info.get("fullName", ""),
                            "bats": bats,
                            "position": pos.get("abbreviation", ""),
                        })
                    if result:
                        log.info(f"  Confirmed lineup for {team_abbrev}: {len(result)} batters")
                        return result
    except Exception:
        pass

    # Fallback: return full roster from hand_lookup
    if hand_lookup:
        try:
            url2 = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={TODAY.year}"
            r2 = requests.get(url2, timeout=10)
            teams = r2.json().get("teams", [])
            team_id = next(
                (t["id"] for t in teams if t.get("abbreviation") == team_abbrev), None
            )
            if team_id:
                roster_url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster?rosterType=active"
                r3 = requests.get(roster_url, timeout=10)
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
                log.info(f"  Active roster for {team_abbrev}: {len(batters)} batters")
                return batters[:13]
        except Exception as e:
            log.error(f"Roster fetch failed for {team_abbrev}: {e}")
    return []


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
            "hr_multiplier": round(hr_wind_mult * temp_mult, 3),
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
        # Below 100 BF, 2025 is far more reliable than noisy early-season data
        bf_2026 = len(df26["batter"].unique()) if len(df26) > 0 else 0
        if bf_2026 < 100:
            # Use 2025 only — 2026 sample too small to be meaningful
            df_all = df25.copy() if not df25.empty else pd.DataFrame()
            log.info(f"  {pitcher_name}: {bf_2026} BF in 2026 — using 2025 data only")
        else:
            # Linear blend: 100 BF = 50% 2026, 200 BF = 100% 2026
            w26 = min((bf_2026 - 100) / 100, 1.0)
            w25 = 1.0 - w26
            df_all = pd.concat([
                df25.assign(_weight=w25),
                df26.assign(_weight=w26)
            ])
            log.info(f"  {pitcher_name}: blending 2025 ({round(w25*100)}%) + 2026 ({round(w26*100)}%)")

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

            # HR% per PA — used internally for scoring (vuln + collision)
            # Minimum 30 PA required before rate is meaningful
            pa_on_pitch = group_vs["at_bat_number"].nunique() if "at_bat_number" in group_vs.columns else len(group_vs) // 4
            hr_pct = (hr_count_pt / pa_on_pitch * 100) if pa_on_pitch >= 30 else None

            barrels = group_vs[group_vs["barrel"].notna()]["barrel"].sum() if "barrel" in group_vs.columns else 0
            barrel_rate = barrels / len(group_vs) if len(group_vs) > 0 else 0
            avg_ev = group_vs["launch_speed"].mean() if "launch_speed" in group_vs.columns else None

            arsenal[str(pt)] = {
                "usage_pct": usage_pct,
                "hr_pct": round(hr_pct, 1) if hr_pct is not None else None,  # for scoring
                "hr_count": hr_count_pt,                                        # for distribution
                "barrel_rate_allowed": round(barrel_rate * 100, 1),
                "avg_ev_allowed": round(avg_ev, 1) if avg_ev and not math.isnan(avg_ev) else None,
                "pitch_count": len(group_vs),
                "batter_hand": batter_hand,
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

        log.info(f"  Pitcher {pitcher_name} vs {batter_hand}HB: {len(arsenal)} pitch types")

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
        df = pd.concat([df25, df26]) if len(df26) > 0 else df25

        if df.empty:
            return {}

        # Global stats
        contact = df[df["launch_speed"].notna()]
        avg_ev = contact["launch_speed"].mean() if len(contact) > 0 else None
        barrels = df[df.get("barrel", pd.Series()).notna()]["barrel"].sum() if "barrel" in df.columns else 0
        barrel_pct = barrels / len(contact) * 100 if len(contact) > 0 else 0

        # L14D form
        two_weeks_ago = (TODAY - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
        df_recent = df26[df26["game_date"] >= two_weeks_ago] if len(df26) > 0 else df25[df25["game_date"] >= two_weeks_ago]
        hr_recent = len(df_recent[df_recent["events"] == "home_run"])

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

            pitch_stats[str(pt)] = {
                "hr_count": len(hrs),
                "xbh_count": len(xbh),
                "run_factor": len(hrs) + len(xbh),  # primary proxy per brief
                "slg": round(slg, 3),
                "avg_launch_angle": round(avg_la, 1) if avg_la and not math.isnan(avg_la) else None,
                "sample_pitches": len(group),
            }

        # Attach global stats to result
        pitch_stats["_meta"] = {
            "avg_ev": round(avg_ev, 1) if avg_ev and not math.isnan(avg_ev) else None,
            "barrel_pct": round(barrel_pct, 1),
            "hr_recent_14d": hr_recent,
            "pa_2026": pa_2026,
            "w26": round(w26, 2),
        }

        log.info(f"  Batter {batter_name}: {len(pitch_stats)-1} pitch types, {pa_2026} PA in 2026")
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
    barrel_pct = meta.get("barrel_pct") or 5.0
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

    # 3. Pitch collision (20%) — batter damage × pitcher usage on that pitch
    collision_score = 0.0
    collision_insights = []
    for pt, pa in arsenal.items():
        if pt.startswith("_"):
            continue
        bs = batter_stats.get(pt, {})
        if not bs:
            continue
        usage = pa.get("usage_pct", 0) / 100
        run_factor = bs.get("run_factor", 0)
        slg = bs.get("slg", 0)
        sample = bs.get("sample_pitches", 0)
        if sample < 20:
            continue
        slg_norm = min(slg / 1.0, 1.0)
        pt_score = usage * slg_norm
        collision_score += pt_score
        if usage > 0.25 and slg > 0.500:
            pt_name = PITCH_NAMES.get(str(pt), str(pt))
            collision_insights.append(
                f"{pt_name} ({round(usage*100)}% usage): {slg:.3f} SLG"
            )
    collision_score = min(collision_score, 1.0)

    # 4. Park factor (15%)
    batter_hand = batter.get("bats", "R")
    pf_key = "lhb" if batter_hand == "L" else "rhb"
    pf = park_factor.get(pf_key, park_factor.get("overall", 100))
    # Normalize: 75=0, 100=0.5, 135=1.0
    park_score = min(max((pf - 75) / 60, 0), 1.0)

    # 5. Platoon (10%)
    p_throws = pitcher.get("throws", "R")
    # Advantage if batter hand ≠ pitcher hand
    platoon_score = 0.65 if batter_hand != p_throws else 0.35

    # 6. Weather (5%)
    weather_score = min(max((weather.get("hr_multiplier", 1.0) - 0.85) / 0.30, 0), 1.0)

    # 7. Recent form (5%)
    form_score = min(hr_recent / 4, 1.0)  # 4+ HR in 14 days = max

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
    if score >= 55 and factors_aligning >= 2:
        tier = "PRIME"
    elif score >= 46 and factors_aligning >= 2:
        tier = "HIGH"
    elif score >= 30:
        tier = "MED"
    else:
        tier = "FADE"

    return score, components, top_insight, tier, dominant_signals


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
    }
    return BATTER_DATA.get(batter_id, {
        "FF": {"hr_count": 8, "xbh_count": 20, "run_factor": 28, "slg": 0.620, "avg_launch_angle": 18.0, "sample_pitches": 200},
        "SL": {"hr_count": 4, "xbh_count": 12, "run_factor": 16, "slg": 0.480, "avg_launch_angle": 15.0, "sample_pitches": 150},
        "CH": {"hr_count": 3, "xbh_count": 9,  "run_factor": 12, "slg": 0.420, "avg_launch_angle": 14.0, "sample_pitches": 120},
        "_meta": {"avg_ev": 88.5, "barrel_pct": 7.2, "hr_recent_14d": 0, "pa_2026": 40, "w26": 0.20},
    })


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run():
    log.info(f"=== MLB HR Props Pipeline — {TODAY} ===")

    # Fetch all weather in one shot from RotoWire
    log.info("Fetching weather from RotoWire…")
    weather_by_team = get_weather_rotowire()

    games = get_todays_schedule()
    if not games:
        log.warning("No games found. Writing empty slate.")
        games = []

    output_games = []
    all_targets = []
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
        }
        output_games.append(game_entry)

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

            batters = get_roster_batters(batting_team, game["game_pk"])
            # Cache arsenal per batter hand to avoid re-fetching for same handedness
            arsenal_cache = {}

            for batter in batters[:9]:  # top 9 in lineup
                b_id = batter["id"]
                b_name = batter["name"]
                b_hand = batter.get("bats", "R")

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

                if tier == "FADE":
                    continue

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
                    "batter_meta": batter_stats.get("_meta", {}),
                    "pitcher_overall_hr9": arsenal.get("_overall_hr9", None),
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
    tier_order = {"PRIME": 0, "HIGH": 1, "MED": 2}
    all_targets.sort(key=lambda x: (tier_order.get(x["tier"], 3), -x["score"]))
    seen_fades = set()
    deduped_fades = []
    for f in auto_fades:
        key = f["pitcher_name"]
        if key not in seen_fades:
            seen_fades.add(key)
            deduped_fades.append(f)

    # Build final output
    output = {
        "generated_at": datetime.datetime.now().isoformat(),
        "date": TODAY.isoformat(),
        "pybaseball_live": PYBASEBALL_AVAILABLE,
        "games": output_games,
        "targets": all_targets,
        "auto_fades": deduped_fades,
        "summary": {
            "total_games": len(output_games),
            "prime_count": sum(1 for t in all_targets if t["tier"] == "PRIME"),
            "high_count": sum(1 for t in all_targets if t["tier"] == "HIGH"),
            "med_count": sum(1 for t in all_targets if t["tier"] == "MED"),
            "fade_count": len(deduped_fades),
        },
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    log.info(f"✓ Wrote {OUTPUT_FILE} — {len(all_targets)} targets, {len(deduped_fades)} fades")
    log.info(f"  PRIME: {output['summary']['prime_count']} | HIGH: {output['summary']['high_count']} | MED: {output['summary']['med_count']}")

    return output


if __name__ == "__main__":
    run()
