
from __future__ import annotations

import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
MLB_API = "https://statsapi.mlb.com/api/v1"

SCORES_TTL = 300        # 5 min
PREDICTIONS_TTL = 900   # 15 min
BOARD_TTL = 28800       # 8 hours
TIMEOUT = 20

session = requests.Session()
session.headers.update({"User-Agent": "Multi-Sport-Insights/2.0"})

_cache: dict[str, tuple[float, Any]] = {}

NHL_TEAM_CODE_TO_ID = {
    "ANA": 24, "BOS": 6, "BUF": 7, "CGY": 20, "CAR": 12, "CHI": 16, "COL": 21,
    "CBJ": 29, "DAL": 25, "DET": 17, "EDM": 22, "FLA": 13, "LAK": 26, "MIN": 30,
    "MTL": 8, "NSH": 18, "NJD": 1, "NYI": 2, "NYR": 3, "OTT": 9, "PHI": 4,
    "PIT": 5, "SJS": 28, "SEA": 55, "STL": 19, "TBL": 14, "TOR": 10, "UTA": 59,
    "VAN": 23, "VGK": 54, "WSH": 15, "WPG": 52,
}

def ttl_cache_get(key: str) -> Any | None:
    hit = _cache.get(key)
    if not hit:
        return None
    expires_at, value = hit
    if time.time() >= expires_at:
        _cache.pop(key, None)
        return None
    return value

def ttl_cache_set(key: str, value: Any, ttl: int) -> Any:
    _cache[key] = (time.time() + ttl, value)
    return value

def fetch_json(url: str, ttl: int) -> Any:
    cached = ttl_cache_get(url)
    if cached is not None:
        return cached
    res = session.get(url, timeout=TIMEOUT)
    res.raise_for_status()
    return ttl_cache_set(url, res.json(), ttl)

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def iso_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def current_mlb_season() -> int:
    return utc_now().year

def current_nhl_season_code() -> str:
    now = utc_now()
    start_year = now.year if now.month >= 7 else now.year - 1
    return f"{start_year}{start_year + 1}"

def today_and_tomorrow() -> list[str]:
    today = utc_now().date()
    return [iso_date(datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc)),
            iso_date(datetime.combine(today + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc))]

# ---------------- NHL ----------------

def nhl_standings_map() -> dict[str, dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/standings/now", ttl=BOARD_TTL)
    out: dict[str, dict[str, Any]] = {}
    for row in data.get("standings", []):
        code = (row.get("teamAbbrev") or {}).get("default")
        if code:
            out[code] = row
    return out

def nhl_team_name(code: str) -> str:
    row = nhl_standings_map().get(code, {})
    return (row.get("teamName") or {}).get("default", code)

def nhl_schedule_for_date(date_code: str) -> list[dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/schedule/{date_code}", ttl=SCORES_TTL)
    out = []
    for day in data.get("gameWeek", []) or []:
        if day.get("date") != date_code:
            continue
        for g in day.get("games", []) or []:
            away = g.get("awayTeam") or {}
            home = g.get("homeTeam") or {}
            state = str(g.get("gameState") or "")
            out.append({
                "sport": "nhl",
                "id": g.get("id"),
                "date": date_code,
                "startTimeUTC": g.get("startTimeUTC"),
                "status": state,
                "statusText": state,
                "awayCode": away.get("abbrev"),
                "homeCode": home.get("abbrev"),
                "awayName": nhl_team_name(away.get("abbrev", "")),
                "homeName": nhl_team_name(home.get("abbrev", "")),
                "awayScore": int(away.get("score") or 0),
                "homeScore": int(home.get("score") or 0),
                "venue": (g.get("venue") or {}).get("default", ""),
                "isLive": state in {"LIVE", "CRIT", "OFF", "FUT", "PRE"},
            })
    out.sort(key=lambda x: x.get("startTimeUTC") or "")
    return out

def nhl_team_schedule(team_code: str) -> list[dict[str, Any]]:
    season = current_nhl_season_code()
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/{season}", ttl=PREDICTIONS_TTL)
    games = []
    for g in data.get("games", []) or []:
        away = g.get("awayTeam") or {}
        home = g.get("homeTeam") or {}
        is_home = home.get("abbrev") == team_code
        team_side = home if is_home else away
        opp_side = away if is_home else home
        status = str(g.get("gameState") or "")
        games.append({
            "id": g.get("id"),
            "gameDate": g.get("gameDate"),
            "startTimeUTC": g.get("startTimeUTC"),
            "completed": status == "OFF",
            "isHome": is_home,
            "opponentCode": opp_side.get("abbrev"),
            "teamScore": int(team_side.get("score") or 0),
            "oppScore": int(opp_side.get("score") or 0),
            "goalDiff": int(team_side.get("score") or 0) - int(opp_side.get("score") or 0),
            "won": status == "OFF" and int(team_side.get("score") or 0) > int(opp_side.get("score") or 0),
        })
    games.sort(key=lambda x: ((x.get("gameDate") or ""), (x.get("startTimeUTC") or "")))
    return games

def nhl_recent_metrics(team_code: str, n: int = 8) -> dict[str, float]:
    schedule = nhl_team_schedule(team_code)
    done = [g for g in schedule if g["completed"]]
    recent = sorted(done, key=lambda g: g["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_gd": 0.0, "gf": 3.0, "ga": 3.0}
    count = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / count,
        "avg_gd": sum(g["goalDiff"] for g in recent) / count,
        "gf": sum(g["teamScore"] for g in recent) / count,
        "ga": sum(g["oppScore"] for g in recent) / count,
    }

def nhl_split_metrics(team_code: str, home: bool, n: int = 6) -> dict[str, float]:
    schedule = nhl_team_schedule(team_code)
    games = [g for g in schedule if g["completed"] and g["isHome"] == home]
    recent = sorted(games, key=lambda g: g["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_gd": 0.0}
    count = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / count,
        "avg_gd": sum(g["goalDiff"] for g in recent) / count,
    }

def nhl_head_to_head(team_code: str, opp_code: str, n: int = 4) -> dict[str, float]:
    games = [g for g in nhl_team_schedule(team_code) if g["completed"] and g["opponentCode"] == opp_code]
    recent = sorted(games, key=lambda g: g["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_gd": 0.0}
    count = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / count,
        "avg_gd": sum(g["goalDiff"] for g in recent) / count,
    }

def nhl_rest_days(team_code: str, game_date: str) -> int:
    done = [g for g in nhl_team_schedule(team_code) if g["completed"] and (g["gameDate"] or "") < game_date]
    if not done:
        return 5
    last = sorted(done, key=lambda g: g["gameDate"], reverse=True)[0]
    try:
        d1 = datetime.fromisoformat(last["gameDate"])
        d2 = datetime.fromisoformat(game_date)
        return max(-1, min((d2 - d1).days - 1, 7))
    except Exception:
        return 1

def nhl_strength(team_code: str) -> dict[str, float]:
    row = nhl_standings_map().get(team_code, {})
    return {
        "points_pct": safe_float(row.get("pointPctg"), 0.5),
        "gfpg": safe_float(row.get("goalForPerGame"), 3.0),
        "gapg": safe_float(row.get("goalAgainstPerGame"), 3.0),
        "goal_diff_pg": safe_float(row.get("goalForPerGame"), 3.0) - safe_float(row.get("goalAgainstPerGame"), 3.0),
    }

def nhl_build_insight(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]
    if not away or not home:
        return {"error": "Missing team code."}

    away_strength = nhl_strength(away)
    home_strength = nhl_strength(home)
    away_recent = nhl_recent_metrics(away)
    home_recent = nhl_recent_metrics(home)
    away_road = nhl_split_metrics(away, home=False)
    home_home = nhl_split_metrics(home, home=True)
    home_h2h = nhl_head_to_head(home, away)
    away_rest = nhl_rest_days(away, game["date"])
    home_rest = nhl_rest_days(home, game["date"])

    score = 0.0
    score += (home_strength["points_pct"] - away_strength["points_pct"]) * 3.4
    score += (home_strength["goal_diff_pg"] - away_strength["goal_diff_pg"]) * 0.9
    score += (home_recent["win_pct"] - away_recent["win_pct"]) * 1.4
    score += (home_recent["avg_gd"] - away_recent["avg_gd"]) * 0.35
    score += (home_home["win_pct"] - away_road["win_pct"]) * 0.8
    score += (home_home["avg_gd"] - away_road["avg_gd"]) * 0.25
    score += (home_h2h["win_pct"] - 0.5) * 0.7
    score += home_h2h["avg_gd"] * 0.15
    score += (home_rest - away_rest) * 0.08
    score += 0.18

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)

    tier = "tight" if confidence < 8 else "lean" if confidence < 16 else "strong"
    color = "yellow" if tier == "tight" else "green"

    projected_total = round(max(4.5, min(8.5,
        (home_strength["gfpg"] + away_strength["gfpg"] + home_strength["gapg"] + away_strength["gapg"]) / 2
        + (home_recent["gf"] + away_recent["gf"] - home_recent["ga"] - away_recent["ga"]) * 0.08
    )), 1)

    likely_point_team = home if home_prob >= 0.5 else away
    reasons = [
        {"label": "Standings edge", "text": f"{predicted_winner} has the better standings profile." if confidence >= 8 else "The standings gap is small."},
        {"label": "Recent form", "text": f"{home} recent win% {home_recent['win_pct']:.3f} vs {away} {away_recent['win_pct']:.3f}."},
        {"label": "Home / road split", "text": f"{home} home form {home_home['win_pct']:.3f}; {away} road form {away_road['win_pct']:.3f}."},
        {"label": "Head-to-head", "text": f"{home} H2H edge {home_h2h['win_pct']:.3f} over last meetings."},
        {"label": "Rest", "text": f"{home} rest {home_rest} days, {away} rest {away_rest} days."},
    ]

    return {
        "sport": "nhl",
        "gameId": game["id"],
        "date": game["date"],
        "awayCode": away,
        "homeCode": home,
        "awayName": game["awayName"],
        "homeName": game["homeName"],
        "startTimeUTC": game["startTimeUTC"],
        "venue": game["venue"],
        "live": {
            "status": game["status"],
            "awayScore": game["awayScore"],
            "homeScore": game["homeScore"],
        },
        "predictedWinner": predicted_winner,
        "predictedLoser": predicted_loser,
        "homeWinProbability": round(home_prob * 100, 1),
        "awayWinProbability": round((1 - home_prob) * 100, 1),
        "confidence": confidence,
        "tier": tier,
        "cardColor": color,
        "projectedTotal": projected_total,
        "likelyPointTeam": likely_point_team,
        "reasons": reasons,
        "lastPredictionRefreshUTC": utc_now().isoformat(),
    }

# ---------------- MLB ----------------

def mlb_team_records() -> dict[int, dict[str, Any]]:
    season = current_mlb_season()
    data = fetch_json(f"{MLB_API}/standings?leagueId=103,104&season={season}", ttl=BOARD_TTL)
    out: dict[int, dict[str, Any]] = {}
    for rec in data.get("records", []) or []:
        for row in rec.get("teamRecords", []) or []:
            team = row.get("team") or {}
            team_id = team.get("id")
            if team_id:
                out[int(team_id)] = row
    return out

def mlb_schedule_for_date(date_code: str) -> list[dict[str, Any]]:
    url = f"{MLB_API}/schedule?sportId=1&date={date_code}&hydrate=linescore,probablePitcher,team"
    data = fetch_json(url, ttl=SCORES_TTL)
    games = []
    for date_row in data.get("dates", []) or []:
        for g in date_row.get("games", []) or []:
            teams = g.get("teams") or {}
            away = (teams.get("away") or {}).get("team") or {}
            home = (teams.get("home") or {}).get("team") or {}
            linescore = g.get("linescore") or {}
            status = (g.get("status") or {}).get("detailedState") or (g.get("status") or {}).get("abstractGameState") or ""
            games.append({
                "sport": "mlb",
                "id": g.get("gamePk"),
                "date": date_code,
                "startTimeUTC": g.get("gameDate"),
                "status": status,
                "statusText": status,
                "awayId": away.get("id"),
                "homeId": home.get("id"),
                "awayCode": away.get("abbreviation") or away.get("name", "AWY")[:3].upper(),
                "homeCode": home.get("abbreviation") or home.get("name", "HME")[:3].upper(),
                "awayName": away.get("name", "Away"),
                "homeName": home.get("name", "Home"),
                "awayScore": int(((teams.get("away") or {}).get("score")) or 0),
                "homeScore": int(((teams.get("home") or {}).get("score")) or 0),
                "venue": ((g.get("venue") or {}).get("name")) or "",
                "awayProbablePitcher": ((teams.get("away") or {}).get("probablePitcher") or {}).get("fullName", ""),
                "homeProbablePitcher": ((teams.get("home") or {}).get("probablePitcher") or {}).get("fullName", ""),
                "inningState": linescore.get("inningState") or "",
                "currentInning": linescore.get("currentInning") or "",
            })
    games.sort(key=lambda x: x.get("startTimeUTC") or "")
    return games

def mlb_team_schedule(team_id: int) -> list[dict[str, Any]]:
    season = current_mlb_season()
    url = f"{MLB_API}/schedule?sportId=1&teamId={team_id}&season={season}&hydrate=linescore,team"
    data = fetch_json(url, ttl=PREDICTIONS_TTL)
    out = []
    for date_row in data.get("dates", []) or []:
        for g in date_row.get("games", []) or []:
            teams = g.get("teams") or {}
            away = (teams.get("away") or {}).get("team") or {}
            home = (teams.get("home") or {}).get("team") or {}
            is_home = home.get("id") == team_id
            team_side = teams.get("home") if is_home else teams.get("away")
            opp_side = teams.get("away") if is_home else teams.get("home")
            status = (g.get("status") or {}).get("abstractGameCode") or ""
            out.append({
                "gameDate": date_row.get("date"),
                "completed": status == "F",
                "isHome": is_home,
                "oppId": ((opp_side or {}).get("team") or {}).get("id"),
                "teamScore": int((team_side or {}).get("score") or 0),
                "oppScore": int((opp_side or {}).get("score") or 0),
                "runDiff": int((team_side or {}).get("score") or 0) - int((opp_side or {}).get("score") or 0),
                "won": status == "F" and int((team_side or {}).get("score") or 0) > int((opp_side or {}).get("score") or 0),
            })
    out.sort(key=lambda x: x["gameDate"])
    return out

def mlb_strength(team_id: int) -> dict[str, float]:
    row = mlb_team_records().get(team_id, {})
    wins = safe_float(row.get("wins"), 0.0)
    losses = safe_float(row.get("losses"), 0.0)
    pct = safe_float(row.get("winningPercentage"), 0.5)
    runs_scored = safe_float(row.get("runsScored"), 0.0)
    runs_allowed = safe_float(row.get("runsAllowed"), 0.0)
    games = max(1.0, wins + losses)
    return {
        "win_pct": pct if pct > 0 else (wins / games if games else 0.5),
        "run_diff_pg": (runs_scored - runs_allowed) / games,
    }

def mlb_recent_metrics(team_id: int, n: int = 10) -> dict[str, float]:
    games = [g for g in mlb_team_schedule(team_id) if g["completed"]]
    recent = sorted(games, key=lambda x: x["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_rd": 0.0, "rf": 4.5, "ra": 4.5}
    c = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / c,
        "avg_rd": sum(g["runDiff"] for g in recent) / c,
        "rf": sum(g["teamScore"] for g in recent) / c,
        "ra": sum(g["oppScore"] for g in recent) / c,
    }

def mlb_split_metrics(team_id: int, home: bool, n: int = 8) -> dict[str, float]:
    games = [g for g in mlb_team_schedule(team_id) if g["completed"] and g["isHome"] == home]
    recent = sorted(games, key=lambda x: x["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_rd": 0.0}
    c = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / c,
        "avg_rd": sum(g["runDiff"] for g in recent) / c,
    }

def mlb_head_to_head(team_id: int, opp_id: int, n: int = 6) -> dict[str, float]:
    games = [g for g in mlb_team_schedule(team_id) if g["completed"] and g["oppId"] == opp_id]
    recent = sorted(games, key=lambda x: x["gameDate"], reverse=True)[:n]
    if not recent:
        return {"win_pct": 0.5, "avg_rd": 0.0}
    c = len(recent)
    return {
        "win_pct": sum(1 for g in recent if g["won"]) / c,
        "avg_rd": sum(g["runDiff"] for g in recent) / c,
    }

def mlb_rest_days(team_id: int, game_date: str) -> int:
    games = [g for g in mlb_team_schedule(team_id) if g["completed"] and g["gameDate"] < game_date]
    if not games:
        return 2
    last = sorted(games, key=lambda x: x["gameDate"], reverse=True)[0]
    try:
        d1 = datetime.fromisoformat(last["gameDate"])
        d2 = datetime.fromisoformat(game_date)
        return max(0, min((d2 - d1).days - 1, 4))
    except Exception:
        return 1

def mlb_build_insight(game: dict[str, Any]) -> dict[str, Any]:
    away_id = int(game["awayId"])
    home_id = int(game["homeId"])
    away_strength = mlb_strength(away_id)
    home_strength = mlb_strength(home_id)
    away_recent = mlb_recent_metrics(away_id)
    home_recent = mlb_recent_metrics(home_id)
    away_road = mlb_split_metrics(away_id, home=False)
    home_home = mlb_split_metrics(home_id, home=True)
    home_h2h = mlb_head_to_head(home_id, away_id)
    away_rest = mlb_rest_days(away_id, game["date"])
    home_rest = mlb_rest_days(home_id, game["date"])

    score = 0.0
    score += (home_strength["win_pct"] - away_strength["win_pct"]) * 3.2
    score += (home_strength["run_diff_pg"] - away_strength["run_diff_pg"]) * 0.9
    score += (home_recent["win_pct"] - away_recent["win_pct"]) * 1.2
    score += (home_recent["avg_rd"] - away_recent["avg_rd"]) * 0.25
    score += (home_home["win_pct"] - away_road["win_pct"]) * 0.7
    score += (home_home["avg_rd"] - away_road["avg_rd"]) * 0.18
    score += (home_h2h["win_pct"] - 0.5) * 0.5
    score += home_h2h["avg_rd"] * 0.12
    score += (home_rest - away_rest) * 0.08
    score += 0.15
    if game.get("homeProbablePitcher") and not game.get("awayProbablePitcher"):
        score += 0.08
    elif game.get("awayProbablePitcher") and not game.get("homeProbablePitcher"):
        score -= 0.08

    home_prob = logistic(score)
    predicted_winner = game["homeCode"] if home_prob >= 0.5 else game["awayCode"]
    predicted_loser = game["awayCode"] if predicted_winner == game["homeCode"] else game["homeCode"]
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if confidence < 8 else "lean" if confidence < 16 else "strong"
    color = "yellow" if tier == "tight" else "green"

    projected_total = round(max(6.5, min(12.5,
        (home_recent["rf"] + away_recent["rf"] + home_recent["ra"] + away_recent["ra"]) / 2
    )), 1)

    reasons = [
        {"label": "Team strength", "text": f"{predicted_winner} carries the stronger season profile." if confidence >= 8 else "The season gap is small."},
        {"label": "Recent form", "text": f"{game['homeCode']} recent win% {home_recent['win_pct']:.3f}; {game['awayCode']} {away_recent['win_pct']:.3f}."},
        {"label": "Home / road split", "text": f"{game['homeCode']} home {home_home['win_pct']:.3f}; {game['awayCode']} road {away_road['win_pct']:.3f}."},
        {"label": "Probable pitchers", "text": f"{game.get('awayProbablePitcher') or 'TBD'} vs {game.get('homeProbablePitcher') or 'TBD'}."},
        {"label": "Rest", "text": f"{game['homeCode']} rest {home_rest} days, {game['awayCode']} rest {away_rest} days."},
    ]

    return {
        "sport": "mlb",
        "gameId": game["id"],
        "date": game["date"],
        "awayCode": game["awayCode"],
        "homeCode": game["homeCode"],
        "awayName": game["awayName"],
        "homeName": game["homeName"],
        "startTimeUTC": game["startTimeUTC"],
        "venue": game["venue"],
        "live": {
            "status": game["status"],
            "awayScore": game["awayScore"],
            "homeScore": game["homeScore"],
        },
        "predictedWinner": predicted_winner,
        "predictedLoser": predicted_loser,
        "homeWinProbability": round(home_prob * 100, 1),
        "awayWinProbability": round((1 - home_prob) * 100, 1),
        "confidence": confidence,
        "tier": tier,
        "cardColor": color,
        "projectedTotal": projected_total,
        "likelyImpactPlayerTeam": predicted_winner,
        "probablePitchers": {
            "away": game.get("awayProbablePitcher", ""),
            "home": game.get("homeProbablePitcher", ""),
        },
        "reasons": reasons,
        "lastPredictionRefreshUTC": utc_now().isoformat(),
    }

# ---------------- Unified board / insights ----------------

def board_payload(sport: str) -> dict[str, Any]:
    dates = today_and_tomorrow()
    days = []
    for date_code in dates:
        games = nhl_schedule_for_date(date_code) if sport == "nhl" else mlb_schedule_for_date(date_code)
        days.append({"date": date_code, "label": "Day 1" if not days else "Day 2", "games": games})
    return {
        "sport": sport,
        "days": days,
        "refreshWindows": {
            "scoresSeconds": SCORES_TTL,
            "predictionsSeconds": PREDICTIONS_TTL,
            "boardSeconds": BOARD_TTL,
        },
        "generatedAtUTC": utc_now().isoformat(),
    }

def insights_payload(sport: str) -> dict[str, Any]:
    board = board_payload(sport)
    insights = []
    for day in board["days"]:
        for game in day["games"]:
            try:
                insight = nhl_build_insight(game) if sport == "nhl" else mlb_build_insight(game)
                insights.append(insight)
            except Exception as exc:
                insights.append({
                    "sport": sport,
                    "gameId": game.get("id"),
                    "date": game.get("date"),
                    "awayCode": game.get("awayCode"),
                    "homeCode": game.get("homeCode"),
                    "error": str(exc),
                    "startTimeUTC": game.get("startTimeUTC"),
                    "live": {"status": game.get("status"), "awayScore": game.get("awayScore"), "homeScore": game.get("homeScore")},
                })
    insights.sort(key=lambda x: ((x.get("date") or ""), (x.get("startTimeUTC") or "")))
    return {
        "sport": sport,
        "insights": insights,
        "generatedAtUTC": utc_now().isoformat(),
        "refreshWindows": {
            "scoresSeconds": SCORES_TTL,
            "predictionsSeconds": PREDICTIONS_TTL,
            "boardSeconds": BOARD_TTL,
        },
    }

@app.get("/")
def index() -> str:
    return render_template("index.html")

@app.get("/api/health")
def api_health() -> Any:
    return jsonify({"ok": True, "time": utc_now().isoformat()})

@app.get("/api/board/<sport>")
def api_board(sport: str) -> Any:
    sport = sport.lower()
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(board_payload(sport))
    except Exception as exc:
        return jsonify({"error": str(exc), "days": []}), 500

@app.get("/api/insights/<sport>")
def api_insights(sport: str) -> Any:
    sport = sport.lower()
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(insights_payload(sport))
    except Exception as exc:
        return jsonify({"error": str(exc), "insights": []}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
