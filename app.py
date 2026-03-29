from __future__ import annotations

import math
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
MLB_API = "https://statsapi.mlb.com/api/v1"
TIMEOUT = 20

BOARD_TTL = 300
PRED_TTL = 900
ANALYZE_TTL = 3600

session = requests.Session()
session.headers.update({"User-Agent": "Game-Insights/3.2"})

_cache: dict[str, tuple[float, Any]] = {}


def ttl_get(key: str) -> Any | None:
    hit = _cache.get(key)
    if not hit:
        return None
    expires_at, value = hit
    if time.time() >= expires_at:
        _cache.pop(key, None)
        return None
    return value


def ttl_set(key: str, value: Any, ttl: int) -> Any:
    _cache[key] = (time.time() + ttl, value)
    return value


def fetch_json(url: str, ttl: int) -> Any:
    cached = ttl_get(url)
    if cached is not None:
        return cached
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return ttl_set(url, response.json(), ttl)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_utc_str() -> str:
    return utc_now().strftime("%Y-%m-%d")


def current_nhl_season() -> str:
    now = utc_now()
    year = now.year
    start_year = year if now.month >= 7 else year - 1
    return f"{start_year}{start_year + 1}"


def current_mlb_season() -> int:
    return utc_now().year


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


# ---------------- NHL ----------------

def nhl_standings_map() -> dict[str, dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/standings/now", BOARD_TTL)
    rows: dict[str, dict[str, Any]] = {}
    for row in data.get("standings", []) or []:
        abbr = row.get("teamAbbrev", {}).get("default")
        if abbr:
            rows[abbr] = row
    return rows


def nhl_team_name_map() -> dict[str, str]:
    rows = nhl_standings_map()
    return {abbr: row.get("teamName", {}).get("default", abbr) for abbr, row in rows.items()}


def nhl_team_list() -> list[dict[str, str]]:
    rows = nhl_standings_map()
    items = []
    for abbr, row in rows.items():
        items.append({"code": abbr, "name": row.get("teamName", {}).get("default", abbr)})
    return sorted(items, key=lambda x: safe_str(x["name"]))


def nhl_parse_board_game(raw: dict[str, Any]) -> dict[str, Any]:
    away = raw.get("awayTeam", {}) or {}
    home = raw.get("homeTeam", {}) or {}
    names = nhl_team_name_map()
    return {
        "sport": "nhl",
        "id": raw.get("id"),
        "date": safe_str(raw.get("gameDate")),
        "startTimeUTC": safe_str(raw.get("startTimeUTC")),
        "status": safe_str(raw.get("gameState") or raw.get("gameScheduleState") or "PRE"),
        "venue": safe_str((raw.get("venue") or {}).get("default", "")),
        "awayCode": safe_str(away.get("abbrev")),
        "homeCode": safe_str(home.get("abbrev")),
        "awayName": names.get(safe_str(away.get("abbrev")), safe_str(away.get("abbrev"), "Away")),
        "homeName": names.get(safe_str(home.get("abbrev")), safe_str(home.get("abbrev"), "Home")),
        "awayScore": int(away.get("score") or 0),
        "homeScore": int(home.get("score") or 0),
    }


def nhl_board() -> dict[str, Any]:
    today = today_utc_str()
    data = fetch_json(f"{NHL_API}/schedule/{today}", BOARD_TTL)
    games: list[dict[str, Any]] = []

    for day in data.get("gameWeek", []) or []:
        if day.get("date") == today:
            games = [nhl_parse_board_game(g) for g in (day.get("games") or [])]
            break

    games.sort(key=lambda g: safe_str(g.get("startTimeUTC")))
    return {
        "sport": "nhl",
        "updatedUTC": utc_now().isoformat(),
        "days": [{"label": "Today", "date": today, "games": games}],
    }


def nhl_schedule(team_code: str) -> list[dict[str, Any]]:
    season = current_nhl_season()
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/{season}", PRED_TTL)
    names = nhl_team_name_map()
    games = []

    for raw in data.get("games", []) or []:
        away = raw.get("awayTeam", {}) or {}
        home = raw.get("homeTeam", {}) or {}
        away_code = safe_str(away.get("abbrev"))
        home_code = safe_str(home.get("abbrev"))
        is_home = home_code == team_code

        team_score = int((home if is_home else away).get("score") or 0)
        opp_score = int((away if is_home else home).get("score") or 0)
        opp_code = away_code if is_home else home_code
        status = safe_str(raw.get("gameState") or raw.get("gameScheduleState") or "PRE")
        ot = (raw.get("gameOutcome") or {}).get("lastPeriodType") in ("OT", "SO")

        games.append(
            {
                "id": raw.get("id"),
                "date": safe_str(raw.get("gameDate")),
                "startTimeUTC": safe_str(raw.get("startTimeUTC")),
                "status": status,
                "teamCode": team_code,
                "opponentCode": opp_code,
                "opponentName": names.get(opp_code, opp_code or "Opponent"),
                "isHome": is_home,
                "teamScore": team_score,
                "opponentScore": opp_score,
                "completed": status == "OFF",
                "won": status == "OFF" and team_score > opp_score,
                "ot": ot,
                "goalDiff": team_score - opp_score,
            }
        )

    return sorted(games, key=lambda x: (safe_str(x.get("date")), safe_str(x.get("startTimeUTC"))))


def nhl_boxscore(game_id: int) -> dict[str, Any]:
    return fetch_json(f"{NHL_API}/gamecenter/{game_id}/boxscore", ANALYZE_TTL)


def nhl_side_players(side: dict[str, Any]) -> list[dict[str, Any]]:
    players: list[dict[str, Any]] = []
    for bucket in ("forwards", "defense", "goalies"):
        players.extend(side.get(bucket, []) or [])
    return players


def nhl_hot_assist_pick(team_code: str, last_n: int = 3) -> dict[str, Any]:
    schedule = nhl_schedule(team_code)
    completed = [g for g in schedule if g["completed"] and g.get("id")]
    completed = sorted(
        completed,
        key=lambda g: (safe_str(g.get("date")), safe_str(g.get("startTimeUTC"))),
        reverse=True
    )[:last_n]

    if not completed:
        return {
            "teamCode": team_code,
            "player": "No data",
            "assistsLast3": 0,
            "pointsLast3": 0,
            "gamesUsed": 0,
            "reason": "No completed games found."
        }

    totals: dict[str, dict[str, Any]] = {}

    for g in completed:
        try:
            box = nhl_boxscore(int(g["id"]))
        except Exception:
            continue

        away = box.get("awayTeam", {}) or {}
        home = box.get("homeTeam", {}) or {}

        side = None
        if safe_str(away.get("abbrev")) == team_code:
            side = away
        elif safe_str(home.get("abbrev")) == team_code:
            side = home

        if not side:
            continue

        for p in nhl_side_players(side):
            first = safe_str((p.get("firstName") or {}).get("default", ""))
            last = safe_str((p.get("lastName") or {}).get("default", ""))
            name = (first + " " + last).strip() or safe_str((p.get("name") or {}).get("default", "Unknown"))
            assists = int(p.get("assists") or 0)
            goals = int(p.get("goals") or 0)
            points = assists + goals

            if name not in totals:
                totals[name] = {
                    "player": name,
                    "assistsLast3": 0,
                    "pointsLast3": 0,
                    "gamesWithPoint": 0,
                    "gamesUsed": 0,
                }

            totals[name]["assistsLast3"] += assists
            totals[name]["pointsLast3"] += points
            totals[name]["gamesUsed"] += 1
            if points > 0:
                totals[name]["gamesWithPoint"] += 1

    if not totals:
        return {
            "teamCode": team_code,
            "player": "No data",
            "assistsLast3": 0,
            "pointsLast3": 0,
            "gamesUsed": len(completed),
            "reason": "Boxscore player data unavailable."
        }

    ranked = sorted(
        totals.values(),
        key=lambda x: (
            -x["assistsLast3"],
            -x["pointsLast3"],
            -x["gamesWithPoint"],
            x["player"]
        )
    )

    best = ranked[0]
    return {
        "teamCode": team_code,
        "player": best["player"],
        "assistsLast3": best["assistsLast3"],
        "pointsLast3": best["pointsLast3"],
        "gamesUsed": best["gamesUsed"],
        "reason": f"Best recent setup trend: {best['assistsLast3']} assists in last {best['gamesUsed']} completed games."
    }


def nhl_recent_form(schedule: list[dict[str, Any]], n: int = 5) -> dict[str, float]:
    completed = [g for g in schedule if g["completed"]]
    completed = sorted(completed, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not completed:
        return {"win_pct": 0.5, "avg_diff": 0.0, "gf": 3.0, "ga": 3.0}
    return {
        "win_pct": sum(1 for g in completed if g["won"]) / len(completed),
        "avg_diff": sum(g["goalDiff"] for g in completed) / len(completed),
        "gf": sum(g["teamScore"] for g in completed) / len(completed),
        "ga": sum(g["opponentScore"] for g in completed) / len(completed),
    }


def nhl_split_form(schedule: list[dict[str, Any]], is_home: bool, n: int = 5) -> dict[str, float]:
    games = [g for g in schedule if g["completed"] and g["isHome"] == is_home]
    games = sorted(games, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0}
    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["goalDiff"] for g in games) / len(games),
    }


def nhl_h2h(team_code: str, opp_code: str, schedule: list[dict[str, Any]], n: int = 4) -> dict[str, float]:
    games = [g for g in schedule if g["completed"] and g["opponentCode"] == opp_code]
    games = sorted(games, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0}
    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["goalDiff"] for g in games) / len(games),
    }


def nhl_rest_days(schedule: list[dict[str, Any]], game_date: str) -> int:
    prior = [g for g in schedule if g["completed"] and safe_str(g.get("date")) < safe_str(game_date)]
    if not prior:
        return 4
    last_date = sorted(prior, key=lambda g: safe_str(g.get("date")), reverse=True)[0]["date"]
    try:
        d1 = datetime.fromisoformat(last_date)
        d2 = datetime.fromisoformat(game_date)
        return max(-1, min((d2 - d1).days - 1, 7))
    except Exception:
        return 1


def nhl_strength(team_code: str) -> dict[str, float]:
    row = nhl_standings_map().get(team_code, {})
    return {
        "points_pct": safe_float(row.get("pointPctg"), 0.5),
        "goal_diff_pg": safe_float(row.get("goalForPerGame"), 3.0) - safe_float(row.get("goalAgainstPerGame"), 3.0),
    }


def nhl_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]

    away_sched = nhl_schedule(away)
    home_sched = nhl_schedule(home)

    away_form = nhl_recent_form(away_sched)
    home_form = nhl_recent_form(home_sched)

    away_split = nhl_split_form(away_sched, False)
    home_split = nhl_split_form(home_sched, True)

    away_h2h = nhl_h2h(away, home, away_sched)
    home_h2h = nhl_h2h(home, away, home_sched)

    away_strength = nhl_strength(away)
    home_strength = nhl_strength(home)

    away_rest = nhl_rest_days(away_sched, game["date"])
    home_rest = nhl_rest_days(home_sched, game["date"])

    score = 0.0
    score += (home_strength["points_pct"] - away_strength["points_pct"]) * 3.4
    score += (home_strength["goal_diff_pg"] - away_strength["goal_diff_pg"]) * 0.95
    score += (home_form["win_pct"] - away_form["win_pct"]) * 1.6
    score += (home_form["avg_diff"] - away_form["avg_diff"]) * 0.35
    score += (home_split["win_pct"] - away_split["win_pct"]) * 0.8
    score += (home_h2h["win_pct"] - away_h2h["win_pct"]) * 0.6
    score += (home_rest - away_rest) * 0.08
    score += 0.20

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 45 <= home_prob * 100 <= 55 else "edge"

    projected_total = max(3.5, min(9.5, (home_form["gf"] + away_form["gf"] + home_form["ga"] + away_form["ga"]) / 2))

    away_player_pick = nhl_hot_assist_pick(away, last_n=3)
    home_player_pick = nhl_hot_assist_pick(home, last_n=3)

    return {
        "sport": "nhl",
        "gameId": game["id"],
        "date": game["date"],
        "startTimeUTC": game["startTimeUTC"],
        "venue": game["venue"],
        "awayCode": away,
        "homeCode": home,
        "awayName": game["awayName"],
        "homeName": game["homeName"],
        "predictedWinner": predicted_winner,
        "predictedLoser": predicted_loser,
        "homeWinProbability": round(home_prob * 100, 1),
        "awayWinProbability": round((1 - home_prob) * 100, 1),
        "confidence": confidence,
        "projectedTotal": round(projected_total, 1),
        "likelyPointTeam": predicted_winner,
        "playerPicks": {
            "away": away_player_pick,
            "home": home_player_pick,
        },
        "tier": tier,
        "lastPredictionRefreshUTC": utc_now().isoformat(),
        "reasons": [
            {
                "label": "Standings edge",
                "text": f"{home} points % {home_strength['points_pct']:.3f} vs {away} {away_strength['points_pct']:.3f}.",
            },
            {
                "label": "Recent form",
                "text": f"{home} recent goal diff {home_form['avg_diff']:.2f} vs {away} {away_form['avg_diff']:.2f}.",
            },
            {
                "label": "Home and road split",
                "text": f"{home} home win% {home_split['win_pct']:.2f} vs {away} road win% {away_split['win_pct']:.2f}.",
            },
            {
                "label": "Rest and head-to-head",
                "text": f"{home} rest {home_rest} days vs {away} rest {away_rest} days. Recent head-to-head leans {home if home_h2h['win_pct'] >= away_h2h['win_pct'] else away}.",
            },
        ],
    }


def nhl_insights() -> dict[str, Any]:
    board = nhl_board()
    insights = [nhl_predict_game(g) for g in board["days"][0]["games"]]
    return {"sport": "nhl", "updatedUTC": utc_now().isoformat(), "insights": insights}


def nhl_team_analyze(team_code: str) -> dict[str, Any]:
    schedule = nhl_schedule(team_code)
    rows = defaultdict(lambda: {"games": 0, "wins": 0, "losses": 0, "otWins": 0, "otLosses": 0, "gf": 0, "ga": 0, "trend": []})
    names = nhl_team_name_map()

    for g in schedule:
        if not g["completed"] or not g["opponentCode"]:
            continue
        r = rows[g["opponentCode"]]
        r["games"] += 1
        r["gf"] += g["teamScore"]
        r["ga"] += g["opponentScore"]

        if g["won"]:
            if g["ot"]:
                r["otWins"] += 1
                r["trend"].append(0.5)
            else:
                r["wins"] += 1
                r["trend"].append(1)
        else:
            if g["ot"]:
                r["otLosses"] += 1
                r["trend"].append(-0.5)
            else:
                r["losses"] += 1
                r["trend"].append(-1)

    items = []
    for opp_code, r in rows.items():
        items.append(
            {
                "opponentCode": opp_code,
                "opponentName": names.get(opp_code, opp_code),
                "games": r["games"],
                "wins": r["wins"],
                "losses": r["losses"],
                "otWins": r["otWins"],
                "otLosses": r["otLosses"],
                "gf": r["gf"],
                "ga": r["ga"],
                "goalDiff": r["gf"] - r["ga"],
                "scorePct": round((r["wins"] + 0.5 * r["otWins"] + 0.25 * r["otLosses"]) / max(r["games"], 1), 3),
                "trend": r["trend"][-10:],
            }
        )

    items.sort(key=lambda x: safe_str(x["opponentName"]))
    return {
        "sport": "nhl",
        "teamCode": team_code,
        "teamName": names.get(team_code, team_code),
        "rows": items,
    }


# ---------------- MLB ----------------

def mlb_teams_map() -> dict[str, dict[str, Any]]:
    data = fetch_json(f"{MLB_API}/teams?sportId=1", ANALYZE_TTL)
    teams: dict[str, dict[str, Any]] = {}
    for t in data.get("teams", []) or []:
        abbr = t.get("abbreviation")
        if abbr:
            teams[abbr] = {
                "id": t.get("id"),
                "code": abbr,
                "name": t.get("name"),
            }
    return teams


def mlb_team_list() -> list[dict[str, str]]:
    teams = mlb_teams_map()
    return sorted(
        [{"code": v["code"], "name": v["name"]} for v in teams.values()],
        key=lambda x: safe_str(x["name"])
    )


def mlb_standings_strength() -> dict[str, dict[str, float]]:
    season = current_mlb_season()
    data = fetch_json(f"{MLB_API}/standings?leagueId=103,104&season={season}&standingsTypes=regularSeason", BOARD_TTL)
    out: dict[str, dict[str, float]] = {}

    for record in data.get("records", []) or []:
        for tr in record.get("teamRecords", []) or []:
            team = tr.get("team", {}) or {}
            code = team.get("abbreviation")
            wins = safe_float(tr.get("wins"), 0)
            losses = safe_float(tr.get("losses"), 0)
            runs_scored = safe_float(tr.get("runsScored"), 0)
            runs_allowed = safe_float(tr.get("runsAllowed"), 0)
            games = max(wins + losses, 1)

            if code:
                out[code] = {
                    "win_pct": wins / games,
                    "run_diff_pg": (runs_scored - runs_allowed) / games,
                }

    return out


def mlb_schedule_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    hydrate = "probablePitcher,team,linescore"
    data = fetch_json(f"{MLB_API}/schedule?sportId=1&startDate={start_date}&endDate={end_date}&hydrate={hydrate}", BOARD_TTL)
    games = []

    for date_block in data.get("dates", []) or []:
        for raw in date_block.get("games", []) or []:
            teams = raw.get("teams", {}) or {}
            away = teams.get("away", {}) or {}
            home = teams.get("home", {}) or {}
            away_team = away.get("team", {}) or {}
            home_team = home.get("team", {}) or {}
            status = raw.get("status", {}) or {}
            linescore = raw.get("linescore", {}) or {}

            games.append(
                {
                    "sport": "mlb",
                    "id": raw.get("gamePk"),
                    "date": safe_str(raw.get("officialDate")),
                    "startTimeUTC": safe_str(raw.get("gameDate")),
                    "status": safe_str(status.get("abstractGameState") or status.get("detailedState") or "Preview"),
                    "venue": safe_str((raw.get("venue") or {}).get("name", "")),
                    "awayCode": safe_str(away_team.get("abbreviation")),
                    "homeCode": safe_str(home_team.get("abbreviation")),
                    "awayName": safe_str(away_team.get("name")),
                    "homeName": safe_str(home_team.get("name")),
                    "awayScore": int(away.get("score") or 0),
                    "homeScore": int(home.get("score") or 0),
                    "awayProbablePitcher": safe_str((away.get("probablePitcher") or {}).get("fullName")),
                    "homeProbablePitcher": safe_str((home.get("probablePitcher") or {}).get("fullName")),
                    "inningState": safe_str(linescore.get("inningState")),
                    "currentInning": safe_str(linescore.get("currentInning")),
                    "completed": status.get("abstractGameState") == "Final",
                }
            )

    return sorted(games, key=lambda x: safe_str(x.get("startTimeUTC")))


def mlb_board() -> dict[str, Any]:
    today = today_utc_str()
    games = [g for g in mlb_schedule_range(today, today) if g["date"] == today]
    return {
        "sport": "mlb",
        "updatedUTC": utc_now().isoformat(),
        "days": [{"label": "Today", "date": today, "games": games}],
    }


def mlb_team_schedule(team_code: str) -> list[dict[str, Any]]:
    teams = mlb_teams_map()
    team_id = teams.get(team_code, {}).get("id")
    if not team_id:
        return []

    season = current_mlb_season()
    start_date = f"{season}-03-01"
    end_date = f"{season}-11-30"

    data = fetch_json(f"{MLB_API}/schedule?sportId=1&teamId={team_id}&startDate={start_date}&endDate={end_date}", PRED_TTL)
    games = []

    for date_block in data.get("dates", []) or []:
        for raw in date_block.get("games", []) or []:
            teams_raw = raw.get("teams", {}) or {}
            away = teams_raw.get("away", {}) or {}
            home = teams_raw.get("home", {}) or {}
            away_team = away.get("team", {}) or {}
            home_team = home.get("team", {}) or {}
            is_home = safe_str(home_team.get("abbreviation")) == team_code
            team_score = int((home if is_home else away).get("score") or 0)
            opp_score = int((away if is_home else home).get("score") or 0)
            status = safe_str((raw.get("status") or {}).get("abstractGameState") or "Preview")

            games.append(
                {
                    "date": safe_str(raw.get("officialDate")),
                    "startTimeUTC": safe_str(raw.get("gameDate")),
                    "teamCode": team_code,
                    "opponentCode": safe_str(away_team.get("abbreviation") if is_home else home_team.get("abbreviation")),
                    "isHome": is_home,
                    "teamScore": team_score,
                    "opponentScore": opp_score,
                    "completed": status == "Final",
                    "won": status == "Final" and team_score > opp_score,
                    "runDiff": team_score - opp_score,
                }
            )

    return sorted(games, key=lambda x: (safe_str(x.get("date")), safe_str(x.get("startTimeUTC"))))


def mlb_recent_form(schedule: list[dict[str, Any]], n: int = 10) -> dict[str, float]:
    completed = [g for g in schedule if g["completed"]]
    completed = sorted(completed, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not completed:
        return {"win_pct": 0.5, "avg_diff": 0.0, "rf": 4.5, "ra": 4.5}
    return {
        "win_pct": sum(1 for g in completed if g["won"]) / len(completed),
        "avg_diff": sum(g["runDiff"] for g in completed) / len(completed),
        "rf": sum(g["teamScore"] for g in completed) / len(completed),
        "ra": sum(g["opponentScore"] for g in completed) / len(completed),
    }


def mlb_split_form(schedule: list[dict[str, Any]], is_home: bool, n: int = 10) -> dict[str, float]:
    games = [g for g in schedule if g["completed"] and g["isHome"] == is_home]
    games = sorted(games, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0}
    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["runDiff"] for g in games) / len(games),
    }


def mlb_h2h(team_code: str, opp_code: str, schedule: list[dict[str, Any]], n: int = 6) -> dict[str, float]:
    games = [g for g in schedule if g["completed"] and g["opponentCode"] == opp_code]
    games = sorted(games, key=lambda g: safe_str(g.get("date")), reverse=True)[:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0}
    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["runDiff"] for g in games) / len(games),
    }


def mlb_rest_days(schedule: list[dict[str, Any]], game_date: str) -> int:
    prior = [g for g in schedule if g["completed"] and safe_str(g.get("date")) < safe_str(game_date)]
    if not prior:
        return 1
    last_date = sorted(prior, key=lambda g: safe_str(g.get("date")), reverse=True)[0]["date"]
    try:
        d1 = datetime.fromisoformat(last_date)
        d2 = datetime.fromisoformat(game_date)
        return max(0, min((d2 - d1).days - 1, 4))
    except Exception:
        return 0


def mlb_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]

    strengths = mlb_standings_strength()
    away_sched = mlb_team_schedule(away)
    home_sched = mlb_team_schedule(home)

    away_form = mlb_recent_form(away_sched)
    home_form = mlb_recent_form(home_sched)

    away_split = mlb_split_form(away_sched, False)
    home_split = mlb_split_form(home_sched, True)

    away_h2h = mlb_h2h(away, home, away_sched)
    home_h2h = mlb_h2h(home, away, home_sched)

    away_strength = strengths.get(away, {"win_pct": 0.5, "run_diff_pg": 0.0})
    home_strength = strengths.get(home, {"win_pct": 0.5, "run_diff_pg": 0.0})

    away_rest = mlb_rest_days(away_sched, game["date"])
    home_rest = mlb_rest_days(home_sched, game["date"])

    score = 0.0
    score += (home_strength["win_pct"] - away_strength["win_pct"]) * 3.2
    score += (home_strength["run_diff_pg"] - away_strength["run_diff_pg"]) * 0.9
    score += (home_form["win_pct"] - away_form["win_pct"]) * 1.4
    score += (home_form["avg_diff"] - away_form["avg_diff"]) * 0.28
    score += (home_split["win_pct"] - away_split["win_pct"]) * 0.6
    score += (home_h2h["win_pct"] - away_h2h["win_pct"]) * 0.4
    score += (home_rest - away_rest) * 0.08
    if game.get("homeProbablePitcher") and not game.get("awayProbablePitcher"):
        score += 0.12
    if game.get("awayProbablePitcher") and not game.get("homeProbablePitcher"):
        score -= 0.12
    score += 0.12

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 46 <= home_prob * 100 <= 54 else "edge"

    projected_total = max(5.5, min(13.5, (home_form["rf"] + away_form["rf"] + home_form["ra"] + away_form["ra"]) / 2))

    return {
        "sport": "mlb",
        "gameId": game["id"],
        "date": game["date"],
        "startTimeUTC": game["startTimeUTC"],
        "venue": game["venue"],
        "awayCode": away,
        "homeCode": home,
        "awayName": game["awayName"],
        "homeName": game["homeName"],
        "predictedWinner": predicted_winner,
        "predictedLoser": predicted_loser,
        "homeWinProbability": round(home_prob * 100, 1),
        "awayWinProbability": round((1 - home_prob) * 100, 1),
        "confidence": confidence,
        "projectedTotal": round(projected_total, 1),
        "probablePitchers": {
            "away": safe_str(game.get("awayProbablePitcher")),
            "home": safe_str(game.get("homeProbablePitcher")),
        },
        "tier": tier,
        "lastPredictionRefreshUTC": utc_now().isoformat(),
        "reasons": [
            {
                "label": "Season strength",
                "text": f"{home} win% {home_strength['win_pct']:.3f} vs {away} {away_strength['win_pct']:.3f}.",
            },
            {
                "label": "Recent form",
                "text": f"{home} recent run diff {home_form['avg_diff']:.2f} vs {away} {away_form['avg_diff']:.2f}.",
            },
            {
                "label": "Home and away split",
                "text": f"{home} home win% {home_split['win_pct']:.2f} vs {away} road win% {away_split['win_pct']:.2f}.",
            },
            {
                "label": "Pitcher and rest context",
                "text": f"Probables: {(game.get('awayProbablePitcher') or 'TBD')} vs {(game.get('homeProbablePitcher') or 'TBD')}. Rest {away_rest} vs {home_rest} days.",
            },
        ],
    }


def mlb_insights() -> dict[str, Any]:
    board = mlb_board()
    insights = [mlb_predict_game(g) for g in board["days"][0]["games"]]
    return {"sport": "mlb", "updatedUTC": utc_now().isoformat(), "insights": insights}


def mlb_team_analyze(team_code: str) -> dict[str, Any]:
    schedule = mlb_team_schedule(team_code)
    teams = mlb_teams_map()
    rows = defaultdict(lambda: {"games": 0, "wins": 0, "losses": 0, "gf": 0, "ga": 0, "trend": []})

    for g in schedule:
        if not g["completed"] or not g["opponentCode"]:
            continue
        r = rows[g["opponentCode"]]
        r["games"] += 1
        r["gf"] += g["teamScore"]
        r["ga"] += g["opponentScore"]
        if g["won"]:
            r["wins"] += 1
            r["trend"].append(1)
        else:
            r["losses"] += 1
            r["trend"].append(-1)

    items = []
    for opp_code, r in rows.items():
        items.append(
            {
                "opponentCode": opp_code,
                "opponentName": teams.get(opp_code, {}).get("name", opp_code),
                "games": r["games"],
                "wins": r["wins"],
                "losses": r["losses"],
                "otWins": 0,
                "otLosses": 0,
                "gf": r["gf"],
                "ga": r["ga"],
                "goalDiff": r["gf"] - r["ga"],
                "scorePct": round(r["wins"] / max(r["games"], 1), 3),
                "trend": r["trend"][-10:],
            }
        )

    items.sort(key=lambda x: safe_str(x["opponentName"]))
    return {
        "sport": "mlb",
        "teamCode": team_code,
        "teamName": teams.get(team_code, {}).get("name", team_code),
        "rows": items,
    }


# ---------------- unified routes ----------------

def board_for_sport(sport: str) -> dict[str, Any]:
    return nhl_board() if sport == "nhl" else mlb_board()


def insights_for_sport(sport: str) -> dict[str, Any]:
    return nhl_insights() if sport == "nhl" else mlb_insights()


def teams_for_sport(sport: str) -> list[dict[str, str]]:
    return nhl_team_list() if sport == "nhl" else mlb_team_list()


def analyze_for_sport(sport: str, team_code: str) -> dict[str, Any]:
    return nhl_team_analyze(team_code) if sport == "nhl" else mlb_team_analyze(team_code)


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/board/<sport>")
def api_board(sport: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(board_for_sport(sport))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/insights/<sport>")
def api_insights(sport: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(insights_for_sport(sport))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/teams/<sport>")
def api_teams(sport: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify({"sport": sport, "teams": teams_for_sport(sport)})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/api/team-analyze/<sport>/<team_code>")
def api_team_analyze(sport: str, team_code: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(analyze_for_sport(sport, team_code.upper()))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
