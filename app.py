from __future__ import annotations

import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
MLB_API = "https://statsapi.mlb.com/api/v1"
TIMEOUT = 20

BOARD_TTL = 300
PRED_TTL = 600
ANALYZE_TTL = 3600

session = requests.Session()
session.headers.update({"User-Agent": "Game-Insights/3.7"})

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


def date_plus_utc_str(days: int) -> str:
    return (utc_now() + timedelta(days=days)).strftime("%Y-%m-%d")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
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
        abbr = safe_str((row.get("teamAbbrev") or {}).get("default"))
        if abbr:
            rows[abbr] = row
    return rows


def nhl_team_name_map() -> dict[str, str]:
    rows = nhl_standings_map()
    return {
        abbr: safe_str((row.get("teamName") or {}).get("default"), abbr)
        for abbr, row in rows.items()
    }


def nhl_team_list() -> list[dict[str, str]]:
    rows = nhl_standings_map()
    items: list[dict[str, str]] = []
    for abbr, row in rows.items():
        items.append(
            {
                "code": abbr,
                "name": safe_str((row.get("teamName") or {}).get("default"), abbr),
            }
        )
    return sorted(items, key=lambda x: safe_str(x["name"]))


def nhl_strength(team_code: str) -> dict[str, float]:
    row = nhl_standings_map().get(team_code, {})
    gf = safe_float(row.get("goalForPerGame"), 3.0)
    ga = safe_float(row.get("goalAgainstPerGame"), 3.0)
    return {
        "points_pct": safe_float(row.get("pointPctg"), 0.5),
        "goal_diff_pg": gf - ga,
        "gf": gf,
        "ga": ga,
    }


def nhl_parse_board_game(raw: dict[str, Any]) -> dict[str, Any]:
    away = raw.get("awayTeam", {}) or {}
    home = raw.get("homeTeam", {}) or {}
    names = nhl_team_name_map()

    away_code = safe_str(away.get("abbrev"))
    home_code = safe_str(home.get("abbrev"))

    return {
        "sport": "nhl",
        "id": raw.get("id"),
        "date": safe_str(raw.get("gameDate")),
        "startTimeUTC": safe_str(raw.get("startTimeUTC")),
        "status": safe_str(raw.get("gameState") or raw.get("gameScheduleState") or "PRE"),
        "venue": safe_str((raw.get("venue") or {}).get("default")),
        "awayCode": away_code,
        "homeCode": home_code,
        "awayName": names.get(away_code, away_code or "Away"),
        "homeName": names.get(home_code, home_code or "Home"),
        "awayScore": safe_int(away.get("score")),
        "homeScore": safe_int(home.get("score")),
    }


def nhl_board() -> dict[str, Any]:
    today = today_utc_str()
    tomorrow = date_plus_utc_str(1)
    data = fetch_json(f"{NHL_API}/schedule/{today}", BOARD_TTL)

    out_days: list[dict[str, Any]] = []
    for target_date, label in ((today, "Today"), (tomorrow, "Tomorrow")):
        games: list[dict[str, Any]] = []
        for day in data.get("gameWeek", []) or []:
            if safe_str(day.get("date")) == target_date:
                games = [nhl_parse_board_game(g) for g in (day.get("games") or [])]
                break
        games.sort(key=lambda g: safe_str(g.get("startTimeUTC")))
        out_days.append({"label": label, "date": target_date, "games": games})

    return {
        "sport": "nhl",
        "updatedUTC": utc_now().isoformat(),
        "days": out_days,
    }


def nhl_player_pick(team_code: str) -> dict[str, Any]:
    return {
        "teamCode": team_code,
        "player": f"{team_code} top point pick",
        "reason": "Any point this game",
    }


def nhl_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]

    away_strength = nhl_strength(away)
    home_strength = nhl_strength(home)

    score = 0.0
    score += (home_strength["points_pct"] - away_strength["points_pct"]) * 3.2
    score += (home_strength["goal_diff_pg"] - away_strength["goal_diff_pg"]) * 1.1
    score += 0.18  # small home edge

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 47 <= home_prob * 100 <= 53 else "edge"

    projected_total = (home_strength["gf"] + away_strength["gf"] + home_strength["ga"] + away_strength["ga"]) / 2.0
    projected_total = max(4.5, min(8.5, projected_total))

    away_player_pick = nhl_player_pick(away)
    home_player_pick = nhl_player_pick(home)

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
                "label": "Season strength",
                "text": (
                    f"{home} points % {home_strength['points_pct']:.3f} vs "
                    f"{away} {away_strength['points_pct']:.3f}."
                ),
            },
            {
                "label": "Goal differential",
                "text": (
                    f"{home} goal diff/game {home_strength['goal_diff_pg']:.2f} vs "
                    f"{away} {away_strength['goal_diff_pg']:.2f}."
                ),
            },
            {
                "label": "Home edge",
                "text": f"{home} gets a small home-ice boost.",
            },
            {
                "label": "Player point picks",
                "text": f"{away}: {away_player_pick['player']} | {home}: {home_player_pick['player']}.",
            },
        ],
    }


def nhl_insights() -> dict[str, Any]:
    board = nhl_board()
    all_games: list[dict[str, Any]] = []
    for day in board["days"]:
        all_games.extend(day["games"])
    insights = [nhl_predict_game(g) for g in all_games]
    return {
        "sport": "nhl",
        "updatedUTC": utc_now().isoformat(),
        "insights": insights,
    }


def nhl_team_analyze(team_code: str) -> dict[str, Any]:
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/now", ANALYZE_TTL)
    names = nhl_team_name_map()
    rows = defaultdict(lambda: {
        "games": 0, "wins": 0, "losses": 0, "otWins": 0, "otLosses": 0,
        "gf": 0, "ga": 0, "trend": []
    })

    for raw in data.get("games", []) or []:
        away = raw.get("awayTeam", {}) or {}
        home = raw.get("homeTeam", {}) or {}
        away_code = safe_str(away.get("abbrev"))
        home_code = safe_str(home.get("abbrev"))
        status = safe_str(raw.get("gameState") or raw.get("gameScheduleState"))
        if status != "OFF":
            continue

        is_home = home_code == team_code
        if not is_home and away_code != team_code:
            continue

        opp_code = away_code if is_home else home_code
        team_score = safe_int((home if is_home else away).get("score"))
        opp_score = safe_int((away if is_home else home).get("score"))
        ot = safe_str(((raw.get("gameOutcome") or {}).get("lastPeriodType"))) in {"OT", "SO"}

        r = rows[opp_code]
        r["games"] += 1
        r["gf"] += team_score
        r["ga"] += opp_score

        if team_score > opp_score:
            if ot:
                r["otWins"] += 1
                r["trend"].append(0.5)
            else:
                r["wins"] += 1
                r["trend"].append(1)
        else:
            if ot:
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
        abbr = safe_str(t.get("abbreviation"))
        if abbr:
            teams[abbr] = {
                "id": t.get("id"),
                "code": abbr,
                "name": safe_str(t.get("name"), abbr),
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
    data = fetch_json(
        f"{MLB_API}/standings?leagueId=103,104&season={season}&standingsTypes=regularSeason",
        BOARD_TTL,
    )
    out: dict[str, dict[str, float]] = {}

    for record in data.get("records", []) or []:
        for tr in record.get("teamRecords", []) or []:
            team = tr.get("team", {}) or {}
            code = safe_str(team.get("abbreviation"))
            wins = safe_float(tr.get("wins"), 0)
            losses = safe_float(tr.get("losses"), 0)
            runs_scored = safe_float(tr.get("runsScored"), 0)
            runs_allowed = safe_float(tr.get("runsAllowed"), 0)
            games = max(wins + losses, 1)

            if code:
                out[code] = {
                    "win_pct": wins / games,
                    "run_diff_pg": (runs_scored - runs_allowed) / games,
                    "rf": runs_scored / games if games else 4.5,
                    "ra": runs_allowed / games if games else 4.5,
                }

    return out


def mlb_schedule_range(start_date: str, end_date: str) -> list[dict[str, Any]]:
    hydrate = "probablePitcher,team,linescore"
    data = fetch_json(
        f"{MLB_API}/schedule?sportId=1&startDate={start_date}&endDate={end_date}&hydrate={hydrate}",
        BOARD_TTL,
    )
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
                    "venue": safe_str((raw.get("venue") or {}).get("name")),
                    "awayCode": safe_str(away_team.get("abbreviation")),
                    "homeCode": safe_str(home_team.get("abbreviation")),
                    "awayName": safe_str(away_team.get("name")),
                    "homeName": safe_str(home_team.get("name")),
                    "awayScore": safe_int(away.get("score")),
                    "homeScore": safe_int(home.get("score")),
                    "awayProbablePitcher": safe_str((away.get("probablePitcher") or {}).get("fullName")),
                    "homeProbablePitcher": safe_str((home.get("probablePitcher") or {}).get("fullName")),
                    "inningState": safe_str(linescore.get("inningState")),
                    "currentInning": safe_str(linescore.get("currentInning")),
                    "completed": safe_str(status.get("abstractGameState")) == "Final",
                }
            )

    return sorted(games, key=lambda x: safe_str(x.get("startTimeUTC")))


def mlb_board() -> dict[str, Any]:
    today = today_utc_str()
    tomorrow = date_plus_utc_str(1)
    all_games = mlb_schedule_range(today, tomorrow)

    return {
        "sport": "mlb",
        "updatedUTC": utc_now().isoformat(),
        "days": [
            {"label": "Today", "date": today, "games": [g for g in all_games if g["date"] == today]},
            {"label": "Tomorrow", "date": tomorrow, "games": [g for g in all_games if g["date"] == tomorrow]},
        ],
    }


def mlb_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    strengths = mlb_standings_strength()
    away = game["awayCode"]
    home = game["homeCode"]

    away_strength = strengths.get(away, {"win_pct": 0.5, "run_diff_pg": 0.0, "rf": 4.5, "ra": 4.5})
    home_strength = strengths.get(home, {"win_pct": 0.5, "run_diff_pg": 0.0, "rf": 4.5, "ra": 4.5})

    score = 0.0
    score += (home_strength["win_pct"] - away_strength["win_pct"]) * 3.0
    score += (home_strength["run_diff_pg"] - away_strength["run_diff_pg"]) * 0.8
    score += 0.16

    if game.get("homeProbablePitcher"):
        score += 0.06
    if game.get("awayProbablePitcher"):
        score -= 0.06

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 46 <= home_prob * 100 <= 54 else "edge"

    projected_total = (home_strength["rf"] + away_strength["rf"] + home_strength["ra"] + away_strength["ra"]) / 2
    projected_total = max(5.5, min(13.5, projected_total))

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
                "label": "Run differential",
                "text": f"{home} run diff/game {home_strength['run_diff_pg']:.2f} vs {away} {away_strength['run_diff_pg']:.2f}.",
            },
            {
                "label": "Pitchers",
                "text": f"Probables: {(game.get('awayProbablePitcher') or 'TBD')} vs {(game.get('homeProbablePitcher') or 'TBD')}.",
            },
        ],
    }


def mlb_insights() -> dict[str, Any]:
    board = mlb_board()
    all_games: list[dict[str, Any]] = []
    for day in board["days"]:
        all_games.extend(day["games"])
    insights = [mlb_predict_game(g) for g in all_games]
    return {
        "sport": "mlb",
        "updatedUTC": utc_now().isoformat(),
        "insights": insights,
    }


def mlb_team_analyze(team_code: str) -> dict[str, Any]:
    teams = mlb_teams_map()
    team_id = teams.get(team_code, {}).get("id")
    if not team_id:
        return {
            "sport": "mlb",
            "teamCode": team_code,
            "teamName": teams.get(team_code, {}).get("name", team_code),
            "rows": [],
        }

    season = current_mlb_season()
    start_date = f"{season}-03-01"
    end_date = f"{season}-11-30"
    data = fetch_json(
        f"{MLB_API}/schedule?sportId=1&teamId={team_id}&startDate={start_date}&endDate={end_date}",
        ANALYZE_TTL,
    )

    rows = defaultdict(lambda: {"games": 0, "wins": 0, "losses": 0, "gf": 0, "ga": 0, "trend": []})

    for date_block in data.get("dates", []) or []:
        for raw in date_block.get("games", []) or []:
            status = safe_str((raw.get("status") or {}).get("abstractGameState"))
            if status != "Final":
                continue

            teams_raw = raw.get("teams", {}) or {}
            away = teams_raw.get("away", {}) or {}
            home = teams_raw.get("home", {}) or {}
            away_team = away.get("team", {}) or {}
            home_team = home.get("team", {}) or {}

            away_code = safe_str(away_team.get("abbreviation"))
            home_code = safe_str(home_team.get("abbreviation"))
            is_home = home_code == team_code
            if not is_home and away_code != team_code:
                continue

            opp_code = away_code if is_home else home_code
            team_score = safe_int((home if is_home else away).get("score"))
            opp_score = safe_int((away if is_home else home).get("score"))

            r = rows[opp_code]
            r["games"] += 1
            r["gf"] += team_score
            r["ga"] += opp_score
            if team_score > opp_score:
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


# ---------------- Shared selectors ----------------

def board_for_sport(sport: str) -> dict[str, Any]:
    return nhl_board() if sport == "nhl" else mlb_board()


def insights_for_sport(sport: str) -> dict[str, Any]:
    return nhl_insights() if sport == "nhl" else mlb_insights()


def teams_for_sport(sport: str) -> list[dict[str, str]]:
    return nhl_team_list() if sport == "nhl" else mlb_team_list()


def analyze_for_sport(sport: str, team_code: str) -> dict[str, Any]:
    return nhl_team_analyze(team_code) if sport == "nhl" else mlb_team_analyze(team_code)


# ---------------- Routes ----------------

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
