from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
TIMEOUT = 20
CACHE_TTL = 300

session = requests.Session()
session.headers.update({"User-Agent": "NHL-Matchup-Predictor/1.1"})

_cache: dict[str, tuple[float, Any]] = {}


def ttl_cache_get(key: str) -> Any | None:
    hit = _cache.get(key)
    if not hit:
        return None
    expires_at, value = hit
    if time.time() >= expires_at:
        _cache.pop(key, None)
        return None
    return value


def ttl_cache_set(key: str, value: Any, ttl: int = CACHE_TTL) -> Any:
    _cache[key] = (time.time() + ttl, value)
    return value


def fetch_json(url: str, ttl: int = CACHE_TTL) -> Any:
    cached = ttl_cache_get(url)
    if cached is not None:
        return cached
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return ttl_cache_set(url, response.json(), ttl=ttl)


def _safe_num(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def current_season_code() -> str:
    now = datetime.now(timezone.utc)
    year = now.year
    start_year = year if now.month >= 7 else year - 1
    return f"{start_year}{start_year + 1}"


def today_nhl_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def standings_map() -> dict[str, dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/standings/now", ttl=900)
    rows: dict[str, dict[str, Any]] = {}
    for row in data.get("standings", []):
        abbr = row.get("teamAbbrev", {}).get("default")
        if abbr:
            rows[abbr] = row
    return rows


def team_name_map() -> dict[str, str]:
    rows = standings_map()
    return {
        abbr: row.get("teamName", {}).get("default", abbr)
        for abbr, row in rows.items()
    }


def get_teams() -> list[dict[str, str]]:
    rows = standings_map()
    items = []
    for abbr, row in rows.items():
        items.append(
            {
                "code": abbr,
                "name": row.get("teamName", {}).get("default", abbr),
                "logo": row.get("teamLogo"),
                "darkLogo": row.get("teamLogoDark"),
                "place": row.get("teamCommonName", {}).get("default", ""),
            }
        )
    items.sort(key=lambda x: x["name"])
    return items


def parse_schedule_game(game: dict[str, Any], team_code: str) -> dict[str, Any]:
    away = game.get("awayTeam", {}) or {}
    home = game.get("homeTeam", {}) or {}
    away_code = away.get("abbrev")
    home_code = home.get("abbrev")
    is_home = home_code == team_code

    team_side = home if is_home else away
    opp_side = away if is_home else home

    team_score = int(team_side.get("score") or 0)
    opp_score = int(opp_side.get("score") or 0)

    game_state = str(game.get("gameState") or "")
    completed = game_state == "OFF"

    return {
        "id": game.get("id"),
        "gameType": game.get("gameType"),
        "season": game.get("season"),
        "gameDate": game.get("gameDate"),
        "startTimeUTC": game.get("startTimeUTC"),
        "venue": (game.get("venue") or {}).get("default", ""),
        "isHome": is_home,
        "teamCode": team_code,
        "opponentCode": opp_side.get("abbrev"),
        "opponentName": team_name_map().get(opp_side.get("abbrev", ""), opp_side.get("abbrev", "Opponent")),
        "teamScore": team_score,
        "opponentScore": opp_score,
        "goalDiff": team_score - opp_score,
        "status": game_state,
        "gameScheduleState": game.get("gameScheduleState"),
        "completed": completed,
        "won": completed and team_score > opp_score,
        "broadcasts": game.get("tvBroadcasts", []) or [],
    }


def get_team_schedule(team_code: str) -> list[dict[str, Any]]:
    season = current_season_code()
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/{season}", ttl=600)
    games = data.get("games") or []
    parsed = [parse_schedule_game(g, team_code) for g in games]
    parsed.sort(key=lambda g: ((g.get("gameDate") or ""), (g.get("startTimeUTC") or "")))
    return parsed


def get_boxscore(game_id: int) -> dict[str, Any]:
    return fetch_json(f"{NHL_API}/gamecenter/{game_id}/boxscore", ttl=3600)


def _players_from_side(side: dict[str, Any]) -> list[dict[str, Any]]:
    players: list[dict[str, Any]] = []
    for bucket in ("forwards", "defense", "goalies"):
        players.extend(side.get(bucket, []) or [])
    return players


def get_team_points_leaders_for_game(game_id: int, team_code: str) -> list[dict[str, Any]]:
    try:
        box = get_boxscore(game_id)
    except requests.RequestException:
        return []

    away = box.get("awayTeam", {}) or {}
    home = box.get("homeTeam", {}) or {}
    away_code = away.get("abbrev")
    home_code = home.get("abbrev")
    side = home if home_code == team_code else away if away_code == team_code else None

    if not side:
        return []

    leaders = []
    for p in _players_from_side(side):
        goals = int(p.get("goals") or 0)
        assists = int(p.get("assists") or 0)
        points = goals + assists
        if points <= 0:
            continue
        first = (p.get("firstName") or {}).get("default", "")
        last = (p.get("lastName") or {}).get("default", "")
        leaders.append(
            {
                "name": f"{first} {last}".strip() or (p.get("name") or {}).get("default", "Unknown"),
                "goals": goals,
                "assists": assists,
                "points": points,
                "position": p.get("position") or p.get("positionCode"),
            }
        )

    leaders.sort(key=lambda x: (-x["points"], -x["goals"], x["name"]))
    return leaders[:3]


def enrich_completed_games(team_code: str, games: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    completed = [g for g in games if g["completed"]]
    completed = sorted(completed, key=lambda g: g.get("gameDate") or "", reverse=True)[:limit]

    results = []
    for game in completed:
        item = dict(game)
        try:
            item["pointsLeaders"] = get_team_points_leaders_for_game(game["id"], team_code)
        except Exception:
            item["pointsLeaders"] = []
        results.append(item)
    return results


def recent_form_metrics(games: list[dict[str, Any]], n: int = 5) -> dict[str, float]:
    completed = [g for g in games if g["completed"]]
    completed = sorted(completed, key=lambda g: g.get("gameDate") or "", reverse=True)[:n]

    if not completed:
        return {
            "win_pct": 0.5,
            "avg_goal_diff": 0.0,
            "avg_goals_for": 0.0,
            "avg_goals_against": 0.0,
            "games": 0,
        }

    wins = sum(1 for g in completed if g["won"])
    goal_diff = sum(g["goalDiff"] for g in completed)
    goals_for = sum(g["teamScore"] for g in completed)
    goals_against = sum(g["opponentScore"] for g in completed)
    count = len(completed)

    return {
        "win_pct": wins / count,
        "avg_goal_diff": goal_diff / count,
        "avg_goals_for": goals_for / count,
        "avg_goals_against": goals_against / count,
        "games": count,
    }


def home_away_form_metrics(games: list[dict[str, Any]], is_home: bool, n: int = 5) -> dict[str, float]:
    filtered = [g for g in games if g["completed"] and g["isHome"] == is_home]
    filtered = sorted(filtered, key=lambda g: g.get("gameDate") or "", reverse=True)[:n]

    if not filtered:
        return {"win_pct": 0.5, "avg_goal_diff": 0.0, "games": 0}

    wins = sum(1 for g in filtered if g["won"])
    goal_diff = sum(g["goalDiff"] for g in filtered)
    count = len(filtered)

    return {
        "win_pct": wins / count,
        "avg_goal_diff": goal_diff / count,
        "games": count,
    }


def head_to_head_metrics(team_code: str, opponent_code: str, games: list[dict[str, Any]], n: int = 3) -> dict[str, float]:
    h2h = [g for g in games if g["completed"] and g["opponentCode"] == opponent_code]
    h2h = sorted(h2h, key=lambda g: g.get("gameDate") or "", reverse=True)[:n]

    if not h2h:
        return {"win_pct": 0.5, "avg_goal_diff": 0.0, "games": 0}

    wins = sum(1 for g in h2h if g["won"])
    gd = sum(g["goalDiff"] for g in h2h)

    return {
        "win_pct": wins / len(h2h),
        "avg_goal_diff": gd / len(h2h),
        "games": len(h2h),
    }


def rest_days_before_game(schedule: list[dict[str, Any]], game: dict[str, Any]) -> int:
    completed = [g for g in schedule if g["completed"] and (g.get("gameDate") or "") < (game.get("gameDate") or "")]
    if not completed:
        return 7

    last_game = sorted(completed, key=lambda g: g.get("gameDate") or "", reverse=True)[0]
    try:
        d1 = datetime.fromisoformat(str(last_game["gameDate"]))
        d2 = datetime.fromisoformat(str(game["gameDate"]))
        days = (d2 - d1).days - 1
        return max(-1, min(days, 7))
    except Exception:
        return 1


def standings_strength(team_code: str) -> dict[str, float]:
    row = standings_map().get(team_code, {})
    points_pct = _safe_num(row.get("pointPctg"), 0.5)
    gfpg = _safe_num(row.get("goalForPerGame"), 3.0)
    gapg = _safe_num(row.get("goalAgainstPerGame"), 3.0)
    wins = _safe_num(row.get("wins"), 0.0)
    losses = _safe_num(row.get("losses"), 0.0)
    ot_losses = _safe_num(row.get("otLosses"), 0.0)

    return {
        "points_pct": points_pct,
        "goal_diff_per_game": gfpg - gapg,
        "gfpg": gfpg,
        "gapg": gapg,
        "wins": wins,
        "losses": losses,
        "ot_losses": ot_losses,
    }


def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def build_prediction(team_code: str, schedule: list[dict[str, Any]], opponent_code: str | None = None) -> dict[str, Any] | None:
    upcoming = [g for g in schedule if not g["completed"]]
    if opponent_code:
        upcoming = [g for g in upcoming if g["opponentCode"] == opponent_code]
    if not upcoming:
        return None

    game = sorted(upcoming, key=lambda g: ((g.get("gameDate") or ""), (g.get("startTimeUTC") or "")))[0]
    opp = game["opponentCode"]
    if not opp:
        return None

    opp_schedule = get_team_schedule(opp)

    team_form = recent_form_metrics(schedule, n=5)
    opp_form = recent_form_metrics(opp_schedule, n=5)

    team_split = home_away_form_metrics(schedule, is_home=game["isHome"], n=5)
    opp_split = home_away_form_metrics(opp_schedule, is_home=not game["isHome"], n=5)

    team_h2h = head_to_head_metrics(team_code, opp, schedule, n=3)
    team_strength = standings_strength(team_code)
    opp_strength = standings_strength(opp)

    team_rest = rest_days_before_game(schedule, game)
    opp_rest = rest_days_before_game(opp_schedule, game)

    score = 0.0
    score += (team_strength["points_pct"] - opp_strength["points_pct"]) * 3.2
    score += (team_strength["goal_diff_per_game"] - opp_strength["goal_diff_per_game"]) * 0.9
    score += (team_form["win_pct"] - opp_form["win_pct"]) * 1.5
    score += (team_form["avg_goal_diff"] - opp_form["avg_goal_diff"]) * 0.45
    score += (team_split["win_pct"] - opp_split["win_pct"]) * 0.8
    score += (team_split["avg_goal_diff"] - opp_split["avg_goal_diff"]) * 0.2
    score += (team_h2h["win_pct"] - 0.5) * 0.8
    score += team_h2h["avg_goal_diff"] * 0.15
    score += (team_rest - opp_rest) * 0.08

    if game["isHome"]:
        score += 0.18

    probability = logistic(score)
    predicted_winner = team_code if probability >= 0.5 else opp
    predicted_loser = opp if predicted_winner == team_code else team_code
    confidence = abs(probability - 0.5) * 200

    reasons = []
    if team_strength["points_pct"] > opp_strength["points_pct"]:
        reasons.append(f"better standings points % ({team_strength['points_pct']:.3f} vs {opp_strength['points_pct']:.3f})")
    else:
        reasons.append(f"worse standings points % ({team_strength['points_pct']:.3f} vs {opp_strength['points_pct']:.3f})")

    if team_form["avg_goal_diff"] > opp_form["avg_goal_diff"]:
        reasons.append("better recent goal differential")
    else:
        reasons.append("worse recent goal differential")

    if team_split["win_pct"] > opp_split["win_pct"]:
        reasons.append("better current home/road split form")
    else:
        reasons.append("worse current home/road split form")

    if team_rest > opp_rest:
        reasons.append("more rest before game")
    elif team_rest < opp_rest:
        reasons.append("less rest before game")

    if game["isHome"]:
        reasons.append("home ice")

    return {
        "game": game,
        "team": team_code,
        "opponent": opp,
        "predictedWinner": predicted_winner,
        "predictedLoser": predicted_loser,
        "teamWinProbability": round(probability * 100, 1),
        "opponentWinProbability": round((1 - probability) * 100, 1),
        "confidence": round(confidence, 1),
        "reasonSummary": reasons,
        "inputs": {
            "teamForm": team_form,
            "opponentForm": opp_form,
            "headToHead": team_h2h,
            "teamStrength": team_strength,
            "opponentStrength": opp_strength,
            "teamSplit": team_split,
            "opponentSplit": opp_split,
            "teamRestDays": team_rest,
            "opponentRestDays": opp_rest,
        },
    }


def team_payload(team_code: str, opponent_code: str | None = None) -> dict[str, Any]:
    code = team_code.upper()
    rows = standings_map()

    if code not in rows:
        return {"error": f"Unknown team code: {code}"}

    schedule = get_team_schedule(code)
    completed = [g for g in schedule if g["completed"]]
    upcoming = [g for g in schedule if not g["completed"]]
    opponents = sorted({g["opponentCode"] for g in schedule if g.get("opponentCode")})

    filtered_schedule = schedule
    if opponent_code:
        opponent_code = opponent_code.upper()
        filtered_schedule = [g for g in schedule if g["opponentCode"] == opponent_code]

    detail_games = enrich_completed_games(code, filtered_schedule, limit=8)
    next_games = sorted(
        [g for g in filtered_schedule if not g["completed"]],
        key=lambda g: ((g.get("gameDate") or ""), (g.get("startTimeUTC") or ""))
    )[:5]

    prediction = build_prediction(code, schedule, opponent_code=opponent_code)

    row = rows[code]
    return {
        "team": row.get("teamName", {}).get("default", code),
        "code": code,
        "logo": row.get("teamLogo"),
        "standing": {
            "leagueSequence": row.get("leagueSequence"),
            "conferenceSequence": row.get("conferenceSequence"),
            "divisionSequence": row.get("divisionSequence"),
            "points": row.get("points"),
            "wins": row.get("wins"),
            "losses": row.get("losses"),
            "otLosses": row.get("otLosses"),
            "pointPctg": row.get("pointPctg"),
            "goalForPerGame": row.get("goalForPerGame"),
            "goalAgainstPerGame": row.get("goalAgainstPerGame"),
        },
        "totals": {
            "gamesPlayed": len(schedule),
            "completed": len(completed),
            "upcoming": len(upcoming),
        },
        "crossReferenceOptions": opponents,
        "recentGames": detail_games,
        "upcomingGames": next_games,
        "prediction": prediction,
    }


def parse_today_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game.get("awayTeam", {}) or {}
    home = game.get("homeTeam", {}) or {}
    return {
        "id": game.get("id"),
        "gameDate": game.get("gameDate"),
        "startTimeUTC": game.get("startTimeUTC"),
        "gameState": game.get("gameState"),
        "awayCode": away.get("abbrev"),
        "homeCode": home.get("abbrev"),
        "awayName": team_name_map().get(away.get("abbrev", ""), away.get("abbrev", "Away")),
        "homeName": team_name_map().get(home.get("abbrev", ""), home.get("abbrev", "Home")),
        "awayScore": int(away.get("score") or 0),
        "homeScore": int(home.get("score") or 0),
        "venue": (game.get("venue") or {}).get("default", ""),
    }


def get_today_games() -> list[dict[str, Any]]:
    date_code = today_nhl_date()
    data = fetch_json(f"{NHL_API}/schedule/{date_code}", ttl=120)
    games = []
    game_week = data.get("gameWeek") or []
    for day in game_week:
        if day.get("date") == date_code:
            for game in day.get("games") or []:
                games.append(parse_today_game(game))
            break

    games.sort(key=lambda g: (g.get("startTimeUTC") or ""))
    return games


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/health")
def api_health() -> Any:
    return jsonify({"ok": True, "time": datetime.now(timezone.utc).isoformat()})


@app.get("/api/teams")
def api_teams() -> Any:
    try:
        return jsonify({"teams": get_teams(), "season": current_season_code()})
    except Exception as exc:
        return jsonify({"error": str(exc), "teams": [], "season": current_season_code()}), 500


@app.get("/api/team/<team_code>")
def api_team(team_code: str) -> Any:
    opponent = request.args.get("opponent", "").strip() or None
    try:
        payload = team_payload(team_code, opponent)
        status = 400 if payload.get("error") else 200
        return jsonify(payload), status
    except Exception as exc:
        return jsonify({"error": f"Could not load team data: {exc}"}), 500


@app.get("/api/predictions")
def api_predictions() -> Any:
    try:
        teams = get_teams()
        results = []

        for t in teams:
            try:
                payload = team_payload(t["code"])
                pred = payload.get("prediction")
                if not pred:
                    continue
                results.append(
                    {
                        "team": payload["team"],
                        "code": t["code"],
                        "logo": payload.get("logo"),
                        "nextGame": pred["game"],
                        "predictedWinner": pred["predictedWinner"],
                        "teamWinProbability": pred["teamWinProbability"],
                        "confidence": pred["confidence"],
                    }
                )
            except Exception:
                continue

        results.sort(key=lambda x: ((x["nextGame"].get("gameDate") or ""), -x["confidence"]))
        return jsonify({"predictions": results[:16]})
    except Exception as exc:
        return jsonify({"error": str(exc), "predictions": []}), 500


@app.get("/api/today")
def api_today() -> Any:
    try:
        return jsonify({"date": today_nhl_date(), "games": get_today_games()})
    except Exception as exc:
        return jsonify({"error": str(exc), "date": today_nhl_date(), "games": []}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
