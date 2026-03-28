from __future__ import annotations

import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
TIMEOUT = 20
CACHE_TTL = 300
MAX_WORKERS = 10

session = requests.Session()
session.headers.update({"User-Agent": "NHL-Matchup-Predictor/2.0"})

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



def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None



def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(f"{value}T00:00:00+00:00")
    except ValueError:
        return None


@lru_cache(maxsize=1)
def standings_map() -> dict[str, dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/standings/now", ttl=900)
    rows: dict[str, dict[str, Any]] = {}
    for row in data.get("standings", []):
        abbr = row.get("teamAbbrev", {}).get("default")
        if abbr:
            rows[abbr] = row
    return rows


@lru_cache(maxsize=1)
def team_name_map() -> dict[str, str]:
    rows = standings_map()
    return {abbr: row.get("teamName", {}).get("default", abbr) for abbr, row in rows.items()}


@lru_cache(maxsize=1)
def get_teams() -> list[dict[str, str]]:
    items = []
    for abbr, row in standings_map().items():
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



def current_season_code() -> str:
    now = datetime.now(timezone.utc)
    start_year = now.year if now.month >= 7 else now.year - 1
    return f"{start_year}{start_year + 1}"



def parse_schedule_game(game: dict[str, Any], team_code: str | None = None) -> dict[str, Any]:
    away = game.get("awayTeam", {})
    home = game.get("homeTeam", {})
    away_code = away.get("abbrev")
    home_code = home.get("abbrev")
    is_home = bool(team_code and home_code == team_code)
    team_side = home if is_home else away if team_code else None
    opp_side = away if is_home else home if team_code else None
    completed = game.get("gameState") == "OFF"

    payload = {
        "id": game.get("id"),
        "gameType": game.get("gameType"),
        "season": game.get("season"),
        "gameDate": game.get("gameDate"),
        "startTimeUTC": game.get("startTimeUTC"),
        "venue": game.get("venue", {}).get("default"),
        "status": game.get("gameState"),
        "gameScheduleState": game.get("gameScheduleState"),
        "completed": completed,
        "awayCode": away_code,
        "homeCode": home_code,
        "awayName": team_name_map().get(away_code or "", away_code or "Away"),
        "homeName": team_name_map().get(home_code or "", home_code or "Home"),
        "awayScore": int(away.get("score") or 0),
        "homeScore": int(home.get("score") or 0),
    }

    if team_code:
        team_score = int(team_side.get("score") or 0) if team_side else 0
        opp_score = int(opp_side.get("score") or 0) if opp_side else 0
        payload.update(
            {
                "isHome": is_home,
                "teamCode": team_code,
                "opponentCode": opp_side.get("abbrev") if opp_side else None,
                "opponentName": team_name_map().get(opp_side.get("abbrev", ""), opp_side.get("abbrev", "Opponent")) if opp_side else "Opponent",
                "teamScore": team_score,
                "opponentScore": opp_score,
                "goalDiff": team_score - opp_score,
                "won": completed and team_score > opp_score,
            }
        )
    return payload



def get_team_schedule(team_code: str) -> list[dict[str, Any]]:
    team_code = team_code.upper()
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/now", ttl=600)
    games = data.get("games") or []
    parsed = [parse_schedule_game(g, team_code) for g in games]
    parsed.sort(key=lambda g: (g.get("gameDate") or "", g.get("startTimeUTC") or ""))
    return parsed



def get_today_schedule() -> list[dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/schedule/now", ttl=180)
    game_week = data.get("gameWeek") or []
    if not game_week:
        return []
    today = game_week[0]
    games = today.get("games") or []
    parsed = [parse_schedule_game(g) for g in games]
    parsed.sort(key=lambda g: g.get("startTimeUTC") or "")
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

    away = box.get("awayTeam", {})
    home = box.get("homeTeam", {})
    side = home if home.get("abbrev") == team_code else away if away.get("abbrev") == team_code else None
    if not side:
        return []

    leaders = []
    for player in _players_from_side(side):
        goals = int(player.get("goals") or 0)
        assists = int(player.get("assists") or 0)
        points = goals + assists
        if points <= 0:
            continue
        first = player.get("firstName", {}).get("default", "")
        last = player.get("lastName", {}).get("default", "")
        leaders.append(
            {
                "name": f"{first} {last}".strip() or player.get("name", {}).get("default", "Unknown"),
                "goals": goals,
                "assists": assists,
                "points": points,
                "position": player.get("position") or player.get("positionCode"),
            }
        )

    leaders.sort(key=lambda x: (-x["points"], -x["goals"], x["name"]))
    return leaders[:3]



def enrich_completed_games(team_code: str, games: list[dict[str, Any]], limit: int = 8) -> list[dict[str, Any]]:
    completed = [g for g in games if g["completed"]]
    completed = sorted(completed, key=lambda g: g.get("gameDate") or "", reverse=True)[:limit]
    if not completed:
        return []

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, max(1, len(completed)))) as pool:
        futures = {pool.submit(get_team_points_leaders_for_game, g["id"], team_code): g for g in completed}
        for fut in as_completed(futures):
            game = futures[fut]
            item = dict(game)
            try:
                item["pointsLeaders"] = fut.result()
            except Exception:
                item["pointsLeaders"] = []
            results.append(item)

    by_id = {g["id"]: g for g in results}
    return [by_id[g["id"]] for g in completed if g["id"] in by_id]



def recent_form_metrics(games: list[dict[str, Any]], n: int = 5) -> dict[str, float]:
    completed = [g for g in games if g["completed"]]
    completed = sorted(completed, key=lambda g: g.get("gameDate") or "", reverse=True)[:n]
    if not completed:
        return {"win_pct": 0.5, "avg_goal_diff": 0.0, "avg_goals_for": 0.0, "avg_goals_against": 0.0}

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
    }



def split_form_metrics(games: list[dict[str, Any]], is_home: bool) -> dict[str, float]:
    split_games = [g for g in games if g["completed"] and g.get("isHome") == is_home]
    if not split_games:
        return {"win_pct": 0.5, "avg_goal_diff": 0.0}
    wins = sum(1 for g in split_games if g["won"])
    count = len(split_games)
    gd = sum(g["goalDiff"] for g in split_games)
    return {"win_pct": wins / count, "avg_goal_diff": gd / count}



def head_to_head_metrics(team_code: str, opponent_code: str, games: list[dict[str, Any]], n: int = 4) -> dict[str, float]:
    h2h = [g for g in games if g["completed"] and g["opponentCode"] == opponent_code]
    h2h = sorted(h2h, key=lambda g: g.get("gameDate") or "", reverse=True)[:n]
    if not h2h:
        return {"win_pct": 0.5, "avg_goal_diff": 0.0, "games": 0}
    wins = sum(1 for g in h2h if g["won"])
    gd = sum(g["goalDiff"] for g in h2h)
    return {"win_pct": wins / len(h2h), "avg_goal_diff": gd / len(h2h), "games": len(h2h)}



def standings_strength(team_code: str) -> dict[str, float]:
    row = standings_map().get(team_code, {})
    points_pct = _safe_num(row.get("pointPctg"), 0.5)
    gfpg = _safe_num(row.get("goalForPerGame"), 3.0)
    gapg = _safe_num(row.get("goalAgainstPerGame"), 3.0)
    return {
        "points_pct": points_pct,
        "goals_for_per_game": gfpg,
        "goals_against_per_game": gapg,
        "goal_diff_per_game": gfpg - gapg,
    }



def days_rest_before_game(games: list[dict[str, Any]], next_game: dict[str, Any]) -> float:
    upcoming_dt = _parse_iso_utc(next_game.get("startTimeUTC")) or _parse_date(next_game.get("gameDate"))
    if not upcoming_dt:
        return 0.0
    completed = [g for g in games if g["completed"]]
    completed = sorted(completed, key=lambda g: g.get("startTimeUTC") or g.get("gameDate") or "", reverse=True)
    if not completed:
        return 7.0
    last_dt = _parse_iso_utc(completed[0].get("startTimeUTC")) or _parse_date(completed[0].get("gameDate"))
    if not last_dt:
        return 0.0
    delta = upcoming_dt - last_dt
    return max(0.0, delta.total_seconds() / 86400.0)



def logistic(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))



def build_prediction(team_code: str, schedule: list[dict[str, Any]], opponent_code: str | None = None) -> dict[str, Any] | None:
    upcoming = [g for g in schedule if not g["completed"]]
    if opponent_code:
        upcoming = [g for g in upcoming if g["opponentCode"] == opponent_code]
    if not upcoming:
        return None

    game = sorted(upcoming, key=lambda g: (g.get("gameDate") or "", g.get("startTimeUTC") or ""))[0]
    opp = game["opponentCode"]
    opp_schedule = get_team_schedule(opp)

    team_form = recent_form_metrics(schedule, n=5)
    opp_form = recent_form_metrics(opp_schedule, n=5)
    team_h2h = head_to_head_metrics(team_code, opp, schedule, n=4)
    team_strength = standings_strength(team_code)
    opp_strength = standings_strength(opp)
    team_split = split_form_metrics(schedule, is_home=game["isHome"])
    opp_split = split_form_metrics(opp_schedule, is_home=not game["isHome"])
    team_rest = days_rest_before_game(schedule, game)
    opp_rest = days_rest_before_game(opp_schedule, game)

    score = 0.0
    score += (team_strength["points_pct"] - opp_strength["points_pct"]) * 3.4
    score += (team_strength["goal_diff_per_game"] - opp_strength["goal_diff_per_game"]) * 0.7
    score += (team_form["win_pct"] - opp_form["win_pct"]) * 1.7
    score += (team_form["avg_goal_diff"] - opp_form["avg_goal_diff"]) * 0.55
    score += (team_form["avg_goals_for"] - opp_form["avg_goals_for"]) * 0.22
    score += (opp_form["avg_goals_against"] - team_form["avg_goals_against"]) * 0.18
    score += (team_split["win_pct"] - opp_split["win_pct"]) * 0.75
    score += (team_split["avg_goal_diff"] - opp_split["avg_goal_diff"]) * 0.25
    score += (team_h2h["win_pct"] - 0.5) * 0.8
    score += team_h2h["avg_goal_diff"] * 0.18
    score += max(-1.0, min(1.0, (team_rest - opp_rest) / 2.0)) * 0.32

    if game["isHome"]:
        score += 0.18
    if team_rest < 1.05:
        score -= 0.18
    if opp_rest < 1.05:
        score += 0.18

    probability = logistic(score)
    predicted_winner = team_code if probability >= 0.5 else opp
    predicted_loser = opp if predicted_winner == team_code else team_code
    confidence = min(99.0, abs(probability - 0.5) * 220)

    reasons = []
    if team_strength["points_pct"] > opp_strength["points_pct"]:
        reasons.append(f"better points % ({team_strength['points_pct']:.3f} vs {opp_strength['points_pct']:.3f})")
    else:
        reasons.append(f"worse points % ({team_strength['points_pct']:.3f} vs {opp_strength['points_pct']:.3f})")

    if team_form["avg_goal_diff"] > opp_form["avg_goal_diff"]:
        reasons.append("better recent goal differential")
    else:
        reasons.append("worse recent goal differential")

    if team_split["win_pct"] > opp_split["win_pct"]:
        reasons.append("stronger home/road split")
    else:
        reasons.append("weaker home/road split")

    if team_h2h["games"]:
        reasons.append(f"head-to-head sample: {team_h2h['games']} game(s)")

    if game["isHome"]:
        reasons.append("home ice")
    if abs(team_rest - opp_rest) >= 0.9:
        reasons.append("rest advantage" if team_rest > opp_rest else "rest disadvantage")

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
            "teamRestDays": round(team_rest, 2),
            "opponentRestDays": round(opp_rest, 2),
        },
    }



def build_cross_reference(schedule: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for game in schedule:
        opp = game.get("opponentCode")
        if not opp:
            continue
        row = rows.setdefault(
            opp,
            {
                "opponentCode": opp,
                "opponentName": game.get("opponentName") or team_name_map().get(opp, opp),
                "completed": 0,
                "wins": 0,
                "losses": 0,
                "goalsFor": 0,
                "goalsAgainst": 0,
                "goalDiff": 0,
                "upcoming": 0,
                "lastPlayed": None,
                "nextMeeting": None,
            },
        )
        if game["completed"]:
            row["completed"] += 1
            row["wins"] += 1 if game["won"] else 0
            row["losses"] += 0 if game["won"] else 1
            row["goalsFor"] += game["teamScore"]
            row["goalsAgainst"] += game["opponentScore"]
            row["goalDiff"] += game["goalDiff"]
            row["lastPlayed"] = max(row["lastPlayed"] or "", game.get("gameDate") or "")
        else:
            row["upcoming"] += 1
            next_date = game.get("gameDate") or ""
            if not row["nextMeeting"] or next_date < row["nextMeeting"]:
                row["nextMeeting"] = next_date

    results = list(rows.values())
    results.sort(key=lambda x: (-(x["completed"] + x["upcoming"]), x["opponentCode"]))
    return results



def team_payload(team_code: str, opponent_code: str | None = None) -> dict[str, Any]:
    code = team_code.upper()
    rows = standings_map()
    if code not in rows:
        return {"error": f"Unknown team code: {code}"}

    schedule = get_team_schedule(code)
    completed = [g for g in schedule if g["completed"]]
    upcoming = [g for g in schedule if not g["completed"]]
    opponents = sorted({g["opponentCode"] for g in schedule if g.get("opponentCode")})
    cross_reference = build_cross_reference(schedule)

    if opponent_code:
        opponent_code = opponent_code.upper()
        filtered_schedule = [g for g in schedule if g["opponentCode"] == opponent_code]
        filtered_cross_reference = [r for r in cross_reference if r["opponentCode"] == opponent_code]
    else:
        filtered_schedule = schedule
        filtered_cross_reference = cross_reference

    detail_games = enrich_completed_games(code, filtered_schedule, limit=8)
    next_games = sorted([g for g in filtered_schedule if not g["completed"]], key=lambda g: (g.get("gameDate") or "", g.get("startTimeUTC") or ""))[:6]
    prediction = build_prediction(code, schedule, opponent_code=opponent_code)

    standing = rows[code]
    return {
        "team": standing.get("teamName", {}).get("default", code),
        "code": code,
        "logo": standing.get("teamLogo"),
        "standing": {
            "leagueSequence": standing.get("leagueSequence"),
            "conferenceSequence": standing.get("conferenceSequence"),
            "divisionSequence": standing.get("divisionSequence"),
            "points": standing.get("points"),
            "wins": standing.get("wins"),
            "losses": standing.get("losses"),
            "otLosses": standing.get("otLosses"),
            "pointPctg": standing.get("pointPctg"),
            "goalForPerGame": standing.get("goalForPerGame"),
            "goalAgainstPerGame": standing.get("goalAgainstPerGame"),
        },
        "totals": {
            "gamesPlayed": len(schedule),
            "completed": len(completed),
            "upcoming": len(upcoming),
        },
        "crossReferenceOptions": opponents,
        "crossReference": filtered_cross_reference,
        "recentGames": detail_games,
        "upcomingGames": next_games,
        "prediction": prediction,
    }



def all_team_predictions() -> list[dict[str, Any]]:
    teams = get_teams()
    results = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(team_payload, t["code"]): t["code"] for t in teams}
        for fut in as_completed(futures):
            code = futures[fut]
            try:
                payload = fut.result()
                pred = payload.get("prediction")
                if pred:
                    results.append(
                        {
                            "team": payload["team"],
                            "code": code,
                            "logo": payload.get("logo"),
                            "nextGame": pred["game"],
                            "predictedWinner": pred["predictedWinner"],
                            "teamWinProbability": pred["teamWinProbability"],
                            "confidence": pred["confidence"],
                            "opponent": pred["opponent"],
                        }
                    )
            except Exception:
                continue
    results.sort(key=lambda x: (x["nextGame"].get("gameDate") or "", -x["confidence"]))
    return results


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/api/teams")
def api_teams() -> Any:
    return jsonify({"teams": get_teams(), "season": current_season_code()})


@app.get("/api/today")
def api_today() -> Any:
    return jsonify({"games": get_today_schedule()})


@app.get("/api/team/<team_code>")
def api_team(team_code: str) -> Any:
    opponent = request.args.get("opponent", "").strip() or None
    payload = team_payload(team_code, opponent)
    status = 400 if payload.get("error") else 200
    return jsonify(payload), status


@app.get("/api/predictions")
def api_predictions() -> Any:
    return jsonify({"predictions": all_team_predictions()})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
