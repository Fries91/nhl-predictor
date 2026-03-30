from __future__ import annotations

import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from flask import Flask, jsonify, render_template
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

NHL_API = "https://api-web.nhle.com/v1"
MLB_API = "https://statsapi.mlb.com/api/v1"

BOARD_TTL = 300
PRED_TTL = 900
ANALYZE_TTL = 3600

session = requests.Session()
session.headers.update({"User-Agent": "Game-Insights/4.0"})

retry_strategy = Retry(
    total=3,
    connect=3,
    read=3,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

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

    last_error: Exception | None = None

    for timeout in (12, 20, 30):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return ttl_set(url, data, ttl)
        except Exception as exc:
            last_error = exc
            time.sleep(0.6)

    cached = ttl_get(url)
    if cached is not None:
        return cached

    raise RuntimeError(f"Upstream fetch failed for {url}: {last_error}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def today_utc_str() -> str:
    return utc_now().strftime("%Y-%m-%d")


def date_plus_utc_str(days: int) -> str:
    return (utc_now() + timedelta(days=days)).strftime("%Y-%m-%d")


def current_mlb_season() -> int:
    return utc_now().year


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


def nhl_schedule(team_code: str) -> list[dict[str, Any]]:
    data = fetch_json(f"{NHL_API}/club-schedule-season/{team_code}/now", ANALYZE_TTL)
    items: list[dict[str, Any]] = []

    for raw in data.get("games", []) or []:
        away = raw.get("awayTeam", {}) or {}
        home = raw.get("homeTeam", {}) or {}
        away_code = safe_str(away.get("abbrev"))
        home_code = safe_str(home.get("abbrev"))
        status = safe_str(raw.get("gameState") or raw.get("gameScheduleState"))
        completed = status == "OFF"

        is_home = home_code == team_code
        if not is_home and away_code != team_code:
            continue

        opp_code = away_code if is_home else home_code
        team_score = safe_int((home if is_home else away).get("score"))
        opp_score = safe_int((away if is_home else home).get("score"))
        ot = safe_str(((raw.get("gameOutcome") or {}).get("lastPeriodType"))) in {"OT", "SO"}

        items.append(
            {
                "id": raw.get("id"),
                "date": safe_str(raw.get("gameDate")),
                "completed": completed,
                "is_home": is_home,
                "opponentCode": opp_code,
                "teamScore": team_score,
                "opponentScore": opp_score,
                "won": completed and team_score > opp_score,
                "ot": completed and ot,
            }
        )

    items.sort(key=lambda x: safe_str(x["date"]))
    return items


def nhl_recent_form(schedule: list[dict[str, Any]], n: int) -> dict[str, float]:
    completed = [g for g in schedule if g["completed"]]
    recent = completed[-n:]
    if not recent:
        return {"win_pct": 0.5, "avg_diff": 0.0, "gf": 3.0, "ga": 3.0}

    wins = sum(1 for g in recent if g["won"])
    gf = sum(g["teamScore"] for g in recent)
    ga = sum(g["opponentScore"] for g in recent)
    games = len(recent)

    return {
        "win_pct": wins / games,
        "avg_diff": (gf - ga) / games,
        "gf": gf / games,
        "ga": ga / games,
    }


def nhl_split_form(schedule: list[dict[str, Any]], want_home: bool, n: int) -> dict[str, float]:
    completed = [g for g in schedule if g["completed"] and g["is_home"] == want_home]
    recent = completed[-n:]
    if not recent:
        return {"win_pct": 0.5, "gf": 3.0, "ga": 3.0}

    wins = sum(1 for g in recent if g["won"])
    gf = sum(g["teamScore"] for g in recent)
    ga = sum(g["opponentScore"] for g in recent)
    games = len(recent)

    return {
        "win_pct": wins / games,
        "gf": gf / games,
        "ga": ga / games,
    }


def nhl_h2h(team_code: str, opp_code: str, schedule: list[dict[str, Any]], n: int) -> dict[str, float]:
    games = [g for g in schedule if g["completed"] and g["opponentCode"] == opp_code][-n:]
    if not games:
        return {"games": 0, "win_pct": 0.5, "avg_diff": 0.0}

    wins = sum(1 for g in games if g["won"])
    diff = sum(g["teamScore"] - g["opponentScore"] for g in games)

    return {
        "games": len(games),
        "win_pct": wins / len(games),
        "avg_diff": diff / len(games),
    }


def nhl_rest_days(schedule: list[dict[str, Any]], game_date: str) -> int:
    completed = [g for g in schedule if g["completed"] and safe_str(g["date"]) < game_date]
    if not completed:
        return 3
    last_date = safe_str(completed[-1]["date"])
    try:
        d1 = datetime.strptime(last_date, "%Y-%m-%d")
        d2 = datetime.strptime(game_date, "%Y-%m-%d")
        return max(0, (d2 - d1).days - 1)
    except Exception:
        return 3


def nhl_streak(schedule: list[dict[str, Any]], n: int) -> int:
    recent = [g for g in schedule if g["completed"]][-n:]
    if not recent:
        return 0
    score = 0
    for g in recent:
        score += 1 if g["won"] else -1
    return score


def nhl_play_by_play(game_id: int) -> dict[str, Any]:
    return fetch_json(f"{NHL_API}/gamecenter/{game_id}/play-by-play", ANALYZE_TTL)


def nhl_period_goal_array(game_id: int, team_code: str) -> list[int]:
    data = nhl_play_by_play(game_id)

    home_team = data.get("homeTeam", {}) or {}
    away_team = data.get("awayTeam", {}) or {}

    home_code = safe_str(
        home_team.get("abbrev")
        or (home_team.get("teamAbbrev") or {}).get("default")
        or home_team.get("triCode")
    )
    away_code = safe_str(
        away_team.get("abbrev")
        or (away_team.get("teamAbbrev") or {}).get("default")
        or away_team.get("triCode")
    )

    home_id = safe_int(home_team.get("id"))
    away_id = safe_int(away_team.get("id"))

    totals = {1: 0, 2: 0, 3: 0}

    for play in data.get("plays", []) or []:
        kind = safe_str(
            play.get("typeDescKey")
            or play.get("eventTypeId")
            or play.get("typeCode")
            or play.get("sortOrder")
        ).lower()

        if "goal" not in kind and kind not in {"505"}:
            continue

        period = safe_int(
            (play.get("periodDescriptor") or {}).get("number")
            or play.get("period")
        )
        if period not in totals:
            continue

        details = play.get("details", {}) or {}
        owner_code = safe_str(
            details.get("eventOwnerTeamAbbrev")
            or play.get("eventOwnerTeamAbbrev")
            or play.get("teamAbbrev")
        )
        owner_id = safe_int(
            details.get("eventOwnerTeamId")
            or play.get("eventOwnerTeamId")
            or (play.get("team") or {}).get("id")
        )

        same_team = False
        if owner_code and owner_code == team_code:
            same_team = True
        elif owner_id:
            if team_code == home_code and owner_id == home_id:
                same_team = True
            elif team_code == away_code and owner_id == away_id:
                same_team = True

        if same_team:
            totals[period] += 1

    return [totals[1], totals[2], totals[3]]


def nhl_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]
    game_date = game.get("date") or today_utc_str()

    away_sched = nhl_schedule(away)
    home_sched = nhl_schedule(home)

    away_form_10 = nhl_recent_form(away_sched, 10)
    home_form_10 = nhl_recent_form(home_sched, 10)

    away_form_5 = nhl_recent_form(away_sched, 5)
    home_form_5 = nhl_recent_form(home_sched, 5)

    away_split = nhl_split_form(away_sched, False, 8)
    home_split = nhl_split_form(home_sched, True, 8)

    away_h2h = nhl_h2h(away, home, away_sched, 6)
    home_h2h = nhl_h2h(home, away, home_sched, 6)

    away_strength = nhl_strength(away)
    home_strength = nhl_strength(home)

    away_rest = nhl_rest_days(away_sched, game_date)
    home_rest = nhl_rest_days(home_sched, game_date)

    away_streak = nhl_streak(away_sched, 8)
    home_streak = nhl_streak(home_sched, 8)

    score = 0.0
    score += (home_strength["points_pct"] - away_strength["points_pct"]) * 3.25
    score += (home_strength["goal_diff_pg"] - away_strength["goal_diff_pg"]) * 1.35

    score += (home_form_10["win_pct"] - away_form_10["win_pct"]) * 1.15
    score += (home_form_10["avg_diff"] - away_form_10["avg_diff"]) * 0.38
    score += (home_form_10["gf"] - away_form_10["gf"]) * 0.16
    score += (away_form_10["ga"] - home_form_10["ga"]) * 0.16

    score += (home_form_5["win_pct"] - away_form_5["win_pct"]) * 1.75
    score += (home_form_5["avg_diff"] - away_form_5["avg_diff"]) * 0.52
    score += (home_form_5["gf"] - away_form_5["gf"]) * 0.24
    score += (away_form_5["ga"] - home_form_5["ga"]) * 0.24

    score += (home_split["win_pct"] - away_split["win_pct"]) * 1.05
    score += ((home_split["gf"] - home_split["ga"]) - (away_split["gf"] - away_split["ga"])) * 0.16

    if home_h2h["games"] and away_h2h["games"]:
        score += (home_h2h["win_pct"] - away_h2h["win_pct"]) * 0.55
        score += (home_h2h["avg_diff"] - away_h2h["avg_diff"]) * 0.22

    score += (home_rest - away_rest) * 0.13
    score += (home_streak - away_streak) * 0.07
    score += 0.22

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home

    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 47 <= home_prob * 100 <= 53 else "edge"

    away_expected = (
        away_form_10["gf"] * 0.22
        + away_form_5["gf"] * 0.28
        + away_split["gf"] * 0.18
        + home_form_10["ga"] * 0.16
        + home_form_5["ga"] * 0.10
        + home_split["ga"] * 0.06
    )

    home_expected = (
        home_form_10["gf"] * 0.22
        + home_form_5["gf"] * 0.28
        + home_split["gf"] * 0.18
        + away_form_10["ga"] * 0.16
        + away_form_5["ga"] * 0.10
        + away_split["ga"] * 0.06
    )

    edge_shift = (home_prob - 0.5) * 1.25
    home_expected = max(1.4, min(6.2, home_expected + edge_shift + 0.12))
    away_expected = max(1.2, min(5.8, away_expected - edge_shift))

    projected_total = round(max(4.5, min(8.5, home_expected + away_expected)), 1)

    home_score = max(1, int(round(home_expected)))
    away_score = max(1, int(round(away_expected)))

    if home_score == away_score:
        if predicted_winner == home:
            home_score += 1
        else:
            away_score += 1

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
        "projectedTotal": projected_total,
        "predictedScore": {
            "away": away_score,
            "home": home_score,
        },
        "tier": tier,
        "lastPredictionRefreshUTC": utc_now().isoformat(),
        "reasons": [
            {
                "label": "Season strength",
                "text": (
                    f"{home} points % {home_strength['points_pct']:.3f}, goal diff/game "
                    f"{home_strength['goal_diff_pg']:.2f} vs {away} "
                    f"{away_strength['points_pct']:.3f}, {away_strength['goal_diff_pg']:.2f}."
                ),
            },
            {
                "label": "Last 10 and last 5",
                "text": (
                    f"Last 10 win%: {home} {home_form_10['win_pct']:.2f} vs {away} {away_form_10['win_pct']:.2f}. "
                    f"Last 5 diff: {home} {home_form_5['avg_diff']:.2f} vs {away} {away_form_5['avg_diff']:.2f}."
                ),
            },
            {
                "label": "Home / road split",
                "text": (
                    f"{home} home win% {home_split['win_pct']:.2f}, GF {home_split['gf']:.2f}, GA {home_split['ga']:.2f}. "
                    f"{away} road win% {away_split['win_pct']:.2f}, GF {away_split['gf']:.2f}, GA {away_split['ga']:.2f}."
                ),
            },
            {
                "label": "Rest / streak / H2H",
                "text": (
                    f"Rest days {home}: {home_rest}, {away}: {away_rest}. "
                    f"Streak score {home}: {home_streak}, {away}: {away_streak}. "
                    f"Recent H2H leans {home if home_h2h['win_pct'] >= away_h2h['win_pct'] else away}."
                ),
            },
            {
                "label": "Predicted score",
                "text": f"Model projects {away} {away_score} - {home_score} {home}.",
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
    schedule = nhl_schedule(team_code)
    names = nhl_team_name_map()

    rows = defaultdict(
        lambda: {
            "games": 0,
            "wins": 0,
            "losses": 0,
            "otWins": 0,
            "otLosses": 0,
            "gf": 0,
            "ga": 0,
            "trend": [],
            "periodsFor": [0, 0, 0],
            "periodsAgainst": [0, 0, 0],
        }
    )

    for g in schedule:
        if not g["completed"] or not g["opponentCode"] or not g.get("id"):
            continue

        opp = g["opponentCode"]
        r = rows[opp]
        r["games"] += 1
        r["gf"] += g["teamScore"]
        r["ga"] += g["opponentScore"]

        try:
            our_periods = nhl_period_goal_array(int(g["id"]), team_code)
            opp_periods = nhl_period_goal_array(int(g["id"]), opp)
        except Exception:
            our_periods = [0, 0, 0]
            opp_periods = [0, 0, 0]

        for i in range(3):
            r["periodsFor"][i] += safe_int(our_periods[i])
            r["periodsAgainst"][i] += safe_int(opp_periods[i])

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
        games = max(r["games"], 1)
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
                "scorePct": round(
                    (r["wins"] + 0.5 * r["otWins"] + 0.25 * r["otLosses"]) / games,
                    3,
                ),
                "trend": r["trend"][-10:],
                "periodsFor": r["periodsFor"],
                "periodsAgainst": r["periodsAgainst"],
                "periodAvgFor": [round(v / games, 2) for v in r["periodsFor"]],
                "periodAvgAgainst": [round(v / games, 2) for v in r["periodsAgainst"]],
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
        return jsonify({
            "sport": sport,
            "updatedUTC": utc_now().isoformat(),
            "days": [
                {"label": "Today", "date": today_utc_str(), "games": []},
                {"label": "Tomorrow", "date": date_plus_utc_str(1), "games": []}
            ],
            "error": str(exc)
        }), 200


@app.get("/api/insights/<sport>")
def api_insights(sport: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(insights_for_sport(sport))
    except Exception as exc:
        return jsonify({
            "sport": sport,
            "updatedUTC": utc_now().isoformat(),
            "insights": [],
            "error": str(exc)
        }), 200


@app.get("/api/teams/<sport>")
def api_teams(sport: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify({"sport": sport, "teams": teams_for_sport(sport)})
    except Exception as exc:
        return jsonify({
            "sport": sport,
            "teams": [],
            "error": str(exc)
        }), 200


@app.get("/api/team-analyze/<sport>/<team_code>")
def api_team_analyze(sport: str, team_code: str):
    if sport not in {"nhl", "mlb"}:
        return jsonify({"error": "Unsupported sport"}), 400
    try:
        return jsonify(analyze_for_sport(sport, team_code))
    except Exception as exc:
        return jsonify({
            "sport": sport,
            "teamCode": team_code,
            "rows": [],
            "error": str(exc)
        }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
