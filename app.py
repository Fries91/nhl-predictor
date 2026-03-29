def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_iso_dt(value: str | None) -> datetime:
    text = safe_str(value)
    if not text:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    try:
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)


def nhl_team_code_from_side(side: dict[str, Any]) -> str:
    return (
        safe_str(side.get("abbrev"))
        or safe_str((side.get("teamAbbrev") or {}).get("default"))
        or safe_str(side.get("triCode"))
        or safe_str(side.get("code"))
    )


def nhl_player_name(player: dict[str, Any]) -> str:
    first = (
        safe_str((player.get("firstName") or {}).get("default"))
        or safe_str(player.get("firstName"))
    )
    last = (
        safe_str((player.get("lastName") or {}).get("default"))
        or safe_str(player.get("lastName"))
    )
    full = (first + " " + last).strip()
    if full:
        return full

    name = player.get("name")
    if isinstance(name, dict):
        return safe_str(name.get("default"), "Unknown")
    return safe_str(name, "Unknown")


def nhl_side_skaters(side: dict[str, Any]) -> list[dict[str, Any]]:
    players: list[dict[str, Any]] = []
    for bucket in ("forwards", "defense"):
        players.extend(side.get(bucket, []) or [])
    return players


def nhl_completed_games(schedule: list[dict[str, Any]]) -> list[dict[str, Any]]:
    completed = [g for g in schedule if g.get("completed") and g.get("id")]
    return sorted(
        completed,
        key=lambda g: parse_iso_dt(g.get("startTimeUTC") or g.get("date")),
        reverse=True,
    )


def nhl_recent_form(schedule: list[dict[str, Any]], n: int = 5) -> dict[str, float]:
    completed = nhl_completed_games(schedule)[:n]
    if not completed:
        return {"win_pct": 0.5, "avg_diff": 0.0, "gf": 3.0, "ga": 3.0}

    return {
        "win_pct": sum(1 for g in completed if g["won"]) / len(completed),
        "avg_diff": sum(g["goalDiff"] for g in completed) / len(completed),
        "gf": sum(g["teamScore"] for g in completed) / len(completed),
        "ga": sum(g["opponentScore"] for g in completed) / len(completed),
    }


def nhl_split_form(schedule: list[dict[str, Any]], is_home: bool, n: int = 5) -> dict[str, float]:
    games = [g for g in nhl_completed_games(schedule) if g["isHome"] == is_home][:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0, "gf": 3.0, "ga": 3.0}

    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["goalDiff"] for g in games) / len(games),
        "gf": sum(g["teamScore"] for g in games) / len(games),
        "ga": sum(g["opponentScore"] for g in games) / len(games),
    }


def nhl_h2h(team_code: str, opp_code: str, schedule: list[dict[str, Any]], n: int = 4) -> dict[str, float]:
    games = [g for g in nhl_completed_games(schedule) if g["opponentCode"] == opp_code][:n]
    if not games:
        return {"win_pct": 0.5, "avg_diff": 0.0, "games": 0}

    return {
        "win_pct": sum(1 for g in games if g["won"]) / len(games),
        "avg_diff": sum(g["goalDiff"] for g in games) / len(games),
        "games": len(games),
    }


def nhl_rest_days(schedule: list[dict[str, Any]], game_date: str) -> int:
    target = parse_iso_dt(game_date)
    prior = [
        g for g in nhl_completed_games(schedule)
        if parse_iso_dt(g.get("date")) < target
    ]
    if not prior:
        return 4

    last_game = prior[0]
    try:
        last_dt = parse_iso_dt(last_game["date"])
        return max(0, min((target - last_dt).days - 1, 7))
    except Exception:
        return 1


def nhl_streak(schedule: list[dict[str, Any]], limit: int = 6) -> int:
    games = nhl_completed_games(schedule)[:limit]
    if not games:
        return 0

    streak = 0
    first_result = games[0]["won"]
    for g in games:
        if g["won"] == first_result:
            streak += 1
        else:
            break
    return streak if first_result else -streak


def nhl_hot_assist_pick(team_code: str, last_n: int = 5) -> dict[str, Any]:
    schedule = nhl_schedule(team_code)
    completed = nhl_completed_games(schedule)[:last_n]

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
    box_games_used = 0

    for idx, g in enumerate(completed):
        try:
            box = nhl_boxscore(int(g["id"]))
        except Exception:
            continue

        away = box.get("awayTeam", {}) or {}
        home = box.get("homeTeam", {}) or {}

        away_code = nhl_team_code_from_side(away)
        home_code = nhl_team_code_from_side(home)

        side = None
        if away_code == team_code:
            side = away
        elif home_code == team_code:
            side = home
        else:
            # fallback in case side abbrev is missing in boxscore
            if g.get("isHome"):
                side = home
            else:
                side = away

        skaters = nhl_side_skaters(side)
        if not skaters:
            continue

        box_games_used += 1
        weight = max(1.0, 1.4 - (idx * 0.15))  # most recent game counts a bit more

        for p in skaters:
            name = nhl_player_name(p)
            assists = safe_int(p.get("assists"))
            goals = safe_int(p.get("goals"))
            points = assists + goals

            row = totals.setdefault(
                name,
                {
                    "player": name,
                    "assistsLast3": 0.0,
                    "pointsLast3": 0.0,
                    "rawAssists": 0,
                    "rawPoints": 0,
                    "gamesUsed": 0,
                    "gamesWithPoint": 0,
                    "gamesWithAssist": 0,
                },
            )

            row["assistsLast3"] += assists * weight
            row["pointsLast3"] += points * weight
            row["rawAssists"] += assists
            row["rawPoints"] += points
            row["gamesUsed"] += 1
            if assists > 0:
                row["gamesWithAssist"] += 1
            if points > 0:
                row["gamesWithPoint"] += 1

    if not totals:
        return {
            "teamCode": team_code,
            "player": "No data",
            "assistsLast3": 0,
            "pointsLast3": 0,
            "gamesUsed": box_games_used,
            "reason": "Boxscore player data unavailable."
        }

    ranked = sorted(
        totals.values(),
        key=lambda x: (
            -x["assistsLast3"],
            -x["gamesWithAssist"],
            -x["pointsLast3"],
            -x["gamesWithPoint"],
            x["player"],
        ),
    )

    best = ranked[0]
    return {
        "teamCode": team_code,
        "player": best["player"],
        "assistsLast3": int(round(best["rawAssists"])),
        "pointsLast3": int(round(best["rawPoints"])),
        "gamesUsed": best["gamesUsed"],
        "reason": (
            f"Best recent setup trend: {best['rawAssists']} assists in "
            f"{best['gamesUsed']} recent completed games."
        ),
    }


def nhl_predict_game(game: dict[str, Any]) -> dict[str, Any]:
    away = game["awayCode"]
    home = game["homeCode"]

    away_sched = nhl_schedule(away)
    home_sched = nhl_schedule(home)

    away_form_5 = nhl_recent_form(away_sched, 5)
    home_form_5 = nhl_recent_form(home_sched, 5)

    away_form_10 = nhl_recent_form(away_sched, 10)
    home_form_10 = nhl_recent_form(home_sched, 10)

    away_split = nhl_split_form(away_sched, False, 6)
    home_split = nhl_split_form(home_sched, True, 6)

    away_h2h = nhl_h2h(away, home, away_sched, 4)
    home_h2h = nhl_h2h(home, away, home_sched, 4)

    away_strength = nhl_strength(away)
    home_strength = nhl_strength(home)

    away_rest = nhl_rest_days(away_sched, game["date"])
    home_rest = nhl_rest_days(home_sched, game["date"])

    away_streak = nhl_streak(away_sched, 6)
    home_streak = nhl_streak(home_sched, 6)

    score = 0.0
    score += (home_strength["points_pct"] - away_strength["points_pct"]) * 3.0
    score += (home_strength["goal_diff_pg"] - away_strength["goal_diff_pg"]) * 1.1

    score += (home_form_10["win_pct"] - away_form_10["win_pct"]) * 1.1
    score += (home_form_10["avg_diff"] - away_form_10["avg_diff"]) * 0.30

    score += (home_form_5["win_pct"] - away_form_5["win_pct"]) * 1.6
    score += (home_form_5["avg_diff"] - away_form_5["avg_diff"]) * 0.45

    score += (home_split["win_pct"] - away_split["win_pct"]) * 0.95
    score += ((home_split["gf"] - home_split["ga"]) - (away_split["gf"] - away_split["ga"])) * 0.12

    if home_h2h["games"] and away_h2h["games"]:
        score += (home_h2h["win_pct"] - away_h2h["win_pct"]) * 0.45
        score += (home_h2h["avg_diff"] - away_h2h["avg_diff"]) * 0.18

    score += (home_rest - away_rest) * 0.12
    score += (home_streak - away_streak) * 0.05

    # mild home-ice edge
    score += 0.18

    home_prob = logistic(score)
    predicted_winner = home if home_prob >= 0.5 else away
    predicted_loser = away if predicted_winner == home else home
    confidence = round(abs(home_prob - 0.5) * 200, 1)
    tier = "tight" if 47 <= home_prob * 100 <= 53 else "edge"

    projected_total = (
        home_form_10["gf"] + away_form_10["gf"] + home_form_10["ga"] + away_form_10["ga"]
    ) / 2.0
    projected_total += ((home_split["gf"] + away_split["gf"]) - (home_split["ga"] + away_split["ga"])) * 0.05
    projected_total = max(4.5, min(8.5, projected_total))

    away_player_pick = nhl_hot_assist_pick(away, last_n=5)
    home_player_pick = nhl_hot_assist_pick(home, last_n=5)

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
                    f"{home} points % {home_strength['points_pct']:.3f} and goal diff/game "
                    f"{home_strength['goal_diff_pg']:.2f} vs {away} "
                    f"{away_strength['points_pct']:.3f} and {away_strength['goal_diff_pg']:.2f}."
                ),
            },
            {
                "label": "Recent momentum",
                "text": (
                    f"Last 5: {home} win% {home_form_5['win_pct']:.2f}, diff {home_form_5['avg_diff']:.2f} "
                    f"vs {away} {away_form_5['win_pct']:.2f}, diff {away_form_5['avg_diff']:.2f}. "
                    f"Streaks: {home} {home_streak}, {away} {away_streak}."
                ),
            },
            {
                "label": "Home and road split",
                "text": (
                    f"{home} home win% {home_split['win_pct']:.2f} with {home_split['gf']:.2f} GF/game "
                    f"vs {away} road win% {away_split['win_pct']:.2f} with {away_split['gf']:.2f} GF/game."
                ),
            },
            {
                "label": "Rest and matchup history",
                "text": (
                    f"Rest: {home} {home_rest} days vs {away} {away_rest} days. "
                    f"Recent H2H leans {home if home_h2h['win_pct'] >= away_h2h['win_pct'] else away}."
                ),
            },
            {
                "label": "Hot assist picks",
                "text": (
                    f"{away}: {away_player_pick['player']} | {home}: {home_player_pick['player']}."
                ),
            },
        ],
    }
