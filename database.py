import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path("data/randomizer.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


@dataclass
class Game:
    id: int
    name: str
    status: str
    created_at: str
    country: str | None


@dataclass
class Giveaway:
    id: int
    game_id: int | None
    name: str | None
    prize_text: str
    prize_entities: str | None
    photo_id: str | None
    winner_count: int
    status: str
    created_at: str
    channel_message_id: int | None
    channel_id: str | None
    draw_deadline: str | None
    admin_chat_id: int | None
    country: str | None


@dataclass
class Participant:
    user_id: int
    username: str | None
    full_name: str


_GIVEAWAY_COLS = (
    "id, game_id, name, prize_text, prize_entities, photo_id, winner_count, "
    "status, created_at, channel_message_id, channel_id, draw_deadline, admin_chat_id, country"
)


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT
        );

        CREATE TABLE IF NOT EXISTS giveaways (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER,
            prize_text TEXT NOT NULL,
            prize_entities TEXT,
            photo_id TEXT,
            winner_count INTEGER DEFAULT 1,
            status TEXT DEFAULT 'queued',
            created_at TEXT,
            channel_message_id INTEGER,
            channel_id TEXT
        );

        CREATE TABLE IF NOT EXISTS participants (
            giveaway_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            joined_at TEXT,
            PRIMARY KEY (giveaway_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS winners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            giveaway_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT,
            full_name TEXT NOT NULL,
            won_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS draw_rejects (
            giveaway_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (giveaway_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(giveaways)").fetchall()}
    if "game_id" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN game_id INTEGER")
    if "draw_deadline" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN draw_deadline TEXT")
    if "admin_chat_id" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN admin_chat_id INTEGER")
    if "name" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN name TEXT")
    if "country" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN country TEXT")

    game_cols = {r[1] for r in conn.execute("PRAGMA table_info(games)").fetchall()}
    if "country" not in game_cols:
        conn.execute("ALTER TABLE games ADD COLUMN country TEXT")

    conn.execute("UPDATE games SET country='mne' WHERE country='me'")
    conn.execute("UPDATE giveaways SET country='mne' WHERE country='me'")
    conn.commit()
    conn.close()


# ── Settings ─────────────────────────────────────────────

def get_setting(key: str, default: str | None = None) -> str | None:
    conn = _conn()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    conn = _conn()
    conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?,?)", (key, value))
    conn.commit()
    conn.close()


# ── Games ─────────────────────────────────────────────────

def add_game(name: str, country: str | None = None) -> int:
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO games (name, created_at, country) VALUES (?,?,?)",
        (name, _now(), country),
    )
    gid = cur.lastrowid
    conn.commit()
    conn.close()
    return gid


def get_games(country: str | None = None) -> list[Game]:
    conn = _conn()
    if country:
        rows = conn.execute(
            "SELECT id, name, status, created_at, country FROM games WHERE status='active' AND country=? ORDER BY id DESC",
            (country,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, name, status, created_at, country FROM games WHERE status='active' ORDER BY id DESC"
        ).fetchall()
    conn.close()
    return [Game(*r) for r in rows]


def get_game(game_id: int) -> Game | None:
    conn = _conn()
    row = conn.execute(
        "SELECT id, name, status, created_at, country FROM games WHERE id=?", (game_id,)
    ).fetchone()
    conn.close()
    return Game(*row) if row else None


def delete_game(game_id: int) -> bool:
    conn = _conn()
    # Clear participants and rejects for finished giveaways returning to library
    conn.execute(
        "DELETE FROM participants WHERE giveaway_id IN "
        "(SELECT id FROM giveaways WHERE game_id=? AND status='finished')",
        (game_id,),
    )
    conn.execute(
        "DELETE FROM draw_rejects WHERE giveaway_id IN "
        "(SELECT id FROM giveaways WHERE game_id=? AND status='finished')",
        (game_id,),
    )
    conn.execute("UPDATE giveaways SET game_id=NULL WHERE game_id=?", (game_id,))
    cur = conn.execute("DELETE FROM games WHERE id=?", (game_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def game_giveaway_count(game_id: int) -> tuple[int, int, int]:
    """Returns (queued, active, finished) counts for a game."""
    conn = _conn()
    queued = conn.execute(
        "SELECT COUNT(*) FROM giveaways WHERE game_id=? AND status='queued'", (game_id,)
    ).fetchone()[0]
    active = conn.execute(
        "SELECT COUNT(*) FROM giveaways WHERE game_id=? AND status='active'", (game_id,)
    ).fetchone()[0]
    finished = conn.execute(
        "SELECT COUNT(*) FROM giveaways WHERE game_id=? AND status='finished'", (game_id,)
    ).fetchone()[0]
    conn.close()
    return queued, active, finished


# ── Giveaways ────────────────────────────────────────────

def add_giveaway(
    prize_text: str,
    prize_entities: str | None = None,
    photo_id: str | None = None,
    winner_count: int = 1,
    game_id: int | None = None,
    name: str | None = None,
    country: str | None = None,
) -> int:
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO giveaways (game_id, name, prize_text, prize_entities, photo_id, winner_count, created_at, country) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (game_id, name, prize_text, prize_entities, photo_id, winner_count, _now(), country),
    )
    gid = cur.lastrowid
    conn.commit()
    conn.close()
    return gid


def get_queued(game_id: int | None = None) -> list[Giveaway]:
    conn = _conn()
    if game_id is not None:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE status='queued' AND game_id=? ORDER BY id",
            (game_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE status='queued' ORDER BY id"
        ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]


def get_free_giveaways(country: str | None = None) -> list[Giveaway]:
    """Giveaways not assigned to any game (reusable library)."""
    conn = _conn()
    if country:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') AND country=? ORDER BY id",
            (country,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') ORDER BY id"
        ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]


def assign_to_game(gid: int, game_id: int) -> bool:
    conn = _conn()
    conn.execute("UPDATE giveaways SET game_id=?, status='queued' WHERE id=?", (game_id, gid))
    conn.commit()
    conn.close()
    return True


def reset_giveaway(gid: int) -> bool:
    """Reset a finished giveaway back to queued for reuse."""
    conn = _conn()
    conn.execute("DELETE FROM participants WHERE giveaway_id=?", (gid,))
    conn.execute("DELETE FROM draw_rejects WHERE giveaway_id=?", (gid,))
    cur = conn.execute(
        "UPDATE giveaways SET status='queued', channel_id=NULL, channel_message_id=NULL WHERE id=?",
        (gid,),
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_giveaway(gid: int) -> Giveaway | None:
    conn = _conn()
    row = conn.execute(
        f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE id=?", (gid,)
    ).fetchone()
    conn.close()
    return Giveaway(*row) if row else None


def get_active() -> Giveaway | None:
    conn = _conn()
    row = conn.execute(
        f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE status='active' LIMIT 1"
    ).fetchone()
    conn.close()
    return Giveaway(*row) if row else None


def get_all_active() -> list[Giveaway]:
    conn = _conn()
    rows = conn.execute(
        f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE status='active' ORDER BY id"
    ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]


def activate_giveaway(
    gid: int, channel_id: str, message_id: int,
    draw_deadline: str | None = None, admin_chat_id: int | None = None,
) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE giveaways SET status='active', channel_id=?, channel_message_id=?, "
        "draw_deadline=?, admin_chat_id=? WHERE id=?",
        (channel_id, message_id, draw_deadline, admin_chat_id, gid),
    )
    conn.commit()
    conn.close()


def finish_giveaway(gid: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE giveaways SET status='finished', draw_deadline=NULL WHERE id=?", (gid,)
    )
    conn.execute("DELETE FROM draw_rejects WHERE giveaway_id=?", (gid,))
    conn.commit()
    conn.close()


def update_giveaway(
    gid: int,
    prize_text: str | None = None,
    prize_entities: str | None = None,
    photo_id: str | None = None,
    clear_photo: bool = False,
    name: str | None = None,
) -> bool:
    conn = _conn()
    fields: list[str] = []
    values: list = []
    if name is not None:
        fields.append("name=?")
        values.append(name)
    if prize_text is not None:
        fields.append("prize_text=?")
        values.append(prize_text)
        fields.append("prize_entities=?")
        values.append(prize_entities)
    if clear_photo:
        fields.append("photo_id=NULL")
    elif photo_id is not None:
        fields.append("photo_id=?")
        values.append(photo_id)
    if not fields:
        conn.close()
        return False
    values.append(gid)
    cur = conn.execute(
        f"UPDATE giveaways SET {', '.join(fields)} WHERE id=? AND status IN ('queued','finished')",
        values,
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def delete_giveaway(gid: int) -> bool:
    conn = _conn()
    cur = conn.execute("DELETE FROM giveaways WHERE id=? AND status IN ('queued','finished')", (gid,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


# ── Participants ─────────────────────────────────────────

def add_participant(gid: int, user_id: int, username: str | None, full_name: str) -> bool:
    """Returns True if newly added, False if duplicate."""
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO participants (giveaway_id, user_id, username, full_name, joined_at) "
            "VALUES (?,?,?,?,?)",
            (gid, user_id, username, full_name, _now()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def participant_count(gid: int) -> int:
    conn = _conn()
    n = conn.execute(
        "SELECT COUNT(*) FROM participants WHERE giveaway_id=?", (gid,)
    ).fetchone()[0]
    conn.close()
    return n


def get_eligible(gid: int, cooldown_minutes: int) -> list[Participant]:
    """Participants minus recent winners (cooldown) and re-roll rejects."""
    conn = _conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)).isoformat()

    cooled = {r[0] for r in conn.execute(
        "SELECT DISTINCT user_id FROM winners WHERE won_at > ?", (cutoff,)
    ).fetchall()}

    rejected = {r[0] for r in conn.execute(
        "SELECT user_id FROM draw_rejects WHERE giveaway_id=?", (gid,)
    ).fetchall()}

    excluded = cooled | rejected

    rows = conn.execute(
        "SELECT user_id, username, full_name FROM participants WHERE giveaway_id=?",
        (gid,),
    ).fetchall()
    conn.close()

    return [Participant(*r) for r in rows if r[0] not in excluded]


def get_participants(gid: int) -> list[Participant]:
    conn = _conn()
    rows = conn.execute(
        "SELECT user_id, username, full_name FROM participants WHERE giveaway_id=?",
        (gid,),
    ).fetchall()
    conn.close()
    return [Participant(*r) for r in rows]


# ── Winners / Rejects ────────────────────────────────────

def add_winner(gid: int, user_id: int, username: str | None, full_name: str) -> None:
    conn = _conn()
    conn.execute(
        "INSERT INTO winners (giveaway_id, user_id, username, full_name, won_at) VALUES (?,?,?,?,?)",
        (gid, user_id, username, full_name, _now()),
    )
    conn.commit()
    conn.close()


def add_reject(gid: int, user_id: int) -> None:
    conn = _conn()
    conn.execute("INSERT OR IGNORE INTO draw_rejects (giveaway_id, user_id) VALUES (?,?)", (gid, user_id))
    conn.commit()
    conn.close()


def cooldown_remaining(user_id: int, cooldown_minutes: int) -> int | None:
    """Minutes remaining on cooldown, or None if not on cooldown."""
    conn = _conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)).isoformat()
    row = conn.execute(
        "SELECT won_at FROM winners WHERE user_id=? AND won_at>? ORDER BY won_at DESC LIMIT 1",
        (user_id, cutoff),
    ).fetchone()
    conn.close()
    if not row:
        return None
    won_at = datetime.fromisoformat(row[0])
    expires = won_at + timedelta(minutes=cooldown_minutes)
    remaining = (expires - datetime.now(timezone.utc)).total_seconds() / 60
    return max(1, int(remaining))


def recent_winners(cooldown_minutes: int) -> list[tuple[str, str | None, str]]:
    """Returns (full_name, username, prize_text) for winners still on cooldown."""
    conn = _conn()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)).isoformat()
    rows = conn.execute(
        "SELECT w.full_name, w.username, g.prize_text "
        "FROM winners w JOIN giveaways g ON w.giveaway_id = g.id "
        "WHERE w.won_at > ? ORDER BY w.won_at DESC",
        (cutoff,),
    ).fetchall()
    conn.close()
    return rows


def clear_winners() -> int:
    conn = _conn()
    cur = conn.execute("DELETE FROM winners")
    conn.execute("DELETE FROM draw_rejects")
    conn.commit()
    n = cur.rowcount
    conn.close()
    return n
