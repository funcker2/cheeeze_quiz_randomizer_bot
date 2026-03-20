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
class Giveaway:
    id: int
    prize_text: str
    prize_entities: str | None
    photo_id: str | None
    winner_count: int
    status: str
    created_at: str
    channel_message_id: int | None
    channel_id: str | None


@dataclass
class Participant:
    user_id: int
    username: str | None
    full_name: str


_GIVEAWAY_COLS = (
    "id, prize_text, prize_entities, photo_id, winner_count, "
    "status, created_at, channel_message_id, channel_id"
)


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS giveaways (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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


# ── Giveaways ────────────────────────────────────────────

def add_giveaway(
    prize_text: str,
    prize_entities: str | None = None,
    photo_id: str | None = None,
    winner_count: int = 1,
) -> int:
    conn = _conn()
    cur = conn.execute(
        "INSERT INTO giveaways (prize_text, prize_entities, photo_id, winner_count, created_at) "
        "VALUES (?,?,?,?,?)",
        (prize_text, prize_entities, photo_id, winner_count, _now()),
    )
    gid = cur.lastrowid
    conn.commit()
    conn.close()
    return gid


def get_queued() -> list[Giveaway]:
    conn = _conn()
    rows = conn.execute(
        f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE status='queued' ORDER BY id"
    ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]


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


def activate_giveaway(gid: int, channel_id: str, message_id: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE giveaways SET status='active', channel_id=?, channel_message_id=? WHERE id=?",
        (channel_id, message_id, gid),
    )
    conn.commit()
    conn.close()


def finish_giveaway(gid: int) -> None:
    conn = _conn()
    conn.execute("UPDATE giveaways SET status='finished' WHERE id=?", (gid,))
    conn.execute("DELETE FROM draw_rejects WHERE giveaway_id=?", (gid,))
    conn.commit()
    conn.close()


def delete_giveaway(gid: int) -> bool:
    conn = _conn()
    cur = conn.execute("DELETE FROM giveaways WHERE id=? AND status='queued'", (gid,))
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
