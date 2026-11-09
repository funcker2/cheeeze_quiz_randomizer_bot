import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import database as db  # noqa: E402


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "randomizer.db")
    db.init_db()
    return db


def _columns(database):
    conn = sqlite3.connect(database.DB_PATH)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(giveaways)").fetchall()]
    conn.close()
    return cols


def test_migration_adds_column(fresh_db):
    assert "sponsor_instagram" in _columns(fresh_db)
    fresh_db.init_db()
    assert _columns(fresh_db).count("sponsor_instagram") == 1


def test_migration_on_old_schema_keeps_rows(tmp_path, monkeypatch):
    """Prod upgrade: table without the column already has rows."""
    path = tmp_path / "old.db"
    monkeypatch.setattr(db, "DB_PATH", path)
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE giveaways (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id INTEGER, "
        "prize_text TEXT NOT NULL, prize_entities TEXT, photo_id TEXT, winner_count INTEGER DEFAULT 1, "
        "status TEXT DEFAULT 'queued', created_at TEXT, channel_message_id INTEGER, channel_id TEXT)"
    )
    conn.execute("INSERT INTO giveaways (prize_text) VALUES ('Старый приз')")
    conn.commit()
    conn.close()

    db.init_db()
    g = db.get_giveaway(1)
    assert g.prize_text == "Старый приз"
    assert g.sponsor_instagram is None


def test_add_with_and_without_sponsor(fresh_db):
    a = fresh_db.add_giveaway("A", country="bg", sponsor_instagram="sunny_cafe.bg")
    b = fresh_db.add_giveaway("B", country="bg")
    assert fresh_db.get_giveaway(a).sponsor_instagram == "sunny_cafe.bg"
    assert fresh_db.get_giveaway(b).sponsor_instagram is None


def test_copy_inherits_sponsor(fresh_db):
    game_id = fresh_db.add_game("Квиз", country="bg")
    src = fresh_db.add_giveaway("A", country="bg", sponsor_instagram="sunny_cafe.bg")
    copy_id = fresh_db.copy_giveaway(src, game_id)
    assert fresh_db.get_giveaway(copy_id).sponsor_instagram == "sunny_cafe.bg"


def test_update_and_clear_sponsor(fresh_db):
    gid = fresh_db.add_giveaway("A", country="bg")
    assert fresh_db.update_giveaway(gid, sponsor_instagram="new_one")
    assert fresh_db.get_giveaway(gid).sponsor_instagram == "new_one"
    assert fresh_db.update_giveaway(gid, clear_sponsor=True)
    assert fresh_db.get_giveaway(gid).sponsor_instagram is None


def test_active_giveaway_not_editable(fresh_db):
    gid = fresh_db.add_giveaway("A", country="bg")
    fresh_db.activate_giveaway(gid, "-100", 1)
    assert not fresh_db.update_giveaway(gid, sponsor_instagram="x")
    assert fresh_db.get_giveaway(gid).sponsor_instagram is None
