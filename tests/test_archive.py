import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import database as db


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    """Изолированная БД на каждый тест: _conn() читает DB_PATH при каждом вызове."""
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "randomizer.db")
    db.init_db()
    return db


def _columns(database):
    conn = sqlite3.connect(database.DB_PATH)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(giveaways)").fetchall()}
    conn.close()
    return cols


def test_migration_adds_archived_at_column(fresh_db):
    assert "archived_at" in _columns(fresh_db)


def test_migration_is_idempotent(fresh_db):
    fresh_db.init_db()
    fresh_db.init_db()
    assert "archived_at" in _columns(fresh_db)


def test_migration_keeps_existing_rows_in_library(fresh_db):
    """Апгрейд боевой БД: колонки ещё нет, розыгрыши есть — после миграции они в библиотеке."""
    gid = fresh_db.add_giveaway("Старый приз", country="mne")
    conn = sqlite3.connect(fresh_db.DB_PATH)
    conn.execute("ALTER TABLE giveaways DROP COLUMN archived_at")
    conn.commit()
    conn.close()

    fresh_db.init_db()

    assert "archived_at" in _columns(fresh_db)
    assert [g.id for g in fresh_db.get_free_giveaways("mne")] == [gid]
    assert fresh_db.get_archived_giveaways("mne") == []


def test_new_giveaway_is_not_archived(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    assert fresh_db.get_giveaway(gid).archived_at is None


def test_archive_removes_from_library(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    assert fresh_db.archive_giveaway(gid) is True
    assert [g.id for g in fresh_db.get_free_giveaways("mne")] == []
    assert [g.id for g in fresh_db.get_archived_giveaways("mne")] == [gid]


def test_archive_sets_timestamp(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    fresh_db.archive_giveaway(gid)
    assert fresh_db.get_giveaway(gid).archived_at is not None


def test_unarchive_returns_to_library(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    fresh_db.archive_giveaway(gid)
    assert fresh_db.unarchive_giveaway(gid) is True
    assert [g.id for g in fresh_db.get_free_giveaways("mne")] == [gid]
    assert fresh_db.get_archived_giveaways("mne") == []
    assert fresh_db.get_giveaway(gid).archived_at is None


def test_unarchive_preserves_finished_status(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    fresh_db.finish_giveaway(gid)
    fresh_db.archive_giveaway(gid)
    fresh_db.unarchive_giveaway(gid)
    assert fresh_db.get_giveaway(gid).status == "finished"


def test_cannot_archive_giveaway_inside_game(fresh_db):
    game_id = fresh_db.add_game("Игра", country="mne")
    gid = fresh_db.add_giveaway("Приз", game_id=game_id, country="mne")
    assert fresh_db.archive_giveaway(gid) is False
    assert fresh_db.get_giveaway(gid).archived_at is None


def test_archive_twice_is_rejected(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    assert fresh_db.archive_giveaway(gid) is True
    assert fresh_db.archive_giveaway(gid) is False


def test_unarchive_non_archived_is_rejected(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    assert fresh_db.unarchive_giveaway(gid) is False


def test_archive_is_scoped_by_country(fresh_db):
    mne = fresh_db.add_giveaway("Приз MNE", country="mne")
    srb = fresh_db.add_giveaway("Приз SRB", country="srb")
    fresh_db.archive_giveaway(mne)
    fresh_db.archive_giveaway(srb)
    assert [g.id for g in fresh_db.get_archived_giveaways("mne")] == [mne]
    assert [g.id for g in fresh_db.get_archived_giveaways("srb")] == [srb]
    assert {g.id for g in fresh_db.get_archived_giveaways()} == {mne, srb}


def test_archived_copy_source_stays_usable_in_game(fresh_db):
    game_id = fresh_db.add_game("Игра", country="mne")
    src = fresh_db.add_giveaway("Приз", country="mne")
    copy_id = fresh_db.copy_giveaway(src, game_id)
    fresh_db.archive_giveaway(src)
    copy = fresh_db.get_giveaway(copy_id)
    assert copy.game_id == game_id
    assert copy.archived_at is None
