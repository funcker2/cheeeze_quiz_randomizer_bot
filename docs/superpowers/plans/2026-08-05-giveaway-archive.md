# Архив розыгрышей — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Дать админу возможность убрать розыгрыш из библиотеки в архив, вернуть его обратно или удалить, при этом архивный розыгрыш нельзя добавить в игру.

**Architecture:** Новая колонка `archived_at TEXT` в `giveaways` — флаг, ортогональный статусу. Библиотека фильтруется по `archived_at IS NULL` в единственной функции `get_free_giveaways`, через которую уже ходят все три её экрана. Архив — отдельный экран по образцу библиотеки с урезанной карточкой розыгрыша.

**Tech Stack:** Python 3.12, aiogram 3.x, sqlite3 (stdlib), pytest (только для тестов слоя БД).

**Spec:** `docs/superpowers/specs/2026-08-05-giveaway-archive-design.md`

## Global Constraints

- Целевой рантайм — **Python 3.12** (`Dockerfile`). Локальный `python3` — 3.9 и **не запустит этот код** (синтаксис `str | None` в теле dataclass). Все команды в плане используют `python3.12` явно.
- Тесты живут в `tests/`, запускаются из корня репозитория, зависят только от `database.py` + stdlib. `aiogram` для них не нужен и не устанавливается.
- `pytest` ставится в `.venv-test/` (Homebrew Python блокирует установку в систему, PEP 668). `.venv-test/` добавляется в `.gitignore`, `pytest` **не** добавляется в `requirements.txt` — это образ бота, а не dev-окружение.
- Порядок полей в dataclass `Giveaway` обязан позиционно совпадать с `_GIVEAWAY_COLS` — распаковка идёт через `Giveaway(*row)`. `archived_at` добавляется **последним** в обоих местах.
- Текст кнопок и тостов — русский, ровно как в спеке, с эмодзи: `🗄 Архив (N)`, `🗄 Перенести в архив`, `↩️ Вернуть в библиотеку`, `◀️ Назад в архив`, `◀️ Назад в библиотеку`.
- Все callback-хендлеры начинаются с `if not is_admin(cb.from_user.id): return` — как весь остальной `bot.py`.
- Коммиты выполняются **только по прямой отмашке пользователя**. Шаги «Commit» ниже — заготовки команд, а не разрешение коммитить самостоятельно.

---

## File Structure

| Файл | Ответственность | Действие |
|---|---|---|
| `database.py` | Миграция колонки, фильтр библиотеки, три новые функции архива | Modify |
| `bot.py` | Экран архива, карточка архивного розыгрыша, хендлеры действий, кнопка в библиотеке, guard'ы от устаревших клавиатур | Modify |
| `tests/test_archive.py` | Тесты слоя БД: миграция, фильтрация, архивация/возврат, изоляция по стране | Create |
| `.gitignore` | Игнор `.venv-test/` | Modify |

Слой БД покрывается тестами (чистые функции над sqlite). Хендлеры `bot.py` проверяются `py_compile` + ручным прогоном в боте — вытаскивать их под тесты потребовало бы мокать весь aiogram, что для этой задачи не окупается.

---

### Task 1: Тестовое окружение и миграция колонки `archived_at`

**Files:**
- Create: `tests/test_archive.py`
- Modify: `.gitignore`
- Modify: `database.py:26-55` (dataclass `Giveaway`, `_GIVEAWAY_COLS`), `database.py:120-140` (миграции в `init_db`)

**Interfaces:**
- Consumes: ничего
- Produces: поле `Giveaway.archived_at: str | None` (последнее в dataclass), колонка `giveaways.archived_at` в схеме БД.

- [ ] **Step 1: Поднять тестовое окружение**

```bash
python3.12 -m venv .venv-test
.venv-test/bin/pip install pytest
mkdir -p tests
```

Добавить в `.gitignore` строку `.venv-test/` (после `data/`).

- [ ] **Step 2: Написать падающий тест**

Создать `tests/test_archive.py`:

```python
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
    import sqlite3
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


def test_new_giveaway_is_not_archived(fresh_db):
    gid = fresh_db.add_giveaway("Приз", country="mne")
    assert fresh_db.get_giveaway(gid).archived_at is None
```

- [ ] **Step 3: Запустить и убедиться, что тесты падают**

Run: `.venv-test/bin/pytest tests/test_archive.py -v`
Expected: FAIL — `test_migration_adds_archived_at_column` и `test_migration_is_idempotent` падают на `assert "archived_at" in ...`; `test_new_giveaway_is_not_archived` падает с `AttributeError: 'Giveaway' object has no attribute 'archived_at'`.

- [ ] **Step 4: Добавить поле в dataclass**

В `database.py` в dataclass `Giveaway` после `source_id: int | None` добавить последней строкой:

```python
    archived_at: str | None
```

- [ ] **Step 5: Добавить колонку в `_GIVEAWAY_COLS`**

```python
_GIVEAWAY_COLS = (
    "id, game_id, name, prize_text, prize_entities, photo_id, winner_count, "
    "status, created_at, channel_message_id, channel_id, draw_deadline, admin_chat_id, country, source_id, "
    "archived_at"
)
```

- [ ] **Step 6: Добавить миграцию в `init_db`**

В `init_db()` после блока `if "source_id" not in cols:` добавить:

```python
    if "archived_at" not in cols:
        conn.execute("ALTER TABLE giveaways ADD COLUMN archived_at TEXT")
```

Блок `CREATE TABLE IF NOT EXISTS giveaways` не трогать — колонка добавляется только миграцией, как `game_id`, `country` и `source_id`. Так путь `ALTER TABLE` исполняется и на свежей БД, то есть тестируется на каждом прогоне.

- [ ] **Step 7: Запустить тесты**

Run: `.venv-test/bin/pytest tests/test_archive.py -v`
Expected: PASS, 3 passed.

- [ ] **Step 8: Commit (по отмашке пользователя)**

```bash
git add .gitignore database.py tests/test_archive.py
git commit -m "feat: add archived_at column to giveaways"
```

---

### Task 2: Функции архива в слое БД

**Files:**
- Modify: `database.py:270-283` (`get_free_giveaways`), новые функции сразу после неё
- Test: `tests/test_archive.py`

**Interfaces:**
- Consumes: `Giveaway.archived_at` из Task 1
- Produces:
  - `archive_giveaway(gid: int) -> bool`
  - `unarchive_giveaway(gid: int) -> bool`
  - `get_archived_giveaways(country: str | None = None) -> list[Giveaway]`
  - изменённое поведение `get_free_giveaways(country) -> list[Giveaway]` (архивные исключены)

- [ ] **Step 1: Написать падающие тесты**

Дописать в конец `tests/test_archive.py`:

```python
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
```

- [ ] **Step 2: Запустить и убедиться, что тесты падают**

Run: `.venv-test/bin/pytest tests/test_archive.py -v`
Expected: FAIL — `AttributeError: module 'database' has no attribute 'archive_giveaway'` (и то же для `unarchive_giveaway`, `get_archived_giveaways`).

- [ ] **Step 3: Отфильтровать архивные из библиотеки**

В `get_free_giveaways` добавить `AND archived_at IS NULL` в **оба** ветвления запроса:

```python
def get_free_giveaways(country: str | None = None) -> list[Giveaway]:
    """Giveaways not assigned to any game and not archived (reusable library)."""
    conn = _conn()
    if country:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') "
            "AND archived_at IS NULL AND country=? ORDER BY id",
            (country,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') "
            "AND archived_at IS NULL ORDER BY id"
        ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]
```

- [ ] **Step 4: Добавить три функции архива**

Сразу после `get_free_giveaways`:

```python
def get_archived_giveaways(country: str | None = None) -> list[Giveaway]:
    """Library giveaways moved to the archive, newest first."""
    conn = _conn()
    if country:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') "
            "AND archived_at IS NOT NULL AND country=? ORDER BY archived_at DESC",
            (country,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT {_GIVEAWAY_COLS} FROM giveaways WHERE game_id IS NULL AND status IN ('queued','finished') "
            "AND archived_at IS NOT NULL ORDER BY archived_at DESC"
        ).fetchall()
    conn.close()
    return [Giveaway(*r) for r in rows]


def archive_giveaway(gid: int) -> bool:
    """Move a library giveaway to the archive. Copies bound to a game cannot be archived."""
    conn = _conn()
    cur = conn.execute(
        "UPDATE giveaways SET archived_at=? WHERE id=? AND game_id IS NULL AND archived_at IS NULL",
        (_now(), gid),
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def unarchive_giveaway(gid: int) -> bool:
    """Return an archived giveaway to the library, keeping its status."""
    conn = _conn()
    cur = conn.execute(
        "UPDATE giveaways SET archived_at=NULL WHERE id=? AND archived_at IS NOT NULL", (gid,)
    )
    conn.commit()
    conn.close()
    return cur.rowcount > 0
```

Фильтр `status IN ('queued','finished')` в архиве зеркалит библиотеку: вместе два запроса покрывают ровно то же множество строк, что библиотека покрывала до фичи.

- [ ] **Step 5: Запустить тесты**

Run: `.venv-test/bin/pytest tests/test_archive.py -v`
Expected: PASS, 12 passed.

- [ ] **Step 6: Commit (по отмашке пользователя)**

```bash
git add database.py tests/test_archive.py
git commit -m "feat: add archive/unarchive giveaway queries"
```

---

### Task 3: Экран архива и кнопка «Архив» в библиотеке

**Files:**
- Modify: `bot.py:613-678` (`_show_library`, добавить `_show_archive` и хендлеры после `on_lib_page`)

**Interfaces:**
- Consumes: `db.get_archived_giveaways(country)` из Task 2
- Produces: корутина `_show_archive(chat_id: int, message_id: int | None = None, page: int = 0, user_id: int | None = None) -> None`; callback'и `show_archive`, `arcpage:{page}`, `arcview:{gid}` (последний обрабатывается в Task 4).

- [ ] **Step 1: Добавить кнопку «Архив» в обе ветки `_show_library`**

В начале `_show_library`, сразу после `free = db.get_free_giveaways(country=country)`:

```python
    archived_count = len(db.get_archived_giveaways(country=country))
    archive_row = [InlineKeyboardButton(text=f"🗄 Архив ({archived_count})", callback_data="show_archive")]
```

В ветке пустой библиотеки заменить сборку `kb` на:

```python
        rows = [[InlineKeyboardButton(text="➕ Добавить розыгрыш", callback_data="go_new_lib")]]
        if archived_count:
            rows.append(archive_row)
        rows.append([InlineKeyboardButton(text="◀️ Меню", callback_data="go_menu")])
        kb = InlineKeyboardMarkup(inline_keyboard=rows)
```

В основной ветке между `buttons.append([... "➕ Добавить розыгрыш" ...])` и `buttons.append([... "◀️ Меню" ...])` вставить:

```python
    if archived_count:
        buttons.append(archive_row)
```

Кнопка в пустой ветке критична: библиотека может опустеть именно потому, что всё уехало в архив, и без неё архив станет недостижим.

- [ ] **Step 2: Добавить `_show_archive`**

После хендлера `on_lib_page` (`bot.py:672-678`):

```python
async def _show_archive(chat_id: int, message_id: int | None = None, page: int = 0, user_id: int | None = None) -> None:
    country = _get_country(user_id if user_id is not None else chat_id)
    archived = db.get_archived_giveaways(country=country)
    buttons: list[list[InlineKeyboardButton]] = []
    if not archived:
        text = "🗄 Архив пуст."
    else:
        page_items, total_pages = _paginate(archived, page)
        for g in page_items:
            label = _giveaway_label(g)
            icon = "🖼" if g.photo_id else "📝"
            buttons.append([
                InlineKeyboardButton(text=f"{icon} #{g.id}: {label}", callback_data=f"arcview:{g.id}"),
            ])
        if total_pages > 1:
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton(text="◀️", callback_data=f"arcpage:{page - 1}"))
            nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
            if page < total_pages - 1:
                nav.append(InlineKeyboardButton(text="▶️", callback_data=f"arcpage:{page + 1}"))
            buttons.append(nav)
        text = f"🗄 Архив розыгрышей ({len(archived)}):"
    buttons.append([InlineKeyboardButton(text="◀️ Назад в библиотеку", callback_data="show_library")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data == "show_archive")
async def on_show_archive(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await _show_archive(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data.startswith("arcpage:"))
async def on_archive_page(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    page = int(cb.data.split(":")[1])
    await _show_archive(cb.message.chat.id, cb.message.message_id, page, user_id=cb.from_user.id)
    await cb.answer()
```

- [ ] **Step 3: Проверить синтаксис**

Run: `python3.12 -m py_compile bot.py database.py && echo OK`
Expected: `OK` без трейсбека.

- [ ] **Step 4: Прогнать тесты БД (регрессия)**

Run: `.venv-test/bin/pytest tests/test_archive.py -v`
Expected: PASS, 12 passed.

- [ ] **Step 5: Commit (по отмашке пользователя)**

```bash
git add bot.py
git commit -m "feat: add archive screen with library entry button"
```

---

### Task 4: Карточка архивного розыгрыша и действия архивации/возврата

**Files:**
- Modify: `bot.py:1246-1288` (`on_view_giveaway` — кнопка «Перенести в архив»), новые хендлеры после `on_view_giveaway`

**Interfaces:**
- Consumes: `db.archive_giveaway`, `db.unarchive_giveaway`, `Giveaway.archived_at` (Task 1–2); `_show_archive` (Task 3)
- Produces: callback'и `arch:{gid}`, `unarch:{gid}`, обработчик `arcview:{gid}`

- [ ] **Step 1: Добавить кнопку «Перенести в архив» в карточку библиотечного розыгрыша**

В `on_view_giveaway` заменить сборку `edit_buttons` на:

```python
    name_label = g.name or "—"
    edit_buttons = [
        [InlineKeyboardButton(text=f"📝 Название: {name_label}", callback_data=f"ednm:{gid}")],
        [
            InlineKeyboardButton(text="🖼 Заменить фото", callback_data=f"edph:{gid}"),
            InlineKeyboardButton(text="✏️ Заменить текст", callback_data=f"edtx:{gid}"),
        ],
    ]
    if not g.game_id:
        edit_buttons.append([InlineKeyboardButton(text="🗄 Перенести в архив", callback_data=f"arch:{gid}")])
    edit_buttons.append([InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del:{gid}")])
```

Остальная часть функции (ветка `if g.game_id:` с кнопками «Назад к игре» / «Назад в библиотеку», заголовок, отправка) не меняется.

- [ ] **Step 2: Добавить хендлеры карточки и действий**

Сразу после `on_view_giveaway` (перед секцией `# ── Edit giveaway ──`):

```python
@router.callback_query(F.data.startswith("arcview:"))
async def on_archive_view(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or not g.archived_at:
        await _show_archive(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
        await cb.answer("Розыгрыш не в архиве")
        return

    entities = _deserialize_entities(g.prize_entities)
    if g.photo_id:
        await cb.message.answer_photo(
            photo=g.photo_id,
            caption=g.prize_text,
            caption_entities=entities,
        )
    else:
        await bot.send_message(
            chat_id=cb.message.chat.id,
            text=g.prize_text,
            entities=entities,
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↩️ Вернуть в библиотеку", callback_data=f"unarch:{gid}")],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del:{gid}")],
        [InlineKeyboardButton(text="◀️ Назад в архив", callback_data="show_archive")],
    ])
    await cb.message.answer(f"🗄 Розыгрыш #{g.id} — в архиве", reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("arch:"))
async def on_archive(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    if db.archive_giveaway(gid):
        await _show_library(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
        await cb.answer(f"#{gid} перенесён в архив")
    else:
        await cb.answer("Не удалось перенести в архив")


@router.callback_query(F.data.startswith("unarch:"))
async def on_unarchive(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    if db.unarchive_giveaway(gid):
        await _show_library(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
        await cb.answer(f"#{gid} возвращён в библиотеку")
    else:
        await cb.answer("Не удалось вернуть")
```

Коллизий префиксов нет: `"arcview:1".startswith("view:")` → `False`, `"unarch:1".startswith("arch:")` → `False`. Удаление из архива переиспользует существующий `del:{gid}` (`bot.py:1423`), который уводит на экран библиотеки — строки больше нет, оставаться в архиве незачем.

- [ ] **Step 3: Проверить синтаксис**

Run: `python3.12 -m py_compile bot.py && echo OK`
Expected: `OK`

- [ ] **Step 4: Проверить отсутствие дублей callback-префиксов**

Run: `grep -n 'arcview:\|"arch:\|unarch:\|show_archive\|arcpage:' bot.py`
Expected: каждый префикс объявлен ровно в одном `@router.callback_query(...)`; остальные вхождения — это `callback_data` в кнопках.

- [ ] **Step 5: Commit (по отмашке пользователя)**

```bash
git add bot.py
git commit -m "feat: add archived giveaway card with restore and delete"
```

---

### Task 5: Guard от устаревших клавиатур при добавлении в игру

**Files:**
- Modify: `bot.py:753-763` (`on_library_pick`), `bot.py:766-776` (`on_lib_to_game`)

**Interfaces:**
- Consumes: `Giveaway.archived_at` (Task 1)
- Produces: ничего для последующих задач

Сообщения в Telegram живут долго: админ может нажать «Добавить в игру» на экране, отрисованном до архивации. Списки такой розыгрыш уже не показывают, но старая клавиатура — да.

- [ ] **Step 1: Закрыть `gpick:`**

В `on_library_pick` после разбора `parts` и перед `db.copy_giveaway(...)`:

```python
    src = db.get_giveaway(gid)
    if not src or src.archived_at:
        await cb.answer("Розыгрыш в архиве — сначала верни его в библиотеку")
        return
```

- [ ] **Step 2: Закрыть `lib2game:`**

В `on_lib_to_game` — тот же блок, тоже перед `db.copy_giveaway(...)`:

```python
    src = db.get_giveaway(gid)
    if not src or src.archived_at:
        await cb.answer("Розыгрыш в архиве — сначала верни его в библиотеку")
        return
```

- [ ] **Step 3: Проверить синтаксис и прогнать тесты**

Run: `python3.12 -m py_compile bot.py && .venv-test/bin/pytest tests/test_archive.py -v`
Expected: `py_compile` молча проходит, pytest — 12 passed.

- [ ] **Step 4: Commit (по отмашке пользователя)**

```bash
git add bot.py
git commit -m "feat: reject adding archived giveaway to game from stale keyboard"
```

---

## Ручной прогон в боте

Автотесты покрывают слой БД; экраны проверяются руками. Из спеки, по шагам:

- [ ] Библиотека → открыть розыгрыш → есть «🗄 Перенести в архив» (у розыгрыша внутри игры её нет).
- [ ] Нажать → он исчез из библиотеки, счётчик «📚 В библиотеке: N» в главном меню уменьшился, появилась «🗄 Архив (1)».
- [ ] Игра → «📚 Добавить из библиотеки» → архивного в списке нет.
- [ ] Архив → тап на розыгрыш → превью корректное (фото/текст/форматирование), кнопок игр и редактирования нет.
- [ ] «↩️ Вернуть в библиотеку» → он снова в библиотеке, название и статус те же, добавляется в игру как обычно.
- [ ] Заархивировать `finished`-розыгрыш и вернуть → статус остался `finished`.
- [ ] Заархивировать всё → библиотека пуста, но «🗄 Архив (N)» на месте и открывается.
- [ ] Удалить из архива → строки нет ни в библиотеке, ни в архиве.
- [ ] Перезапуск бота на существующей `data/randomizer.db` → миграция прошла, старые розыгрыши в библиотеке.
- [ ] Архив одной страны не виден админам другой.
