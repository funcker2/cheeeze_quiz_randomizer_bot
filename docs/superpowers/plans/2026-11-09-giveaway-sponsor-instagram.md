# Instagram спонсора — Implementation Plan

> **For agentic workers:** Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Необязательное поле «Instagram спонсора» у розыгрыша: шаг при создании с пропуском, правка в карточке, строка со ссылкой в конце поста анонса и в посте итогов.

**Architecture:** Колонка `sponsor_instagram TEXT` (handle). Чистый парсер в `instagram.py`. Строка спонсора рендерится в `_render_post(g)` поверх неизменённых `prize_text` / `prize_entities`; клавиатуры не меняются.

**Tech Stack:** Python 3.12, aiogram 3.x, sqlite3 (stdlib), pytest (только `instagram.py` и слой БД).

**Spec:** `docs/superpowers/specs/2026-11-09-giveaway-sponsor-instagram-design.md`

## Global Constraints

- Рантайм — **Python 3.12** (`Dockerfile`); команды используют `python3.12` явно.
- Тесты в `tests/`, запуск из корня, зависят только от `database.py`, `instagram.py` и stdlib. `pytest` — в `.venv-test/`, не в `requirements.txt`.
- `sponsor_instagram` добавляется **последним** в dataclass `Giveaway` и в `_GIVEAWAY_COLS`.
- Тексты кнопок и сообщений — русские, как в спеке: `⏭ Без спонсора`, `📸 Спонсор: @handle`, `📸 Спонсор: —`, `✖️ Убрать спонсора`; в канале — `Спонсор: 📸 Instagram @handle` и `Спонсор приза: 📸 Instagram @handle`.
- Все callback-хендлеры начинаются с `if not is_admin(cb.from_user.id): return`.
- Коммиты, push и деплой — **только по прямой отмашке пользователя**.

## File Structure

| Файл | Действие |
|---|---|
| `instagram.py` | Create |
| `database.py` | Modify |
| `bot.py` | Modify |
| `tests/test_instagram.py` | Create |
| `tests/test_sponsor_db.py` | Create |

---

### Task 0: Подготовка

- [ ] `git pull` локального `main` (отстаёт от `origin/main` на 3 коммита: архив).
- [ ] С OK пользователя (SSH на прод) сравнить сервер: `cd ~/cheeeze_quiz_randomizer_bot && git log -1 --oneline && git status -sb`. Если на сервере локальные правки — стоп, сообщить.
- [ ] Ветка `feat/giveaway-sponsor-instagram`.

### Task 1: Парсер `instagram.py`

- [ ] `tests/test_instagram.py`: все строки таблицы из спеки; отказ для `p/…`, `reel/…`, `stories/…`, чужого хоста, `..`, точки в начале/конце, >30 символов, пустой строки.
- [ ] `parse_instagram(raw) -> str | None`, `instagram_url(handle) -> str`.
- [ ] `python3.12 -m pytest tests/test_instagram.py` — зелёный.

### Task 2: Слой БД

- [ ] `tests/test_sponsor_db.py`: миграция на старой схеме добавляет колонку; `add_giveaway` с/без спонсора; `copy_giveaway` наследует; `update_giveaway` меняет и `clear_sponsor` обнуляет; guard статуса `active`.
- [ ] Поле в dataclass и `_GIVEAWAY_COLS` (последним), миграция в `init_db()`.
- [ ] `add_giveaway(..., sponsor_instagram=None)`, `copy_giveaway()` копирует, `update_giveaway(..., sponsor_instagram=None, clear_sponsor=False)`.
- [ ] `python3.12 -m pytest` — зелёный, включая `tests/test_archive.py`.

### Task 3: Рендер поста

- [ ] `_utf16_len(s)` и `_render_post(g) -> (text, entities)` в `bot.py`.
- [ ] Проверить вручную (REPL на 3.12) на тексте с эмодзи, кириллицей, жирным и ссылкой: offset `text_link` указывает ровно на `@handle`.
- [ ] `_publish_giveaway` (фото и текст) и `on_view_giveaway` используют `_render_post`.
- [ ] Текстовый пост без своих `url` / `text_link` — `link_preview_options=LinkPreviewOptions(is_disabled=True)`.

### Task 4: Шаг спонсора при создании

- [ ] `NewGiveaway.waiting_sponsor`; три контент-хендлера переводят в него вместо `_save_giveaway()`.
- [ ] Вопрос + inline `⏭ Без спонсора` (`spn_skip`); текст `-` / `нет` тоже пропускает.
- [ ] Ошибка разбора и ссылка на пост — разные тексты, остаёмся в состоянии.
- [ ] Проверка длины (1024 фото / 4096 текст) с подсказкой, на сколько сократить.
- [ ] `_save_giveaway()` передаёт спонсора; подтверждение показывает `📸 Спонсор: @handle`.

### Task 5: Правка в карточке

- [ ] `EditGiveaway.waiting_sponsor`; кнопки `edsp:{gid}` и `edsp_clr:{gid}` в `on_view_giveaway`; заголовок со спонсором.
- [ ] Проверка длины также в «Заменить текст» и «Заменить фото».
- [ ] Спонсор в панели «✅ Опубликовано» и в карточке завершённого розыгрыша.

### Task 6: Пост итогов

- [ ] `_send_result_to_channel()`: строка `Спонсор приза: 📸 <a href=…>Instagram @handle</a>` если спонсор есть, превью отключено.

### Task 7: Проверка

- [ ] `python3.12 -m pytest`; `python3.12 -m py_compile bot.py database.py instagram.py`.
- [ ] Ручной прогон с тестовым токеном и тестовым каналом: создание со спонсором, пропуск, ссылка на пост, длинная подпись, правка, удаление спонсора, публикация, участие, завершение по таймеру и по подтверждению, перевыбор после подтверждения. Строка спонсора на месте и кликабельна; форматирование текста админа цело; превью нет.

### Task 8: Деплой (только с OK)

- [ ] Нет активных розыгрышей на проде.
- [ ] Merge в `main`, push.
- [ ] Сервер: `cd ~/cheeeze_quiz_randomizer_bot && git pull && docker compose up --build -d`.
- [ ] Проверить миграцию: `docker exec cheeeze_quiz_randomizer_bot python3 -c "import sqlite3; print([r[1] for r in sqlite3.connect('data/randomizer.db').execute('PRAGMA table_info(giveaways)')])"` — есть `sponsor_instagram`.
- [ ] Откат: `git checkout <prev sha> && docker compose up --build -d` (колонка остаётся, старый код её игнорирует).
