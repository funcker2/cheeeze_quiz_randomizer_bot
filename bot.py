import asyncio
import json
import logging
import random
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
)

import database as db
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

cfg = Config.from_env()
bot = Bot(token=cfg.bot_token)
cmd_router = Router(name="commands")
router = Router(name="states")

_draw_timers: dict[int, asyncio.Task] = {}
_draw_deadlines: dict[int, datetime] = {}
_pending_winners: dict[int, list[db.Participant]] = {}
_admin_panels: dict[int, tuple[int, int]] = {}
_button_update_tasks: dict[int, asyncio.Task] = {}
_user_country: dict[int, str] = {}


class NewGame(StatesGroup):
    waiting_name = State()


class NewGiveaway(StatesGroup):
    waiting_name = State()
    waiting_content = State()
    waiting_text_for_photo = State()


class EditGiveaway(StatesGroup):
    waiting_name = State()
    waiting_photo = State()
    waiting_text = State()


class PublishGiveaway(StatesGroup):
    waiting_custom_time = State()


def is_admin(user_id: int) -> bool:
    return cfg.is_admin(user_id)


def _get_country(user_id: int) -> str | None:
    if user_id in _user_country:
        return _user_country[user_id]
    saved = db.get_setting(f"user_country:{user_id}")
    if saved:
        _user_country[user_id] = saved
    return saved


def _country_channels(country_code: str):
    c = cfg.country_by_code(country_code)
    return c.channels if c else ()


def _channel_by_country_idx(country_code: str, idx: int):
    channels = _country_channels(country_code)
    if 0 <= idx < len(channels):
        return channels[idx]
    return None


def _cooldown(country_code: str | None = None) -> int:
    if country_code:
        val = db.get_setting(f"cooldown_minutes:{country_code}")
        if val:
            return int(val)
        c = cfg.country_by_code(country_code)
        if c:
            return c.default_cooldown_minutes
    val = db.get_setting("cooldown_minutes")
    if val:
        return int(val)
    return 120


def _display(p: db.Participant) -> str:
    if p.username:
        return f"@{p.username} ({p.full_name})"
    return p.full_name


def _serialize_entities(entities: list[MessageEntity] | None) -> str | None:
    if not entities:
        return None
    return json.dumps([e.model_dump(exclude_none=True) for e in entities])


def _deserialize_entities(data: str | None) -> list[MessageEntity] | None:
    if not data:
        return None
    try:
        return [MessageEntity.model_validate(e) for e in json.loads(data)]
    except Exception:
        return None


# ── Keyboards ────────────────────────────────────────────

def _kb_participate(gid: int, count: int = 0) -> InlineKeyboardMarkup:
    text = "Участвую! 🎲"
    if count > 0:
        text += f" ({count})"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=text, callback_data=f"p:{gid}")],
    ])


def _kb_finished(gid: int, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Завершён ✅ ({count})", callback_data=f"p:{gid}")],
    ])


def _kb_control(gid: int, count: int) -> InlineKeyboardMarkup:
    g = db.get_giveaway(gid)
    buttons = [
        [InlineKeyboardButton(text=f"⚡ Определить победителя сейчас ({count})", callback_data=f"drw:{gid}")],
        [InlineKeyboardButton(text=f"👤 Выбрать из списка ({count})", callback_data=f"pick:{gid}:0")],
        [InlineKeyboardButton(text="🔄 Обновить кол-во участников", callback_data=f"ref:{gid}")],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 Назад к игре", callback_data=f"game:{g.game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _kb_active_list(active: list[db.Giveaway]) -> InlineKeyboardMarkup:
    buttons = []
    for g in active:
        label = g.prize_text[:30] + ("…" if len(g.prize_text) > 30 else "")
        count = db.participant_count(g.id)
        buttons.append([
            InlineKeyboardButton(
                text=f"🟢 #{g.id}: {label} ({count} уч.)",
                callback_data=f"act:{g.id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="🎮 К играм", callback_data="show_games")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _remaining_time(gid: int) -> str:
    deadline = _draw_deadlines.get(gid)
    if not deadline:
        g = db.get_giveaway(gid)
        if g and g.draw_deadline:
            deadline = datetime.fromisoformat(g.draw_deadline)
        else:
            return "✋ Ручной режим"
    now = datetime.now(timezone.utc)
    diff = (deadline - now).total_seconds()
    if diff <= 0:
        return "⏱ Розыгрыш вот-вот начнётся"
    minutes = int(diff // 60)
    seconds = int(diff % 60)
    if minutes > 0:
        return f"⏱ До розыгрыша: {minutes} мин {seconds} сек"
    return f"⏱ До розыгрыша: {seconds} сек"


def _kb_winner(gid: int) -> InlineKeyboardMarkup:
    g = db.get_giveaway(gid)
    buttons = [
        [
            InlineKeyboardButton(text="🔄 Перевыбрать", callback_data=f"rr:{gid}"),
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"cfm:{gid}"),
        ],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _kb_post_winner(gid: int) -> InlineKeyboardMarkup:
    g = db.get_giveaway(gid)
    buttons = [
        [
            InlineKeyboardButton(text="🔄 Перевыбрать", callback_data=f"post_rr:{gid}"),
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"post_cfm:{gid}"),
        ],
        [InlineKeyboardButton(text="◀️ К розыгрышу", callback_data=f"fin:{gid}")],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _giveaway_label(g: db.Giveaway) -> str:
    if g.name:
        return g.name[:30] + ("…" if len(g.name) > 30 else "")
    return g.prize_text[:30] + ("…" if len(g.prize_text) > 30 else "")


PAGE_SIZE = 10


def _paginate(items: list, page: int) -> tuple[list, int]:
    total_pages = max(1, (len(items) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    return items[start:start + PAGE_SIZE], total_pages


def _kb_game_giveaways(game: db.Game, queue: list[db.Giveaway], page: int = 0) -> InlineKeyboardMarkup:
    page_items, total_pages = _paginate(queue, page)
    buttons = []
    for g in page_items:
        label = _giveaway_label(g)
        icon = "🖼" if g.photo_id else "📝"
        buttons.append([
            InlineKeyboardButton(
                text=f"{icon} #{g.id}: {label}",
                callback_data=f"view:{g.id}",
            ),
        ])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"gpage:{game.id}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"gpage:{game.id}:{page + 1}"))
        buttons.append(nav)
    buttons.append([
        InlineKeyboardButton(text="📚 Добавить из библиотеки", callback_data=f"glib:{game.id}"),
    ])
    buttons.append([
        InlineKeyboardButton(text="🟢 Активные", callback_data="show_active"),
        InlineKeyboardButton(text="🔴 Завершённые", callback_data=f"show_fin:{game.id}"),
    ])
    buttons.append([InlineKeyboardButton(text="◀️ К списку игр", callback_data="show_games")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ── Debounced button count update on channel post ────────

def _schedule_button_update(gid: int) -> None:
    if gid in _button_update_tasks and not _button_update_tasks[gid].done():
        return
    _button_update_tasks[gid] = asyncio.create_task(_do_button_update(gid))


async def _do_button_update(gid: int) -> None:
    await asyncio.sleep(2)
    _button_update_tasks.pop(gid, None)
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        return
    count = db.participant_count(gid)
    try:
        await bot.edit_message_reply_markup(
            chat_id=g.channel_id,
            message_id=g.channel_message_id,
            reply_markup=_kb_participate(gid, count),
        )
    except Exception:
        pass


# ── /start ───────────────────────────────────────────────

@cmd_router.message(Command("start", "menu"))
async def cmd_start(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    countries = cfg.admin_countries(message.from_user.id)
    if len(countries) == 1:
        _user_country[message.from_user.id] = countries[0].code
        db.set_setting(f"user_country:{message.from_user.id}", countries[0].code)
        await _show_main_menu(message.chat.id, user_id=message.from_user.id)
    else:
        await _show_country_picker(message.chat.id)


async def _show_country_picker(chat_id: int, message_id: int | None = None) -> None:
    buttons = [
        [InlineKeyboardButton(text=c.label, callback_data=f"country:{c.code}")]
        for c in cfg.countries
    ]
    text = "🌍 Выбери страну:"
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data.startswith("country:"))
async def on_country_select(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    code = cb.data.split(":")[1]
    country = cfg.country_by_code(code)
    if not country:
        await cb.answer("Неизвестная страна")
        return
    if cb.from_user.id not in country.admin_ids:
        await cb.answer("Нет доступа к этой стране")
        return
    _user_country[cb.from_user.id] = code
    db.set_setting(f"user_country:{cb.from_user.id}", code)
    await _show_main_menu(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer(f"{country.label}")


async def _show_main_menu(chat_id: int, message_id: int | None = None, user_id: int | None = None) -> None:
    uid = user_id if user_id is not None else chat_id
    country_code = _get_country(uid)
    country = cfg.country_by_code(country_code) if country_code else None
    country_label = country.label if country else "?"
    cd = _cooldown(country_code)
    buttons = [
        [InlineKeyboardButton(text="🎮 Мои игры", callback_data="show_games")],
        [InlineKeyboardButton(text="📚 Библиотека", callback_data="show_library")],
        [InlineKeyboardButton(text=f"⏳ Кулдаун: {cd} мин", callback_data="show_cooldown")],
        [InlineKeyboardButton(text="🏆 Победители на кулдауне", callback_data="show_winners")],
        [InlineKeyboardButton(text="🔄 Сброс кулдауна", callback_data="reset_cooldowns")],
        [InlineKeyboardButton(text=f"🌍 {country_label}", callback_data="switch_country")],
    ]
    text = f"🎲 Бот-рандомайзер — {country_label}"
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data == "switch_country")
async def on_switch_country(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await _show_country_picker(cb.message.chat.id, cb.message.message_id)
    await cb.answer()


# ── Game management ──────────────────────────────────────

@cmd_router.message(Command("newgame"))
async def cmd_newgame(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    country = _get_country(message.from_user.id)
    if not country:
        await message.answer("Сначала выбери страну: /start")
        return
    parts = message.text.strip().split(maxsplit=1)
    if len(parts) > 1 and parts[1].strip():
        name = parts[1].strip()
        gid = db.add_game(name, country=country)
        await message.answer(
            f"🎮 Игра «{name}» создана!\n\nДобавляй розыгрыши:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Добавить розыгрыш", callback_data=f"gadd:{gid}")],
                [InlineKeyboardButton(text="◀️ К списку игр", callback_data="show_games")],
            ]),
        )
    else:
        await state.clear()
        await message.answer("🎮 Введи название игры:")
        await state.set_state(NewGame.waiting_name)


@router.message(NewGame.waiting_name, F.text)
async def on_game_name(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    name = message.text.strip()
    if not name:
        await message.answer("Название не может быть пустым.")
        return
    country = _get_country(message.from_user.id)
    gid = db.add_game(name, country=country)
    await state.clear()
    game = db.get_game(gid)
    await _show_game(message.chat.id, game)


@cmd_router.message(Command("games"))
async def cmd_games(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await _show_games(message.chat.id, user_id=message.from_user.id)


async def _show_games(chat_id: int, message_id: int | None = None, user_id: int | None = None) -> None:
    country = _get_country(user_id if user_id is not None else chat_id)
    games = db.get_games(country=country)
    if not games:
        text = "🎮 Нет игр."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать игру", callback_data="new_game")],
            [InlineKeyboardButton(text="◀️ Меню", callback_data="go_menu")],
        ])
        if message_id:
            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
                return
            except Exception:
                pass
        await bot.send_message(chat_id, text, reply_markup=kb)
        return

    buttons = []
    for g in games:
        queued, active, finished = db.game_giveaway_count(g.id)
        status = ""
        if active > 0:
            status += f" 🟢{active}"
        if queued > 0:
            status += f" 📋{queued}"
        if finished > 0:
            status += f" ✅{finished}"
        buttons.append([
            InlineKeyboardButton(
                text=f"🎮 {g.name}{status}",
                callback_data=f"game:{g.id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="➕ Создать игру", callback_data="new_game")])
    buttons.append([InlineKeyboardButton(text="◀️ Меню", callback_data="go_menu")])

    text = f"🎮 Мои игры ({len(games)}):"
    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data == "show_games")
async def on_show_games(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await _show_games(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data == "new_game")
async def on_new_game(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.edit_text("🎮 Введи название игры:")
    await state.set_state(NewGame.waiting_name)
    await cb.answer()


@router.callback_query(F.data.startswith("game:"))
async def on_game(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    game = db.get_game(game_id)
    if not game:
        await cb.answer("Игра не найдена")
        return
    await _show_game(cb.message.chat.id, game, cb.message.message_id)
    await cb.answer()


async def _show_game(chat_id: int, game: db.Game, message_id: int | None = None) -> None:
    queue = db.get_queued(game.id)
    queued, active, finished = db.game_giveaway_count(game.id)
    text = (
        f"🎮 {game.name}\n\n"
        f"📋 В очереди: {queued}\n"
        f"🟢 Активных: {active}\n"
        f"✅ Завершено: {finished}"
    )
    kb = _kb_game_giveaways(game, queue)
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🗑 Удалить игру", callback_data=f"delask:{game.id}")
    ])

    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data.startswith("delask:"))
async def on_delete_game_ask(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    game = db.get_game(game_id)
    if not game:
        await cb.answer("Не найдена")
        return
    queued, active, finished = db.game_giveaway_count(game_id)
    text = (
        f"❗ Удалить игру «{game.name}»?\n\n"
        f"📋 В очереди: {queued}\n"
        f"🟢 Активных: {active}\n"
        f"✅ Завершено: {finished}\n\n"
        "Все копии розыгрышей в этой игре будут удалены. Оригиналы в библиотеке сохранятся."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"delgame:{game_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"game:{game_id}"),
        ],
    ])
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("delgame:"))
async def on_delete_game(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    game = db.get_game(game_id)
    if not game:
        await cb.answer("Не найдена")
        return
    db.delete_game(game_id)
    await _show_games(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer(f"Игра «{game.name}» удалена")


# ── Library (free giveaways) ────────────────────────────

async def _show_library(chat_id: int, message_id: int | None = None, page: int = 0, user_id: int | None = None) -> None:
    country = _get_country(user_id if user_id is not None else chat_id)
    free = db.get_free_giveaways(country=country)
    if not free:
        text = "📚 Библиотека пуста."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить розыгрыш", callback_data="go_new_lib")],
            [InlineKeyboardButton(text="◀️ Меню", callback_data="go_menu")],
        ])
        if message_id:
            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
                return
            except Exception:
                pass
        await bot.send_message(chat_id, text, reply_markup=kb)
        return

    page_items, total_pages = _paginate(free, page)
    buttons = []
    for g in page_items:
        label = _giveaway_label(g)
        icon = "🖼" if g.photo_id else "📝"
        buttons.append([
            InlineKeyboardButton(
                text=f"{icon} #{g.id}: {label}",
                callback_data=f"view:{g.id}",
            ),
        ])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"libpage:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"libpage:{page + 1}"))
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="➕ Добавить розыгрыш", callback_data="go_new_lib")])
    buttons.append([InlineKeyboardButton(text="◀️ Меню", callback_data="go_menu")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    text = f"📚 Библиотека розыгрышей ({len(free)}):"
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            return
        except Exception:
            pass
    await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data == "show_library")
async def on_show_library(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await _show_library(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data.startswith("libpage:"))
async def on_lib_page(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    page = int(cb.data.split(":")[1])
    await _show_library(cb.message.chat.id, cb.message.message_id, page, user_id=cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data.startswith("gpage:"))
async def on_game_page(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    game_id, page = int(parts[1]), int(parts[2])
    game = db.get_game(game_id)
    if not game:
        await cb.answer("Игра не найдена")
        return
    queue = db.get_queued(game.id)
    queued, active, finished = db.game_giveaway_count(game.id)
    text = (
        f"🎮 {game.name}\n\n"
        f"📋 В очереди: {queued}\n"
        f"🟢 Активных: {active}\n"
        f"✅ Завершено: {finished}"
    )
    kb = _kb_game_giveaways(game, queue, page)
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="🗑 Удалить игру", callback_data=f"delask:{game.id}")
    ])
    try:
        await bot.edit_message_text(text, chat_id=cb.message.chat.id, message_id=cb.message.message_id, reply_markup=kb)
    except Exception:
        pass
    await cb.answer()


@router.callback_query(F.data == "noop")
async def on_noop(cb: CallbackQuery) -> None:
    await cb.answer()


@router.callback_query(F.data.startswith("glib:"))
async def on_game_library(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    country = _get_country(cb.from_user.id)
    free = db.get_free_giveaways(country=country)
    if not free:
        await cb.answer("Библиотека пуста — все розыгрыши привязаны к играм")
        return

    buttons = []
    for g in free:
        label = _giveaway_label(g)
        icon = "🖼" if g.photo_id else "📝"
        buttons.append([
            InlineKeyboardButton(
                text=f"{icon} #{g.id}: {label}",
                callback_data=f"gpick:{game_id}:{g.id}",
            )
        ])
    buttons.append([InlineKeyboardButton(text="◀️ Назад к игре", callback_data=f"game:{game_id}")])

    try:
        await cb.message.edit_text(
            f"📚 Библиотека розыгрышей ({len(free)}):\n"
            "Нажми чтобы добавить в игру:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    except Exception:
        await cb.message.answer(
            f"📚 Библиотека розыгрышей ({len(free)}):\n"
            "Нажми чтобы добавить в игру:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        )
    await cb.answer()


@router.callback_query(F.data.startswith("gpick:"))
async def on_library_pick(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    game_id, gid = int(parts[1]), int(parts[2])
    db.copy_giveaway(gid, game_id)
    game = db.get_game(game_id)
    if game:
        await _show_game(cb.message.chat.id, game, cb.message.message_id)
    await cb.answer(f"Копия розыгрыша #{gid} добавлена в игру")


@router.callback_query(F.data.startswith("lib2game:"))
async def on_lib_to_game(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    gid, game_id = int(parts[1]), int(parts[2])
    db.copy_giveaway(gid, game_id)
    game = db.get_game(game_id)
    if game:
        await _show_game(cb.message.chat.id, game, cb.message.message_id)
    await cb.answer(f"Копия розыгрыша #{gid} добавлена в «{game.name}»" if game else f"Копия розыгрыша #{gid} добавлена")


# ── Add giveaway to game ────────────────────────────────

@router.callback_query(F.data.startswith("gadd:"))
async def on_game_add(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    await state.clear()
    await state.update_data(game_id=game_id)
    await bot.send_message(
        chat_id=cb.message.chat.id,
        text="✏️ Введи внутреннее название розыгрыша (видно только админам):",
    )
    await state.set_state(NewGiveaway.waiting_name)
    await cb.answer()


# ── Create giveaway ──────────────────────────────────────

@cmd_router.message(Command("new"))
async def cmd_new(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    country = _get_country(message.from_user.id)
    games = db.get_games(country=country)
    if not games:
        await message.answer("Сначала создай игру: /newgame")
        return
    if len(games) == 1:
        await state.clear()
        await state.update_data(game_id=games[0].id)
        await message.answer(
            f"🎮 Игра: {games[0].name}\n\n"
            "✏️ Введи внутреннее название розыгрыша (видно только админам):",
        )
        await state.set_state(NewGiveaway.waiting_name)
    else:
        buttons = [
            [InlineKeyboardButton(text=f"🎮 {g.name}", callback_data=f"gadd:{g.id}")]
            for g in games
        ]
        await message.answer("Выбери игру для розыгрыша:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@router.message(NewGiveaway.waiting_name, F.text)
async def on_giveaway_name(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.update_data(giveaway_name=message.text.strip())
    await message.answer(
        "🎁 Теперь отправь пост для розыгрыша:\n"
        "• фото с подписью (текст + фото в одном сообщении)\n"
        "• или просто текст\n\n"
        "Можно использовать форматирование: жирный, курсив, ссылки, эмодзи."
    )
    await state.set_state(NewGiveaway.waiting_content)


@router.message(NewGiveaway.waiting_name)
async def on_giveaway_name_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Введи текстовое название. /cancel — отмена.")


@router.message(NewGiveaway.waiting_content, F.photo)
async def on_content_photo(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    photo_id = message.photo[-1].file_id
    if message.caption:
        entities_json = _serialize_entities(message.caption_entities)
        await state.update_data(
            photo_id=photo_id,
            prize_text=message.caption,
            prize_entities=entities_json,
        )
        await _save_giveaway(message, state)
    else:
        await state.update_data(photo_id=photo_id)
        await message.answer("✅ Фото получено. Теперь отправь описание приза:")
        await state.set_state(NewGiveaway.waiting_text_for_photo)


@router.message(NewGiveaway.waiting_content, F.text)
async def on_content_text(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    entities_json = _serialize_entities(message.entities)
    await state.update_data(
        photo_id=None,
        prize_text=message.text,
        prize_entities=entities_json,
    )
    await _save_giveaway(message, state)


@router.message(NewGiveaway.waiting_content)
async def on_content_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Отправь фото с подписью или текстовое сообщение.")


@router.message(NewGiveaway.waiting_text_for_photo, F.text)
async def on_text_for_photo(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    entities_json = _serialize_entities(message.entities)
    await state.update_data(prize_text=message.text, prize_entities=entities_json)
    await _save_giveaway(message, state)


@router.message(NewGiveaway.waiting_text_for_photo)
async def on_text_for_photo_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Нужен текст. Отправь описание приза:")


async def _save_giveaway(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    game_id = data.get("game_id")
    country = _get_country(message.from_user.id)
    if game_id:
        game_obj = db.get_game(game_id)
        if game_obj and game_obj.country:
            country = game_obj.country
    gid = db.add_giveaway(
        prize_text=data["prize_text"],
        prize_entities=data.get("prize_entities"),
        photo_id=data.get("photo_id"),
        game_id=game_id,
        name=data.get("giveaway_name"),
        country=country,
    )
    await state.clear()

    game = db.get_game(game_id) if game_id else None

    buttons = [
        [InlineKeyboardButton(text="➕ Добавить ещё", callback_data=f"gadd:{game_id}" if game_id else "go_new_lib")],
    ]
    if game_id:
        queue = db.get_queued(game_id)
        count_label = f"📋 В очереди: {len(queue)}"
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{game_id}")])
    else:
        free = db.get_free_giveaways(country=country)
        count_label = f"📚 В библиотеке: {len(free)}"
        buttons.append([InlineKeyboardButton(text="📚 Библиотека", callback_data="show_library")])

    game_label = f" в игру «{game.name}»" if game else ""
    await bot.send_message(
        chat_id=message.chat.id,
        text=(
            f"✅ Розыгрыш #{gid} сохранён{game_label}\n"
            f"{count_label}"
        ),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data == "go_new")
async def on_go_new(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await cb.message.edit_reply_markup(reply_markup=None)
    await state.clear()
    await bot.send_message(
        chat_id=cb.message.chat.id,
        text="✏️ Введи внутреннее название розыгрыша (видно только админам):",
    )
    await state.set_state(NewGiveaway.waiting_name)
    await cb.answer()


# ── /cancel ──────────────────────────────────────────────

@router.callback_query(F.data == "go_new_lib")
async def on_new_lib(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await bot.send_message(
        chat_id=cb.message.chat.id,
        text="✏️ Введи внутреннее название розыгрыша (видно только админам):",
    )
    await state.set_state(NewGiveaway.waiting_name)
    await cb.answer()


@router.callback_query(F.data == "go_menu")
async def on_go_menu(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    await _show_main_menu(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
    await cb.answer()


@router.callback_query(F.data == "show_cooldown")
async def on_show_cooldown(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    country = _get_country(cb.from_user.id)
    cd = _cooldown(country)
    await cb.answer(f"⏳ Кулдаун: {cd} мин\n\nИзменить: /cooldown 90", show_alert=True)


@router.callback_query(F.data == "show_winners")
async def on_show_winners(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    country = _get_country(cb.from_user.id)
    cd = _cooldown(country)
    rows = db.recent_winners(cd)
    if not rows:
        await cb.answer("Нет победителей на кулдауне", show_alert=True)
        return
    lines = [f"🏆 Победители (кулдаун {cd} мин):\n"]
    for name, username, prize in rows:
        who = f"@{username}" if username else name
        lines.append(f"• {who} — {prize[:30]}")
    await bot.send_message(cb.message.chat.id, "\n".join(lines))
    await cb.answer()


@router.callback_query(F.data == "reset_cooldowns")
async def on_reset_cooldowns(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    n = db.clear_winners()
    await cb.answer(f"✅ Кулдауны сброшены ({n} записей)", show_alert=True)


@cmd_router.message(Command("cancel"), StateFilter("*"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено.")


# ── /active ──────────────────────────────────────────────

@cmd_router.message(Command("active"))
async def cmd_active(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    active = db.get_all_active()
    if not active:
        await message.answer("Нет активных розыгрышей.")
        return
    await message.answer(
        f"🟢 Активные розыгрыши ({len(active)}):",
        reply_markup=_kb_active_list(active),
    )


@router.callback_query(F.data == "show_active")
async def on_show_active(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    active = db.get_all_active()
    if not active:
        await cb.answer("Нет активных розыгрышей")
        return
    await cb.message.answer(
        f"🟢 Активные розыгрыши ({len(active)}):",
        reply_markup=_kb_active_list(active),
    )
    await cb.answer()


# ── Finished giveaways ──────────────────────────────────

@router.callback_query(F.data.startswith("show_fin:"))
async def on_show_finished(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    game_id = int(cb.data.split(":")[1])
    game = db.get_game(game_id)
    if not game:
        await cb.answer("Игра не найдена")
        return
    finished = db.get_finished_by_game(game_id)
    if not finished:
        await cb.answer("Нет завершённых розыгрышей", show_alert=True)
        return
    buttons = []
    for g in finished:
        label = _giveaway_label(g)
        winners = db.get_giveaway_winners(g.id)
        w_str = ", ".join(_display(w) for w in winners) if winners else "—"
        buttons.append([InlineKeyboardButton(
            text=f"🔴 #{g.id}: {label}",
            callback_data=f"fin:{g.id}",
        )])
    buttons.append([InlineKeyboardButton(text="◀️ К игре", callback_data=f"game:{game_id}")])
    await cb.message.edit_text(
        f"🔴 Завершённые — {game.name} ({len(finished)}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("fin:"))
async def on_finished_giveaway(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g:
        await cb.answer("Не найден")
        return
    winners = db.get_giveaway_winners(gid)
    total = db.participant_count(gid)
    cd = _cooldown(g.country)
    eligible = db.get_eligible(gid, cd)
    winners_text = _format_winners(winners) if winners else "🏆 Победителей нет"
    text = (
        f"🔴 Розыгрыш #{gid}: {_giveaway_label(g)}\n"
        f"🎁 {g.prize_text[:80]}\n"
        f"👥 Участников: {total} (подходящих для перевыбора: {len(eligible)})\n\n"
        f"{winners_text}"
    )
    buttons = []
    if eligible:
        buttons.append([InlineKeyboardButton(text="🔄 Перевыбрать из возможных", callback_data=f"post_rr:{gid}")])
    buttons.append([InlineKeyboardButton(text="👤 Назначить из списка", callback_data=f"assign_list:{gid}")])
    if g.game_id:
        buttons.append([InlineKeyboardButton(text="◀️ К завершённым", callback_data=f"show_fin:{g.game_id}")])
    await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@router.callback_query(F.data.startswith("assign_list:"))
async def on_assign_list(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    gid = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    g = db.get_giveaway(gid)
    if not g:
        await cb.answer("Не найден")
        return
    participants = db.get_participants(gid)
    if not participants:
        await cb.answer("Нет участников", show_alert=True)
        return
    page_items, total_pages = _paginate(participants, page)
    buttons = []
    for p in page_items:
        buttons.append([InlineKeyboardButton(
            text=_display(p),
            callback_data=f"assignpick:{gid}:{p.user_id}",
        )])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"assign_list:{gid}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"assign_list:{gid}:{page + 1}"))
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"fin:{gid}")])
    await cb.message.edit_text(
        f"👤 Выбери победителя ({len(participants)} участников):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("assignpick:"))
async def on_assign_pick(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    _, gid_s, uid_s = cb.data.split(":")
    gid, user_id = int(gid_s), int(uid_s)
    g = db.get_giveaway(gid)
    p = db.get_participant(gid, user_id)
    if not g or not p:
        await cb.answer("Не найден")
        return
    db.add_winner(gid, p.user_id, p.username, p.full_name)
    buttons = [
        [InlineKeyboardButton(text="🔄 Перевыбрать из возможных", callback_data=f"post_rr:{gid}")],
        [InlineKeyboardButton(text="👤 Назначить из списка", callback_data=f"assign_list:{gid}")],
    ]
    if g.game_id:
        buttons.append([InlineKeyboardButton(text="◀️ К завершённым", callback_data=f"show_fin:{g.game_id}")])
    await cb.message.edit_text(
        f"✅ Победитель назначен\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"🏆 {_display(p)}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )
    await cb.answer()


# ── View giveaway from queue ────────────────────────────

@router.callback_query(F.data.startswith("view:"))
async def on_view_giveaway(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g:
        await cb.answer("Не найден")
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

    top_buttons: list[list[InlineKeyboardButton]] = []
    country_code = g.country or _get_country(cb.from_user.id)
    if g.game_id:
        channels = _country_channels(country_code) if country_code else ()
        top_buttons = [
            [InlineKeyboardButton(text=ch.label, callback_data=f"ch:{i}:{gid}")]
            for i, ch in enumerate(channels)
        ]
    else:
        games = db.get_games(country=country_code)
        if games:
            for gm in games:
                top_buttons.append([
                    InlineKeyboardButton(
                        text=f"🎮 Добавить в «{gm.name}»",
                        callback_data=f"lib2game:{gid}:{gm.id}",
                    )
                ])
    name_label = g.name or "—"
    edit_buttons = [
        [InlineKeyboardButton(text=f"📝 Название: {name_label}", callback_data=f"ednm:{gid}")],
        [
            InlineKeyboardButton(text="🖼 Заменить фото", callback_data=f"edph:{gid}"),
            InlineKeyboardButton(text="✏️ Заменить текст", callback_data=f"edtx:{gid}"),
        ],
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"del:{gid}")],
    ]
    if g.game_id:
        edit_buttons.append([InlineKeyboardButton(text="🎮 Назад к игре", callback_data=f"game:{g.game_id}")])
    else:
        edit_buttons.append([InlineKeyboardButton(text="📚 Назад в библиотеку", callback_data="show_library")])

    title = f"🎁 Розыгрыш #{g.id}"
    if g.game_id:
        title += "\n\nОпубликовать или отредактировать:"
    else:
        title += "\n\nДобавить в игру или отредактировать:"

    await cb.message.answer(
        title,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=top_buttons + edit_buttons),
    )
    await cb.answer()


# ── Edit giveaway ────────────────────────────────────────

@router.callback_query(F.data.startswith("ednm:"))
async def on_edit_name(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status not in ("queued", "finished"):
        await cb.answer("Розыгрыш недоступен")
        return
    await state.update_data(edit_gid=gid)
    await state.set_state(EditGiveaway.waiting_name)
    await cb.message.edit_text(f"✏️ Введи новое название для розыгрыша #{gid}:")
    await cb.answer()


@router.message(EditGiveaway.waiting_name, F.text)
async def on_edit_name_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    gid = data["edit_gid"]
    db.update_giveaway(gid, name=message.text.strip())
    g = db.get_giveaway(gid)
    await state.clear()
    buttons = [[InlineKeyboardButton(text="👁 Посмотреть", callback_data=f"view:{gid}")]]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="📚 Библиотека", callback_data="show_library")])
    await message.answer(
        f"✅ Название розыгрыша #{gid} обновлено.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.message(EditGiveaway.waiting_name)
async def on_edit_name_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Введи текстовое название. /cancel — отмена.")


@router.callback_query(F.data.startswith("edph:"))
async def on_edit_photo(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status != "queued":
        await cb.answer("Розыгрыш недоступен")
        return
    await state.update_data(edit_gid=gid)
    await state.set_state(EditGiveaway.waiting_photo)
    await cb.message.edit_text(f"🖼 Отправь новое фото для розыгрыша #{gid}:")
    await cb.answer()


@router.callback_query(F.data.startswith("edtx:"))
async def on_edit_text(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status != "queued":
        await cb.answer("Розыгрыш недоступен")
        return
    await state.update_data(edit_gid=gid)
    await state.set_state(EditGiveaway.waiting_text)
    await cb.message.edit_text(f"✏️ Отправь новый текст для розыгрыша #{gid}:")
    await cb.answer()


@router.message(EditGiveaway.waiting_photo, F.photo)
async def on_edit_photo_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    gid = data["edit_gid"]
    photo_id = message.photo[-1].file_id
    db.update_giveaway(gid, photo_id=photo_id)
    g = db.get_giveaway(gid)
    await state.clear()
    buttons = [[InlineKeyboardButton(text="👁 Посмотреть", callback_data=f"view:{gid}")]]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="📚 Библиотека", callback_data="show_library")])
    await message.answer(
        f"✅ Фото розыгрыша #{gid} обновлено.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.message(EditGiveaway.waiting_photo)
async def on_edit_photo_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Отправь фото. /cancel — отмена.")


@router.message(EditGiveaway.waiting_text, F.text)
async def on_edit_text_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    gid = data["edit_gid"]
    entities_json = _serialize_entities(message.entities)
    db.update_giveaway(gid, prize_text=message.text, prize_entities=entities_json)
    g = db.get_giveaway(gid)
    await state.clear()
    buttons = [[InlineKeyboardButton(text="👁 Посмотреть", callback_data=f"view:{gid}")]]
    if g and g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    else:
        buttons.append([InlineKeyboardButton(text="📚 Библиотека", callback_data="show_library")])
    await message.answer(
        f"✅ Текст розыгрыша #{gid} обновлён.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.message(EditGiveaway.waiting_text)
async def on_edit_text_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Отправь текст. /cancel — отмена.")


# ── Delete giveaway ──────────────────────────────────────

@router.callback_query(F.data.startswith("del:"))
async def on_delete(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    game_id = g.game_id if g else None
    if db.delete_giveaway(gid):
        if game_id:
            game = db.get_game(game_id)
            if game:
                await _show_game(cb.message.chat.id, game, cb.message.message_id)
                await cb.answer(f"#{gid} удалён")
                return
        await _show_library(cb.message.chat.id, cb.message.message_id, user_id=cb.from_user.id)
        await cb.answer(f"#{gid} удалён")
    else:
        await cb.answer("Не удалось удалить")


# ── Active giveaway details ─────────────────────────────

@router.callback_query(F.data.startswith("act:"))
async def on_active_detail(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        await cb.answer("Розыгрыш не найден или уже завершён")
        return

    country_code = g.country or _get_country(cb.from_user.id)
    count = db.participant_count(gid)
    eligible = len(db.get_eligible(gid, _cooldown(country_code)))
    timer_info = _remaining_time(gid)
    channel_label = g.channel_id
    for ch in _country_channels(country_code) if country_code else ():
        if ch.id == g.channel_id:
            channel_label = ch.label
            break

    text = (
        f"🟢 Активный розыгрыш #{g.id}\n\n"
        f"🎁 {g.prize_text[:80]}\n"
        f"📢 Канал: {channel_label}\n"
        f"👥 Участников: {count} (подходящих: {eligible})\n"
        f"{timer_info}"
    )

    buttons = [
        [InlineKeyboardButton(text=f"⚡ Определить победителя ({count})", callback_data=f"drw:{gid}")],
        [InlineKeyboardButton(text=f"👤 Выбрать из списка ({count})", callback_data=f"pick:{gid}:0")],
        [InlineKeyboardButton(text="🛑 Завершить досрочно", callback_data=f"frc:{gid}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"act:{gid}")],
        [InlineKeyboardButton(text="◀️ Назад к списку", callback_data="show_active")],
    ]
    if g.game_id:
        buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])

    kb = InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)
    await cb.answer()


@router.callback_query(F.data.startswith("frc:"))
async def on_force_finish(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        await cb.answer("Розыгрыш не найден или уже завершён")
        return

    if gid in _draw_timers:
        task = _draw_timers.pop(gid)
        if not task.done():
            task.cancel()
    _draw_deadlines.pop(gid, None)
    _pending_winners.pop(gid, None)
    _admin_panels.pop(gid, None)

    db.finish_giveaway(gid)
    total = db.participant_count(gid)

    try:
        await bot.edit_message_reply_markup(
            chat_id=g.channel_id,
            message_id=g.channel_message_id,
            reply_markup=_kb_finished(gid, total),
        )
    except Exception:
        pass

    await cb.message.edit_text(
        f"🛑 Розыгрыш #{gid} завершён досрочно без победителя.\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников было: {total}"
    )
    await cb.answer("Завершён!")


# ── Publish giveaway ────────────────────────────────────

@router.callback_query(F.data.startswith("ch:"))
async def on_channel(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    ch_idx, gid = int(parts[1]), int(parts[2])

    g = db.get_giveaway(gid)
    if not g or g.status != "queued":
        await cb.answer("Розыгрыш недоступен")
        return

    await cb.message.edit_text(
        "⏱ Через сколько определить победителя?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⏱ 5 мин", callback_data=f"tm:5:{gid}:{ch_idx}"),
                InlineKeyboardButton(text="⏱ 10 мин", callback_data=f"tm:10:{gid}:{ch_idx}"),
                InlineKeyboardButton(text="⏱ 30 мин", callback_data=f"tm:30:{gid}:{ch_idx}"),
            ],
            [InlineKeyboardButton(text="✋ Сам нажму когда надо", callback_data=f"tm:0:{gid}:{ch_idx}")],
            [InlineKeyboardButton(text="⌨️ Ввести время вручную", callback_data=f"tmc:{gid}:{ch_idx}")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("tm:"))
async def on_timing(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    minutes, gid, ch_idx = int(parts[1]), int(parts[2]), int(parts[3])
    await _publish_giveaway(gid, ch_idx, minutes, cb.message.chat.id, cb.message.message_id)
    await cb.answer()


@router.callback_query(F.data.startswith("tmc:"))
async def on_custom_time(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    gid, ch_idx = int(parts[1]), int(parts[2])
    await state.update_data(publish_gid=gid, publish_ch_idx=ch_idx)
    await state.set_state(PublishGiveaway.waiting_custom_time)
    await cb.message.edit_text("⌨️ Введи время в минутах (число):")
    await cb.answer()


@router.message(PublishGiveaway.waiting_custom_time, F.text)
async def on_custom_time_received(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        minutes = int(message.text.strip())
        if minutes < 1:
            raise ValueError
    except ValueError:
        await message.answer("Введи положительное число минут. /cancel — отмена.")
        return

    data = await state.get_data()
    gid = data["publish_gid"]
    ch_idx = data["publish_ch_idx"]
    await state.clear()
    await _publish_giveaway(gid, ch_idx, minutes, message.chat.id)


async def _publish_giveaway(
    gid: int, ch_idx: int, minutes: int, chat_id: int, edit_message_id: int | None = None
) -> None:
    g = db.get_giveaway(gid)
    if not g or g.status != "queued":
        await bot.send_message(chat_id, "Розыгрыш недоступен.")
        return

    country_code = g.country or _get_country(chat_id)
    channel = _channel_by_country_idx(country_code, ch_idx) if country_code else None
    if not channel:
        await bot.send_message(chat_id, "Канал не найден для этой страны.")
        return
    entities = _deserialize_entities(g.prize_entities)

    if g.photo_id:
        msg = await bot.send_photo(
            chat_id=channel.id,
            photo=g.photo_id,
            caption=g.prize_text,
            caption_entities=entities,
            reply_markup=_kb_participate(gid),
        )
    else:
        msg = await bot.send_message(
            chat_id=channel.id,
            text=g.prize_text,
            entities=entities,
            reply_markup=_kb_participate(gid),
        )

    if minutes > 0:
        deadline = datetime.now(timezone.utc) + timedelta(minutes=minutes)
        deadline_iso = deadline.isoformat()
    else:
        deadline = None
        deadline_iso = None

    db.activate_giveaway(gid, channel.id, msg.message_id, deadline_iso, chat_id)
    count = db.participant_count(gid)

    if minutes > 0:
        mode = f"⏱ Автоматический розыгрыш через {minutes} мин"
        _draw_deadlines[gid] = deadline
        task = asyncio.create_task(_timer_draw(gid, minutes, chat_id))
        _draw_timers[gid] = task
    else:
        mode = "✋ Ручной режим — разыграй когда будешь готов"

    text = (
        f"✅ Опубликовано в {channel.label}\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"{mode}\n"
        f"👥 Участников: {count}"
    )

    if edit_message_id:
        try:
            ctrl = await bot.edit_message_text(
                text, chat_id=chat_id, message_id=edit_message_id,
                reply_markup=_kb_control(gid, count),
            )
            _admin_panels[gid] = (chat_id, ctrl.message_id)
            return
        except Exception:
            pass

    ctrl = await bot.send_message(chat_id, text, reply_markup=_kb_control(gid, count))
    _admin_panels[gid] = (chat_id, ctrl.message_id)


# ── Participation (channel users) ────────────────────────

@router.callback_query(F.data.startswith("p:"))
async def on_participate(cb: CallbackQuery) -> None:
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        await cb.answer("Розыгрыш завершён", show_alert=False)
        return

    user = cb.from_user
    name = user.full_name or user.first_name or "User"
    added = db.add_participant(gid, user.id, user.username, name)

    if added:
        await cb.answer("🎲 Вы успешно зарегистрированы в розыгрыше!", show_alert=True)
        _schedule_button_update(gid)
    else:
        await cb.answer("Вы уже участвуете!", show_alert=False)


# ── Refresh participant count ────────────────────────────

@router.callback_query(F.data.startswith("ref:"))
async def on_refresh(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g:
        await cb.answer("Не найден")
        return

    count = db.participant_count(gid)
    eligible = len(db.get_eligible(gid, _cooldown(g.country)))
    has_timer = gid in _draw_timers and not _draw_timers[gid].done()
    mode = "⏱ Таймер активен" if has_timer else "✋ Ручной режим"

    try:
        await cb.message.edit_text(
            f"🎁 {g.prize_text[:50]}\n"
            f"{mode}\n"
            f"👥 Участников: {count} (подходящих: {eligible})",
            reply_markup=_kb_control(gid, count),
        )
    except Exception:
        pass
    await cb.answer(f"👥 {count} (подходящих: {eligible})")


# ── Draw logic ───────────────────────────────────────────

def _format_winners(winners: list[db.Participant]) -> str:
    if len(winners) == 1:
        return f"🏆 Победитель: {_display(winners[0])}"
    lines = ["🏆 Победители:"]
    for i, w in enumerate(winners, 1):
        lines.append(f"  {i}. {_display(w)}")
    return "\n".join(lines)


async def _do_draw(gid: int, chat_id: int, message_id: int | None = None) -> None:
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        return

    if gid in _draw_timers:
        task = _draw_timers.pop(gid)
        if not task.done():
            task.cancel()
    _draw_deadlines.pop(gid, None)

    cd = _cooldown(g.country)
    eligible = db.get_eligible(gid, cd)
    total = db.participant_count(gid)

    if not eligible:
        text = f"🎁 {g.prize_text[:50]}\n👥 Участников: {total}\n\n❌ Нет подходящих участников"
        if total > 0:
            text += " (все на кулдауне)"
        kb = _kb_control(gid, total)
        if message_id:
            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
            except Exception:
                await bot.send_message(chat_id, text, reply_markup=kb)
        else:
            await bot.send_message(chat_id, text, reply_markup=kb)
        return

    pick_count = min(g.winner_count, len(eligible))
    winners = random.sample(eligible, pick_count)
    _pending_winners[gid] = winners

    text = (
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total} (подходящих: {len(eligible)})\n\n"
        f"{_format_winners(winners)}"
    )
    if pick_count < g.winner_count:
        text += f"\n\n⚠️ Подходящих участников меньше чем нужно ({pick_count}/{g.winner_count})"

    kb = _kb_winner(gid)
    if message_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=message_id, reply_markup=kb)
        except Exception:
            await bot.send_message(chat_id, text, reply_markup=kb)
    else:
        await bot.send_message(chat_id, text, reply_markup=kb)


@router.callback_query(F.data.startswith("drw:"))
async def on_draw(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    await _do_draw(gid, cb.message.chat.id, cb.message.message_id)
    await cb.answer()


# ── Manual pick from participant list ────────────────────

@router.callback_query(F.data.startswith("pick:"))
async def on_pick_list(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    gid, page = int(parts[1]), int(parts[2])
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        await cb.answer("Розыгрыш не активен")
        return
    participants = db.get_participants(gid)
    if not participants:
        await cb.answer("Нет участников")
        return

    page_items, total_pages = _paginate(participants, page)
    buttons = []
    for p in page_items:
        label = f"@{p.username}" if p.username else p.full_name
        buttons.append([
            InlineKeyboardButton(text=label, callback_data=f"setwin:{gid}:{p.user_id}"),
        ])
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"pick:{gid}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"pick:{gid}:{page + 1}"))
        buttons.append(nav)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"act:{gid}")])

    text = f"👤 Выбери победителя ({len(participants)}):"
    try:
        await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    except Exception:
        await cb.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@router.callback_query(F.data.startswith("setwin:"))
async def on_set_winner(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    gid, user_id = int(parts[1]), int(parts[2])
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        await cb.answer("Розыгрыш не активен")
        return

    participants = db.get_participants(gid)
    winner = next((p for p in participants if p.user_id == user_id), None)
    if not winner:
        await cb.answer("Участник не найден")
        return

    _pending_winners[gid] = [winner]
    total = len(participants)
    has_timer = gid in _draw_timers and not _draw_timers[gid].done()
    timer_note = "\n⏱ Таймер продолжается — победитель будет объявлен автоматически" if has_timer else ""
    text = (
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total}\n\n"
        f"👤 Выбран вручную:\n"
        f"{_format_winners([winner])}"
        f"{timer_note}"
    )
    kb = _kb_winner(gid)
    try:
        await cb.message.edit_text(text, reply_markup=kb)
    except Exception:
        await cb.message.answer(text, reply_markup=kb)
    await cb.answer(f"✅ {_display(winner)} выбран победителем")


async def _timer_draw(gid: int, minutes: int, admin_chat_id: int) -> None:
    try:
        await asyncio.sleep(minutes * 60)
    except asyncio.CancelledError:
        return
    _draw_timers.pop(gid, None)
    _draw_deadlines.pop(gid, None)
    await _auto_finish(gid, admin_chat_id)


async def _auto_finish(gid: int, admin_chat_id: int) -> None:
    g = db.get_giveaway(gid)
    if not g or g.status != "active":
        return

    cd = _cooldown(g.country)
    total = db.participant_count(gid)
    panel = _admin_panels.pop(gid, None)

    pre_selected = _pending_winners.pop(gid, None)
    if pre_selected:
        winners = pre_selected
        eligible_count = total
        pick_count = len(winners)
    else:
        eligible = db.get_eligible(gid, cd)
        eligible_count = len(eligible)

        if not eligible:
            db.finish_giveaway(gid)
            try:
                await bot.edit_message_reply_markup(
                    chat_id=g.channel_id,
                    message_id=g.channel_message_id,
                    reply_markup=_kb_finished(gid, total),
                )
            except Exception:
                pass
            text = (
                f"⏱ Таймер розыгрыша #{gid} истёк\n\n"
                f"🎁 {g.prize_text[:50]}\n"
                f"👥 Участников: {total}\n"
                f"❌ Нет подходящих участников"
            )
            if total > 0:
                text += " (все на кулдауне)"
            if panel:
                try:
                    await bot.edit_message_text(text, chat_id=panel[0], message_id=panel[1])
                except Exception:
                    await bot.send_message(admin_chat_id, text)
            else:
                await bot.send_message(admin_chat_id, text)
            return

        pick_count = min(g.winner_count, len(eligible))
        winners = random.sample(eligible, pick_count)

    for w in winners:
        db.add_winner(gid, w.user_id, w.username, w.full_name)
    db.finish_giveaway(gid)

    try:
        announce = f"🎉 Результаты розыгрыша:\n\n{_format_winners(winners)}\n\nПриз: {g.prize_text}"
        await bot.send_message(chat_id=g.channel_id, text=announce)
    except Exception:
        log.exception("Auto-finish: failed to announce in %s", g.channel_id)

    try:
        await bot.edit_message_reply_markup(
            chat_id=g.channel_id,
            message_id=g.channel_message_id,
            reply_markup=_kb_finished(gid, total),
        )
    except Exception:
        pass

    admin_text = (
        f"⏱ Розыгрыш #{gid} завершён автоматически\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total} (подходящих: {eligible_count})\n"
        f"{_format_winners(winners)}\n"
        f"⏳ Кулдаун: {cd} мин"
    )
    if pick_count < g.winner_count:
        admin_text += f"\n⚠️ Подходящих меньше чем нужно ({pick_count}/{g.winner_count})"

    nav_buttons = [
        [InlineKeyboardButton(text="🔄 Перевыбрать победителя", callback_data=f"post_rr:{gid}")],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g.game_id:
        nav_buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    nav_buttons.append([InlineKeyboardButton(text="🎮 К играм", callback_data="show_games")])
    admin_kb = InlineKeyboardMarkup(inline_keyboard=nav_buttons)

    if panel:
        try:
            await bot.edit_message_text(admin_text, chat_id=panel[0], message_id=panel[1], reply_markup=admin_kb)
        except Exception:
            await bot.send_message(admin_chat_id, admin_text, reply_markup=admin_kb)
    else:
        await bot.send_message(admin_chat_id, admin_text, reply_markup=admin_kb)


# ── Re-roll ──────────────────────────────────────────────

@router.callback_query(F.data.startswith("rr:"))
async def on_reroll(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    prev = _pending_winners.get(gid, [])
    for p in prev:
        db.add_reject(gid, p.user_id)
    await _do_draw(gid, cb.message.chat.id, cb.message.message_id)
    await cb.answer()


# ── Confirm winner(s) ───────────────────────────────────

@router.callback_query(F.data.startswith("cfm:"))
async def on_confirm(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    winners = _pending_winners.pop(gid, [])
    g = db.get_giveaway(gid)
    if not g or not winners:
        await cb.answer("Нет данных")
        return

    for w in winners:
        db.add_winner(gid, w.user_id, w.username, w.full_name)
    db.finish_giveaway(gid)
    _admin_panels.pop(gid, None)

    total = db.participant_count(gid)

    try:
        announce = f"🎉 Результаты розыгрыша:\n\n{_format_winners(winners)}\n\nПриз: {g.prize_text}"
        await bot.send_message(chat_id=g.channel_id, text=announce)
    except Exception:
        log.exception("Failed to announce winner in %s", g.channel_id)

    try:
        await bot.edit_message_reply_markup(
            chat_id=g.channel_id,
            message_id=g.channel_message_id,
            reply_markup=_kb_finished(gid, total),
        )
    except Exception:
        log.exception("Failed to update channel post button")

    cd = _cooldown(g.country)
    nav_buttons = [
        [InlineKeyboardButton(text="🔄 Перевыбрать победителя", callback_data=f"post_rr:{gid}")],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g.game_id:
        nav_buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    nav_buttons.append([InlineKeyboardButton(text="🎮 К играм", callback_data="show_games")])
    await cb.message.edit_text(
        f"✅ Розыгрыш #{gid} завершён\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total}\n"
        f"{_format_winners(winners)}\n"
        f"⏳ Кулдаун: {cd} мин",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
    )
    await cb.answer("Подтверждено!")


# ── Post-confirm re-roll ─────────────────────────────────

@router.callback_query(F.data.startswith("post_rr:"))
async def on_post_reroll(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    g = db.get_giveaway(gid)
    if not g:
        await cb.answer("Розыгрыш не найден")
        return

    prev = _pending_winners.get(gid, [])
    for p in prev:
        db.add_reject(gid, p.user_id)

    cd = _cooldown(g.country)
    eligible = db.get_eligible(gid, cd)
    total = db.participant_count(gid)

    if not eligible:
        text = f"🎁 {g.prize_text[:50]}\n👥 Участников: {total}\n\n❌ Нет подходящих участников"
        if total > 0:
            text += " (все на кулдауне или перебраны)"
        nav = [[InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")]]
        if g.game_id:
            nav.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
        await cb.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=nav))
        await cb.answer()
        return

    winner = random.choice(eligible)
    _pending_winners[gid] = [winner]

    text = (
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total} (подходящих: {len(eligible)})\n\n"
        f"{_format_winners([winner])}"
    )
    await cb.message.edit_text(text, reply_markup=_kb_post_winner(gid))
    await cb.answer()


@router.callback_query(F.data.startswith("post_cfm:"))
async def on_post_confirm(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    winners = _pending_winners.pop(gid, [])
    g = db.get_giveaway(gid)
    if not g or not winners:
        await cb.answer("Нет данных")
        return

    for w in winners:
        db.add_winner(gid, w.user_id, w.username, w.full_name)

    cd = _cooldown(g.country)
    nav_buttons = [
        [InlineKeyboardButton(text="🔄 Перевыбрать победителя", callback_data=f"post_rr:{gid}")],
        [InlineKeyboardButton(text="🟢 Активные розыгрыши", callback_data="show_active")],
    ]
    if g.game_id:
        nav_buttons.append([InlineKeyboardButton(text="🎮 К игре", callback_data=f"game:{g.game_id}")])
    nav_buttons.append([InlineKeyboardButton(text="🎮 К играм", callback_data="show_games")])
    await cb.message.edit_text(
        f"✅ Победитель перевыбран\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"{_format_winners(winners)}\n"
        f"⏳ Кулдаун: {cd} мин",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=nav_buttons),
    )
    await cb.answer("Подтверждено!")


# ── /finish — force-close active giveaway ────────────────

@cmd_router.message(Command("finish"))
async def cmd_finish(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return

    active = db.get_active()
    if not active:
        await message.answer("Нет активного розыгрыша.")
        return

    if active.id in _draw_timers:
        task = _draw_timers.pop(active.id)
        if not task.done():
            task.cancel()
    _draw_deadlines.pop(active.id, None)
    _pending_winners.pop(active.id, None)
    _admin_panels.pop(active.id, None)

    db.finish_giveaway(active.id)
    total = db.participant_count(active.id)

    try:
        await bot.edit_message_reply_markup(
            chat_id=active.channel_id,
            message_id=active.channel_message_id,
            reply_markup=_kb_finished(active.id, total),
        )
    except Exception:
        pass

    await message.answer(f"🛑 Розыгрыш #{active.id} завершён без победителя.")


# ── /cooldown ────────────────────────────────────────────

@cmd_router.message(Command("cooldown"))
async def cmd_cooldown(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    country = _get_country(message.from_user.id)
    if not country:
        await message.answer("Сначала выбери страну: /start")
        return
    country_obj = cfg.country_by_code(country)
    country_label = country_obj.label if country_obj else country
    parts = message.text.strip().split()
    current = _cooldown(country)
    if len(parts) < 2:
        await message.answer(f"⏳ Кулдаун ({country_label}): {current} мин\n\nИзменить: /cooldown 90")
        return

    try:
        val = int(parts[1])
        if val < 0:
            raise ValueError
    except ValueError:
        await message.answer("Нужно положительное число.")
        return

    db.set_setting(f"cooldown_minutes:{country}", str(val))
    await message.answer(f"✅ Кулдаун ({country_label}): {current} → {val} мин")


# ── /winners ─────────────────────────────────────────────

@cmd_router.message(Command("winners"))
async def cmd_winners(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    cd = _cooldown(_get_country(message.from_user.id))
    rows = db.recent_winners(cd)
    if not rows:
        await message.answer("Нет победителей на кулдауне.")
        return

    lines = [f"🏆 Победители (кулдаун {cd} мин):\n"]
    for name, username, prize in rows:
        who = f"@{username}" if username else name
        lines.append(f"• {who} — {prize[:30]}")
    await message.answer("\n".join(lines))


# ── /resetcooldowns ──────────────────────────────────────

@cmd_router.message(Command("resetcooldowns"))
async def cmd_reset(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    n = db.clear_winners()
    await message.answer(f"✅ Кулдауны сброшены ({n} записей).")


# ── Entry point ──────────────────────────────────────────

async def _restore_timers() -> None:
    """Restore draw timers for active giveaways with deadlines after bot restart."""
    active = db.get_all_active()
    now = datetime.now(timezone.utc)
    restored = 0
    auto_finished = 0
    for g in active:
        if not g.draw_deadline:
            continue
        deadline = datetime.fromisoformat(g.draw_deadline)
        _draw_deadlines[g.id] = deadline
        remaining = (deadline - now).total_seconds()
        fallback_admin = next(iter(cfg.all_admin_ids), None)
        admin_chat = g.admin_chat_id or fallback_admin
        if remaining <= 0:
            log.info("Giveaway #%d deadline passed, auto-finishing", g.id)
            asyncio.create_task(_auto_finish(g.id, admin_chat))
            auto_finished += 1
        else:
            minutes = remaining / 60
            log.info("Restoring timer for giveaway #%d (%.1f min left)", g.id, minutes)
            task = asyncio.create_task(_timer_draw_seconds(g.id, remaining, admin_chat))
            _draw_timers[g.id] = task
            restored += 1
    if restored or auto_finished:
        log.info("Timers: restored=%d, auto-finished=%d", restored, auto_finished)


async def _timer_draw_seconds(gid: int, seconds: float, admin_chat_id: int) -> None:
    """Like _timer_draw but takes seconds directly (for restoring partial timers)."""
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        return
    _draw_timers.pop(gid, None)
    _draw_deadlines.pop(gid, None)
    await _auto_finish(gid, admin_chat_id)


async def main() -> None:
    db.init_db()
    dp = Dispatcher()
    dp.include_router(cmd_router)
    dp.include_router(router)

    for c in cfg.countries:
        ch_names = ", ".join(ch.label for ch in c.channels)
        log.info(
            "Country %s (%s) — admins=%s, channels=[%s], cooldown=%d min",
            c.code, c.label, c.admin_ids, ch_names, c.default_cooldown_minutes,
        )
    log.info("Bot starting")

    await bot.set_my_commands([
        BotCommand(command="start", description="Главное меню"),
    ])

    await _restore_timers()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
