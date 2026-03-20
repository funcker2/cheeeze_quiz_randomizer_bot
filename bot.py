import asyncio
import json
import logging
import random

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
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
router = Router()

_draw_timers: dict[int, asyncio.Task] = {}
_pending_winners: dict[int, list[db.Participant]] = {}
_admin_panels: dict[int, tuple[int, int]] = {}
_button_update_tasks: dict[int, asyncio.Task] = {}


class NewGiveaway(StatesGroup):
    waiting_content = State()
    waiting_text_for_photo = State()
    waiting_winner_count = State()


def is_admin(user_id: int) -> bool:
    return user_id in cfg.admin_ids


def _cooldown() -> int:
    val = db.get_setting("cooldown_minutes")
    return int(val) if val else cfg.default_cooldown_minutes


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
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🎲 Разыграть ({count})", callback_data=f"drw:{gid}")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data=f"ref:{gid}")],
    ])


def _kb_winner(gid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Перевыбрать", callback_data=f"rr:{gid}"),
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"cfm:{gid}"),
        ],
    ])


def _kb_queue(queue: list[db.Giveaway]) -> InlineKeyboardMarkup:
    buttons = []
    for g in queue:
        label = g.prize_text[:30] + ("…" if len(g.prize_text) > 30 else "")
        icon = "🖼" if g.photo_id else "📝"
        winners_mark = f" ×{g.winner_count}" if g.winner_count > 1 else ""
        buttons.append([
            InlineKeyboardButton(
                text=f"{icon} #{g.id}: {label}{winners_mark}",
                callback_data="noop",
            ),
            InlineKeyboardButton(text="🗑", callback_data=f"del:{g.id}"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def _kb_winner_count() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1", callback_data="wc:1"),
            InlineKeyboardButton(text="2", callback_data="wc:2"),
            InlineKeyboardButton(text="3", callback_data="wc:3"),
        ],
    ])


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

@router.message(Command("start"))
async def cmd_start(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer(
        "🎲 Бот-рандомайзер для розыгрышей\n\n"
        "/new — создать розыгрыш\n"
        "/queue — очередь розыгрышей\n"
        "/next — опубликовать следующий\n"
        "/finish — завершить активный без победителя\n"
        "/cooldown [мин] — настройка кулдауна\n"
        "/winners — победители на кулдауне\n"
        "/resetcooldowns — сбросить все кулдауны\n"
        "/cancel — отмена"
    )


# ── Create giveaway (single-step) ───────────────────────

@router.message(Command("new", "newgiveaway"))
async def cmd_new(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer(
        "🎁 Отправь пост для розыгрыша:\n"
        "• фото с подписью (текст + фото в одном сообщении)\n"
        "• или просто текст\n\n"
        "Можно использовать форматирование: жирный, курсив, ссылки, эмодзи.",
    )
    await state.set_state(NewGiveaway.waiting_content)


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
        await message.answer("🏆 Сколько победителей?", reply_markup=_kb_winner_count())
        await state.set_state(NewGiveaway.waiting_winner_count)
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
    await message.answer("🏆 Сколько победителей?", reply_markup=_kb_winner_count())
    await state.set_state(NewGiveaway.waiting_winner_count)


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
    await message.answer("🏆 Сколько победителей?", reply_markup=_kb_winner_count())
    await state.set_state(NewGiveaway.waiting_winner_count)


@router.message(NewGiveaway.waiting_text_for_photo)
async def on_text_for_photo_invalid(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    await message.answer("Нужен текст. Отправь описание приза:")


@router.callback_query(NewGiveaway.waiting_winner_count, F.data.startswith("wc:"))
async def on_winner_count_btn(cb: CallbackQuery, state: FSMContext) -> None:
    if not is_admin(cb.from_user.id):
        return
    count = int(cb.data.split(":")[1])
    await _save_giveaway(cb.message, state, count)
    await cb.answer()


@router.message(NewGiveaway.waiting_winner_count, F.text)
async def on_winner_count_text(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    try:
        count = int(message.text.strip())
        if count < 1:
            raise ValueError
    except ValueError:
        await message.answer("Введи число от 1. Или нажми кнопку.")
        return
    await _save_giveaway(message, state, count)


async def _save_giveaway(message: Message, state: FSMContext, winner_count: int) -> None:
    data = await state.get_data()
    gid = db.add_giveaway(
        prize_text=data["prize_text"],
        prize_entities=data.get("prize_entities"),
        photo_id=data.get("photo_id"),
        winner_count=winner_count,
    )
    await state.clear()
    queue = db.get_queued()
    winners_label = f", победителей: {winner_count}" if winner_count > 1 else ""
    await message.answer(
        f"✅ Розыгрыш #{gid} сохранён{winners_label}\n"
        f"📋 В очереди: {len(queue)}\n\n"
        "/next — опубликовать | /new — добавить ещё"
    )


# ── /cancel ──────────────────────────────────────────────

@router.message(Command("cancel"), StateFilter("*"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено.")


# ── /queue ───────────────────────────────────────────────

@router.message(Command("queue"))
async def cmd_queue(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    queue = db.get_queued()
    if not queue:
        await message.answer("📋 Очередь пуста. /new — создать розыгрыш.")
        return
    await message.answer(f"📋 Очередь ({len(queue)}):", reply_markup=_kb_queue(queue))


@router.callback_query(F.data == "noop")
async def on_noop(cb: CallbackQuery) -> None:
    await cb.answer()


@router.callback_query(F.data.startswith("del:"))
async def on_delete(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    gid = int(cb.data.split(":")[1])
    if db.delete_giveaway(gid):
        queue = db.get_queued()
        if queue:
            await cb.message.edit_text(
                f"📋 Очередь ({len(queue)}):", reply_markup=_kb_queue(queue)
            )
        else:
            await cb.message.edit_text("📋 Очередь пуста.")
        await cb.answer(f"#{gid} удалён")
    else:
        await cb.answer("Не удалось удалить")


# ── /next — publish next giveaway ────────────────────────

@router.message(Command("next"))
async def cmd_next(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return

    active = db.get_active()
    if active:
        count = db.participant_count(active.id)
        await message.answer(
            f"⚠️ Активный розыгрыш #{active.id}: {active.prize_text[:40]}\n"
            f"👥 Участников: {count}\n\n"
            "Заверши его или /finish для отмены.",
            reply_markup=_kb_control(active.id, count),
        )
        return

    queue = db.get_queued()
    if not queue:
        await message.answer("📋 Очередь пуста. /new — создать розыгрыш.")
        return

    g = queue[0]
    entities = _deserialize_entities(g.prize_entities)
    winners_label = f"\n🏆 Победителей: {g.winner_count}" if g.winner_count > 1 else ""

    if g.photo_id:
        await message.answer_photo(
            photo=g.photo_id,
            caption=g.prize_text,
            caption_entities=entities,
        )
        await message.answer(f"🎁 Розыгрыш #{g.id}{winners_label}")
    else:
        await message.answer(
            text=g.prize_text,
            entities=entities,
        )
        await message.answer(f"🎁 Розыгрыш #{g.id}{winners_label}")

    buttons = [
        [InlineKeyboardButton(text=ch.label, callback_data=f"ch:{i}:{g.id}")]
        for i, ch in enumerate(cfg.channels)
    ]
    await message.answer("📢 Выбери канал:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


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
        "⏱ Когда разыграть?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="⏱ 10 мин", callback_data=f"tm:10:{gid}:{ch_idx}"),
                InlineKeyboardButton(text="⏱ 30 мин", callback_data=f"tm:30:{gid}:{ch_idx}"),
            ],
            [InlineKeyboardButton(text="✋ Вручную", callback_data=f"tm:0:{gid}:{ch_idx}")],
        ]),
    )
    await cb.answer()


@router.callback_query(F.data.startswith("tm:"))
async def on_timing(cb: CallbackQuery) -> None:
    if not is_admin(cb.from_user.id):
        return
    parts = cb.data.split(":")
    minutes, gid, ch_idx = int(parts[1]), int(parts[2]), int(parts[3])

    g = db.get_giveaway(gid)
    if not g or g.status != "queued":
        await cb.answer("Розыгрыш недоступен")
        return

    channel = cfg.channels[ch_idx]
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

    db.activate_giveaway(gid, channel.id, msg.message_id)
    count = db.participant_count(gid)

    if minutes > 0:
        mode = f"⏱ Автоматический розыгрыш через {minutes} мин"
        task = asyncio.create_task(_timer_draw(gid, minutes, cb.message.chat.id))
        _draw_timers[gid] = task
    else:
        mode = "✋ Ручной режим — разыграй когда будешь готов"

    winners_label = f"🏆 Победителей: {g.winner_count}\n" if g.winner_count > 1 else ""
    ctrl = await cb.message.edit_text(
        f"✅ Опубликовано в {channel.label}\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"{winners_label}"
        f"{mode}\n"
        f"👥 Участников: {count}",
        reply_markup=_kb_control(gid, count),
    )
    _admin_panels[gid] = (cb.message.chat.id, ctrl.message_id)
    await cb.answer()


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
        cd = db.cooldown_remaining(user.id, _cooldown())
        if cd:
            await cb.answer(
                f"✅ Записал! (кулдаун ещё {cd} мин — не сможешь выиграть)",
                show_alert=False,
            )
        else:
            await cb.answer("✅ Ты участвуешь!", show_alert=False)
        _schedule_button_update(gid)
    else:
        await cb.answer("Ты уже участвуешь!", show_alert=False)


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
    eligible = len(db.get_eligible(gid, _cooldown()))
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

    cd = _cooldown()
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


async def _timer_draw(gid: int, minutes: int, admin_chat_id: int) -> None:
    try:
        await asyncio.sleep(minutes * 60)
    except asyncio.CancelledError:
        return
    _draw_timers.pop(gid, None)
    panel = _admin_panels.get(gid)
    mid = panel[1] if panel else None
    await _do_draw(gid, admin_chat_id, mid)


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

    # Announce in channel
    try:
        announce = f"🎉 Результаты розыгрыша:\n\n{_format_winners(winners)}\n\nПриз: {g.prize_text}"
        await bot.send_message(chat_id=g.channel_id, text=announce)
    except Exception:
        log.exception("Failed to announce winner in %s", g.channel_id)

    # Update channel post button to "Завершён"
    try:
        await bot.edit_message_reply_markup(
            chat_id=g.channel_id,
            message_id=g.channel_message_id,
            reply_markup=_kb_finished(gid, total),
        )
    except Exception:
        log.exception("Failed to update channel post button")

    cd = _cooldown()
    await cb.message.edit_text(
        f"✅ Розыгрыш #{gid} завершён\n\n"
        f"🎁 {g.prize_text[:50]}\n"
        f"👥 Участников: {total}\n"
        f"{_format_winners(winners)}\n"
        f"⏳ Кулдаун: {cd} мин"
    )
    await cb.answer("Подтверждено!")


# ── /finish — force-close active giveaway ────────────────

@router.message(Command("finish"))
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

@router.message(Command("cooldown"))
async def cmd_cooldown(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = message.text.strip().split()
    current = _cooldown()
    if len(parts) < 2:
        await message.answer(f"⏳ Кулдаун: {current} мин\n\nИзменить: /cooldown 90")
        return

    try:
        val = int(parts[1])
        if val < 0:
            raise ValueError
    except ValueError:
        await message.answer("Нужно положительное число.")
        return

    db.set_setting("cooldown_minutes", str(val))
    await message.answer(f"✅ Кулдаун: {current} → {val} мин")


# ── /winners ─────────────────────────────────────────────

@router.message(Command("winners"))
async def cmd_winners(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    cd = _cooldown()
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

@router.message(Command("resetcooldowns"))
async def cmd_reset(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    n = db.clear_winners()
    await message.answer(f"✅ Кулдауны сброшены ({n} записей).")


# ── Entry point ──────────────────────────────────────────

async def main() -> None:
    db.init_db()
    dp = Dispatcher()
    dp.include_router(router)

    ch_names = ", ".join(ch.label for ch in cfg.channels)
    log.info(
        "Bot starting — admins=%s, channels=[%s], cooldown=%d min",
        cfg.admin_ids, ch_names, cfg.default_cooldown_minutes,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
