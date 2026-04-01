import asyncio
import calendar
import logging
import os
import re
import sqlite3
from datetime import date, datetime, time, timedelta

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

load_dotenv()
DB_PATH = os.getenv("DB_PATH", "tasks.db")
WEEKDAY_LABELS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
MONTH_LABELS = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]
LOCAL_TZ = datetime.now().astimezone().tzinfo
BUTTON_ADD = "➕ Добавить задачу"
BUTTON_SKIP = "⏭ Пропустить описание"
BUTTON_CALENDAR = "📅 Календарь"
BUTTON_LIST = "📋 Активные задачи"
BUTTON_ALL = "🗂 Все задачи"
BUTTON_DONE = "✅ Отметить выполненной"
BUTTON_DELETE = "🗑 Удалить задачу"
BUTTON_CLEAR_DONE = "🧹 Удалить выполненные"
BUTTON_HELP = "ℹ️ Помощь"

BUTTON_TODAY = "📌 Задачи на сегодня"
BUTTON_WEEK = "🗓 Задачи на неделю"
BUTTON_MONTH = "🗓 Задачи на месяц"


def main_menu_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BUTTON_ADD), KeyboardButton(BUTTON_SKIP)],
            [KeyboardButton(BUTTON_LIST), KeyboardButton(BUTTON_ALL)],
            [KeyboardButton(BUTTON_TODAY), KeyboardButton(BUTTON_WEEK), KeyboardButton(BUTTON_MONTH)],
            [KeyboardButton(BUTTON_CALENDAR), KeyboardButton(BUTTON_CLEAR_DONE)],
            [KeyboardButton(BUTTON_DONE), KeyboardButton(BUTTON_DELETE)],
            [KeyboardButton(BUTTON_HELP)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
    )


def normalize_text(s: str) -> str:
    # Some clients may add emoji variation selectors (e.g. 🗓️ vs 🗓).
    return (s or "").replace("\ufe0f", "").strip()


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_connection() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER,
                text TEXT NOT NULL,
                description TEXT,
                due_at TEXT,
                notified_3d INTEGER NOT NULL DEFAULT 0,
                notified_1d INTEGER NOT NULL DEFAULT 0,
                notified_1h INTEGER NOT NULL DEFAULT 0,
                is_done INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        columns = [row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()]
        if "chat_id" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN chat_id INTEGER")
        if "description" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN description TEXT")
        if "due_at" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN due_at TEXT")
        if "notified_3d" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN notified_3d INTEGER NOT NULL DEFAULT 0")
        if "notified_1d" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN notified_1d INTEGER NOT NULL DEFAULT 0")
        if "notified_1h" not in columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN notified_1h INTEGER NOT NULL DEFAULT 0")
        conn.commit()


def add_task(
    user_id: int, chat_id: int, text: str, description: str | None, due_at: datetime
) -> int:
    created_at = datetime.utcnow().isoformat()
    with get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO tasks (user_id, chat_id, text, description, due_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, chat_id, text, description, due_at.isoformat(), created_at),
        )
        conn.commit()
        return int(cursor.lastrowid)


def list_tasks(user_id: int, show_all: bool = False) -> list[sqlite3.Row]:
    query = "SELECT * FROM tasks WHERE user_id = ?"
    params: list[object] = [user_id]
    if not show_all:
        query += " AND is_done = 0"
    query += " ORDER BY is_done ASC, due_at ASC, id ASC"
    with get_connection() as conn:
        return list(conn.execute(query, tuple(params)).fetchall())


def mark_done(user_id: int, task_id: int) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "UPDATE tasks SET is_done = 1 WHERE user_id = ? AND id = ?",
            (user_id, task_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def delete_task(user_id: int, task_id: int) -> bool:
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM tasks WHERE user_id = ? AND id = ?",
            (user_id, task_id),
        )
        conn.commit()
        return cursor.rowcount > 0


def clear_completed(user_id: int) -> int:
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM tasks WHERE user_id = ? AND is_done = 1",
            (user_id,),
        )
        conn.commit()
        return cursor.rowcount


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Привет! Я Бабуков Андрей Валерьевич - твой ручной блокнотик, говори мне о своих задачах и я буду запоминать их и напоминать!!!\n\n"
        "Команды (можно вводить вручную или пользоваться кнопками снизу):\n"
        "/add <название> - добавить задачу (название → описание → дата → время)\n"
        "/skip - пропустить ввод описания\n"
        "/calendar - открыть календарь\n"
        "/list - показать активные задачи\n"
        "/all - показать все задачи\n"
        "/done <id> - отметить задачу выполненной\n"
        "/delete <id> - удалить задачу\n"
        "/clear_done - удалить все выполненные\n"
        "/help - помощь"
    )
    await update.message.reply_text(text, reply_markup=main_menu_markup())


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await start_command(update, context)


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    task_title = " ".join(context.args).strip()
    if not task_title:
        await update.message.reply_text("Использование: /add <название задачи>")
        return
    context.user_data["pending_task_title"] = task_title
    context.user_data["pending_task_description"] = None
    context.user_data["pending_due_date"] = None
    context.user_data["add_stage"] = "await_description"

    await update.message.reply_text(
        "Ок. Теперь можешь отправить описание задачи одним сообщением.\n"
        "Если описание не нужно — отправь /skip."
    )


async def begin_add_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["add_stage"] = "await_title"
    context.user_data.pop("pending_task_title", None)
    context.user_data.pop("pending_task_description", None)
    context.user_data.pop("pending_due_date", None)
    await update.message.reply_text("Напиши название задачи одним сообщением.")


def build_calendar_markup(year: int, month: int) -> InlineKeyboardMarkup:
    month_name = MONTH_LABELS[month - 1]
    rows: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton(f"{month_name} {year}", callback_data="cal:noop")],
        [InlineKeyboardButton(day, callback_data="cal:noop") for day in WEEKDAY_LABELS],
    ]

    month_days = calendar.monthcalendar(year, month)
    for week in month_days:
        row: list[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="cal:noop"))
            else:
                row.append(
                    InlineKeyboardButton(
                        str(day), callback_data=f"cal:day:{year}-{month:02d}-{day:02d}"
                    )
                )
        rows.append(row)

    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1

    next_month = month + 1
    next_year = year
    if next_month == 13:
        next_month = 1
        next_year += 1

    rows.append(
        [
            InlineKeyboardButton(
                "◀", callback_data=f"cal:nav:{prev_year}-{prev_month:02d}"
            ),
            InlineKeyboardButton("Отмена", callback_data="cal:cancel"),
            InlineKeyboardButton(
                "▶", callback_data=f"cal:nav:{next_year}-{next_month:02d}"
            ),
        ]
    )
    return InlineKeyboardMarkup(rows)


async def calendar_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()
    await update.message.reply_text(
        "Календарь:",
        reply_markup=build_calendar_markup(now.year, now.month),
    )


async def skip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if context.user_data.get("add_stage") != "await_description":
        await update.message.reply_text("Сейчас нечего пропускать.")
        return
    context.user_data["pending_task_description"] = None
    context.user_data["add_stage"] = "await_date"
    now = datetime.now()
    await update.message.reply_text(
        "Описание пропущено. Выбери дату для задачи:",
        reply_markup=build_calendar_markup(now.year, now.month),
    )


async def calendar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    if data == "cal:noop":
        return

    if data == "cal:cancel":
        context.user_data.pop("pending_task_title", None)
        context.user_data.pop("pending_task_description", None)
        context.user_data.pop("pending_due_date", None)
        context.user_data.pop("add_stage", None)
        await query.edit_message_text("Добавление задачи отменено.")
        return

    if data.startswith("cal:nav:"):
        raw = data.split("cal:nav:", maxsplit=1)[1]
        year_str, month_str = raw.split("-")
        year = int(year_str)
        month = int(month_str)
        await query.edit_message_reply_markup(reply_markup=build_calendar_markup(year, month))
        return

    if data.startswith("cal:day:"):
        raw_date = data.split("cal:day:", maxsplit=1)[1]
        selected_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
        context.user_data["pending_due_date"] = selected_date.isoformat()
        context.user_data["add_stage"] = "await_time"
        await query.edit_message_text(
            f"Дата выбрана: {selected_date.strftime('%d.%m.%Y')}\n"
            "Теперь отправь время в формате ЧЧ:ММ (например, 14:30)."
        )
        return


def parse_user_time(raw: str) -> time | None:
    try:
        return datetime.strptime(raw.strip(), "%H:%M").time()
    except ValueError:
        return None


def format_task(row: sqlite3.Row) -> str:
    status = "✅" if row["is_done"] else "🕒"
    due_at_raw = row["due_at"]
    if due_at_raw:
        due_at = datetime.fromisoformat(due_at_raw).strftime("%d.%m.%Y %H:%M")
    else:
        due_at = "без даты"
    base = f'{status} #{row["id"]}: {row["text"]} ({due_at})'
    desc = (row["description"] or "").strip()
    if desc:
        return base + f"\n   └ {desc}"
    return base


def get_distinct_chat_ids() -> list[int]:
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT chat_id FROM tasks WHERE chat_id IS NOT NULL"
        ).fetchall()
    return [int(r[0]) for r in rows if r[0] is not None]


def get_tasks_due_on(chat_id: int, day: date) -> list[sqlite3.Row]:
    start = datetime.combine(day, time(0, 0))
    end = start + timedelta(days=1)
    with get_connection() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM tasks
                WHERE chat_id = ?
                  AND is_done = 0
                  AND due_at IS NOT NULL
                  AND due_at >= ?
                  AND due_at < ?
                ORDER BY due_at ASC, id ASC
                """,
                (chat_id, start.isoformat(), end.isoformat()),
            ).fetchall()
        )


def get_tasks_due_between(chat_id: int, start_day: date, end_day_exclusive: date) -> list[sqlite3.Row]:
    start = datetime.combine(start_day, time(0, 0))
    end = datetime.combine(end_day_exclusive, time(0, 0))
    with get_connection() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM tasks
                WHERE chat_id = ?
                  AND is_done = 0
                  AND due_at IS NOT NULL
                  AND due_at >= ?
                  AND due_at < ?
                ORDER BY due_at ASC, id ASC
                """,
                (chat_id, start.isoformat(), end.isoformat()),
            ).fetchall()
        )


def format_tasks_grouped_by_day(tasks: list[sqlite3.Row], start_day: date, end_day_exclusive: date) -> str:
    if not tasks:
        return "Задач на этот период нет."

    by_day: dict[date, list[sqlite3.Row]] = {}
    for t in tasks:
        if not t["due_at"]:
            continue
        d = datetime.fromisoformat(t["due_at"]).date()
        if start_day <= d < end_day_exclusive:
            by_day.setdefault(d, []).append(t)

    lines: list[str] = []
    day = start_day
    while day < end_day_exclusive:
        header = f"{day.strftime('%d.%m.%Y')} ({WEEKDAY_LABELS[day.weekday()]}):"
        day_tasks = by_day.get(day, [])
        if not day_tasks:
            lines.append(header + " —")
        else:
            lines.append(header)
            for t in sorted(day_tasks, key=lambda r: (r["due_at"] or "", int(r["id"]))):
                lines.append("- " + format_task(t))
        day += timedelta(days=1)

    return "\n".join(lines)


def month_range(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1, day=1)
    else:
        end = start.replace(month=start.month + 1, day=1)
    return start, end


def parse_natural_query(raw: str) -> tuple[str, date, date] | None:
    text = (raw or "").strip().lower()
    if not text:
        return None

    today = datetime.now().date()

    m = re.search(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", text)
    if m:
        d = date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        return ("day", d, d + timedelta(days=1))

    if "сегодня" in text:
        return ("today", today, today + timedelta(days=1))
    if "завтра" in text:
        d = today + timedelta(days=1)
        return ("tomorrow", d, d + timedelta(days=1))

    if ("на месяц" in text) or ("в этом месяце" in text) or ("этот месяц" in text):
        start, end = month_range(today)
        return ("month", start, end)

    if ("на неделю" in text) or ("на этой неделе" in text) or ("эту неделю" in text) or ("эта неделя" in text):
        # If user explicitly says "эта/этой", show calendar week (Mon..Sun)
        if ("эт" in text) and ("недел" in text):
            start = monday_of_week(today)
            end = start + timedelta(days=7)
            return ("week", start, end)
        # Otherwise next 7 days starting today
        return ("7days", today, today + timedelta(days=7))

    if ("какие задачи" in text) or ("задачи" in text) or ("что у меня" in text) or ("что запланировано" in text):
        # default fallback for generic "tasks?" questions
        return ("today_default", today, today + timedelta(days=1))

    return None


async def chat_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Don't interfere with the add-task flow: description/time capture is handled elsewhere
    if context.user_data.get("add_stage") in {"await_description", "await_time", "await_date"}:
        return

    query = update.message.text or ""
    parsed = parse_natural_query(query)
    if parsed is None:
        await update.message.reply_text(
            "Я могу подсказать по задачам. Примеры запросов:\n"
            "- Какие задачи на сегодня?\n"
            "- Какие задачи на завтра?\n"
            "- Какие задачи на неделю?\n"
            "- Какие задачи на этот месяц?\n"
            "- Какие задачи на 15.04.2026?\n\n"
            "Или используй /help для списка команд."
        )
        return

    _kind, start_day, end_day = parsed
    tasks = get_tasks_due_between(update.effective_chat.id, start_day, end_day)
    await update.message.reply_text(format_tasks_grouped_by_day(tasks, start_day, end_day))


async def send_period_tasks(update: Update, start_day: date, end_day: date) -> None:
    tasks = get_tasks_due_between(update.effective_chat.id, start_day, end_day)
    await update.message.reply_text(format_tasks_grouped_by_day(tasks, start_day, end_day))


async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    raw_text = update.message.text or ""
    text = normalize_text(raw_text)

    # --- Buttons (reply keyboard) ---
    if text == normalize_text(BUTTON_ADD):
        await begin_add_from_button(update, context)
        return

    if text == normalize_text(BUTTON_SKIP):
        await skip_command(update, context)
        return

    if text == normalize_text(BUTTON_CALENDAR):
        await calendar_command(update, context)
        return

    if text == normalize_text(BUTTON_LIST):
        await list_command(update, context)
        return

    if text == normalize_text(BUTTON_ALL):
        await all_command(update, context)
        return

    if text == normalize_text(BUTTON_CLEAR_DONE):
        await clear_done_command(update, context)
        return

    if text == normalize_text(BUTTON_DONE):
        context.user_data["add_stage"] = "await_done_id"
        await update.message.reply_text("Отправь номер задачи (id), которую нужно отметить выполненной. Пример: 12")
        return

    if text == normalize_text(BUTTON_DELETE):
        context.user_data["add_stage"] = "await_delete_id"
        await update.message.reply_text("Отправь номер задачи (id), которую нужно удалить. Пример: 12")
        return

    if text == normalize_text(BUTTON_TODAY):
        today = datetime.now().date()
        await send_period_tasks(update, today, today + timedelta(days=1))
        return

    if text == normalize_text(BUTTON_WEEK):
        today = datetime.now().date()
        await send_period_tasks(update, today, today + timedelta(days=7))
        return

    if text == normalize_text(BUTTON_MONTH):
        start, end = month_range(datetime.now().date())
        await send_period_tasks(update, start, end)
        return

    if text == normalize_text(BUTTON_HELP):
        await help_command(update, context)
        return

    # --- Flows (add / done / delete) ---
    if context.user_data.get("add_stage") == "await_title":
        title = text
        if not title:
            await update.message.reply_text("Название пустое. Напиши текст задачи.")
            return
        context.user_data["pending_task_title"] = title
        context.user_data["pending_task_description"] = None
        context.user_data["pending_due_date"] = None
        context.user_data["add_stage"] = "await_description"
        await update.message.reply_text(
            "Теперь можешь отправить описание задачи.\n"
            "Если описание не нужно — нажми кнопку «Пропустить описание» или отправь /skip."
        )
        return

    if context.user_data.get("add_stage") == "await_description":
        description = text
        if not description:
            await update.message.reply_text("Описание пустое. Отправь текст или нажми «Пропустить описание».")
            return
        context.user_data["pending_task_description"] = description
        context.user_data["add_stage"] = "await_date"
        now = datetime.now()
        await update.message.reply_text(
            "Описание сохранено. Выбери дату для задачи:",
            reply_markup=build_calendar_markup(now.year, now.month),
        )
        return

    if context.user_data.get("add_stage") == "await_time":
        pending_task_title = context.user_data.get("pending_task_title")
        pending_due_date = context.user_data.get("pending_due_date")
        if not pending_task_title or not pending_due_date:
            await update.message.reply_text("Не вижу выбранной даты. Нажми «Календарь» и выбери день заново.")
            return

        user_time = parse_user_time(text)
        if user_time is None:
            await update.message.reply_text("Неверный формат. Отправь время как ЧЧ:ММ.")
            return

        due_date = datetime.strptime(pending_due_date, "%Y-%m-%d").date()
        due_at = datetime.combine(due_date, user_time)
        task_id = add_task(
            update.effective_user.id,
            update.effective_chat.id,
            pending_task_title,
            context.user_data.get("pending_task_description"),
            due_at,
        )

        context.user_data.pop("pending_task_title", None)
        context.user_data.pop("pending_task_description", None)
        context.user_data.pop("pending_due_date", None)
        context.user_data.pop("add_stage", None)

        await update.message.reply_text(
            f"Задача добавлена: #{task_id}\n"
            f"Дата и время: {due_at.strftime('%d.%m.%Y %H:%M')}"
        )
        return

    if context.user_data.get("add_stage") == "await_done_id":
        try:
            task_id = int(text)
        except ValueError:
            await update.message.reply_text("Нужно число (id задачи). Пример: 12")
            return
        context.user_data.pop("add_stage", None)
        context.args = [str(task_id)]
        await done_command(update, context)
        return

    if context.user_data.get("add_stage") == "await_delete_id":
        try:
            task_id = int(text)
        except ValueError:
            await update.message.reply_text("Нужно число (id задачи). Пример: 12")
            return
        context.user_data.pop("add_stage", None)
        context.args = [str(task_id)]
        await delete_command(update, context)
        return

    # --- Free-form questions about tasks ---
    await chat_query_handler(update, context)


def get_tasks_for_notification(chat_id: int, day: date, which: str) -> list[sqlite3.Row]:
    if which == "3d":
        flag_col = "notified_3d"
    elif which == "1d":
        flag_col = "notified_1d"
    else:
        raise ValueError("which must be '3d' or '1d'")
    start = datetime.combine(day, time(0, 0))
    end = start + timedelta(days=1)
    with get_connection() as conn:
        return list(
            conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE chat_id = ?
                  AND is_done = 0
                  AND due_at IS NOT NULL
                  AND due_at >= ?
                  AND due_at < ?
                  AND {flag_col} = 0
                ORDER BY due_at ASC, id ASC
                """,
                (chat_id, start.isoformat(), end.isoformat()),
            ).fetchall()
        )

def get_tasks_for_1h_notification(chat_id: int, target_dt: datetime, window_end: datetime) -> list[sqlite3.Row]:
    with get_connection() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM tasks
                WHERE chat_id = ?
                  AND is_done = 0
                  AND due_at IS NOT NULL
                  AND due_at >= ?
                  AND due_at < ?
                  AND notified_1h = 0
                ORDER BY due_at ASC, id ASC
                """,
                (chat_id, target_dt.isoformat(), window_end.isoformat()),
            ).fetchall()
        )


def mark_notified(task_ids: list[int], which: str) -> None:
    if not task_ids:
        return
    if which == "3d":
        flag_col = "notified_3d"
    elif which == "1d":
        flag_col = "notified_1d"
    elif which == "1h":
        flag_col = "notified_1h"
    else:
        raise ValueError("which must be '3d', '1d' or '1h'")
    placeholders = ",".join("?" for _ in task_ids)
    with get_connection() as conn:
        conn.execute(
            f"UPDATE tasks SET {flag_col} = 1 WHERE id IN ({placeholders})",
            tuple(task_ids),
        )
        conn.commit()


async def notify_daily(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = datetime.now().date()
    for chat_id in get_distinct_chat_ids():
        for delta_days, which, label in [(3, "3d", "через 3 дня"), (1, "1d", "завтра")]:
            target = today + timedelta(days=delta_days)
            tasks = get_tasks_for_notification(chat_id, target, which)
            if not tasks:
                continue
            lines = "\n".join(format_task(t) for t in tasks)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"Напоминание: задачи {label} ({target.strftime('%d.%m.%Y')}):\n{lines}",
            )
            mark_notified([int(t["id"]) for t in tasks], which)


async def notify_daily_app(app: Application) -> None:
    class _C:
        bot = app.bot

    await notify_daily(_C())  # type: ignore[arg-type]


async def notify_1h(context: ContextTypes.DEFAULT_TYPE) -> None:
    # "Strictly 60 minutes" with minute precision.
    # Tasks are stored as HH:MM, so we round current time down to minute
    # to avoid missing tasks due to seconds/microseconds.
    now = datetime.now().replace(second=0, microsecond=0)
    target = now + timedelta(minutes=60)
    window_end = target + timedelta(minutes=1)
    for chat_id in get_distinct_chat_ids():
        tasks = get_tasks_for_1h_notification(chat_id, target, window_end)
        if not tasks:
            continue
        lines = "\n".join(format_task(t) for t in tasks)
        await context.bot.send_message(
            chat_id=chat_id,
            text="Напоминание: до задачи остался 1 час:\n" + lines,
        )
        mark_notified([int(t["id"]) for t in tasks], "1h")


async def notify_1h_app(app: Application) -> None:
    class _C:
        bot = app.bot

    await notify_1h(_C())  # type: ignore[arg-type]


def monday_of_week(d: date) -> date:
    return d - timedelta(days=d.weekday())


async def weekly_overview(context: ContextTypes.DEFAULT_TYPE) -> None:
    today = datetime.now().date()
    week_start = monday_of_week(today)
    week_end = week_start + timedelta(days=7)

    for chat_id in get_distinct_chat_ids():
        tasks = get_tasks_due_between(chat_id, week_start, week_end)
        if not tasks:
            continue

        by_day: dict[date, list[sqlite3.Row]] = {week_start + timedelta(days=i): [] for i in range(7)}
        for t in tasks:
            due = datetime.fromisoformat(t["due_at"]).date()
            if week_start <= due < week_end:
                by_day[due].append(t)

        parts: list[str] = []
        for i in range(7):
            day = week_start + timedelta(days=i)
            day_tasks = by_day[day]
            header = f"{WEEKDAY_LABELS[i]} {day.strftime('%d.%m')}:"
            if not day_tasks:
                parts.append(header + " —")
            else:
                parts.append(header)
                parts.extend([f"- {format_task(t)}" for t in day_tasks])

        await context.bot.send_message(
            chat_id=chat_id,
            text="Задачи на эту неделю:\n" + "\n".join(parts),
        )


async def weekly_overview_app(app: Application) -> None:
    class _C:
        bot = app.bot

    await weekly_overview(_C())  # type: ignore[arg-type]


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    tasks = list_tasks(user_id, show_all=False)
    if not tasks:
        await update.message.reply_text("Активных задач пока нет.")
        return
    text = "Твои активные задачи:\n" + "\n".join(format_task(task) for task in tasks)
    await update.message.reply_text(text)


async def all_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    tasks = list_tasks(user_id, show_all=True)
    if not tasks:
        await update.message.reply_text("Список задач пуст.")
        return
    text = "Все задачи:\n" + "\n".join(format_task(task) for task in tasks)
    await update.message.reply_text(text)


def parse_task_id(args: list[str]) -> int | None:
    if not args:
        return None
    try:
        return int(args[0])
    except ValueError:
        return None


async def done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    task_id = parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("Использование: /done <id>")
        return
    success = mark_done(user_id, task_id)
    if not success:
        await update.message.reply_text("Задача не найдена.")
        return
    await update.message.reply_text(f"Задача #{task_id} отмечена выполненной.")


async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    task_id = parse_task_id(context.args)
    if task_id is None:
        await update.message.reply_text("Использование: /delete <id>")
        return
    success = delete_task(user_id, task_id)
    if not success:
        await update.message.reply_text("Задача не найдена.")
        return
    await update.message.reply_text(f"Задача #{task_id} удалена.")


async def clear_done_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    removed = clear_completed(user_id)
    await update.message.reply_text(f"Удалено выполненных задач: {removed}")

def get_next_tasks(chat_id: int, limit: int = 10) -> list[sqlite3.Row]:
    now = datetime.now().replace(second=0, microsecond=0)
    with get_connection() as conn:
        return list(
            conn.execute(
                """
                SELECT * FROM tasks
                WHERE chat_id = ?
                  AND is_done = 0
                  AND due_at IS NOT NULL
                  AND due_at >= ?
                ORDER BY due_at ASC, id ASC
                LIMIT ?
                """,
                (chat_id, now.isoformat(), limit),
            ).fetchall()
        )


async def debug_reminders_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()
    app = context.application
    jq = getattr(app, "job_queue", None)
    jq_enabled = bool(jq)

    lines: list[str] = []
    lines.append(f"Время сервера: {now.strftime('%d.%m.%Y %H:%M:%S')}")
    lines.append(f"JobQueue: {'включен' if jq_enabled else 'ОТКЛЮЧЕН'}")

    if jq_enabled:
        try:
            jobs = jq.jobs()
            lines.append(f"Jobs: {len(jobs)}")
            for j in jobs:
                # PTB Job has name and next_t attributes; guard just in case.
                name = getattr(j, "name", None) or "<без имени>"
                next_t = getattr(j, "next_t", None)
                if next_t:
                    lines.append(f"- {name}: next={next_t}")
                else:
                    lines.append(f"- {name}")
        except Exception as e:
            lines.append(f"Jobs: не удалось прочитать ({type(e).__name__}: {e})")

    chat_id = update.effective_chat.id
    upcoming = get_next_tasks(chat_id, limit=10)
    lines.append("")
    lines.append("Ближайшие задачи (топ-10):")
    if not upcoming:
        lines.append("— нет")
    else:
        for t in upcoming:
            due = datetime.fromisoformat(t["due_at"]).strftime("%d.%m.%Y %H:%M")
            flags = f"1h={t['notified_1h']} 1d={t['notified_1d']} 3d={t['notified_3d']}"
            lines.append(f"- #{t['id']} {due} | {t['text']} | {flags}")

    # What would be picked up by strict 60m check right now?
    now_floor = now.replace(second=0, microsecond=0)
    target = now_floor + timedelta(minutes=60)
    window_end = target + timedelta(minutes=1)
    due_soon = get_tasks_for_1h_notification(chat_id, target, window_end)
    lines.append("")
    lines.append(
        f"Окно 'строго +60 мин': {target.strftime('%H:%M')}–{window_end.strftime('%H:%M')} (сегодня/возможен переход даты)"
    )
    if not due_soon:
        lines.append("Совпадений нет.")
    else:
        for t in due_soon:
            due = datetime.fromisoformat(t["due_at"]).strftime("%d.%m.%Y %H:%M")
            lines.append(f"- #{t['id']} {due} | {t['text']}")

    await update.message.reply_text("\n".join(lines))


def _now_local() -> datetime:
    try:
        return datetime.now(LOCAL_TZ)  # type: ignore[arg-type]
    except TypeError:
        return datetime.now().astimezone()


async def fallback_scheduler(app: Application) -> None:
    # 1h reminders: aligned to minute boundary
    async def loop_1h() -> None:
        while True:
            now = _now_local()
            sleep_s = max(1.0, 60.0 - now.second - (now.microsecond / 1_000_000.0))
            await asyncio.sleep(sleep_s)
            await notify_1h_app(app)

    # Daily reminders at 09:00 local time
    async def loop_daily_0900() -> None:
        while True:
            now = _now_local()
            target = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())
            await notify_daily_app(app)

    # Weekly overview Mondays at 09:05 local time
    async def loop_weekly_mon_0905() -> None:
        while True:
            now = _now_local()
            target = now.replace(hour=9, minute=5, second=0, microsecond=0)
            days_ahead = (0 - target.weekday()) % 7
            if days_ahead == 0 and target <= now:
                days_ahead = 7
            target = target + timedelta(days=days_ahead)
            await asyncio.sleep((target - now).total_seconds())
            await weekly_overview_app(app)

    app.create_task(loop_1h())
    app.create_task(loop_daily_0900())
    app.create_task(loop_weekly_mon_0905())


async def post_init(application: Application) -> None:
    # This runs inside a running event loop (safe to create tasks here).
    if application.job_queue is None:
        logger.warning(
            "JobQueue is disabled. Using fallback asyncio scheduler for reminders."
        )
        application.create_task(fallback_scheduler(application))


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Переменная окружения BOT_TOKEN не установлена")

    # Python 3.14+ may not create a default loop automatically.
    asyncio.set_event_loop(asyncio.new_event_loop())

    init_db()
    app = Application.builder().token(token).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("add", add_command))
    app.add_handler(CommandHandler("skip", skip_command))
    app.add_handler(CommandHandler("calendar", calendar_command))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("all", all_command))
    app.add_handler(CommandHandler("done", done_command))
    app.add_handler(CommandHandler("delete", delete_command))
    app.add_handler(CommandHandler("clear_done", clear_done_command))
    app.add_handler(CommandHandler("debug_reminders", debug_reminders_command))
    app.add_handler(CallbackQueryHandler(calendar_callback, pattern=r"^cal:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))

    if app.job_queue is not None:
        app.job_queue.run_daily(
            notify_daily,
            time=time(9, 0, tzinfo=LOCAL_TZ),
            name="notify_daily",
        )
        # Align "strict 60 minutes" checks to minute boundaries to avoid missing events
        # if the job starts slightly after the minute tick.
        now = datetime.now()
        seconds_to_next_minute = 60 - now.second
        first_in = max(1, seconds_to_next_minute + 1)
        app.job_queue.run_repeating(
            notify_1h,
            interval=60,
            first=first_in,
            name="notify_1h",
        )
        app.job_queue.run_daily(
            weekly_overview,
            time=time(9, 5, tzinfo=LOCAL_TZ),
            days=(0,),
            name="weekly_overview",
        )

    logger.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
