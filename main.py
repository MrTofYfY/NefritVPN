import os
import json
import uuid
import base64
import asyncio
import secrets
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from aiohttp import web, WSMsgType, ClientSession
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()

# ============== КОНФИГУРАЦИЯ ==============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "mellfreezy")
BASE_URL = os.getenv("BASE_URL", "https://nefritvpn.onrender.com")
PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "vpn.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

SUPPORT_USERNAME = "mellfreezy"
CHANNEL_USERNAME = "nefrit_vpn"

xray_process = None


# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                key_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                days INTEGER,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER,
                used_by_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activated_at TIMESTAMP,
                expires_at TIMESTAMP,
                is_revoked BOOLEAN DEFAULT 0
            )
        ''')
        await db.commit()


async def create_key(days=None):
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO keys (key, days) VALUES (?, ?)",
            (key, days)
        )
        await db.commit()
        cursor = await db.execute("SELECT id FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        key_id = row[0] if row else None
    return key, key_id, days


async def get_key_info(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      created_at, activated_at, expires_at, is_revoked 
               FROM keys WHERE id = ?""",
            (key_id,)
        )
        return await cursor.fetchone()


async def revoke_key(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE keys SET is_revoked = 1 WHERE id = ?", (key_id,))
        await db.execute("UPDATE users SET is_active = 0 WHERE key_id = ?", (key_id,))
        await db.commit()
    await restart_xray()


async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT user_uuid, path FROM users 
               WHERE is_active = 1 
               AND (expires_at IS NULL OR expires_at > datetime('now'))"""
        )
        return await cursor.fetchall()


async def check_expired_users():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE users SET is_active = 0 
               WHERE expires_at IS NOT NULL 
               AND expires_at <= datetime('now') 
               AND is_active = 1"""
        )
        await db.commit()


async def activate_key(key, user_id, username):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?",
            (key,)
        )
        row = await cursor.fetchone()

        if not row:
            return None, "❌ Ключ не найден"

        key_id, is_used, days, is_revoked = row

        if is_revoked:
            return None, "❌ Ключ аннулирован"
        if is_used:
            return None, "❌ Ключ уже использован"

        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None

        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"

        now = datetime.now()
        expires_at = now + timedelta(days=days) if days else None

        await db.execute(
            """INSERT INTO users (user_id, username, user_uuid, path, key_id, expires_at) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, username, user_uuid, user_path, key_id, expires_at)
        )
        await db.execute(
            """UPDATE keys SET is_used = 1, used_by = ?, used_by_username = ?, 
                              activated_at = ?, expires_at = ? 
               WHERE key = ?""",
            (user_id, username, now, expires_at, key)
        )
        await db.commit()
        await restart_xray()
        return user_path, None


async def get_user_info(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active, expires_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        active_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0")
        free_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        return {
            "active_users": active_users,
            "total_users": total_users,
            "free_keys": free_keys,
            "total_keys": total_keys
        }


async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      expires_at, is_revoked, created_at
               FROM keys ORDER BY id DESC LIMIT 20"""
        )
        return await cursor.fetchall()


# ============== XRAY ==============
async def generate_xray_config():
    await check_expired_users()
    users = await get_all_users()

    clients = [{"id": user_uuid, "level": 0} for user_uuid, path in users]
    if not clients:
        clients.append({"id": str(uuid.uuid4()), "level": 0})

    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": XRAY_PORT,
            "listen": "127.0.0.1",
            "protocol": "vless",
            "settings": {"clients": clients, "decryption": "none"},
            "streamSettings": {"network": "ws", "wsSettings": {"path": "/tunnel"}}
        }],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}],
        "dns": {"servers": ["8.8.8.8", "1.1.1.1"]}
    }

    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    print(f"✅ Xray config: {len(clients)} clients")


def start_xray():
    global xray_process
    if not XRAY_CONFIG_PATH.exists():
        return False
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅ Xray started, PID: {xray_process.pid}")
        return True
    except Exception as e:
        print(f"❌ Failed to start Xray: {e}")
        return False


def stop_xray():
    global xray_process
    if xray_process:
        xray_process.terminate()
        xray_process.wait()
        xray_process = None


async def restart_xray():
    stop_xray()
    await generate_xray_config()
    await asyncio.sleep(1)
    start_xray()
    await asyncio.sleep(2)


def generate_vless_link(user_uuid, user_path):
    host = BASE_URL.replace("https://", "").replace("http://", "")
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=tls&type=ws&host={host}&path=%2Ftunnel#Nefrit-{user_path}"


def generate_subscription(user_uuid, user_path):
    return base64.b64encode(generate_vless_link(user_uuid, user_path).encode()).decode()


# ============== WEB СЕРВЕР ==============
async def handle_index(request):
    return web.Response(text="<h1>🟢 Nefrit VPN Active</h1>", content_type="text/html")


async def handle_health(request):
    return web.json_response({
        "status": "ok",
        "xray": xray_process is not None and xray_process.poll() is None
    })


async def handle_subscription(request):
    path = request.match_info["path"]
    await check_expired_users()

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active, expires_at FROM users WHERE path = ?",
            (path,)
        )
        row = await cursor.fetchone()

    if not row:
        return web.Response(text="Not found", status=404)

    user_uuid, is_active, expires_at = row

    if not is_active:
        return web.Response(text="Subscription expired", status=403)

    if expires_at:
        if datetime.fromisoformat(expires_at) <= datetime.now():
            return web.Response(text="Subscription expired", status=403)

    return web.Response(
        text=generate_subscription(user_uuid, path),
        content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )


async def handle_tunnel(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)

    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)

    try:
        async with ClientSession() as session:
            async with session.ws_connect(
                f"http://127.0.0.1:{XRAY_PORT}/tunnel",
                timeout=30
            ) as ws_xray:

                async def forward(src, dst):
                    try:
                        async for msg in src:
                            if msg.type == WSMsgType.BINARY:
                                await dst.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await dst.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except Exception:
                        pass

                await asyncio.gather(
                    forward(ws_client, ws_xray),
                    forward(ws_xray, ws_client),
                    return_exceptions=True
                )
    except Exception:
        pass
    finally:
        if not ws_client.closed:
            await ws_client.close()

    return ws_client


# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()
    waiting_revoke_id = State()


def is_admin(user):
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()


def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="keys")],
        [InlineKeyboardButton(text="🚫 Аннулировать ключ", callback_data="revoke")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🔄 Перезапустить Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])


def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="back")]
    ])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")]
    ])


def days_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 дней", callback_data="days_7"),
            InlineKeyboardButton(text="14 дней", callback_data="days_14"),
            InlineKeyboardButton(text="30 дней", callback_data="days_30")
        ],
        [
            InlineKeyboardButton(text="60 дней", callback_data="days_60"),
            InlineKeyboardButton(text="90 дней", callback_data="days_90"),
            InlineKeyboardButton(text="180 дней", callback_data="days_180")
        ],
        [InlineKeyboardButton(text="365 дней", callback_data="days_365")],
        [InlineKeyboardButton(text="♾ Бесконечный ключ", callback_data="days_infinite")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin")]
    ])


def format_expires(expires_at, is_revoked=False):
    if is_revoked:
        return "🚫 Аннулирован"
    if expires_at is None:
        return "♾ Бессрочно"
    try:
        exp_date = datetime.fromisoformat(str(expires_at))
        now = datetime.now()
        if exp_date <= now:
            return "⏰ Истёк"
        days = (exp_date - now).days
        if days == 0:
            hours = (exp_date - now).seconds // 3600
            return f"⏳ {hours} ч."
        return f"⏳ {days} дн."
    except Exception:
        return "❓"


# ============== БЕЗОПАСНЫЕ ФУНКЦИИ ==============
async def safe_edit(message, text, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")


async def safe_send(target, text, reply_markup=None):
    await target.answer(text, reply_markup=reply_markup, parse_mode="HTML")


# ============== ХЕНДЛЕРЫ БОТА ==============
@dp.message(CommandStart())
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        f"🌟 <b>Добро пожаловать в Nefrit VPN!</b>\n\n"
        f"Привет, <b>{msg.from_user.first_name}</b>! 👋\n\n"
        f"🚀 Быстрый и надёжный VPN\n"
        f"🔒 Полная безопасность\n"
        f"🌍 Доступ к любым сайтам\n\n"
        f"Выберите действие 👇",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(
        cb.message,
        "🌟 <b>Nefrit VPN</b> — Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user))
    )
    await cb.answer()


@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await safe_edit(
        cb.message,
        "🔑 <b>Введите ваш ключ активации:</b>\n\n"
        "<i>Пример: NEFRIT-A1B2C3D4E5F6G7H8</i>\n\n"
        "Ключ можно получить у администратора.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ])
    )
    await cb.answer()


@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(
        key,
        msg.from_user.id,
        msg.from_user.username or msg.from_user.first_name
    )
    await state.clear()

    if error:
        await safe_send(msg, error, reply_markup=back_kb())
        return

    info = await get_user_info(msg.from_user.id)
    if not info:
        await safe_send(msg, "❌ Ошибка получения данных", reply_markup=back_kb())
        return

    path, user_uuid, is_active, expires_at = info
    link = generate_vless_link(user_uuid, path)
    sub_url = f"{BASE_URL}/sub/{path}"

    if expires_at:
        exp_date = datetime.fromisoformat(str(expires_at))
        exp_info = f"\n⏰ <b>Действует до:</b> {exp_date.strftime('%d.%m.%Y %H:%M')}"
    else:
        exp_info = "\n♾ <b>Срок действия:</b> Бессрочно"

    await safe_send(
        msg,
        f"✅ <b>Подписка активирована!</b>{exp_info}\n\n"
        f"📱 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"🔗 <b>Прямой конфиг:</b>\n<code>{link}</code>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<b>📲 Как подключиться:</b>\n\n"
        f"<b>Android:</b> V2rayNG\n"
        f"<b>iOS:</b> Streisand / V2Box\n"
        f"<b>Windows:</b> V2rayN\n"
        f"<b>macOS:</b> V2rayU",
        reply_markup=back_kb()
    )


@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    await check_expired_users()
    info = await get_user_info(cb.from_user.id)

    if not info:
        await safe_edit(
            cb.message,
            "❌ <b>У вас нет активной подписки</b>\n\n"
            "Нажмите «🔑 Активировать подписку» для активации.",
            reply_markup=back_kb()
        )
        await cb.answer()
        return

    path, user_uuid, is_active, expires_at = info
    link = generate_vless_link(user_uuid, path)
    sub_url = f"{BASE_URL}/sub/{path}"

    status = "✅ Активна" if is_active else "❌ Неактивна"

    if expires_at:
        exp_date = datetime.fromisoformat(str(expires_at))
        now = datetime.now()
        if exp_date > now:
            days_left = (exp_date - now).days
            exp_str = f"{exp_date.strftime('%d.%m.%Y')} ({days_left} дн.)"
        else:
            exp_str = "Истёк"
    else:
        exp_str = "♾ Бессрочно"

    await safe_edit(
        cb.message,
        f"📊 <b>Ваша подписка</b>\n\n"
        f"├ <b>Статус:</b> {status}\n"
        f"├ <b>Срок:</b> {exp_str}\n"
        f"└ <b>ID:</b> {path}\n\n"
        f"🔗 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"📱 <b>Конфиг:</b>\n<code>{link}</code>",
        reply_markup=back_kb()
    )
    await cb.answer()


# ============== АДМИН-ПАНЕЛЬ ==============
@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await state.clear()
    stats = await get_stats()
    xray_running = xray_process is not None and xray_process.poll() is None
    xray_status = "🟢" if xray_running else "🔴"

    await safe_edit(
        cb.message,
        f"⚙️ <b>Админ-панель</b>\n\n"
        f"👥 Активных: {stats['active_users']} / {stats['total_users']}\n"
        f"🔑 Свободных ключей: {stats['free_keys']} / {stats['total_keys']}\n"
        f"📡 Xray: {xray_status}",
        reply_markup=admin_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "newkey")
async def new_k
xray_process = None

# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                key_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                days INTEGER,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER,
                used_by_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activated_at TIMESTAMP,
                expires_at TIMESTAMP,
                is_revoked BOOLEAN DEFAULT 0
            )
        ''')
        await db.commit()

async def create_key(days: int = None):
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO keys (key, days) VALUES (?, ?)",
            (key, days)
        )
        await db.commit()
        cursor = await db.execute("SELECT id FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        key_id = row[0] if row else None
    return key, key_id, days

async def get_key_info(key_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      created_at, activated_at, expires_at, is_revoked 
               FROM keys WHERE id = ?""",
            (key_id,)
        )
        return await cursor.fetchone()

async def revoke_key(key_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE keys SET is_revoked = 1 WHERE id = ?", (key_id,))
        await db.execute("UPDATE users SET is_active = 0 WHERE key_id = ?", (key_id,))
        await db.commit()
    await restart_xray()

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT user_uuid, path FROM users 
               WHERE is_active = 1 
               AND (expires_at IS NULL OR expires_at > datetime('now'))"""
        )
        return await cursor.fetchall()

async def check_expired_users():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE users SET is_active = 0 
               WHERE expires_at IS NOT NULL 
               AND expires_at <= datetime('now') 
               AND is_active = 1"""
        )
        await db.commit()

async def activate_key(key: str, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?", 
            (key,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return None, "❌ Ключ не найден"
        
        key_id, is_used, days, is_revoked = row
        
        if is_revoked:
            return None, "❌ Ключ аннулирован"
        if is_used:
            return None, "❌ Ключ уже использован"
        
        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None
        
        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"
        
        now = datetime.now()
        expires_at = now + timedelta(days=days) if days else None
        
        await db.execute(
            """INSERT INTO users (user_id, username, user_uuid, path, key_id, expires_at) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, username, user_uuid, user_path, key_id, expires_at)
        )
        await db.execute(
            """UPDATE keys SET is_used = 1, used_by = ?, used_by_username = ?, 
                              activated_at = ?, expires_at = ? 
               WHERE key = ?""",
            (user_id, username, now, expires_at, key)
        )
        await db.commit()
        await restart_xray()
        return user_path, None

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active, expires_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        active_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0")
        free_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        return {
            "active_users": active_users,
            "total_users": total_users,
            "free_keys": free_keys,
            "total_keys": total_keys
        }

async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      expires_at, is_revoked, created_at
               FROM keys ORDER BY id DESC LIMIT 20"""
        )
        return await cursor.fetchall()

# ============== XRAY ==============
async def generate_xray_config():
    await check_expired_users()
    users = await get_all_users()
    
    clients = [{"id": user_uuid, "level": 0} for user_uuid, path in users]
    if not clients:
        clients.append({"id": str(uuid.uuid4()), "level": 0})
    
    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": XRAY_PORT,
            "listen": "127.0.0.1",
            "protocol": "vless",
            "settings": {"clients": clients, "decryption": "none"},
            "streamSettings": {"network": "ws", "wsSettings": {"path": "/tunnel"}}
        }],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}],
        "dns": {"servers": ["8.8.8.8", "1.1.1.1"]}
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    print(f"✅ Xray config: {len(clients)} clients")

def start_xray():
    global xray_process
    if not XRAY_CONFIG_PATH.exists():
        return False
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        print(f"✅ Xray started, PID: {xray_process.pid}")
        return True
    except Exception as e:
        print(f"❌ Failed to start Xray: {e}")
        return False

def stop_xray():
    global xray_process
    if xray_process:
        xray_process.terminate()
        xray_process.wait()
        xray_process = None

async def restart_xray():
    stop_xray()
    await generate_xray_config()
    await asyncio.sleep(1)
    start_xray()
    await asyncio.sleep(2)

def generate_vless_link(user_uuid: str, user_path: str):
    host = BASE_URL.replace("https://", "").replace("http://", "")
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=tls&type=ws&host={host}&path=%2Ftunnel#Nefrit-{user_path}"

def generate_subscription(user_uuid: str, user_path: str):
    return base64.b64encode(generate_vless_link(user_uuid, user_path).encode()).decode()

# ============== WEB СЕРВЕР ==============
async def handle_index(request):
    return web.Response(text="<h1>🟢 Nefrit VPN Active</h1>", content_type="text/html")

async def handle_health(request):
    return web.json_response({"status": "ok", "xray": xray_process and xray_process.poll() is None})

async def handle_subscription(request):
    path = request.match_info["path"]
    await check_expired_users()
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active, expires_at FROM users WHERE path = ?", (path,)
        )
        row = await cursor.fetchone()
    
    if not row:
        return web.Response(text="Not found", status=404)
    
    user_uuid, is_active, expires_at = row
    
    if not is_active:
        return web.Response(text="Subscription expired", status=403)
    
    if expires_at:
        if datetime.fromisoformat(expires_at) <= datetime.now():
            return web.Response(text="Subscription expired", status=403)
    
    return web.Response(
        text=generate_subscription(user_uuid, path),
        content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )

async def handle_tunnel(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    try:
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{XRAY_PORT}/tunnel", timeout=30) as ws_xray:
                async def forward(src, dst, is_binary=True):
                    try:
                        async for msg in src:
                            if msg.type == WSMsgType.BINARY:
                                await dst.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await dst.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except:
                        pass
                
                await asyncio.gather(
                    forward(ws_client, ws_xray),
                    forward(ws_xray, ws_client),
                    return_exceptions=True
                )
    except:
        pass
    finally:
        if not ws_client.closed:
            await ws_client.close()
    return ws_client

# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()
    waiting_revoke_id = State()

def is_admin(user: types.User) -> bool:
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="keys")],
        [InlineKeyboardButton(text="🚫 Аннулировать ключ", callback_data="revoke")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🔄 Перезапустить Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="back")]
    ])

def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")]
    ])

def days_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 дней", callback_data="days_7"),
            InlineKeyboardButton(text="14 дней", callback_data="days_14"),
            InlineKeyboardButton(text="30 дней", callback_data="days_30")
        ],
        [
            InlineKeyboardButton(text="60 дней", callback_data="days_60"),
            InlineKeyboardButton(text="90 дней", callback_data="days_90"),
            InlineKeyboardButton(text="180 дней", callback_data="days_180")
        ],
        [InlineKeyboardButton(text="365 дней", callback_data="days_365")],
        [InlineKeyboardButton(text="♾ Бесконечный ключ", callback_data="days_infinite")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin")]
    ])

def format_expires(expires_at, is_revoked=False):
    if is_revoked:
        return "🚫 Аннулирован"
    if expires_at is None:
        return "♾ Бессрочно"
    try:
        exp_date = datetime.fromisoformat(str(expires_at))
        now = datetime.now()
        if exp_date <= now:
            return "⏰ Истёк"
        days = (exp_date - now).days
        if days == 0:
            return f"⏳ {(exp_date - now).seconds // 3600} ч."
        return f"⏳ {days} дн."
    except:
        return "❓"

# ============== Безопасное редактирование сообщений ==============
async def safe_edit(message: types.Message, text: str, reply_markup=None):
    """Безопасное редактирование сообщения с обработкой ошибок"""
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        # Если не удалось отредактировать - отправляем новое
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")

async def safe_send(target, text: str, reply_markup=None):
    """Отправка сообщения (для msg.answer)"""
    await target.answer(text, reply_markup=reply_markup, parse_mode="HTML")

# ============== ХЕНДЛЕРЫ БОТА ==============
@dp.message(CommandStart())
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    await msg.answer(
        f"🌟 <b>Добро пожаловать в Nefrit VPN!</b>\n\n"
        f"Привет, <b>{msg.from_user.first_name}</b>! 👋\n\n"
        f"🚀 Быстрый и надёжный VPN\n"
        f"🔒 Полная безопасность\n"
        f"🌍 Доступ к любым сайтам\n\n"
        f"Выберите действие 👇",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(
        cb.message,
        "🌟 <b>Nefrit VPN</b> — Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user))
    )
    await cb.answer()

@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await safe_edit(
        cb.message,
        "🔑 <b>Введите ваш ключ активации:</b>\n\n"
        "<i>Пример: NEFRIT-A1B2C3D4E5F6G7H8</i>\n\n"
        "Ключ можно получить у администратора.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ])
    )
    await cb.answer()

@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(key, msg.from_user.id, msg.from_user.username or msg.from_user.first_name)
    await state.clear()
    
    if error:
        await safe_send(msg, error, reply_markup=back_kb())
        return
    
    info = await get_user_info(msg.from_user.id)
    if not info:
        await safe_send(msg, "❌ Ошибка получения данных", reply_markup=back_kb())
        return
    
    path, user_uuid, is_active, expires_at = info
    link = generate_vless_link(user_uuid, path)
    sub_url = f"{BASE_URL}/sub/{path}"
    
    if expires_at:
        exp_date = datetime.fromisoformat(str(expires_at))
        exp_info = f"\n⏰ <b>Действует до:</b> {exp_date.strftime('%d.%m.%Y %H:%M')}"
    else:
        exp_info = "\n♾ <b>Срок действия:</b> Бессрочно"
    
    await safe_send(
        msg,
        f"✅ <b>Подписка активирована!</b>{exp_info}\n\n"
        f"📱 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"🔗 <b>Прямой конфиг:</b>\n<code>{link}</code>\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<b>📲 Как подключиться:</b>\n\n"
        f"<b>Android:</b> V2rayNG\n"
        f"<b>iOS:</b> Streisand / V2Box\n"
        f"<b>Windows:</b> V2rayN\n"
        f"<b>macOS:</b> V2rayU",
        reply_markup=back_kb()
    )

@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    await check_expired_users()
    info = await get_user_info(cb.from_user.id)
    
    if not info:
        await safe_edit(
            cb.message,
            "❌ <b>У вас нет активной подписки</b>\n\n"
            "Нажмите «🔑 Активировать подписку» для активации.",
            reply_markup=back_kb()
        )
        await cb.answer()
        return
    
    path, user_uuid, is_active, expires_at = info
    link = generate_vless_link(user_uuid, path)
    sub_url = f"{BASE_URL}/sub/{path}"
    
    status = "✅ Активна" if is_active else "❌ Неактивна"
    
    if expires_at:
        exp_date = datetime.fromisoformat(str(expires_at))
        now = datetime.now()
        if exp_date > now:
            days_left = (exp_date - now).days
            exp_str = f"{exp_date.strftime('%d.%m.%Y')} ({days_left} дн.)"
        else:
            exp_str = "Истёк"
    else:
        exp_str = "♾ Бессрочно"
    
    await safe_edit(
        cb.message,
        f"📊 <b>Ваша подписка</b>\n\n"
        f"├ <b>Статус:</b> {status}\n"
        f"├ <b>Срок:</b> {exp_str}\n"
        f"└ <b>ID:</b> {path}\n\n"
        f"🔗 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"📱 <b>Конфиг:</b>\n<code>{link}</code>",
        reply_markup=back_kb()
    )
    await cb.answer()

# ============== АДМИН-ПАНЕЛЬ ==============
@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("⛔ Доступ запрещён", show_alert=True)
        return
    
    await state.clear()
    stats = await get_stats()
    xray_status = "🟢" if (xray_process and xray_process.poll() is None) else "🔴"
    
    await safe_edit(
        cb.message,
        f"⚙️ <b>Админ-панель</b>\n\n"
        f"👥 Активных: {stats['active_users']} / {stats['total_users']}\n"
f"🔑 Ключей: {stats['free_keys']} / {stats['total_keys']}\n"

# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                key_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                days INTEGER,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER,
                used_by_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                activated_at TIMESTAMP,
                expires_at TIMESTAMP,
                is_revoked BOOLEAN DEFAULT 0
            )
        ''')
        await db.commit()

async def create_key(days: int = None):
    """Создать ключ. days=None означает бесконечный"""
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO keys (key, days) VALUES (?, ?)",
            (key, days)
        )
        await db.commit()
        
        # Получаем ID созданного ключа
        cursor = await db.execute("SELECT id FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        key_id = row[0] if row else None
        
    return key, key_id, days

async def get_key_info(key_id: int):
    """Получить информацию о ключе по ID"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      created_at, activated_at, expires_at, is_revoked 
               FROM keys WHERE id = ?""",
            (key_id,)
        )
        return await cursor.fetchone()

async def revoke_key(key_id: int):
    """Аннулировать ключ и деактивировать пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Помечаем ключ как аннулированный
        await db.execute(
            "UPDATE keys SET is_revoked = 1 WHERE id = ?",
            (key_id,)
        )
        # Деактивируем пользователя с этим ключом
        await db.execute(
            "UPDATE users SET is_active = 0 WHERE key_id = ?",
            (key_id,)
        )
        await db.commit()
    
    # Обновляем конфиг Xray
    await restart_xray()

async def get_all_users():
    """Получить всех активных пользователей с непросроченной подпиской"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT user_uuid, path FROM users 
               WHERE is_active = 1 
               AND (expires_at IS NULL OR expires_at > datetime('now'))"""
        )
        return await cursor.fetchall()

async def check_expired_users():
    """Проверить и деактивировать просроченные подписки"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """UPDATE users SET is_active = 0 
               WHERE expires_at IS NOT NULL 
               AND expires_at <= datetime('now') 
               AND is_active = 1"""
        )
        await db.commit()

async def activate_key(key: str, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?", 
            (key,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return None, "❌ Ключ не найден"
        
        key_id, is_used, days, is_revoked = row
        
        if is_revoked:
            return None, "❌ Ключ аннулирован"
        if is_used:
            return None, "❌ Ключ уже использован"
        
        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None
        
        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"
        
        # Вычисляем дату истечения
        now = datetime.now()
        if days is not None:
            expires_at = now + timedelta(days=days)
        else:
            expires_at = None  # Бесконечный
        
        await db.execute(
            """INSERT INTO users (user_id, username, user_uuid, path, key_id, expires_at) 
               VALUES (?, ?, ?, ?, ?, ?)""",
            (user_id, username, user_uuid, user_path, key_id, expires_at)
        )
        await db.execute(
            """UPDATE keys SET is_used = 1, used_by = ?, used_by_username = ?, 
                              activated_at = ?, expires_at = ? 
               WHERE key = ?""",
            (user_id, username, now, expires_at, key)
        )
        await db.commit()
        
        await restart_xray()
        
        return user_path, None

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT path, user_uuid, is_active, expires_at 
               FROM users WHERE user_id = ?""",
            (user_id,)
        )
        return await cursor.fetchone()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        active_users = (await cursor.fetchone())[0]
        
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total_users = (await cursor.fetchone())[0]
        
        cursor = await db.execute(
            "SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0"
        )
        free_keys = (await cursor.fetchone())[0]
        
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        
        return {
            "active_users": active_users,
            "total_users": total_users,
            "free_keys": free_keys,
            "total_keys": total_keys
        }

async def get_keys_list():
    """Получить список ключей с подробной информацией"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT id, key, days, is_used, used_by_username, 
                      expires_at, is_revoked, created_at
               FROM keys ORDER BY id DESC LIMIT 20"""
        )
        return await cursor.fetchall()

# ============== XRAY ==============
async def generate_xray_config():
    # Сначала проверяем просроченные подписки
    await check_expired_users()
    
    users = await get_all_users()
    
    clients = []
    for user_uuid, path in users:
        clients.append({
            "id": user_uuid,
            "level": 0
        })
    
    if not clients:
        clients.append({
            "id": str(uuid.uuid4()),
            "level": 0
        })
    
    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": XRAY_PORT,
            "listen": "127.0.0.1",
            "protocol": "vless",
            "settings": {
                "clients": clients,
                "decryption": "none"
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {"path": "/tunnel"}
            }
        }],
        "outbounds": [{"protocol": "freedom", "tag": "direct"}],
        "dns": {"servers": ["8.8.8.8", "1.1.1.1"]}
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Xray config: {len(clients)} clients")

def start_xray():
    global xray_process
    
    if not XRAY_CONFIG_PATH.exists():
        print("❌ Xray config not found!")
        return False
    
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅ Xray started, PID: {xray_process.pid}")
        return True
    except Exception as e:
        print(f"❌ Failed to start Xray: {e}")
        return False

def stop_xray():
    global xray_process
    if xray_process:
        xray_process.terminate()
        xray_process.wait()
        xray_process = None
        print("🛑 Xray stopped")

async def restart_xray():
    stop_xray()
    await generate_xray_config()
    await asyncio.sleep(1)
    start_xray()
    await asyncio.sleep(2)

def generate_vless_link(user_uuid: str, user_path: str):
    host = BASE_URL.replace("https://", "").replace("http://", "")
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=tls&type=ws&host={host}&path=%2Ftunnel#Nefrit-{user_path}"

def generate_subscription(user_uuid: str, user_path: str):
    link = generate_vless_link(user_uuid, user_path)
    return base64.b64encode(link.encode()).decode()

# ============== WEB СЕРВЕР ==============
async def handle_index(request):
    return web.Response(text="<h1>🟢 Nefrit VPN Active</h1>", content_type="text/html")

async def handle_health(request):
    xray_running = xray_process is not None and xray_process.poll() is None
    return web.json_response({"status": "ok", "xray": xray_running})

async def handle_subscription(request):
    path = request.match_info["path"]
    
    # Проверяем просроченные подписки
    await check_expired_users()
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """SELECT user_uuid, is_active, expires_at 
               FROM users WHERE path = ?""",
            (path,)
        )
        row = await cursor.fetchone()
    
    if not row:
        return web.Response(text="Not found", status=404)
    
    user_uuid, is_active, expires_at = row
    
    if not is_active:
        return web.Response(text="Subscription expired", status=403)
    
    # Проверяем срок действия
    if expires_at:
        exp_date = datetime.fromisoformat(expires_at)
        if exp_date <= datetime.now():
            return web.Response(text="Subscription expired", status=403)
    
    sub = generate_subscription(user_uuid, path)
    return web.Response(
        text=sub,
        content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )

async def handle_tunnel(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    try:
        async with ClientSession() as session:
            async with session.ws_connect(
                f"http://127.0.0.1:{XRAY_PORT}/tunnel",
                timeout=30
            ) as ws_xray:
                
                async def client_to_xray():
                    try:
                        async for msg in ws_client:
                            if msg.type == WSMsgType.BINARY:
                                await ws_xray.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await ws_xray.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except:
                        pass
                
                async def xray_to_client():
                    try:
                        async for msg in ws_xray:
                            if msg.type == WSMsgType.BINARY:
                                await ws_client.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await ws_client.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except:
                        pass
                
                await asyncio.gather(
                    client_to_xray(),
                    xray_to_client(),
                    return_exceptions=True
                )
    except:
        pass
    finally:
        if not ws_client.closed:
            await ws_client.close()
    
    return ws_client

# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()
    waiting_revoke_id = State()

def is_admin(user: types.User) -> bool:
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="keys")],
        [InlineKeyboardButton(text="🚫 Аннулировать ключ", callback_data="revoke")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🔄 Перезапустить Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="back")]
    ])

def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Админ-панель", callback_data="admin")]
    ])

def days_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 дней", callback_data="days_7"),
            InlineKeyboardButton(text="14 дней", callback_data="days_14"),
            InlineKeyboardButton(text="30 дней", callback_data="days_30")
        ],
        [
            InlineKeyboardButton(text="60 дней", callback_data="days_60"),
            InlineKeyboardButton(text="90 дней", callback_data="days_90"),
            InlineKeyboardButton(text="180 дней", callback_data="days_180")
        ],
        [InlineKeyboardButton(text="365 дней", callback_data="days_365")],
        [InlineKeyboardButton(text="♾ Бесконечный ключ", callback_data="days_infinite")],
        [InlineKeyboardButton(text="◀️ Отмена", callback_data="admin")]
    ])

def format_expires(expires_at, is_revoked=False):
    """Форматирование срока действия"""
    if is_revoked:
        return "🚫 Аннулирован"
    if expires_at is None:
        return "♾ Бессрочно"
    
    try:
        exp_date = datetime.fromisoformat(str(expires_at))
        now = datetime.now()
        
        if exp_date <= now:
            return "⏰ Истёк"
        
        delta = exp_date - now
        days = delta.days
        
        if days == 0:
            hours = delta.seconds // 3600
            return f"⏳ {hours} ч."
        elif days == 1:
            return "⏳ 1 день"
        elif days < 7:
            return f"⏳ {days} дн."
        else:
            return f"✅ {days} дн."
    except:
        return "❓"

# ============== ХЕНДЛЕРЫ БОТА ==============

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    await msg.answer(
        f"🌟 <b>Добро пожаловать в Nefrit VPN!</b>\n\n"
        f"Привет, <b>{msg.from_user.first_name}</b>! 👋\n\n"
        f"🚀 Быстрый и надёжный VPN\n"
        f"🔒 Полная безопасность\n"
        f"🌍 Доступ к любым сайтам\n\n"
        f"Выберите действие 👇",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🌟 <b>Nefrit VPN</b> — Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user)),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await cb.message.edit_text(
        "🔑 <b>Введите ваш ключ активации:</b>\n\n"
        "<i>Пример: NEFRIT-A1B2C3D4E5F6G7H8</i>\n\n"
        "Ключ можно получить у администратора.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ]),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(key, msg.from_user.id, msg.from_user.username or msg.from_user.first_name)
    await state.clear()
    
    if error:
        await msg.answer(error, reply_markup=back_kb())
    else:
        info = await get_user_info(msg.from_user.id)
        if info:
            path, user_uuid, is_active, expires_at = info
            link = generate_vless_link(user_uuid, path)
            sub_url = f"{BASE_URL}/sub/{path}"
            
            # Форматируем срок действия
            if expires_at:
                exp_date = datetime.fromisoformat(str(expires_at))
                exp_str = exp_date.strftime("%d.%m.%Y %H:%M")
                exp_info = f"\n⏰ <b>Действует до:</b> {exp_str}"
            else:
                exp_info = "\n♾ <b>Срок действия:</b> Бессрочно"
            
            await msg.answer(
                f"✅ <b>Подписка активирована!</b>{exp_info}\n\n"
                f"📱 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
                f"🔗 <b>Прямой конфиг:</b>\n<code>{link}</code>\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"<b>📲 Как подключиться:</b>\n\n"
                f"<b>Android:</b> V2rayNG\n"
                f"<b>iOS:</b> Streisand / V2Box\n"
                f"<b>Windows:</b> V2rayN\n"
                f"<b>macOS:</b> V2rayU",
                reply_markup=back_kb(),
                parse_mode="HTML"
            )
        else:
            await msg.answer("Ошибка получения данных", reply_markup=back_kb())

@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    # Проверяем просроченные подписки
    await check_expired_users()
    
    info = await get_user_info(cb.from_user.id)
xray_process = None

# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER
            )
        ''')
        await db.commit()

async def create_key():
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO keys (key) VALUES (?)", (key,))
        await db.commit()
    return key

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_uuid, path FROM users WHERE is_active = 1")
        return await cursor.fetchall()

async def activate_key(key: str, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT is_used FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        
        if not row:
            return None, "❌ Ключ не найден"
        if row[0]:
            return None, "❌ Ключ уже использован"
        
        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None
        
        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"
        
        await db.execute(
            "INSERT INTO users (user_id, username, user_uuid, path) VALUES (?, ?, ?, ?)",
            (user_id, username, user_uuid, user_path)
        )
        await db.execute(
            "UPDATE keys SET is_used = 1, used_by = ? WHERE key = ?",
            (user_id, key)
        )
        await db.commit()
        
        # Перезапускаем Xray с новым конфигом
        await restart_xray()
        
        return user_path, None

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active FROM users WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0")
        keys = (await cursor.fetchone())[0]
        return users, keys

async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT key, is_used FROM keys ORDER BY id DESC LIMIT 15"
        )
        return await cursor.fetchall()

# ============== XRAY ==============
async def generate_xray_config():
    """Генерация конфига Xray"""
    users = await get_all_users()
    
    clients = []
    for user_uuid, path in users:
        clients.append({
            "id": user_uuid,
            "level": 0
        })
    
    # Дефолтный клиент если нет пользователей
    if not clients:
        clients.append({
            "id": str(uuid.uuid4()),
            "level": 0
        })
    
    config = {
        "log": {
            "loglevel": "warning"
        },
        "inbounds": [{
            "port": XRAY_PORT,
            "listen": "127.0.0.1",
            "protocol": "vless",
            "settings": {
                "clients": clients,
                "decryption": "none"
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {
                    "path": "/tunnel"
                }
            }
        }],
        "outbounds": [{
            "protocol": "freedom",
            "tag": "direct"
        }],
        "dns": {
            "servers": ["8.8.8.8", "1.1.1.1"]
        }
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"✅ Xray config saved: {len(clients)} clients")

def start_xray():
    """Запуск Xray"""
    global xray_process
    
    if not XRAY_CONFIG_PATH.exists():
        print("❌ Xray config not found!")
        return False
    
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅ Xray started, PID: {xray_process.pid}")
        return True
    except Exception as e:
        print(f"❌ Failed to start Xray: {e}")
        return False

def stop_xray():
    """Остановка Xray"""
    global xray_process
    if xray_process:
        xray_process.terminate()
        xray_process.wait()
        xray_process = None
        print("🛑 Xray stopped")

async def restart_xray():
    """Перезапуск Xray с новым конфигом"""
    stop_xray()
    await generate_xray_config()
    await asyncio.sleep(1)
    start_xray()
    await asyncio.sleep(2)  # Даём время на запуск

def generate_vless_link(user_uuid: str, user_path: str):
    """Генерация VLESS ссылки"""
    host = BASE_URL.replace("https://", "").replace("http://", "")
    # TLS обрабатывает Render, поэтому security=tls
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=tls&type=ws&host={host}&path=%2Ftunnel#Nefrit-{user_path}"

def generate_subscription(user_uuid: str, user_path: str):
    """Подписка в base64"""
    link = generate_vless_link(user_uuid, user_path)
    return base64.b64encode(link.encode()).decode()

# ============== WEB СЕРВЕР ==============
async def handle_index(request):
    return web.Response(
        text="<h1>🟢 Nefrit VPN Active</h1>",
        content_type="text/html"
    )

async def handle_health(request):
    xray_running = xray_process is not None and xray_process.poll() is None
    return web.json_response({
        "status": "ok",
        "xray": xray_running
    })

async def handle_subscription(request):
    path = request.match_info["path"]
    
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active FROM users WHERE path = ?", (path,)
        )
        row = await cursor.fetchone()
    
    if not row or not row[1]:
        return web.Response(text="Not found", status=404)
    
    sub = generate_subscription(row[0], path)
    return web.Response(
        text=sub,
        content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )

async def handle_tunnel(request):
    """WebSocket туннель - проксирование к Xray"""
    
    # Проверяем, что это WebSocket запрос
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)
    
    # Создаём WebSocket соединение с клиентом
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    print(f"🔗 New WS connection from {request.remote}")
    
    try:
        # Подключаемся к Xray
        async with ClientSession() as session:
            async with session.ws_connect(
                f"http://127.0.0.1:{XRAY_PORT}/tunnel",
                timeout=30
            ) as ws_xray:
                
                async def client_to_xray():
                    """Пересылка от клиента к Xray"""
                    try:
                        async for msg in ws_client:
                            if msg.type == WSMsgType.BINARY:
                                await ws_xray.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await ws_xray.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except Exception as e:
                        print(f"⚠️ client_to_xray error: {e}")
                
                async def xray_to_client():
                    """Пересылка от Xray к клиенту"""
                    try:
                        async for msg in ws_xray:
                            if msg.type == WSMsgType.BINARY:
                                await ws_client.send_bytes(msg.data)
                            elif msg.type == WSMsgType.TEXT:
                                await ws_client.send_str(msg.data)
                            elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                                break
                    except Exception as e:
                        print(f"⚠️ xray_to_client error: {e}")
                
                # Запускаем обе задачи параллельно
                await asyncio.gather(
                    client_to_xray(),
                    xray_to_client(),
                    return_exceptions=True
                )
                
    except Exception as e:
        print(f"❌ Tunnel error: {e}")
    finally:
        if not ws_client.closed:
            await ws_client.close()
        print(f"🔌 WS connection closed")
    
    return ws_client

# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class States(StatesGroup):
    waiting_key = State()

def is_admin(user: types.User) -> bool:
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Список ключей", callback_data="keys")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="🔄 Перезапустить Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="back")]
    ])

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    await msg.answer(
        f"🌟 <b>Добро пожаловать в Nefrit VPN!</b>\n\n"
        f"Привет, <b>{msg.from_user.first_name}</b>! 👋\n\n"
        f"🚀 Быстрый и надёжный VPN\n"
        f"🔒 Полная безопасность\n"
        f"🌍 Доступ к любым сайтам\n\n"
        f"Выберите действие 👇",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🌟 <b>Nefrit VPN</b> — Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user)),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await cb.message.edit_text(
        "🔑 <b>Введите ваш ключ активации:</b>\n\n"
        "<i>Пример: NEFRIT-A1B2C3D4E5F6G7H8</i>\n\n"
        "Ключ можно получить у администратора.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ]),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(key, msg.from_user.id, msg.from_user.username or "")
    await state.clear()
    
    if error:
        await msg.answer(error, reply_markup=back_kb())
    else:
        info = await get_user_info(msg.from_user.id)
        if info:
            link = generate_vless_link(info[1], info[0])
            sub_url = f"{BASE_URL}/sub/{info[0]}"
            
            await msg.answer(
                f"✅ <b>Подписка активирована!</b>\n\n"
                f"📱 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
                f"🔗 <b>Прямой конфиг:</b>\n<code>{link}</code>\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"<b>📲 Как подключиться:</b>\n\n"
                f"<b>Android:</b> V2rayNG\n"
                f"<b>iOS:</b> Streisand / V2Box\n"
                f"<b>Windows:</b> V2rayN\n"
                f"<b>macOS:</b> V2rayU\n\n"
                f"1️⃣ Скачайте приложение\n"
                f"2️⃣ Скопируйте ссылку\n"
                f"3️⃣ Импортируйте конфиг\n"
                f"4️⃣ Подключайтесь! 🚀",
                reply_markup=back_kb(),
                parse_mode="HTML"
            )
        else:
            await msg.answer("Ошибка получения данных", reply_markup=back_kb())

@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)
    
    if not info:
        await cb.message.edit_text(
            "❌ <b>У вас нет активной подписки</b>\n\n"
            "Нажмите «🔑 Активировать подписку» для активации.",
            reply_markup=back_kb(),
            parse_mode="HTML"
        )
    else:
        path, user_uuid, is_active = info
        link = generate_vless_link(user_uuid, path)
        sub_url = f"{BASE_URL}/sub/{path}"
        status = "✅ Активна" if is_active else "❌ Неактивна"
        
        await cb.message.edit_text(
            f"📊 <b>Ваша подписка</b>\n\n"
            f"├ <b>Статус:</b> {status}\n"
            f"└ <b>ID:</b> {path}\n\n"
            f"🔗 <b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
            f"📱 <b>Конфиг:</b>\n<code>{link}</code>",
            reply_markup=back_kb(),
            parse_mode="HTML"
        )
    await cb.answer()

@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("⛔ Доступ запрещён", show_alert=True)
        return
    
    users, keys = await get_stats()
    xray_status = "🟢 Работает" if (xray_process and xray_process.poll() is None) else "🔴 Остановлен"
    
    await cb.message.edit_text(
        f"⚙️ <b>Админ-панель</b>\n\n"
        f"👥 Пользователей: {users}\n"
        f"🔑 Свободных ключей: {keys}\n"
        f"📡 Xray: {xray_status}",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "newkey")
async def new_key(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("⛔", show_alert=True)
        return
    
    key = await create_key()
    await cb.message.edit_text(
        f"✅ <b>Ключ создан!</b>\n\n"
        f"🔑 <code>{key}</code>\n\n"
        f"<i>Нажмите на ключ чтобы скопировать</i>",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "keys")
async def list_keys(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("⛔", show_alert=True)
        return
    
    keys = await get_keys_list()
    
    if not keys:
        text = "📋 <b>Ключей пока нет</b>"
    else:
        lines = ["📋 <b>Последние ключи:</b>\n"]
        for key, used in keys:
            status = "✅" if used else "🔓"
            lines.append(f"{status} <code>{key}</code>")
        text = "\n".join(lines)
    
    await cb.message.edit_text(text, reply_markup=admin_kb(), parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data == "stats")
async def stats(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("⛔", show_alert=True)
        return
    
    users, keys = await get_stats()
    
    await cb.message.edit_text(
        f"📈 <b>Статистика</b>\n\n"
        f"👥 Всего пользователей: {users}\n"
        f"🔑 Свободных ключей: {keys}",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "restart_xray")
async def restart_xray_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("⛔", show_alert=True)
        return
    
    await cb.answer("🔄 Перезапуск Xray...")
    await restart_xray()
    
    await cb.message.edit_text(
        "✅ <b>Xray перезапущен!</b>",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )

# ============== ЗАПУСК ==============
async def run_bot():
    """Запуск Telegram бота"""
    print("🤖 Starting Telegram bot...")
    await dp.start_polling(bot)

async def run_web():
    """Запуск веб-сервера"""
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/sub/{path}", handle_subscription)
    app.router.add_get("/tunnel", handle_tunnel)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Web server started on port {PORT}")
    
    # Держим сервер запущенным
    while True:
        await asyncio.sleep(3600)

async def main():
    print("=" * 50)
    print("🚀 NEFRIT VPN SERVER")
    print("=" * 50)
    
    # Инициализация
    await init_db()
    print("✅ Database initialized")
    
    # Генерируем и запускаем Xray
    await generate_xray_config()
    start_xray()
    await asyncio.sleep(3)  # Даём Xray время запуститься
    
    # Проверяем статус Xray
    if xray_process and xray_process.poll() is None:
        print("✅ Xray is running")
    else:
        print("⚠️ Xray may not be running, check logs")
    
    # Запускаем веб-сервер и бота
    try:
        await asyncio.gather(
            run_web(),
            run_bot()
        )
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        stop_xray()

if __name__ == "__main__":
    asyncio.run(main())# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER
            )
        ''')
        await db.commit()

async def create_key():
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO keys (key) VALUES (?)", (key,))
        await db.commit()
    return key

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_uuid, path FROM users WHERE is_active = 1")
        return await cursor.fetchall()

async def activate_key(key: str, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT is_used FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        
        if not row:
            return None, "❌ Ключ не найден"
        if row[0]:
            return None, "❌ Ключ уже использован"
        
        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None  # Уже есть подписка
        
        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"
        
        await db.execute(
            "INSERT INTO users (user_id, username, user_uuid, path) VALUES (?, ?, ?, ?)",
            (user_id, username, user_uuid, user_path)
        )
        await db.execute(
            "UPDATE keys SET is_used = 1, used_by = ? WHERE key = ?",
            (user_id, key)
        )
        await db.commit()
        
        await regenerate_xray_config()
        return user_path, None

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active FROM users WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0")
        keys = (await cursor.fetchone())[0]
        return users, keys

async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT key, is_used FROM keys ORDER BY id DESC LIMIT 15"
        )
        return await cursor.fetchall()

# ============== XRAY — КРИТИЧЕСКИ ВАЖНАЯ ЧАСТЬ ==============
async def regenerate_xray_config():
    """Генерация КОРРЕКТНОГО конфига Xray"""
    users = await get_all_users()
    
    clients = []
    for user_uuid, path in users:
        clients.append({
            "id": user_uuid,
            "level": 0,
            "email": "user@example.com"
        })
    
    if not clients:
        clients.append({
            "id": str(uuid.uuid4()),
            "level": 0,
            "email": "default@example.com"
        })
    
    config = {
        "log": {
            "loglevel": "warning",
            "access": "/tmp/xray_access.log",
            "error": "/tmp/xray_error.log"
        },
        "inbounds": [{
            "port": XRAY_PORT,
            "protocol": "vless",
            "listen": "0.0.0.0",
            "settings": {
                "clients": clients,
                "decryption": "none",
                "fallbacks": [{
                    "path": "/fallback",
                    "dest": 53
                }]
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {
                    "path": "/vpn-ws"
                }
            },
            "sniffing": {
                "enabled": True,
                "destOverride": ["http", "tls"]
            }
        }],
        "outbounds": [
            {
                "protocol": "freedom",
                "tag": "direct",
                "settings": {
                    "domainStrategy": "UseIP"
                }
            },
            {
                "protocol": "blackhole",
                "tag": "blocked"
            }
        ],
        "routing": {
            "domainStrategy": "IPOnDemand",
            "rules": [
                {
                    "type": "field",
                    "outboundTag": "direct",
                    "domain": ["geosite:private"]
                },
                {
                    "type": "field",
                    "outboundTag": "direct",
                    "ip": ["geoip:private"]
                },
                {
                    "type": "field",
                    "outboundTag": "blocked",
                    "domain": ["geosite:category-ads"]
                }
            ]
        },
        "dns": {
            "servers": [
                "8.8.8.8",
                "1.1.1.1",
                "119.29.29.29"
            ]
        }
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

def start_xray():
    """Запуск Xray с правильными параметрами"""
    try:
        if os.path.exists("/tmp/xray.pid"):
            os.remove("/tmp/xray.pid")
            
        process = subprocess.Popen(
            [
                "xray", 
                "-config", str(XRAY_CONFIG_PATH),
                "-configdir", str(DATA_DIR)
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"✅ Xray запущен (PID: {process.pid})")
        return process
    except Exception as e:
        print(f"❌ Ошибка запуска Xray: {e}")
        return None

def generate_vless_link(user_uuid: str):
    """Генерация ПРАВИЛЬНОЙ ссылки VLESS"""
    host = BASE_URL.replace("https://", "").replace("http://", "")
    
    # 🔑 ВАЖНО: security=none, потому что TLS обрабатывает Render!
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=none&type=ws&host={host}&path=%2Fvpn-ws#{user_uuid}"

def generate_subscription(user_uuid: str):
    """Подписка в base64"""
    link = generate_vless_link(user_uuid)
    return base64.b64encode(link.encode()).decode()

# ============== WEB СЕРВЕР ==============
routes = web.RouteTableDef()

@routes.get("/")
async def index(request):
    return web.Response(
        text="🟢 Nefrit VPN is Running<br><a href='/health'>Check Health</a>",
        content_type="text/html"
    )

@routes.get("/health")
async def health(request):
    return web.json_response({
        "status": "ok",
        "xray_config": str(XRAY_CONFIG_PATH.exists())
    })

@routes.get("/sub/{path}")
async def subscription(request):
    path = request.match_info["path"]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active FROM users WHERE path = ?", (path,)
        )
        row = await cursor.fetchone()
    
    if not row or not row[1]:
        return web.Response(text="Not found", status=404)
    
    sub = generate_subscription(row[0])
    return web.Response(
        text=sub,
        content_type="text/plain",
        headers={
            "Profile-Update-Interval": "6",
            "Content-Disposition": f"attachment; filename=nefrit_{path}.txt"
        }
    )

@routes.get("/vpn-ws")
async def vpn_ws(request):
    """Проксирование WebSocket на Xray"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    try:
        async with asyncio.TaskGroup() as tg:
            # Пересылка клиент -> Xray
            async def client_to_xray():
                async for msg in ws:
                    if msg.type == web.WSMsgType.BINARY:
                        writer = request.app["xray_writer"]
                        writer.write(msg.data)
                        await writer.drain()
                    elif msg.type == web.WSMsgType.ERROR:
                        break
            
            # Пересылка Xray -> клиент
            async def xray_to_client():
                while True:
                    data = await request.app["xray_reader"].read(65535)
                    if not data:
                        break
                    await ws.send_bytes(data)
            
            tg.create_task(client_to_xray())
            tg.create_task(xray_to_client())
    except:
        pass
    
    return ws

# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class States(StatesGroup):
    waiting_key = State()

def is_admin(user: types.User) -> bool:
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админ", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Ключи", callback_data="keys")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    await msg.answer(
        f"🌟 <b>Nefrit VPN</b>\n\nПривет, <b>{msg.from_user.first_name}</b>!\n\n"
        "🔒 Надежный VPN с реальной скоростью\n🌍 Доступ к любым сайтам",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🌟 Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await cb.message.edit_text(
        "🔑 <b>Введите ключ:</b>\n\n<i>Пример: NEFRIT-A1B2C3D4...</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="back")]
        ]),
        parse_mode="HTML"
    )

@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(key, msg.from_user.id, msg.from_user.username or "")
    await state.clear()
    
    if error:
        await msg.answer(error, reply_markup=main_kb(is_admin(msg.from_user)))
    else:
        info = await get_user_info(msg.from_user.id)
        link = generate_vless_link(info[1])
        
        await msg.answer(
            f"✅ <b>Подписка активирована!</b>\n\n"
            f"🔗 <b>Ссылка:</b>\n<code>{BASE_URL}/sub/{path}</code>\n\n"
            f"📱 <b>Конфиг:</b>\n<code>{link}</code>\n\n"
            f"<b>Приложения:</b>\n"
            f"• Android: V2rayNG\n"
            f"• iOS: Streisand\n"
            f"• Windows: V2rayN",
            reply_markup=main_kb(is_admin(msg.from_user)),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)
    if not info:
        await cb.message.edit_text("❌ Нет подписки", reply_markup=main_kb(is_admin(cb.from_user)))
    else:
        link = generate_vless_link(info[1])
        await cb.message.edit_text(
            f"📊 <b>Ваша подписка</b>\n\n"
            f"🔗 <code>{BASE_URL}/sub/{info[0]}</code>\n\n"
            f"🛡️ <code>{link}</code>",
            reply_markup=main_kb(is_admin(cb.from_user)),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔ Доступ запрещён", show_alert=True)
    await cb.message.edit_text("⚙️ Админ-панель", reply_markup=admin_kb())

@dp.callback_query(F.data == "newkey")
async def new_key(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔", show_alert=True)
    key = await create_key()
    await cb.message.edit_text(
        f"✅ <b>Ключ создан:</b>\n\n<code>{key}</code>",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )

# ... Остальные хендлеры админки без изменений ...

# ============== ЗАПУСК Xray + WEB + BOT ==============
async def run_bot():
    await dp.start_polling(bot)

async def run_web():
    app = web.Application()
    app.add_routes(routes)
    
    # Запускаем Xray в отдельном процессе и создаем pipe
    xray_process = start_xray()
    if not xray_process:
        print("❌ Не удалось запустить Xray!")
        return
    
    # Создаем pipe для обмена данными с Xray
    import socket
    reader, writer = await asyncio.open_connection("127.0.0.1", XRAY_PORT)
    app["xray_reader"] = reader
    app["xray_writer"] = writer
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Web server на порту {PORT}")

async def main():
    await init_db()
    await regenerate_xray_config()
    
    await asyncio.gather(
        run_web(),
        run_bot()
    )

if __name__ == "__main__":
    asyncio.run(main())# ============== БАЗА ДАННЫХ ==============
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1
            )
        ''')
        await db.execute('''
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY,
                key TEXT UNIQUE,
                is_used BOOLEAN DEFAULT 0,
                used_by INTEGER
            )
        ''')
        await db.commit()

async def create_key():
    key = f"NEFRIT-{secrets.token_hex(8).upper()}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO keys (key) VALUES (?)", (key,))
        await db.commit()
    return key

async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_uuid, path FROM users WHERE is_active = 1")
        return await cursor.fetchall()

async def activate_key(key: str, user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT is_used FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
        
        if not row:
            return None, "❌ Ключ не найден"
        if row[0]:
            return None, "❌ Ключ уже использован"
        
        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None  # Уже есть подписка
        
        user_uuid = str(uuid.uuid4())
        user_path = f"u{user_id}"
        
        await db.execute(
            "INSERT INTO users (user_id, username, user_uuid, path) VALUES (?, ?, ?, ?)",
            (user_id, username, user_uuid, user_path)
        )
        await db.execute(
            "UPDATE keys SET is_used = 1, used_by = ? WHERE key = ?",
            (user_id, key)
        )
        await db.commit()
        
        # Перегенерируем конфиг Xray
        await regenerate_xray_config()
        
        return user_path, None

async def get_user_info(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active FROM users WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone()

async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        users = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0")
        keys = (await cursor.fetchone())[0]
        return users, keys

async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT key, is_used FROM keys ORDER BY id DESC LIMIT 15"
        )
        return await cursor.fetchall()

# ============== XRAY ==============
async def regenerate_xray_config():
    """Генерация конфига Xray со всеми пользователями"""
    users = await get_all_users()
    
    clients = []
    for user_uuid, path in users:
        clients.append({
            "id": user_uuid,
            "flow": ""
        })
    
    # Если нет пользователей, добавляем дефолтного
    if not clients:
        clients.append({"id": str(uuid.uuid4()), "flow": ""})
    
    config = {
        "log": {"loglevel": "warning"},
        "inbounds": [{
            "port": XRAY_PORT,
            "protocol": "vless",
            "settings": {
                "clients": clients,
                "decryption": "none"
            },
            "streamSettings": {
                "network": "ws",
                "wsSettings": {
                    "path": "/vless"
                }
            }
        }],
        "outbounds": [{
            "protocol": "freedom",
            "tag": "direct"
        }]
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)

def start_xray():
    """Запуск Xray процесса"""
    return subprocess.Popen(
        ["xray", "run", "-config", str(XRAY_CONFIG_PATH)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

def generate_vless_link(user_uuid: str, path: str):
    """Генерация VLESS ссылки"""
    host = BASE_URL.replace("https://", "").replace("http://", "")
    return f"vless://{user_uuid}@{host}:443?encryption=none&security=tls&type=ws&path=%2Fvless&host={host}#NefritVPN-{path}"

def generate_subscription(user_uuid: str, path: str):
    """Подписка в base64"""
    link = generate_vless_link(user_uuid, path)
    return base64.b64encode(link.encode()).decode()

# ============== WEB СЕРВЕР ==============
routes = web.RouteTableDef()

@routes.get("/")
async def index(request):
    return web.Response(text="🟢 Nefrit VPN Active", content_type="text/html")

@routes.get("/health")
async def health(request):
    return web.json_response({"status": "ok"})

@routes.get("/sub/{path}")
async def subscription(request):
    path = request.match_info["path"]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active FROM users WHERE path = ?", (path,)
        )
        row = await cursor.fetchone()
    
    if not row or not row[1]:
        return web.Response(text="Not found", status=404)
    
    sub = generate_subscription(row[0], path)
    return web.Response(
        text=sub,
        content_type="text/plain",
        headers={"Profile-Update-Interval": "12"}
    )

# Проксирование WebSocket к Xray
@routes.get("/vless")
async def vless_ws(request):
    import aiohttp
    
    ws_response = web.WebSocketResponse()
    await ws_response.prepare(request)
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(f"http://127.0.0.1:{XRAY_PORT}/vless") as xray_ws:
            async def forward_to_xray():
                async for msg in ws_response:
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        await xray_ws.send_bytes(msg.data)
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        await xray_ws.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        break
            
            async def forward_to_client():
                async for msg in xray_ws:
                    if msg.type == aiohttp.WSMsgType.BINARY:
                        await ws_response.send_bytes(msg.data)
                    elif msg.type == aiohttp.WSMsgType.TEXT:
                        await ws_response.send_str(msg.data)
                    elif msg.type == aiohttp.WSMsgType.CLOSE:
                        break
            
            await asyncio.gather(forward_to_xray(), forward_to_client())
    
    return ws_response

# ============== TELEGRAM БОТ ==============
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

class States(StatesGroup):
    waiting_key = State()

def is_admin(user: types.User) -> bool:
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()

def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="🔑 Активировать подписку", callback_data="activate")],
        [InlineKeyboardButton(text="📊 Моя подписка", callback_data="mysub")],
        [
            InlineKeyboardButton(text="💬 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="📢 Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="⚙️ Админка", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="📋 Ключи", callback_data="keys")],
        [InlineKeyboardButton(text="📈 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back")]
    ])

def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Меню", callback_data="back")]
    ])

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])

@dp.message(CommandStart())
async def cmd_start(msg: types.Message):
    await msg.answer(
        f"🌟 <b>Nefrit VPN</b>\n\nПривет, <b>{msg.from_user.first_name}</b>!\n\n"
        "⚡ Быстрый и надёжный VPN\n🔒 Безопасность\n🌍 Доступ везде",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "back")
@dp.callback_query(F.data == "cancel")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text(
        "🌟 <b>Nefrit VPN</b> — Главное меню",
        reply_markup=main_kb(is_admin(cb.from_user)),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await cb.message.edit_text(
        "🔑 <b>Введите ключ активации:</b>\n\n<i>Например: NEFRIT-A1B2C3D4...</i>",
        reply_markup=cancel_kb(),
        parse_mode="HTML"
    )

@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    path, error = await activate_key(key, msg.from_user.id, msg.from_user.username or "")
    await state.clear()
    
    if error:
        await msg.answer(error, reply_markup=back_kb())
    else:
        info = await get_user_info(msg.from_user.id)
        link = generate_vless_link(info[1], info[0])
        
        await msg.answer(
            f"✅ <b>Подписка активирована!</b>\n\n"
            f"🔗 <b>Ссылка подписки:</b>\n<code>{BASE_URL}/sub/{path}</code>\n\n"
            f"📱 <b>Или прямой конфиг:</b>\n<code>{link}</code>\n\n"
            f"<b>Приложения:</b>\n"
            f"• Android: V2rayNG\n"
            f"• iOS: Streisand\n"
            f"• Windows: V2rayN",
            reply_markup=back_kb(),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)
    if not info:
        await cb.message.edit_text("❌ У вас нет подписки", reply_markup=back_kb())
    else:
        link = generate_vless_link(info[1], info[0])
        await cb.message.edit_text(
            f"📊 <b>Ваша подписка</b>\n\n"
            f"Статус: {'✅ Активна' if info[2] else '❌ Неактивна'}\n\n"
            f"🔗 <code>{BASE_URL}/sub/{info[0]}</code>",
            reply_markup=back_kb(),
            parse_mode="HTML"
        )

@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔ Нет доступа", show_alert=True)
    await cb.message.edit_text("⚙️ <b>Админ-панель</b>", reply_markup=admin_kb(), parse_mode="HTML")

@dp.callback_query(F.data == "newkey")
async def new_key(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔", show_alert=True)
    key = await create_key()
    await cb.message.edit_text(
        f"✅ <b>Новый ключ:</b>\n\n<code>{key}</code>",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "keys")
async def list_keys(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔", show_alert=True)
    keys = await get_keys_list()
    text = "📋 <b>Ключи:</b>\n\n"
    for k, used in keys:
        text += f"{'✅' if used else '🔓'} <code>{k}</code>\n"
    await cb.message.edit_text(text or "Пусто", reply_markup=admin_kb(), parse_mode="HTML")

@dp.callback_query(F.data == "stats")
async def stats(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        return await cb.answer("⛔", show_alert=True)
    users, keys = await get_stats()
    await cb.message.edit_text(
        f"📈 <b>Статистика</b>\n\n👥 Пользователей: {users}\n🔑 Свободных ключей: {keys}",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )

# ============== ЗАПУСК ==============
async def run_bot():
    await dp.start_polling(bot)

async def run_web():
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"🌐 Web server on port {PORT}")

async def main():
    await init_db()
    await regenerate_xray_config()
    
    # Запускаем Xray
    xray_process = start_xray()
    print(f"🚀 Xray started (PID: {xray_process.pid})")
    
    # Запускаем веб-сервер и бота параллельно
    await asyncio.gather(
        run_web(),
        run_bot()
    )

if __name__ == "__main__":
    asyncio.run(main())
