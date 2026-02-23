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
from aiogram.filters import CommandStart, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.types import LabeledPrice, PreCheckoutQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "mellfreezy")
BASE_URL = os.getenv("BASE_URL", "https://nefritvpn.onrender.com")
BOT_USERNAME = os.getenv("BOT_USERNAME", "nefrit_vpn_bot")
SERVER_SECRET = os.getenv("SERVER_SECRET", "default-secret")
PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "vpn.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

SUPPORT_USERNAME = "mellfreezy"
CHANNEL_USERNAME = "nefrit_vpn"

SERVERS = [
    {
        "id": 1,
        "name": "Орегон",
        "url": BASE_URL,
        "emoji": "🇺🇸",
        "location": "США",
        "is_master": True
    },
    {
        "id": 2,
        "name": "Огайо",
        "url": "https://nefritvpn-ohio.onrender.com",
        "emoji": "🇺🇸",
        "location": "США",
        "is_master": False
    },
    {
        "id": 3,
        "name": "Франкфурт",
        "url": "https://nefritvpn-frankfurt.onrender.com",
        "emoji": "🇪🇺",
        "location": "Германия",
        "is_master": False
    },
    {
        "id": 4,
        "name": "Сингапур",
        "url": "https://nefrit-singapure.onrender.com",
        "emoji": "🇸🇬",
        "location": "Сингапур",
        "is_master": False
    }
]

PRICES = {
    "week": {"days": 7, "stars": 5, "name": "1 неделя"},
    "month": {"days": 30, "stars": 10, "name": "1 месяц"},
    "year": {"days": 365, "stars": 100, "name": "1 год"},
    "forever": {"days": None, "stars": 300, "name": "Навсегда"}
}

TRIAL_DAYS = 3
TRIAL_DAYS_REFERRAL = 5
REFERRAL_BONUS_DAYS = 3

xray_process = None
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()


def generate_path():
    """Генерирует случайный path для подписки"""
    return secrets.token_urlsafe(16)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                user_id INTEGER UNIQUE,
                username TEXT,
                user_uuid TEXT UNIQUE,
                path TEXT UNIQUE,
                key_id INTEGER,
                created_at TEXT,
                expires_at TEXT,
                is_active INTEGER DEFAULT 1,
                referred_by INTEGER
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trial_used (
                user_id INTEGER PRIMARY KEY
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE,
                days INTEGER,
                is_used INTEGER DEFAULT 0,
                used_by INTEGER,
                used_by_username TEXT,
                created_at TEXT,
                activated_at TEXT,
                expires_at TEXT,
                is_revoked INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                amount INTEGER,
                plan TEXT,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_id INTEGER UNIQUE,
                created_at TEXT,
                bonus_given INTEGER DEFAULT 0
            )
        """)
        await db.commit()


async def sync_user_to_servers(user_uuid, user_path, action="add"):
    for server in SERVERS:
        if server["is_master"]:
            continue
        try:
            async with ClientSession() as session:
                await session.post(
                    f"{server['url']}/api/{action}_user",
                    json={"uuid": user_uuid, "path": user_path, "secret": SERVER_SECRET},
                    timeout=10
                )
        except:
            pass


def generate_vless_link(user_uuid, server):
    host = server["url"].replace("https://", "").replace("http://", "")
    return (
        f"vless://{user_uuid}@{host}:443"
        f"?encryption=none&security=tls&type=ws"
        f"&host={host}&path=%2Ftunnel"
        f"#{server['emoji']} {server['name']} - {server['location']}"
    )


def generate_subscription(user_uuid):
    configs = [generate_vless_link(user_uuid, s) for s in SERVERS]
    return base64.b64encode("\n".join(configs).encode()).decode()


async def create_key(days=None):
    key = "NEFRIT-" + secrets.token_hex(8).upper()
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO keys (key, days, created_at) VALUES (?, ?, ?)",
            (key, days, now)
        )
        await db.commit()
        cursor = await db.execute("SELECT id FROM keys WHERE key = ?", (key,))
        row = await cursor.fetchone()
    return key, row[0] if row else 0


async def check_trial_used(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM trial_used WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone() is not None


async def mark_trial_used(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)", (user_id,)
        )
        await db.commit()


async def get_user_info(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active, expires_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()


async def create_subscription(user_id, username, days=None):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, expires_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        existing = await cursor.fetchone()
        now = datetime.now()

        if existing:
            old_path, old_uuid, old_expires = existing
            
            is_expired = False
            if old_expires:
                try:
                    is_expired = datetime.fromisoformat(old_expires) <= now
                except:
                    pass

            if not is_expired:
                if days is None:
                    new_expires = None
                elif old_expires is None:
                    new_expires = None
                else:
                    old_exp = datetime.fromisoformat(old_expires)
                    new_expires = (old_exp + timedelta(days=days)).isoformat()
                
                await db.execute(
                    "UPDATE users SET expires_at = ?, is_active = 1 WHERE user_id = ?",
                    (new_expires, user_id)
                )
                await db.commit()
                await sync_user_to_servers(old_uuid, old_path, "add")
                return old_path, old_uuid
            else:
                await db.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                await db.commit()
                await sync_user_to_servers(old_uuid, old_path, "remove")

        user_uuid = str(uuid.uuid4())
        user_path = generate_path()
        expires_at = (now + timedelta(days=days)).isoformat() if days else None
        
        await db.execute(
            "INSERT INTO users (user_id, username, user_uuid, path, created_at, expires_at, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, 1)",
            (user_id, username, user_uuid, user_path, now.isoformat(), expires_at)
        )
        await db.commit()
        await sync_user_to_servers(user_uuid, user_path, "add")
        await restart_xray()
        return user_path, user_uuid


async def activate_key(key, user_id, username):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?",
            (key,)
        )
        row = await cursor.fetchone()
        
        if not row:
            return None, None, "Ключ не найден"
        
        key_id, is_used, days, is_revoked = row
        
        if is_revoked:
            return None, None, "Ключ аннулирован"
        if is_used:
            return None, None, "Ключ уже использован"

        now = datetime.now().isoformat()
        await db.execute(
            "UPDATE keys SET is_used = 1, used_by = ?, used_by_username = ?, activated_at = ? WHERE key = ?",
            (user_id, username, now, key)
        )
        await db.commit()

    path, user_uuid = await create_subscription(user_id, username, days)

    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT expires_at FROM users WHERE user_id = ?", (user_id,)
        )
        user_row = await cursor.fetchone()
        if user_row:
            await db.execute(
                "UPDATE keys SET expires_at = ? WHERE id = ?",
                (user_row[0], key_id)
            )
            await db.execute(
                "UPDATE users SET key_id = ? WHERE user_id = ?",
                (key_id, user_id)
            )
            await db.commit()

    return path, user_uuid, None


async def add_days_to_user(user_id, days):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT expires_at FROM users WHERE user_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        if not row:
            return False
        
        now = datetime.now()
        old_expires = row[0]
        
        if old_expires is None:
            return True
        
        try:
            old_exp = datetime.fromisoformat(old_expires)
            base = old_exp if old_exp > now else now
            new_expires = (base + timedelta(days=days)).isoformat()
        except:
            new_expires = (now + timedelta(days=days)).isoformat()
        
        await db.execute(
            "UPDATE users SET expires_at = ?, is_active = 1 WHERE user_id = ?",
            (new_expires, user_id)
        )
        await db.commit()
        return True


async def save_referral(referrer_id, referred_id):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO referrals (referrer_id, referred_id, created_at) VALUES (?, ?, ?)",
                (referrer_id, referred_id, datetime.now().isoformat())
            )
            await db.commit()
            return True
        except:
            return False


async def give_referral_bonus(referrer_id, referred_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT bonus_given FROM referrals WHERE referrer_id = ? AND referred_id = ?",
            (referrer_id, referred_id)
        )
        row = await cursor.fetchone()
        if row and row[0] == 0:
            await add_days_to_user(referrer_id, REFERRAL_BONUS_DAYS)
            await db.execute(
                "UPDATE referrals SET bonus_given = 1 WHERE referrer_id = ? AND referred_id = ?",
                (referrer_id, referred_id)
            )
            await db.commit()
            return True
        return False


async def get_referral_stats(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
        )
        count = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT referrer_id FROM referrals WHERE referred_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        return count, row[0] if row else None


async def check_user_exists(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM users WHERE user_id = ?", (user_id,)
        )
        return await cursor.fetchone() is not None


async def save_payment(user_id, username, amount, plan):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO payments (user_id, username, amount, plan, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, amount, plan, datetime.now().isoformat())
        )
        await db.commit()


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        active = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0")
        free_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COALESCE(SUM(amount), 0) FROM payments")
        total_stars = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM referrals")
        total_refs = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM payments")
        total_payments = (await cursor.fetchone())[0]
        return active, free_keys, total_keys, total_stars, total_refs, total_payments


async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, expires_at, is_revoked "
            "FROM keys ORDER BY id DESC LIMIT 20"
        )
        return await cursor.fetchall()


async def get_key_info(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, expires_at, is_revoked "
            "FROM keys WHERE id = ?",
            (key_id,)
        )
        return await cursor.fetchone()


async def delete_key(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, path FROM users WHERE key_id = ?", (key_id,)
        )
        user_info = await cursor.fetchone()
        await db.execute("UPDATE keys SET is_revoked = 1 WHERE id = ?", (key_id,))
        if user_info:
            await db.execute("DELETE FROM users WHERE key_id = ?", (key_id,))
            await sync_user_to_servers(user_info[0], user_info[1], "remove")
        await db.commit()
    await restart_xray()


async def cleanup_expired():
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, path FROM users WHERE expires_at IS NOT NULL AND expires_at < ?",
            (now,)
        )
        expired = await cursor.fetchall()
        if expired:
            await db.execute(
                "DELETE FROM users WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,)
            )
            await db.commit()
    for user_uuid, path in expired:
        await sync_user_to_servers(user_uuid, path, "remove")
    return len(expired)


async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, path FROM users WHERE is_active = 1"
        )
        return await cursor.fetchall()


async def generate_xray_config():
    users = await get_all_users()
    clients = [{"id": u[0], "level": 0} for u in users]
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


def start_xray():
    global xray_process
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
    except:
        pass


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


async def handle_index(request):
    return web.Response(text="Nefrit VPN Master Server")


async def handle_health(request):
    xray_ok = xray_process is not None and xray_process.poll() is None
    return web.json_response({"status": "ok", "xray": xray_ok})


async def handle_subscription(request):
    path = request.match_info["path"]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active, expires_at FROM users WHERE path = ?",
            (path,)
        )
        row = await cursor.fetchone()
    
    if not row:
        return web.Response(text="Not found", status=404)
    if not row[1]:
        return web.Response(text="Inactive", status=403)
    if row[2]:
        try:
            if datetime.fromisoformat(row[2]) <= datetime.now():
                return web.Response(text="Expired", status=403)
        except:
            pass
    
    return web.Response(
        text=generate_subscription(row[0]),
        content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )


async def handle_tunnel(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WS only", status=400)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    try:
        async with ClientSession() as session:
            async with session.ws_connect(f"http://127.0.0.1:{XRAY_PORT}/tunnel", timeout=30) as ws_xray:
                async def fwd(src, dst):
                    async for msg in src:
                        if msg.type == WSMsgType.BINARY:
                            await dst.send_bytes(msg.data)
                        elif msg.type == WSMsgType.TEXT:
                            await dst.send_str(msg.data)
                        elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                            break
                await asyncio.gather(fwd(ws_client, ws_xray), fwd(ws_xray, ws_client), return_exceptions=True)
    except:
        pass
    finally:
        await ws_client.close()
    
    return ws_client


def is_admin(user):
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()


def main_kb(admin=False):
    kb = [
        [InlineKeyboardButton(text="Купить подписку", callback_data="buy")],
        [InlineKeyboardButton(text="Активировать ключ", callback_data="activate")],
        [InlineKeyboardButton(text="Моя подписка", callback_data="mysub")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral")],
        [
            InlineKeyboardButton(text="Поддержка", url=f"https://t.me/{SUPPORT_USERNAME}"),
            InlineKeyboardButton(text="Канал", url=f"https://t.me/{CHANNEL_USERNAME}")
        ]
    ]
    if admin:
        kb.append([InlineKeyboardButton(text="Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=kb)


def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Меню", callback_data="back")]
    ])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Админ-панель", callback_data="admin")]
    ])


def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="back")]
    ])


async def safe_edit(msg, text, kb=None):
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except TelegramBadRequest:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


@dp.message(CommandStart())
async def cmd_start(msg: types.Message, command: CommandObject, state: FSMContext):
    await state.clear()
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.first_name
    
    referrer_id = None
    if command.args and command.args.startswith("ref_"):
        try:
            ref = int(command.args[4:])
            if ref != user_id:
                referrer_id = ref
        except:
            pass
    
    user_exists = await check_user_exists(user_id)
    
    if referrer_id and not user_exists:
        await save_referral(referrer_id, user_id)
        await mark_trial_used(user_id)
        path, user_uuid = await create_subscription(user_id, username, TRIAL_DAYS_REFERRAL)
        await give_referral_bonus(referrer_id, user_id)
        await restart_xray()
        
        sub_url = f"{BASE_URL}/sub/{path}"
        link = generate_vless_link(user_uuid, SERVERS[0])
        
        await msg.answer(
            f"<b>Добро пожаловать в Nefrit VPN!</b>\n\n"
            f"Вы пришли по реферальной ссылке!\n"
            f"Вам начислено <b>{TRIAL_DAYS_REFERRAL} дней</b>!\n\n"
            f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
            f"<b>Конфиг:</b>\n<code>{link}</code>\n\n"
            f"<b>Приложения:</b>\nAndroid: V2rayNG\niOS: Streisand\nWindows: V2rayN",
            reply_markup=main_kb(is_admin(msg.from_user)),
            parse_mode="HTML"
        )
        
        try:
            await bot.send_message(
                referrer_id,
                f"🎉 {username} присоединился по вашей ссылке!\n+{REFERRAL_BONUS_DAYS} дней к подписке!"
            )
        except:
            pass
        return
    
    await msg.answer(
        f"<b>Nefrit VPN</b>\n\nДобро пожаловать, {msg.from_user.first_name}!\n\nВыберите действие:",
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(cb.message, "<b>Nefrit VPN</b>\n\nГлавное меню", main_kb(is_admin(cb.from_user)))
    await cb.answer()


@dp.callback_query(F.data == "buy")
async def buy_menu(cb: types.CallbackQuery):
    trial_used = await check_trial_used(cb.from_user.id)
    buttons = []
    if not trial_used:
        buttons.append([InlineKeyboardButton(text=f"Пробный период ({TRIAL_DAYS} дня)", callback_data="trial")])
    buttons += [
        [InlineKeyboardButton(text="1 неделя - 5 ⭐", callback_data="pay_week")],
        [InlineKeyboardButton(text="1 месяц - 10 ⭐", callback_data="pay_month")],
        [InlineKeyboardButton(text="1 год - 100 ⭐", callback_data="pay_year")],
        [InlineKeyboardButton(text="Навсегда - 300 ⭐", callback_data="pay_forever")],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ]
    await safe_edit(cb.message, "<b>Выберите тариф:</b>", InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@dp.callback_query(F.data == "trial")
async def trial_menu(cb: types.CallbackQuery):
    if await check_trial_used(cb.from_user.id):
        await cb.answer("Пробный период уже использован!", show_alert=True)
        return
    await safe_edit(
        cb.message,
        f"Активировать пробный период на <b>{TRIAL_DAYS} дня</b>?",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Активировать", callback_data="trial_confirm")],
            [InlineKeyboardButton(text="Отмена", callback_data="buy")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "trial_confirm")
async def trial_confirm(cb: types.CallbackQuery):
    if await check_trial_used(cb.from_user.id):
        await cb.answer("Пробный период уже использован!", show_alert=True)
        return
    
    await mark_trial_used(cb.from_user.id)
    username = cb.from_user.username or cb.from_user.first_name
    path, user_uuid = await create_subscription(cb.from_user.id, username, TRIAL_DAYS)
    await restart_xray()
    
    sub_url = f"{BASE_URL}/sub/{path}"
    link = generate_vless_link(user_uuid, SERVERS[0])
    exp = (datetime.now() + timedelta(days=TRIAL_DAYS)).strftime("%d.%m.%Y %H:%M")
    
    await safe_edit(
        cb.message,
        f"<b>✅ Пробная подписка активирована!</b>\n\n"
        f"Действует до: {exp}\n\n"
        f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>\n\n"
        f"<b>Приложения:</b>\nAndroid: V2rayNG\niOS: Streisand\nWindows: V2rayN",
        back_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "referral")
async def referral_menu(cb: types.CallbackQuery):
    count, _ = await get_referral_stats(cb.from_user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{cb.from_user.id}"
    
    await safe_edit(
        cb.message,
        f"<b>Реферальная система</b>\n\n"
        f"За каждого друга: <b>+{REFERRAL_BONUS_DAYS} дней</b>\n"
        f"Друг получит: <b>{TRIAL_DAYS_REFERRAL} дней</b>\n\n"
        f"Приглашено: <b>{count}</b>\n\n"
        f"<b>Ваша ссылка:</b>\n<code>{ref_link}</code>",
        back_kb()
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("pay_"))
async def process_payment(cb: types.CallbackQuery):
    plan = cb.data[4:]
    if plan not in PRICES:
        await cb.answer("Ошибка", show_alert=True)
        return
    info = PRICES[plan]
    await bot.send_invoice(
        cb.from_user.id,
        f"Nefrit VPN - {info['name']}",
        f"Подписка: {info['name']}",
        f"vpn_{plan}", "", "XTR",
        [LabeledPrice(label=info["name"], amount=info["stars"])]
    )
    await cb.answer()


@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(msg: types.Message):
    plan = msg.successful_payment.invoice_payload[4:]
    if plan not in PRICES:
        return
    
    info = PRICES[plan]
    username = msg.from_user.username or msg.from_user.first_name
    
    await save_payment(msg.from_user.id, username, info["stars"], plan)
    path, user_uuid = await create_subscription(msg.from_user.id, username, info["days"])
    await restart_xray()
    
    user_info = await get_user_info(msg.from_user.id)
    sub_url = f"{BASE_URL}/sub/{path}"
    link = generate_vless_link(user_uuid, SERVERS[0])
    
    if user_info and user_info[3]:
        exp_str = datetime.fromisoformat(user_info[3]).strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Бессрочно"
    
    await msg.answer(
        f"<b>✅ Оплата принята!</b>\n\n"
        f"Срок: {exp_str}\n\n"
        f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>",
        reply_markup=back_kb(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    await safe_edit(
        cb.message,
        "<b>Введите ключ:</b>\n\nПример: NEFRIT-XXXXXXXX",
        cancel_kb()
    )
    await cb.answer()


@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    await state.clear()
    key = msg.text.strip().upper()
    username = msg.from_user.username or msg.from_user.first_name
    
    path, user_uuid, error = await activate_key(key, msg.from_user.id, username)
    
    if error:
        await msg.answer(f"❌ {error}", reply_markup=back_kb())
        return
    
    user_info = await get_user_info(msg.from_user.id)
    sub_url = f"{BASE_URL}/sub/{path}"
    link = generate_vless_link(user_uuid, SERVERS[0])
    
    if user_info and user_info[3]:
        exp_str = datetime.fromisoformat(user_info[3]).strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Бессрочно"
    
    await msg.answer(
        f"<b>✅ Подписка активирована!</b>\n\n"
        f"Срок: {exp_str}\n\n"
        f"<b>Ссылка:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>",
        reply_markup=back_kb(),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)
    
    if not info:
        await safe_edit(cb.message, "<b>У вас нет подписки</b>\n\nКупите или активируйте ключ.", back_kb())
        await cb.answer()
        return
    
    user_path, user_uuid, is_active, expires_at = info
    
    if expires_at:
        try:
            if datetime.fromisoformat(expires_at) <= datetime.now():
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("DELETE FROM users WHERE user_id = ?", (cb.from_user.id,))
                    await db.commit()
                await sync_user_to_servers(user_uuid, user_path, "remove")
                await safe_edit(cb.message, "<b>Подписка истекла</b>\n\nКупите новую.", back_kb())
                await cb.answer()
                return
        except:
            pass
    
    sub_url = f"{BASE_URL}/sub/{user_path}"
    link = generate_vless_link(user_uuid, SERVERS[0])
    
    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        diff = (exp - datetime.now()).days
        exp_str = f"{exp.strftime('%d.%m.%Y')} ({diff} дн.)"
    else:
        exp_str = "Бессрочно"
    
    await safe_edit(
        cb.message,
        f"<b>Ваша подписка</b>\n\n"
        f"Статус: {'✅ Активна' if is_active else '❌ Неактивна'}\n"
        f"Срок: {exp_str}\n\n"
        f"<b>Ссылка:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>",
        back_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    await state.clear()
    active, free_keys, total_keys, total_stars, total_refs, total_payments = await get_stats()
    xray_ok = xray_process and xray_process.poll() is None
    
    await safe_edit(
        cb.message,
        f"<b>Админ-панель</b>\n\n"
        f"Подписок: {active}\n"
        f"Ключей: {free_keys}/{total_keys}\n"
        f"Звёзд: {total_stars} ⭐\n"
        f"Рефералов: {total_refs}\n"
        f"Оплат: {total_payments}\n"
        f"Xray: {'✅' if xray_ok else '❌'}",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Создать ключ", callback_data="newkey")],
            [InlineKeyboardButton(text="Все ключи", callback_data="keys")],
            [InlineKeyboardButton(text="Статистика", callback_data="stats")],
            [InlineKeyboardButton(text="Перезапустить Xray", callback_data="restart_xray")],
            [InlineKeyboardButton(text="Назад", callback_data="back")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data == "newkey")
async def new_key_menu(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    await state.set_state(States.waiting_days)
    await safe_edit(
        cb.message,
        "<b>Выберите срок:</b>",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="7", callback_data="mkkey_7"),
                InlineKeyboardButton(text="14", callback_data="mkkey_14"),
                InlineKeyboardButton(text="30", callback_data="mkkey_30")
            ],
            [
                InlineKeyboardButton(text="60", callback_data="mkkey_60"),
                InlineKeyboardButton(text="90", callback_data="mkkey_90"),
                InlineKeyboardButton(text="180", callback_data="mkkey_180")
            ],
            [InlineKeyboardButton(text="365", callback_data="mkkey_365")],
            [InlineKeyboardButton(text="Бессрочно", callback_data="mkkey_0")],
            [InlineKeyboardButton(text="Отмена", callback_data="admin")]
        ])
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("mkkey_"))
async def create_key_handler(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    await state.clear()
    val = cb.data[6:]
    days = None if val == "0" else int(val)
    
    key, key_id = await create_key(days)
    days_str = "Бессрочно" if days is None else f"{days} дней"
    
    await safe_edit(
        cb.message,
        f"<b>✅ Ключ создан!</b>\n\n"
        f"ID: #{key_id}\n"
        f"Ключ: <code>{key}</code>\n"
        f"Срок: {days_str}",
        back_admin_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "keys")
async def list_keys(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    keys = await get_keys_list()
    if not keys:
        await safe_edit(cb.message, "<b>Ключей нет</b>", back_admin_kb())
        await cb.answer()
        return
    
    buttons = []
    for row in keys:
        key_id, _, days, is_used, username, _, is_revoked = row
        status = "❌" if is_revoked else ("✅" if is_used else "🔑")
        days_str = "∞" if days is None else f"{days}d"
        user_str = f"@{username}" if username else "-"
        buttons.append([InlineKeyboardButton(
            text=f"{status} #{key_id} {days_str} {user_str}",
            callback_data=f"keyinfo_{key_id}"
        )])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="admin")])
    
    await safe_edit(cb.message, "<b>Ключи:</b>", InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@dp.callback_query(F.data.startswith("keyinfo_"))
async def key_info(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    key_id = int(cb.data[8:])
    info = await get_key_info(key_id)
    
    if not info:
        await cb.answer("Ключ не найден", show_alert=True)
        return
    
    _, key, days, is_used, username, expires_at, is_revoked = info
    status = "Удалён" if is_revoked else ("Использован" if is_used else "Свободен")
    days_str = "Бессрочно" if days is None else f"{days} дней"
    
    if is_revoked:
        exp_str = "—"
    elif not expires_at:
        exp_str = "Бессрочно"
    else:
        try:
            exp = datetime.fromisoformat(expires_at)
            diff = (exp - datetime.now()).days
            exp_str = f"{diff} дн." if diff > 0 else "Истёк"
        except:
            exp_str = "?"
    
    kb = back_admin_kb() if is_revoked else InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"delkey_{key_id}")],
        [InlineKeyboardButton(text="Назад", callback_data="keys")]
    ])
    
    await safe_edit(
        cb.message,
        f"<b>Ключ #{key_id}</b>\n\n"
        f"<code>{key}</code>\n\n"
        f"Статус: {status}\n"
        f"Срок: {days_str}\n"
        f"Юзер: @{username if username else '—'}\n"
        f"Осталось: {exp_str}",
        kb
    )
    await cb.answer()


@dp.callback_query(F.data.startswith("delkey_"))
async def del_key(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    key_id = int(cb.data[7:])
    await delete_key(key_id)
    
    await safe_edit(cb.message, f"<b>✅ Ключ #{key_id} удалён</b>", back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "stats")
async def stats_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    active, free_keys, total_keys, total_stars, total_refs, total_payments = await get_stats()
    
    await safe_edit(
        cb.message,
        f"<b>Статистика</b>\n\n"
        f"Подписок: {active}\n"
        f"Ключей: {free_keys}/{total_keys}\n"
        f"Звёзд: {total_stars} ⭐\n"
        f"Рефералов: {total_refs}\n"
        f"Оплат: {total_payments}",
        back_admin_kb()
    )
    await cb.answer()


@dp.callback_query(F.data == "restart_xray")
async def restart_xray_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    
    await restart_xray()
    await safe_edit(cb.message, "<b>✅ Xray перезапущен</b>", back_admin_kb())
    await cb.answer()


@dp.message(F.text == "/stars")
async def check_stars(msg: types.Message):
    if not is_admin(msg.from_user):
        return
    try:
        result = await bot.get_star_transactions()
        total = sum(t.amount for t in result.transactions) if result.transactions else 0
        await msg.answer(f"Звёзды: {total} ⭐\nДо вывода: {max(0, 1000-total)}")
    except Exception as e:
        await msg.answer(f"Ошибка: {e}")


async def run_bot():
    await dp.start_polling(bot)


async def run_web():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/sub/{path}", handle_subscription)
    app.router.add_get("/tunnel", handle_tunnel)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    
    while True:
        await asyncio.sleep(3600)


async def expiry_checker():
    while True:
        await asyncio.sleep(3600)
        deleted = await cleanup_expired()
        if deleted:
            await restart_xray()


async def main():
    print("NEFRIT VPN MASTER")
    await init_db()
    await generate_xray_config()
    start_xray()
    await asyncio.sleep(2)
    await asyncio.gather(run_web(), run_bot(), expiry_checker())


if __name__ == "__main__":
    asyncio.run(main())
