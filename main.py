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
PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "vpn.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

SUPPORT_USERNAME = "mellfreezy"
CHANNEL_USERNAME = "nefrit_vpn"

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
    waiting_revoke_id = State()


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "CREATE TABLE IF NOT EXISTS users ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER UNIQUE, "
            "username TEXT, "
            "user_uuid TEXT UNIQUE, "
            "path TEXT UNIQUE, "
            "key_id INTEGER, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "expires_at TIMESTAMP, "
            "is_active BOOLEAN DEFAULT 1, "
            "trial_used BOOLEAN DEFAULT 0, "
            "referred_by INTEGER DEFAULT NULL)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS keys ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "key TEXT UNIQUE, "
            "days INTEGER, "
            "is_used BOOLEAN DEFAULT 0, "
            "used_by INTEGER, "
            "used_by_username TEXT, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "activated_at TIMESTAMP, "
            "expires_at TIMESTAMP, "
            "is_revoked BOOLEAN DEFAULT 0)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS payments ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "user_id INTEGER, "
            "username TEXT, "
            "amount INTEGER, "
            "plan TEXT, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS referrals ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "referrer_id INTEGER, "
            "referred_id INTEGER UNIQUE, "
            "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
            "bonus_given BOOLEAN DEFAULT 0)"
        )
        await db.commit()


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
        key_id = row[0] if row else 0
    return key, key_id


async def get_key_info(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, expires_at, is_revoked FROM keys WHERE id = ?",
            (key_id,)
        )
        return await cursor.fetchone()


async def revoke_key(key_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE keys SET is_revoked = 1 WHERE id = ?", (key_id,))
        await db.execute("UPDATE users SET is_active = 0 WHERE key_id = ?", (key_id,))
        await db.commit()
    await restart_xray()


async def check_expired_users():
    now = datetime.now().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET is_active = 0 WHERE expires_at IS NOT NULL AND expires_at < ? AND is_active = 1",
            (now,)
        )
        await db.commit()


async def get_all_users():
    await check_expired_users()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_uuid, path FROM users WHERE is_active = 1")
        return await cursor.fetchall()


async def check_trial_used(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT trial_used FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        if row:
            return row[0] == 1
        return False


async def activate_trial(user_id, username, days):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT path, user_uuid, is_active, expires_at FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        
        now = datetime.now()
        expires_at = (now + timedelta(days=days)).isoformat()
        
        if existing:
            old_path = existing[0]
            old_uuid = existing[1]
            old_active = existing[2]
            old_expires = existing[3]
            
            if old_active and old_expires:
                try:
                    old_exp = datetime.fromisoformat(old_expires)
                    if old_exp > now:
                        expires_at = (old_exp + timedelta(days=days)).isoformat()
                except:
                    pass
            
            await db.execute(
                "UPDATE users SET expires_at = ?, is_active = 1, trial_used = 1 WHERE user_id = ?",
                (expires_at, user_id)
            )
            await db.commit()
            return old_path, old_uuid
        else:
            user_uuid = str(uuid.uuid4())
            user_path = "u" + str(user_id)
            
            await db.execute(
                "INSERT INTO users (user_id, username, user_uuid, path, created_at, expires_at, is_active, trial_used) VALUES (?, ?, ?, ?, ?, ?, 1, 1)",
                (user_id, username, user_uuid, user_path, now.isoformat(), expires_at)
            )
            await db.commit()
            await restart_xray()
            return user_path, user_uuid


async def add_days_to_user(user_id, days):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT expires_at, is_active FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        
        if not row:
            return False
        
        now = datetime.now()
        old_expires = row[0]
        
        if old_expires:
            try:
                old_exp = datetime.fromisoformat(old_expires)
                if old_exp > now:
                    new_expires = (old_exp + timedelta(days=days)).isoformat()
                else:
                    new_expires = (now + timedelta(days=days)).isoformat()
            except:
                new_expires = (now + timedelta(days=days)).isoformat()
        else:
            return True
        
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
            await db.execute(
                "UPDATE users SET referred_by = ? WHERE user_id = ?",
                (referrer_id, referred_id)
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
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?",
            (user_id,)
        )
        count = (await cursor.fetchone())[0]
        
        cursor = await db.execute(
            "SELECT referred_by FROM users WHERE user_id = ?",
            (user_id,)
        )
        row = await cursor.fetchone()
        referred_by = row[0] if row else None
        
        return count, referred_by


async def check_user_exists(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT id FROM users WHERE user_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row is not None


async def create_subscription(user_id, username, days=None):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT path, expires_at, is_active FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()

        now = datetime.now()

        if existing and existing[2]:
            old_expires = existing[1]
            if old_expires and days:
                try:
                    old_exp = datetime.fromisoformat(old_expires)
                    if old_exp > now:
                        new_expires = (old_exp + timedelta(days=days)).isoformat()
                    else:
                        new_expires = (now + timedelta(days=days)).isoformat()
                except:
                    new_expires = (now + timedelta(days=days)).isoformat() if days else None
            elif days:
                new_expires = (now + timedelta(days=days)).isoformat()
            else:
                new_expires = None

            await db.execute(
                "UPDATE users SET expires_at = ?, is_active = 1 WHERE user_id = ?",
                (new_expires, user_id)
            )
            await db.commit()
            return existing[0]
        else:
            user_uuid = str(uuid.uuid4())
            user_path = "u" + str(user_id)

            if days:
                expires_at = (now + timedelta(days=days)).isoformat()
            else:
                expires_at = None

            await db.execute(
                "INSERT OR REPLACE INTO users (user_id, username, user_uuid, path, created_at, expires_at, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)",
                (user_id, username, user_uuid, user_path, now.isoformat(), expires_at)
            )
            await db.commit()
            await restart_xray()
            return user_path


async def activate_key(key, user_id, username):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?",
            (key,)
        )
        row = await cursor.fetchone()

        if not row:
            return None, "Ключ не найден"

        key_id = row[0]
        is_used = row[1]
        days = row[2]
        is_revoked = row[3]

        if is_revoked:
            return None, "Ключ аннулирован"
        if is_used:
            return None, "Ключ уже использован"

        cursor = await db.execute("SELECT path FROM users WHERE user_id = ?", (user_id,))
        existing = await cursor.fetchone()
        if existing:
            return existing[0], None

        user_uuid = str(uuid.uuid4())
        user_path = "u" + str(user_id)

        now = datetime.now()
        if days:
            expires_at = (now + timedelta(days=days)).isoformat()
        else:
            expires_at = None

        await db.execute(
            "INSERT INTO users (user_id, username, user_uuid, path, key_id, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, username, user_uuid, user_path, key_id, now.isoformat(), expires_at)
        )
        await db.execute(
            "UPDATE keys SET is_used = 1, used_by = ?, used_by_username = ?, activated_at = ?, expires_at = ? WHERE key = ?",
            (user_id, username, now.isoformat(), expires_at, key)
        )
        await db.commit()

        await restart_xray()
        return user_path, None


async def get_user_info(user_id):
    await check_expired_users()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active, expires_at FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()


async def get_stats():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
        active = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        total = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0")
        free_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT SUM(amount) FROM payments")
        row = await cursor.fetchone()
        total_stars = row[0] if row[0] else 0
        cursor = await db.execute("SELECT COUNT(*) FROM referrals")
        total_refs = (await cursor.fetchone())[0]
        return active, total, free_keys, total_keys, total_stars, total_refs


async def get_keys_list():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, expires_at, is_revoked FROM keys ORDER BY id DESC LIMIT 20"
        )
        return await cursor.fetchall()


async def save_payment(user_id, username, amount, plan):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO payments (user_id, username, amount, plan, created_at) VALUES (?, ?, ?, ?, ?)",
            (user_id, username, amount, plan, datetime.now().isoformat())
        )
        await db.commit()


async def generate_xray_config():
    users = await get_all_users()

    clients = []
    for user_uuid, path in users:
        clients.append({"id": user_uuid, "level": 0})

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
    if not XRAY_CONFIG_PATH.exists():
        return False
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        return True
    except:
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
    link = "vless://" + user_uuid + "@" + host + ":443"
    link = link + "?encryption=none&security=tls&type=ws"
    link = link + "&host=" + host + "&path=%2Ftunnel"
    link = link + "#Nefrit-" + user_path
    return link


def generate_subscription(user_uuid, user_path):
    link = generate_vless_link(user_uuid, user_path)
    return base64.b64encode(link.encode()).decode()


async def handle_index(request):
    return web.Response(text="Nefrit VPN Active", content_type="text/html")


async def handle_health(request):
    xray_running = xray_process is not None and xray_process.poll() is None
    return web.json_response({"status": "ok", "xray": xray_running})


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

    if not row[1]:
        return web.Response(text="Expired", status=403)

    if row[2]:
        exp = datetime.fromisoformat(row[2])
        if exp <= datetime.now():
            return web.Response(text="Expired", status=403)

    sub = generate_subscription(row[0], path)
    return web.Response(text=sub, content_type="text/plain", headers={"Profile-Update-Interval": "6"})


async def handle_tunnel(request):
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WS only", status=400)

    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)

    try:
        url = "http://127.0.0.1:" + str(XRAY_PORT) + "/tunnel"
        async with ClientSession() as session:
            async with session.ws_connect(url, timeout=30) as ws_xray:

                async def fwd(src, dst):
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

                await asyncio.gather(fwd(ws_client, ws_xray), fwd(ws_xray, ws_client), return_exceptions=True)
    except:
        pass
    finally:
        if not ws_client.closed:
            await ws_client.close()

    return ws_client


def is_admin(user):
    if user.username:
        return user.username.lower() == ADMIN_USERNAME.lower()
    return False


def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="Купить подписку", callback_data="buy")],
        [InlineKeyboardButton(text="Активировать ключ", callback_data="activate")],
        [InlineKeyboardButton(text="Моя подписка", callback_data="mysub")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral")],
        [
            InlineKeyboardButton(text="Поддержка", url="https://t.me/" + SUPPORT_USERNAME),
            InlineKeyboardButton(text="Канал", url="https://t.me/" + CHANNEL_USERNAME)
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def buy_kb(user_id):
    trial_used = await check_trial_used(user_id)
    
    buttons = []
    
    if not trial_used:
        buttons.append([InlineKeyboardButton(text="Пробный период (3 дня)", callback_data="trial")])
    
    buttons.append([InlineKeyboardButton(text="1 неделя - 5 звёзд", callback_data="pay_week")])
    buttons.append([InlineKeyboardButton(text="1 месяц - 10 звёзд", callback_data="pay_month")])
    buttons.append([InlineKeyboardButton(text="1 год - 100 звёзд", callback_data="pay_year")])
    buttons.append([InlineKeyboardButton(text="Навсегда - 300 звёзд", callback_data="pay_forever")])
    buttons.append([InlineKeyboardButton(text="Назад", callback_data="back")])
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def trial_confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Активировать", callback_data="trial_confirm"),
            InlineKeyboardButton(text="Отмена", callback_data="buy")
        ]
    ])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="Все ключи", callback_data="keys")],
        [InlineKeyboardButton(text="Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="Перезапустить Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])


def days_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 дней", callback_data="mkkey_7"),
            InlineKeyboardButton(text="14 дней", callback_data="mkkey_14"),
            InlineKeyboardButton(text="30 дней", callback_data="mkkey_30")
        ],
        [
            InlineKeyboardButton(text="60 дней", callback_data="mkkey_60"),
            InlineKeyboardButton(text="90 дней", callback_data="mkkey_90"),
            InlineKeyboardButton(text="180 дней", callback_data="mkkey_180")
        ],
        [InlineKeyboardButton(text="365 дней", callback_data="mkkey_365")],
        [InlineKeyboardButton(text="Бессрочно", callback_data="mkkey_0")],
        [InlineKeyboardButton(text="Отмена", callback_data="admin")]
    ])


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


def confirm_revoke_kb(key_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да, удалить", callback_data="confirmrev_" + str(key_id)),
            InlineKeyboardButton(text="Нет", callback_data="keys")
        ]
    ])


def format_expiry(expires_at, is_revoked):
    if is_revoked:
        return "Аннулирован"
    if not expires_at:
        return "Бессрочно"
    try:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        if exp <= now:
            return "Истёк"
        diff = (exp - now).days
        if diff == 0:
            hours = (exp - now).seconds // 3600
            return str(hours) + " ч."
        return str(diff) + " дн."
    except:
        return "?"


async def safe_edit(message, text, reply_markup=None):
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode="HTML")
    except TelegramBadRequest:
        await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")


async def safe_send(message, text, reply_markup=None):
    await message.answer(text, reply_markup=reply_markup, parse_mode="HTML")


@dp.message(CommandStart())
async def cmd_start(msg: types.Message, command: CommandObject, state: FSMContext):
    await state.clear()
    
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.first_name
    name = msg.from_user.first_name
    
    referrer_id = None
    if command.args and command.args.startswith("ref_"):
        try:
            referrer_id = int(command.args.replace("ref_", ""))
            if referrer_id == user_id:
                referrer_id = None
        except:
            referrer_id = None
    
    user_exists = await check_user_exists(user_id)
    
    if referrer_id and not user_exists:
        await save_referral(referrer_id, user_id)
        
        trial_days = TRIAL_DAYS_REFERRAL
        path, user_uuid = await activate_trial(user_id, username, trial_days)
        
        await give_referral_bonus(referrer_id, user_id)
        
        await restart_xray()
        
        link = generate_vless_link(user_uuid, path)
        sub_url = BASE_URL + "/sub/" + path
        
        text = "<b>Добро пожаловать в Nefrit VPN!</b>\n\n"
        text = text + "Вы пришли по реферальной ссылке!\n"
        text = text + "Вам начислен пробный период на <b>" + str(trial_days) + " дней</b>!\n\n"
        text = text + "<b>Ссылка подписки:</b>\n"
        text = text + "<code>" + sub_url + "</code>\n\n"
        text = text + "<b>Конфиг:</b>\n"
        text = text + "<code>" + link + "</code>\n\n"
        text = text + "<b>Приложения:</b>\n"
        text = text + "Android: V2rayNG\n"
        text = text + "iOS: Streisand / V2Box\n"
        text = text + "Windows: V2rayN"
        
        await msg.answer(text, reply_markup=main_kb(is_admin(msg.from_user)), parse_mode="HTML")
        
        try:
            bonus_text = "Пользователь " + str(username) + " присоединился по вашей реферальной ссылке!\n"
            bonus_text = bonus_text + "Вам начислено +" + str(REFERRAL_BONUS_DAYS) + " дней к подписке!"
            await bot.send_message(referrer_id, bonus_text)
        except:
            pass
        
        return
    
    text = "<b>Nefrit VPN</b>\n\n"
    text = text + "Добро пожаловать, " + str(name) + "!\n\n"
    text = text + "Быстрый и надёжный VPN сервис.\n\n"
    text = text + "Выберите действие:"
    await msg.answer(text, reply_markup=main_kb(is_admin(msg.from_user)), parse_mode="HTML")


@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    text = "<b>Nefrit VPN</b>\n\nГлавное меню"
    await safe_edit(cb.message, text, main_kb(is_admin(cb.from_user)))
    await cb.answer()


@dp.callback_query(F.data == "buy")
async def buy_menu(cb: types.CallbackQuery):
    text = "<b>Купить подписку</b>\n\n"
    text = text + "Выберите тариф:\n\n"
    text = text + "1 неделя - 5 звёзд\n"
    text = text + "1 месяц - 10 звёзд\n"
    text = text + "1 год - 100 звёзд\n"
    text = text + "Навсегда - 300 звёзд\n\n"
    text = text + "Оплата через Telegram Stars"
    
    kb = await buy_kb(cb.from_user.id)
    await safe_edit(cb.message, text, kb)
    await cb.answer()


@dp.callback_query(F.data == "trial")
async def trial_menu(cb: types.CallbackQuery):
    trial_used = await check_trial_used(cb.from_user.id)
    
    if trial_used:
        await cb.answer("Вы уже использовали пробный период!", show_alert=True)
        return
    
    text = "<b>Пробный период</b>\n\n"
    text = text + "Активировать пробный период на <b>" + str(TRIAL_DAYS) + " дня</b>?\n\n"
    text = text + "Пробный период можно использовать только один раз."
    
    await safe_edit(cb.message, text, trial_confirm_kb())
    await cb.answer()


@dp.callback_query(F.data == "trial_confirm")
async def trial_confirm(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    username = cb.from_user.username or cb.from_user.first_name
    
    trial_used = await check_trial_used(user_id)
    
    if trial_used:
        await cb.answer("Вы уже использовали пробный период!", show_alert=True)
        return
    
    path, user_uuid = await activate_trial(user_id, username, TRIAL_DAYS)
    
    await restart_xray()
    
    link = generate_vless_link(user_uuid, path)
    sub_url = BASE_URL + "/sub/" + path
    
    exp = datetime.now() + timedelta(days=TRIAL_DAYS)
    exp_str = exp.strftime("%d.%m.%Y %H:%M")
    
    text = "<b>Пробная подписка активирована!</b>\n\n"
    text = text + "Действует до: " + exp_str + "\n\n"
    text = text + "<b>Ссылка подписки:</b>\n"
    text = text + "<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Конфиг:</b>\n"
    text = text + "<code>" + link + "</code>\n\n"
    text = text + "<b>Приложения:</b>\n"
    text = text + "Android: V2rayNG\n"
    text = text + "iOS: Streisand / V2Box\n"
    text = text + "Windows: V2rayN"
    
    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data == "referral")
async def referral_menu(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    
    count, referred_by = await get_referral_stats(user_id)
    
    ref_link = "https://t.me/" + BOT_USERNAME + "?start=ref_" + str(user_id)
    
    text = "<b>Реферальная система</b>\n\n"
    text = text + "Приглашайте друзей и получайте бонусы!\n\n"
    text = text + "За каждого приглашённого друга вы получите <b>+" + str(REFERRAL_BONUS_DAYS) + " дня</b> к подписке.\n"
    text = text + "Ваш друг получит <b>" + str(TRIAL_DAYS_REFERRAL) + " дней</b> пробного периода!\n\n"
    text = text + "Приглашено людей: <b>" + str(count) + "</b>\n"
    
    if referred_by:
        text = text + "Вас пригласил: <b>" + str(referred_by) + "</b>\n"
    
    text = text + "\n<b>Ваша реферальная ссылка:</b>\n"
    text = text + "<code>" + ref_link + "</code>"
    
    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("pay_"))
async def process_payment(cb: types.CallbackQuery):
    plan = cb.data.replace("pay_", "")

    if plan not in PRICES:
        await cb.answer("Ошибка", show_alert=True)
        return

    price_info = PRICES[plan]
    stars = price_info["stars"]
    name = price_info["name"]

    await cb.answer()

    await bot.send_invoice(
        chat_id=cb.from_user.id,
        title="Nefrit VPN - " + name,
        description="Подписка на VPN: " + name,
        payload="vpn_" + plan,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=name, amount=stars)]
    )


@dp.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message(F.successful_payment)
async def successful_payment(msg: types.Message):
    payment = msg.successful_payment
    payload = payment.invoice_payload

    plan = payload.replace("vpn_", "")

    if plan not in PRICES:
        await msg.answer("Ошибка обработки платежа")
        return

    price_info = PRICES[plan]
    days = price_info["days"]
    stars = price_info["stars"]

    username = msg.from_user.username or msg.from_user.first_name

    await save_payment(msg.from_user.id, username, stars, plan)

    path = await create_subscription(msg.from_user.id, username, days)

    await restart_xray()

    info = await get_user_info(msg.from_user.id)
    if not info:
        await msg.answer("Ошибка создания подписки")
        return

    user_uuid = info[1]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, path)
    sub_url = BASE_URL + "/sub/" + path

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        exp_str = "Действует до: " + exp.strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Срок: Бессрочно"

    text = "<b>Оплата принята!</b>\n\n"
    text = text + "Спасибо за покупку!\n\n"
    text = text + exp_str + "\n\n"
    text = text + "<b>Ссылка подписки:</b>\n"
    text = text + "<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Конфиг:</b>\n"
    text = text + "<code>" + link + "</code>\n\n"
    text = text + "<b>Приложения:</b>\n"
    text = text + "Android: V2rayNG\n"
    text = text + "iOS: Streisand / V2Box\n"
    text = text + "Windows: V2rayN"

    await msg.answer(text, reply_markup=back_kb(), parse_mode="HTML")


@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    text = "<b>Введите ключ активации:</b>\n\n"
    text = text + "Пример: NEFRIT-A1B2C3D4E5F6G7H8"
    await safe_edit(cb.message, text, cancel_kb())
    await cb.answer()


@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    username = msg.from_user.username or msg.from_user.first_name

    path, error = await activate_key(key, msg.from_user.id, username)
    await state.clear()

    if error:
        await safe_send(msg, "Ошибка: " + error, back_kb())
        return

    info = await get_user_info(msg.from_user.id)
    if not info:
        await safe_send(msg, "Ошибка", back_kb())
        return

    user_uuid = info[1]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, path)
    sub_url = BASE_URL + "/sub/" + path

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        exp_str = "Действует до: " + exp.strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Срок: Бессрочно"

    text = "<b>Подписка активирована!</b>\n\n"
    text = text + exp_str + "\n\n"
    text = text + "<b>Ссылка:</b>\n<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Конфиг:</b>\n<code>" + link + "</code>"

    await safe_send(msg, text, back_kb())


@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)

    if not info:
        text = "<b>У вас нет подписки</b>\n\nКупите или активируйте ключ."
        await safe_edit(cb.message, text, back_kb())
        await cb.answer()
        return

    user_path = info[0]
    user_uuid = info[1]
    is_active = info[2]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, user_path)
    sub_url = BASE_URL + "/sub/" + user_path

    status = "Активна" if is_active else "Неактивна"

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        if exp > now:
            diff = (exp - now).days
            exp_str = exp.strftime("%d.%m.%Y") + " (" + str(diff) + " дн.)"
        else:
            exp_str = "Истёк"
    else:
        exp_str = "Бессрочно"

    text = "<b>Ваша подписка</b>\n\n"
    text = text + "Статус: " + status + "\n"
    text = text + "Срок: " + exp_str + "\n\n"
    text = text + "<b>Ссылка:</b>\n<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Конфиг:</b>\n<code>" + link + "</code>"

    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    await state.clear()
    active, total, free_keys, total_keys, total_stars, total_refs = await get_stats()

    xray_ok = xray_process is not None and xray_process.poll() is None
    xray_status = "Работает" if xray_ok else "Остановлен"

    text = "<b>Админ-панель</b>\n\n"
    text = text + "Пользователей: " + str(active) + " / " + str(total) + "\n"
    text = text + "Ключей: " + str(free_keys) + " / " + str(total_keys) + "\n"
    text = text + "Заработано звёзд: " + str(total_stars) + "\n"
    text = text + "Рефералов: " + str(total_refs) + "\n"
    text = text + "Xray: " + xray_status

    await safe_edit(cb.message, text, admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "newkey")
async def new_key_menu(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    await state.set_state(States.waiting_days)
    text = "<b>Создание ключа</b>\n\n"
    text = text + "Выберите срок действия:"
    await safe_edit(cb.message, text, days_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("mkkey_"))
async def create_key_handler(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    val = cb.data.replace("mkkey_", "")

    if val == "0":
        days = None
        days_str = "Бессрочно"
    else:
        days = int(val)
        days_str = str(days) + " дней"

    await state.clear()
    key, key_id = await create_key(days)

    text = "<b>Ключ создан!</b>\n\n"
    text = text + "ID: #" + str(key_id) + "\n"
    text = text + "Ключ: <code>" + key + "</code>\n"
    text = text + "Срок: " + days_str

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.message(States.waiting_days)
async def process_days_manual(msg: types.Message, state: FSMContext):
    if not is_admin(msg.from_user):
        return

    try:
        days = int(msg.text.strip())
        if days <= 0:
            await safe_send(msg, "Введите положительное число", back_admin_kb())
            return
    except:
        await safe_send(msg, "Введите число", back_admin_kb())
        return

    await state.clear()
    key, key_id = await create_key(days)

    text = "<b>Ключ создан!</b>\n\n"
    text = text + "ID: #" + str(key_id) + "\n"
    text = text + "Ключ: <code>" + key + "</code>\n"
    text = text + "Срок: " + str(days) + " дней"

    await safe_send(msg, text, back_admin_kb())


@dp.callback_query(F.data == "keys")
async def list_keys(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    keys = await get_keys_list()

    if not keys:
        text = "<b>Ключей нет</b>"
        await safe_edit(cb.message, text, back_admin_kb())
        await cb.answer()
        return

    text = "<b>Все ключи:</b>\n\nНажмите для удаления:"

    buttons = []
    for row in keys:
        key_id = row[0]
        days = row[2]
        is_used = row[3]
        username = row[4]
        is_revoked = row[6]

        if is_revoked:
            status = "X"
        elif is_used:
            status = "V"
        else:
            status = "O"

        if days is None:
            days_str = "inf"
        else:
            days_str = str(days) + "d"

        if username:
            user_str = "@" + str(username)
        elif is_used:
            user_str = "?"
        else:
            user_str = "-"

        btn_text = "[" + status + "] #" + str(key_id) + " " + days_str + " " + user_str
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data="keyinfo_" + str(key_id))])

    buttons.append([InlineKeyboardButton(text="Назад", callback_data="admin")])

    await safe_edit(cb.message, text, InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@dp.callback_query(F.data.startswith("keyinfo_"))
async def key_info(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    key_id = int(cb.data.replace("keyinfo_", ""))
    info = await get_key_info(key_id)

    if not info:
        await cb.answer("Ключ не найден", show_alert=True)
        return

    key = info[1]
    days = info[2]
    is_used = info[3]
    username = info[4]
    expires_at = info[5]
    is_revoked = info[6]

    if is_revoked:
        status = "Аннулирован"
    elif is_used:
        status = "Использован"
    else:
        status = "Свободен"

    if days is None:
        days_str = "Бессрочно"
    else:
        days_str = str(days) + " дней"

    if username:
        user_str = "@" + str(username)
    else:
        user_str = "-"

    exp_str = format_expiry(expires_at, is_revoked)

    text = "<b>Ключ #" + str(key_id) + "</b>\n\n"
    text = text + "Ключ: <code>" + str(key) + "</code>\n"
    text = text + "Статус: " + status + "\n"
    text = text + "Срок: " + days_str + "\n"
    text = text + "Пользователь: " + user_str + "\n"
    text = text + "Осталось: " + exp_str + "\n\n"

    if not is_revoked:
        text = text + "Удалить этот ключ?"
        await safe_edit(cb.message, text, confirm_revoke_kb(key_id))
    else:
        await safe_edit(cb.message, text, back_admin_kb())

    await cb.answer()


@dp.callback_query(F.data.startswith("confirmrev_"))
async def confirm_revoke(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    key_id = int(cb.data.replace("confirmrev_", ""))

    await revoke_key(key_id)

    text = "<b>Ключ #" + str(key_id) + " аннулирован!</b>\n\n"
    text = text + "Пользователь потерял доступ."

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "stats")
async def stats_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    active, total, free_keys, total_keys, total_stars, total_refs = await get_stats()

    text = "<b>Статистика</b>\n\n"
    text = text + "<b>Пользователи:</b>\n"
    text = text + "Активных: " + str(active) + "\n"
    text = text + "Всего: " + str(total) + "\n\n"
    text = text + "<b>Ключи:</b>\n"
    text = text + "Свободных: " + str(free_keys) + "\n"
    text = text + "Всего: " + str(total_keys) + "\n\n"
    text = text + "<b>Доход:</b>\n"
    text = text + "Всего звёзд: " + str(total_stars) + "\n\n"
    text = text + "<b>Рефералы:</b>\n"
    text = text + "Всего приглашений: " + str(total_refs)

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "restart_xray")
async def restart_xray_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return

    await cb.answer("Перезапуск...")
    await restart_xray()

    await safe_edit(cb.message, "<b>Xray перезапущен!</b>", back_admin_kb())


async def run_bot():
    print("Bot starting...")
    await dp.start_polling(bot)


async def run_web():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/sub/{path}", handle_subscription)
    app.router.add_get("/tunnel", handle_tunnel)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print("Web on port " + str(PORT))

    while True:
        await asyncio.sleep(3600)


async def expiry_checker():
    while True:
        await asyncio.sleep(3600)
        await check_expired_users()
        await restart_xray()


async def main():
    print("NEFRIT VPN SERVER")

    await init_db()
    print("DB OK")

    await generate_xray_config()
    start_xray()
    await asyncio.sleep(3)

    if xray_process and xray_process.poll() is None:
        print("Xray OK")

    await asyncio.gather(
        run_web(),
        run_bot(),
        expiry_checker()
    )


if __name__ == "__main__":
    asyncio.run(main())
