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
        "name": "Oregon",
        "url": BASE_URL,
        "emoji": "🇺🇸",
        "location": "Oregon, USA",
        "is_master": True
    },
    {
        "id": 2,
        "name": "Ohio",
        "url": "https://nefritvpn-ohio.onrender.com",
        "emoji": "🇺🇸",
        "location": "Ohio, USA",
        "is_master": False
    },
    {
        "id": 3,
        "name": "Frankfurt",
        "url": "https://nefritvpn-frankfurt.onrender.com",
        "emoji": "🇪🇺",
        "location": "Frankfurt, Germany",
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
db_lock = asyncio.Lock()  # Добавлен лок для БД


class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()


def generate_path():
    return secrets.token_urlsafe(12)


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
            "referred_by INTEGER DEFAULT NULL)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS trial_used ("
            "user_id INTEGER PRIMARY KEY)"
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


async def sync_user_to_servers(user_uuid, user_path, action="add"):
    """Синхронизация пользователя на worker-серверы"""
    tasks = []
    for server in SERVERS:
        if server["is_master"]:
            continue
        task = notify_server(server["url"], user_uuid, user_path, action)
        tasks.append(task)
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Failed to sync to {SERVERS[i+1]['name']}: {result}")


async def notify_server(server_url, user_uuid, user_path, action):
    """Уведомление worker-сервера об изменении"""
    async with ClientSession() as session:
        try:
            endpoint = f"{server_url}/api/{action}_user"
            async with session.post(
                endpoint,
                json={"uuid": user_uuid, "path": user_path, "secret": SERVER_SECRET},
                timeout=10
            ) as resp:
                if resp.status != 200:
                    print(f"Server {server_url} returned {resp.status}")
        except asyncio.TimeoutError:
            print(f"Timeout connecting to {server_url}")
        except Exception as e:
            print(f"Error notifying {server_url}: {e}")


def generate_vless_link_multi(user_uuid, server):
    """Генерация VLESS ссылки для конкретного сервера"""
    host = server["url"].replace("https://", "").replace("http://", "")
    return (
        f"vless://{user_uuid}@{host}:443"
        f"?encryption=none&security=tls&type=ws"
        f"&host={host}&path=%2Ftunnel"
        f"#{server['emoji']} {server['name']} - {server['location']}"
    )


def generate_subscription_multi(user_uuid, user_path):
    """Генерация файла подписки с конфигами всех серверов"""
    configs = []
    for server in SERVERS:
        vless = generate_vless_link_multi(user_uuid, server)
        configs.append(vless)
    all_configs = "\n".join(configs)
    return base64.b64encode(all_configs.encode()).decode()


async def create_key(days=None):
    """Создание нового ключа активации"""
    key = "NEFRIT-" + secrets.token_hex(8).upper()
    now = datetime.now().isoformat()
    
    async with db_lock:
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


async def check_trial_used(user_id):
    """Проверка использования пробного периода"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM trial_used WHERE user_id = ?", (user_id,)
        )
        return (await cursor.fetchone()) is not None


async def activate_trial(user_id, username, days):
    """Активация пробного периода"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO trial_used (user_id) VALUES (?)", (user_id,)
            )
            await db.commit()
    return await create_subscription(user_id, username, days)


async def add_days_to_user(user_id, days):
    """Добавление дней к подписке пользователя"""
    async with db_lock:
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
    """Сохранение реферальной связи"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                await db.execute(
                    "INSERT INTO referrals (referrer_id, referred_id, created_at) "
                    "VALUES (?, ?, ?)",
                    (referrer_id, referred_id, datetime.now().isoformat())
                )
                await db.commit()
                return True
            except:
                return False


async def give_referral_bonus(referrer_id, referred_id):
    """Начисление реферального бонуса"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT bonus_given FROM referrals "
                "WHERE referrer_id = ? AND referred_id = ?",
                (referrer_id, referred_id)
            )
            row = await cursor.fetchone()
            if row and row[0] == 0:
                await add_days_to_user(referrer_id, REFERRAL_BONUS_DAYS)
                await db.execute(
                    "UPDATE referrals SET bonus_given = 1 "
                    "WHERE referrer_id = ? AND referred_id = ?",
                    (referrer_id, referred_id)
                )
                await db.commit()
                return True
        return False


async def get_referral_stats(user_id):
    """Получение реферальной статистики"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
        )
        count = (await cursor.fetchone())[0]
        cursor = await db.execute(
            "SELECT referrer_id FROM referrals WHERE referred_id = ?", (user_id,)
        )
        row = await cursor.fetchone()
        referred_by = row[0] if row else None
        return count, referred_by


async def check_user_exists(user_id):
    """Проверка существования пользователя"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id FROM users WHERE user_id = ?", (user_id,)
        )
        return (await cursor.fetchone()) is not None


async def create_subscription(user_id, username, days=None):
    """Создание или обновление подписки"""
    async with db_lock:
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
                    if old_expires is None:
                        new_expires = None
                    elif days is None:
                        new_expires = None
                    else:
                        old_exp = datetime.fromisoformat(old_expires)
                        new_expires = (old_exp + timedelta(days=days)).isoformat()
                    
                    await db.execute(
                        "UPDATE users SET expires_at = ?, is_active = 1 "
                        "WHERE user_id = ?",
                        (new_expires, user_id)
                    )
                    await db.commit()
                    await sync_user_to_servers(old_uuid, old_path, "add")
                    return old_path, old_uuid
                else:
                    await db.execute(
                        "DELETE FROM users WHERE user_id = ?", (user_id,)
                    )
                    await db.commit()
                    await sync_user_to_servers(old_uuid, old_path, "remove")

            user_uuid = str(uuid.uuid4())
            user_path = generate_path()
            expires_at = (now + timedelta(days=days)).isoformat() if days else None
            
            await db.execute(
                "INSERT INTO users (user_id, username, user_uuid, path, "
                "created_at, expires_at, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, 1)",
                (user_id, username, user_uuid, user_path,
                 now.isoformat(), expires_at)
            )
            await db.commit()
    
    await sync_user_to_servers(user_uuid, user_path, "add")
    await restart_xray()
    return user_path, user_uuid


async def activate_key(key, user_id, username):
    """Активация ключа"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT id, is_used, days, is_revoked FROM keys WHERE key = ?",
                (key,)
            )
            row = await cursor.fetchone()
            if not row:
                return None, "Ключ не найден"
            
            key_id, is_used, days, is_revoked = row
            
            if is_revoked:
                return None, "Ключ аннулирован"
            if is_used:
                return None, "Ключ уже использован"

            now = datetime.now()
            await db.execute(
                "UPDATE keys SET is_used = 1, used_by = ?, "
                "used_by_username = ?, activated_at = ? WHERE key = ?",
                (user_id, username, now.isoformat(), key)
            )
            await db.commit()

    path, user_uuid = await create_subscription(user_id, username, days)

    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT expires_at FROM users WHERE user_id = ?", (user_id,)
            )
            user_row = await cursor.fetchone()
            actual_expires = user_row[0] if user_row else None
            
            await db.execute(
                "UPDATE keys SET expires_at = ? WHERE id = ?",
                (actual_expires, key_id)
            )
            await db.execute(
                "UPDATE users SET key_id = ? WHERE user_id = ?",
                (key_id, user_id)
            )
            await db.commit()

    return path, None


async def cleanup_expired():
    """Очистка истекших подписок"""
    now = datetime.now().isoformat()
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT user_uuid, path FROM users "
                "WHERE expires_at IS NOT NULL AND expires_at < ?",
                (now,)
            )
            expired = await cursor.fetchall()
            
            if expired:
                await db.execute(
                    "DELETE FROM users "
                    "WHERE expires_at IS NOT NULL AND expires_at < ?",
                    (now,)
                )
                await db.commit()
    
    for user_uuid, path in expired:
        await sync_user_to_servers(user_uuid, path, "remove")
    
    return len(expired)


async def get_user_info(user_id):
    """Получение информации о пользователе"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT path, user_uuid, is_active, expires_at "
            "FROM users WHERE user_id = ?",
            (user_id,)
        )
        return await cursor.fetchone()


async def get_all_users():
    """Получение всех активных пользователей"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, path FROM users WHERE is_active = 1"
        )
        return await cursor.fetchall()


async def get_stats():
    """Получение статистики"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        active = (await cursor.fetchone())[0]
        
        cursor = await db.execute(
            "SELECT COUNT(*) FROM keys WHERE is_used = 0 AND is_revoked = 0"
        )
        free_keys = (await cursor.fetchone())[0]
        
        cursor = await db.execute("SELECT COUNT(*) FROM keys")
        total_keys = (await cursor.fetchone())[0]
        
        cursor = await db.execute("SELECT SUM(amount) FROM payments")
        row = await cursor.fetchone()
        total_stars = row[0] if row[0] else 0
        
        cursor = await db.execute("SELECT COUNT(*) FROM referrals")
        total_refs = (await cursor.fetchone())[0]
        
        cursor = await db.execute("SELECT COUNT(*) FROM payments")
        total_payments = (await cursor.fetchone())[0]
        
        return active, free_keys, total_keys, total_stars, total_refs, total_payments


async def get_keys_list():
    """Получение списка ключей"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, "
            "expires_at, is_revoked "
            "FROM keys ORDER BY id DESC LIMIT 20"
        )
        return await cursor.fetchall()


async def get_key_info(key_id):
    """Получение информации о ключе"""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT id, key, days, is_used, used_by_username, "
            "expires_at, is_revoked "
            "FROM keys WHERE id = ?",
            (key_id,)
        )
        return await cursor.fetchone()


async def delete_key(key_id):
    """Удаление ключа и связанной подписки"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT user_uuid, path FROM users WHERE key_id = ?", (key_id,)
            )
            user_info = await cursor.fetchone()
            
            await db.execute(
                "UPDATE keys SET is_revoked = 1 WHERE id = ?", (key_id,)
            )
            
            if user_info:
                await db.execute(
                    "DELETE FROM users WHERE key_id = ?", (key_id,)
                )
            
            await db.commit()
    
    if user_info:
        await sync_user_to_servers(user_info[0], user_info[1], "remove")
    
    await restart_xray()


async def save_payment(user_id, username, amount, plan):
    """Сохранение информации о платеже"""
    async with db_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO payments (user_id, username, amount, plan, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (user_id, username, amount, plan, datetime.now().isoformat())
            )
            await db.commit()


async def generate_xray_config():
    """Генерация конфигурации Xray"""
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


def start_xray():
    """Запуск Xray"""
    global xray_process
    if not XRAY_CONFIG_PATH.exists():
        return False
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        return True
    except Exception as e:
        print(f"Failed to start Xray: {e}")
        return False


def stop_xray():
    """Остановка Xray"""
    global xray_process
    if xray_process:
        try:
            xray_process.terminate()
            xray_process.wait(timeout=5)
        except:
            xray_process.kill()
        finally:
            xray_process = None


async def restart_xray():
    """Перезапуск Xray"""
    stop_xray()
    await generate_xray_config()
    await asyncio.sleep(1)
    start_xray()
    await asyncio.sleep(2)


# ============= WEB HANDLERS =============

async def handle_index(request):
    return web.Response(text="Nefrit VPN Master Server", content_type="text/html")


async def handle_health(request):
    xray_running = xray_process is not None and xray_process.poll() is None
    return web.json_response({"status": "ok", "xray": xray_running})


async def handle_subscription(request):
    path = request.match_info["path"]
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT user_uuid, is_active, expires_at "
            "FROM users WHERE path = ?",
            (path,)
        )
        row = await cursor.fetchone()
    
    if not row:
        return web.Response(text="Not found", status=404)
    
    if not row[1]:
        return web.Response(text="Expired", status=403)
    
    if row[2]:
        try:
            exp = datetime.fromisoformat(row[2])
            if exp <= datetime.now():
                return web.Response(text="Expired", status=403)
        except:
            pass
    
    sub = generate_subscription_multi(row[0], path)
    return web.Response(
        text=sub, content_type="text/plain",
        headers={"Profile-Update-Interval": "6"}
    )


async def handle_tunnel(request):
    """WebSocket туннель к Xray"""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WS only", status=400)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    ws_xray = None
    session = None
    
    try:
        url = f"http://127.0.0.1:{XRAY_PORT}/tunnel"
        session = ClientSession()
        ws_xray = await session.ws_connect(url, timeout=30)
        
        async def forward(src, dst, name):
            try:
                async for msg in src:
                    if msg.type == WSMsgType.BINARY:
                        await dst.send_bytes(msg.data)
                    elif msg.type == WSMsgType.TEXT:
                        await dst.send_str(msg.data)
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                        break
            except Exception as e:
                print(f"Error in {name}: {e}")
        
        await asyncio.gather(
            forward(ws_client, ws_xray, "client->xray"),
            forward(ws_xray, ws_client, "xray->client"),
            return_exceptions=True
        )
    
    except Exception as e:
        print(f"Tunnel error: {e}")
    
    finally:
        if ws_xray and not ws_xray.closed:
            await ws_xray.close()
        if session:
            await session.close()
        if not ws_client.closed:
            await ws_client.close()
    
    return ws_client


# ============= BOT HANDLERS =============

def is_admin(user):
    return user.username and user.username.lower() == ADMIN_USERNAME.lower()


def main_kb(admin=False):
    buttons = [
        [InlineKeyboardButton(text="Купить подписку", callback_data="buy")],
        [InlineKeyboardButton(text="Активировать ключ", callback_data="activate")],
        [InlineKeyboardButton(text="Моя подписка", callback_data="mysub")],
        [InlineKeyboardButton(text="Реферальная система", callback_data="referral")],
        [
            InlineKeyboardButton(
                text="Поддержка",
                url=f"https://t.me/{SUPPORT_USERNAME}"
            ),
            InlineKeyboardButton(
                text="Канал",
                url=f"https://t.me/{CHANNEL_USERNAME}"
            )
        ]
    ]
    if admin:
        buttons.append([
            InlineKeyboardButton(text="Админ-панель", callback_data="admin")
        ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def buy_kb(user_id):
    trial_used = await check_trial_used(user_id)
    buttons = []
    if not trial_used:
        buttons.append([
            InlineKeyboardButton(
                text=f"Пробный период ({TRIAL_DAYS} дня)", callback_data="trial"
            )
        ])
    buttons.extend([
        [InlineKeyboardButton(
            text="1 неделя - 5 звёзд", callback_data="pay_week"
        )],
        [InlineKeyboardButton(
            text="1 месяц - 10 звёзд", callback_data="pay_month"
        )],
        [InlineKeyboardButton(
            text="1 год - 100 звёзд", callback_data="pay_year"
        )],
        [InlineKeyboardButton(
            text="Навсегда - 300 звёзд", callback_data="pay_forever"
        )],
        [InlineKeyboardButton(text="Назад", callback_data="back")]
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def trial_confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Активировать", callback_data="trial_confirm"
        ),
        InlineKeyboardButton(text="Отмена", callback_data="buy")
    ]])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Создать ключ", callback_data="newkey")],
        [InlineKeyboardButton(text="Все ключи", callback_data="keys")],
        [InlineKeyboardButton(text="Статистика", callback_data="stats")],
        [InlineKeyboardButton(
            text="Перезапустить Xray", callback_data="restart_xray"
        )],
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
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Меню", callback_data="back")
    ]])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Админ-панель", callback_data="admin")
    ]])


def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Отмена", callback_data="back")
    ]])


def confirm_delete_kb(key_id):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="Да, удалить",
            callback_data=f"confirmdel_{key_id}"
        ),
        InlineKeyboardButton(text="Нет", callback_data="keys")
    ]])


def format_expiry(expires_at, is_revoked):
    if is_revoked:
        return "Удалён"
    if not expires_at:
        return "Бессрочно"
    try:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        if exp <= now:
            return "Истёк"
        diff = (exp - now).days
        if diff > 0:
            return f"{diff} дн."
        return f"{(exp - now).seconds // 3600} ч."
    except:
        return "?"


async def safe_edit(message, text, reply_markup=None):
    try:
        await message.edit_text(
            text, reply_markup=reply_markup, parse_mode="HTML"
        )
    except TelegramBadRequest:
        await message.answer(
            text, reply_markup=reply_markup, parse_mode="HTML"
        )


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

        link = generate_vless_link_multi(user_uuid, SERVERS[0])
        sub_url = f"{BASE_URL}/sub/{path}"

        text = (
            "<b>Добро пожаловать в Nefrit VPN!</b>\n\n"
            "Вы пришли по реферальной ссылке!\n"
            f"Вам начислен пробный период на <b>{trial_days} дней</b>!\n\n"
            f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
            f"<b>Конфиг:</b>\n<code>{link}</code>\n\n"
            "<b>Приложения:</b>\n"
            "Android: V2rayNG\niOS: Streisand / V2Box\nWindows: V2rayN"
        )
        await msg.answer(
            text,
            reply_markup=main_kb(is_admin(msg.from_user)),
            parse_mode="HTML"
        )

        try:
            bonus_text = (
                f"Пользователь {username} "
                "присоединился по вашей реферальной ссылке!\n"
                f"Вам начислено +{REFERRAL_BONUS_DAYS} дней к подписке!"
            )
            await bot.send_message(referrer_id, bonus_text)
        except:
            pass
        return

    text = (
        f"<b>Nefrit VPN</b>\n\nДобро пожаловать, {name}!\n\n"
        "Быстрый и надёжный VPN сервис.\n\nВыберите действие:"
    )
    await msg.answer(
        text,
        reply_markup=main_kb(is_admin(msg.from_user)),
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(
        cb.message,
        "<b>Nefrit VPN</b>\n\nГлавное меню",
        main_kb(is_admin(cb.from_user))
    )
    await cb.answer()


@dp.callback_query(F.data == "buy")
async def buy_menu(cb: types.CallbackQuery):
    text = (
        "<b>Купить подписку</b>\n\nВыберите тариф:\n\n"
        "1 неделя - 5 звёзд\n1 месяц - 10 звёзд\n"
        "1 год - 100 звёзд\nНавсегда - 300 звёзд\n\n"
        "Оплата через Telegram Stars"
    )
    kb = await buy_kb(cb.from_user.id)
    await safe_edit(cb.message, text, kb)
    await cb.answer()


@dp.callback_query(F.data == "trial")
async def trial_menu(cb: types.CallbackQuery):
    trial_used = await check_trial_used(cb.from_user.id)
    if trial_used:
        await cb.answer(
            "Вы уже использовали пробный период!", show_alert=True
        )
        return
    text = (
        "<b>Пробный период</b>\n\n"
        f"Активировать пробный период на <b>{TRIAL_DAYS} дня</b>?\n\n"
        "Пробный период можно использовать только один раз."
    )
    await safe_edit(cb.message, text, trial_confirm_kb())
    await cb.answer()


@dp.callback_query(F.data == "trial_confirm")
async def trial_confirm(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    username = cb.from_user.username or cb.from_user.first_name
    trial_used = await check_trial_used(user_id)
    if trial_used:
        await cb.answer(
            "Вы уже использовали пробный период!", show_alert=True
        )
        return
    path, user_uuid = await activate_trial(user_id, username, TRIAL_DAYS)
    await restart_xray()

    link = generate_vless_link_multi(user_uuid, SERVERS[0])
    sub_url = f"{BASE_URL}/sub/{path}"
    exp = datetime.now() + timedelta(days=TRIAL_DAYS)
    exp_str = exp.strftime("%d.%m.%Y %H:%M")

    text = (
        "<b>Пробная подписка активирована!</b>\n\n"
        f"Действует до: {exp_str}\n\n"
        f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>\n\n"
        "<b>Приложения:</b>\n"
        "Android: V2rayNG\niOS: Streisand / V2Box\nWindows: V2rayN"
    )
    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data == "referral")
async def referral_menu(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    count, referred_by = await get_referral_stats(user_id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"

    text = (
        "<b>Реферальная система</b>\n\n"
        "Приглашайте друзей и получайте бонусы!\n\n"
        f"За каждого приглашённого друга вы получите <b>+{REFERRAL_BONUS_DAYS} дня</b> к подписке.\n"
        f"Ваш друг получит <b>{TRIAL_DAYS_REFERRAL} дней</b> пробного периода!\n\n"
        f"Приглашено людей: <b>{count}</b>\n"
    )
    if referred_by:
        text += f"Вас пригласил: <b>{referred_by}</b>\n"
    text += f"\n<b>Ваша реферальная ссылка:</b>\n<code>{ref_link}</code>"
    
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
        cb.from_user.id,
        f"Nefrit VPN - {name}",
        f"Подписка на VPN: {name}",
        f"vpn_{plan}", "", "XTR",
        [LabeledPrice(label=name, amount=stars)]
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
    path, user_uuid = await create_subscription(
        msg.from_user.id, username, days
    )
    await restart_xray()

    info = await get_user_info(msg.from_user.id)
    if not info:
        await msg.answer("Ошибка создания подписки")
        return
    expires_at = info[3]
    link = generate_vless_link_multi(user_uuid, SERVERS[0])
    sub_url = f"{BASE_URL}/sub/{path}"
    if expires_at:
        exp_str = (
            "Действует до: " +
            datetime.fromisoformat(expires_at).strftime("%d.%m.%Y %H:%M")
        )
    else:
        exp_str = "Срок: Бессрочно"
    text = (
        f"<b>Оплата принята!</b>\n\nСпасибо за покупку!\n\n{exp_str}\n\n"
        f"<b>Ссылка подписки:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>\n\n"
        "<b>Приложения:</b>\n"
        "Android: V2rayNG\niOS: Streisand / V2Box\nWindows: V2rayN"
    )
    await msg.answer(text, reply_markup=back_kb(), parse_mode="HTML")


@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    text = (
        "<b>Введите ключ активации:</b>\n\n"
        "Пример: NEFRIT-A1B2C3D4E5F6G7H8"
    )
    await safe_edit(cb.message, text, cancel_kb())
    await cb.answer()


@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    username = msg.from_user.username or msg.from_user.first_name
    path, error = await activate_key(key, msg.from_user.id, username)
    await state.clear()
    if error:
        await safe_send(msg, f"Ошибка: {error}", back_kb())
        return
    info = await get_user_info(msg.from_user.id)
    if not info:
        await safe_send(msg, "Ошибка", back_kb())
        return
    user_uuid = info[1]
    expires_at = info[3]
    link = generate_vless_link_multi(user_uuid, SERVERS[0])
    sub_url = f"{BASE_URL}/sub/{path}"
    if expires_at:
        exp_str = (
            "Действует до: " +
            datetime.fromisoformat(expires_at).strftime("%d.%m.%Y %H:%M")
        )
    else:
        exp_str = "Срок: Бессрочно"
    text = (
        f"<b>Подписка активирована!</b>\n\n{exp_str}\n\n"
        f"<b>Ссылка:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>"
    )
    await safe_send(msg, text, back_kb())


@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)
    if not info:
        text = (
            "<b>У вас нет подписки</b>\n\n"
            "Купите или активируйте ключ."
        )
        await safe_edit(cb.message, text, back_kb())
        await cb.answer()
        return

    user_path, user_uuid, is_active, expires_at = info

    is_expired = False
    if expires_at:
        try:
            exp = datetime.fromisoformat(expires_at)
            is_expired = exp <= datetime.now()
        except:
            pass

    if is_expired:
        async with db_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "DELETE FROM users WHERE user_id = ?", (cb.from_user.id,)
                )
                await db.commit()
        await sync_user_to_servers(user_uuid, user_path, "remove")
        text = (
            "<b>Ваша подписка истекла</b>\n\n"
            "Купите новую подписку или активируйте ключ."
        )
        await safe_edit(cb.message, text, back_kb())
        await cb.answer()
        return

    link = generate_vless_link_multi(user_uuid, SERVERS[0])
    sub_url = f"{BASE_URL}/sub/{user_path}"
    status = "Активна" if is_active else "Неактивна"
    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        exp_str = (
            exp.strftime("%d.%m.%Y") +
            f" ({(exp - now).days} дн.)"
        )
    else:
        exp_str = "Бессрочно"
    text = (
        "<b>Ваша подписка</b>\n\n"
        f"Статус: {status}\nСрок: {exp_str}\n\n"
        f"<b>Ссылка:</b>\n<code>{sub_url}</code>\n\n"
        f"<b>Конфиг:</b>\n<code>{link}</code>"
    )
    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.clear()
    active, free_keys, total_keys, total_stars, total_refs, total_payments = (
        await get_stats()
    )
    xray_ok = xray_process is not None and xray_process.poll() is None
    xray_status = "Работает" if xray_ok else "Остановлен"
    text = (
        "<b>Админ-панель</b>\n\n"
        f"Активных подписок: {active}\n"
        f"Ключей свободно: {free_keys} / {total_keys}\n"
        f"Заработано звёзд: {total_stars}\n"
        f"Рефералов: {total_refs}\n"
        f"Оплат всего: {total_payments}\n"
        f"Xray: {xray_status}"
    )
    await safe_edit(cb.message, text, admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "newkey")
async def new_key_menu(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await state.set_state(States.waiting_days)
    text = "<b>Создание ключа</b>\n\nВыберите срок действия:"
    await safe_edit(cb.message, text, days_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("mkkey_"))
async def create_key_handler(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    val = cb.data.replace("mkkey_", "")
    days = None if val == "0" else int(val)
    days_str = "Бессрочно" if days is None else f"{days} дней"
    await state.clear()
    key, key_id = await create_key(days)
    text = (
        "<b>Ключ создан!</b>\n\n"
        f"ID: #{key_id}\n"
        f"Ключ: <code>{key}</code>\n"
        f"Срок: {days_str}"
    )
    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.message(States.waiting_days)
async def process_days_manual(msg: types.Message, state: FSMContext):
    if not is_admin(msg.from_user):
        return
    try:
        days = int(msg.text.strip())
        if days <= 0:
            await safe_send(
                msg, "Введите положительное число", back_admin_kb()
            )
            return
    except:
        await safe_send(msg, "Введите число", back_admin_kb())
        return
    await state.clear()
    key, key_id = await create_key(days)
    text = (
        "<b>Ключ создан!</b>\n\n"
        f"ID: #{key_id}\n"
        f"Ключ: <code>{key}</code>\n"
        f"Срок: {days} дней"
    )
    await safe_send(msg, text, back_admin_kb())


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
    text = "<b>Все ключи:</b>\n\nНажмите для управления:"
    buttons = []
    for row in keys:
        key_id = row[0]
        days = row[2]
        is_used = row[3]
        username = row[4]
        is_revoked = row[6]
        status = "X" if is_revoked else ("V" if is_used else "O")
        days_str = "inf" if days is None else f"{days}d"
        user_str = (
            f"@{username}" if username
            else ("?" if is_used else "-")
        )
        btn_text = f"[{status}] #{key_id} {days_str} {user_str}"
        buttons.append([
            InlineKeyboardButton(
                text=btn_text,
                callback_data=f"keyinfo_{key_id}"
            )
        ])
    buttons.append([
        InlineKeyboardButton(text="Назад", callback_data="admin")
    ])
    await safe_edit(
        cb.message, text, InlineKeyboardMarkup(inline_keyboard=buttons)
    )
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
    status = (
        "Удалён" if is_revoked
        else ("Использован" if is_used else "Свободен")
    )
    days_str = "Бессрочно" if days is None else f"{days} дней"
    user_str = f"@{username}" if username else "-"
    exp_str = format_expiry(expires_at, is_revoked)
    text = (
        f"<b>Ключ #{key_id}</b>\n\n"
        f"Ключ: <code>{key}</code>\n"
        f"Статус: {status}\n"
        f"Срок: {days_str}\n"
        f"Пользователь: {user_str}\n"
        f"Осталось: {exp_str}\n\n"
    )
    if not is_revoked:
        text += "Удалить этот ключ и подписку пользователя?"
        await safe_edit(cb.message, text, confirm_delete_kb(key_id))
    else:
        await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("confirmdel_"))
async def confirm_delete(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    key_id = int(cb.data.replace("confirmdel_", ""))
    await delete_key(key_id)
    text = (
        f"<b>Ключ #{key_id} удалён!</b>\n\n"
        "Подписка пользователя полностью удалена."
    )
    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "stats")
async def stats_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    active, free_keys, total_keys, total_stars, total_refs, total_payments = (
        await get_stats()
    )
    text = (
        "<b>Статистика</b>\n\n"
        f"<b>Подписки:</b>\nАктивных: {active}\n\n"
        f"<b>Ключи:</b>\nСвободных: {free_keys}\nВсего: {total_keys}\n\n"
        f"<b>Доход:</b>\nВсего звёзд: {total_stars}\nОплат: {total_payments}\n\n"
        f"<b>Рефералы:</b>\nВсего приглашений: {total_refs}"
    )
    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "restart_xray")
async def restart_xray_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer("Перезапуск...")
    await restart_xray()
    await safe_edit(
        cb.message, "<b>Xray перезапущен!</b>", back_admin_kb()
    )


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
    print(f"Web server running on port {PORT}")
    while True:
        await asyncio.sleep(3600)


async def expiry_checker():
    """Фоновая задача очистки истекших подписок"""
    while True:
        await asyncio.sleep(3600)
        try:
            deleted = await cleanup_expired()
            if deleted > 0:
                print(f"Cleaned up {deleted} expired subscriptions")
                await restart_xray()
        except Exception as e:
            print(f"Error in expiry checker: {e}")


async def main():
    print("=" * 50)
    print("NEFRIT VPN MASTER SERVER")
    print("=" * 50)
    
    await init_db()
    await generate_xray_config()
    start_xray()
    await asyncio.sleep(3)
    
    await asyncio.gather(run_web(), run_bot(), expiry_checker())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")
        stop_xray()
