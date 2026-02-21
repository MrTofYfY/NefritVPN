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
PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "vpn.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

SUPPORT_USERNAME = "mellfreezy"
CHANNEL_USERNAME = "nefrit_vpn"

PRICES = {
    "week": {"days": 7, "stars": 5, "name": "1 nedelya"},
    "month": {"days": 30, "stars": 10, "name": "1 mesyac"},
    "year": {"days": 365, "stars": 100, "name": "1 god"},
    "forever": {"days": None, "stars": 300, "name": "Navsegda"}
}

xray_process = None
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class States(StatesGroup):
    waiting_key = State()
    waiting_days = State()
    waiting_revoke_id = State()


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
        await db.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                username TEXT,
                amount INTEGER,
                plan TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
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
            return None, "Kluch ne nayden"

        key_id = row[0]
        is_used = row[1]
        days = row[2]
        is_revoked = row[3]

        if is_revoked:
            return None, "Kluch annulirovan"
        if is_used:
            return None, "Kluch uzhe ispolzovan"

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
        return active, total, free_keys, total_keys, total_stars


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

    print("Xray config: " + str(len(clients)) + " clients")


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
        print("Xray PID: " + str(xray_process.pid))
        return True
    except Exception as e:
        print("Xray error: " + str(e))
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
    return web.Response(text="<h1>Nefrit VPN Active</h1>", content_type="text/html")


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
        [InlineKeyboardButton(text="Kupit podpisku", callback_data="buy")],
        [InlineKeyboardButton(text="Aktivirovat kluch", callback_data="activate")],
        [InlineKeyboardButton(text="Moya podpiska", callback_data="mysub")],
        [
            InlineKeyboardButton(text="Podderzhka", url="https://t.me/" + SUPPORT_USERNAME),
            InlineKeyboardButton(text="Kanal", url="https://t.me/" + CHANNEL_USERNAME)
        ]
    ]
    if admin:
        buttons.append([InlineKeyboardButton(text="Admin panel", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def buy_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1 nedelya - 5 zvezd", callback_data="pay_week")],
        [InlineKeyboardButton(text="1 mesyac - 10 zvezd", callback_data="pay_month")],
        [InlineKeyboardButton(text="1 god - 100 zvezd", callback_data="pay_year")],
        [InlineKeyboardButton(text="Navsegda - 300 zvezd", callback_data="pay_forever")],
        [InlineKeyboardButton(text="Nazad", callback_data="back")]
    ])


def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Sozdat kluch", callback_data="newkey")],
        [InlineKeyboardButton(text="Vse kluchi", callback_data="keys")],
        [InlineKeyboardButton(text="Statistika", callback_data="stats")],
        [InlineKeyboardButton(text="Restart Xray", callback_data="restart_xray")],
        [InlineKeyboardButton(text="Nazad", callback_data="back")]
    ])


def days_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="7 dney", callback_data="mkkey_7"),
            InlineKeyboardButton(text="14 dney", callback_data="mkkey_14"),
            InlineKeyboardButton(text="30 dney", callback_data="mkkey_30")
        ],
        [
            InlineKeyboardButton(text="60 dney", callback_data="mkkey_60"),
            InlineKeyboardButton(text="90 dney", callback_data="mkkey_90"),
            InlineKeyboardButton(text="180 dney", callback_data="mkkey_180")
        ],
        [InlineKeyboardButton(text="365 dney", callback_data="mkkey_365")],
        [InlineKeyboardButton(text="Bessrochno", callback_data="mkkey_0")],
        [InlineKeyboardButton(text="Otmena", callback_data="admin")]
    ])


def back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Menu", callback_data="back")]
    ])


def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Admin", callback_data="admin")]
    ])


def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Otmena", callback_data="back")]
    ])


def confirm_revoke_kb(key_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Da, udalit", callback_data="confirmrev_" + str(key_id)),
            InlineKeyboardButton(text="Net", callback_data="keys")
        ]
    ])


def format_expiry(expires_at, is_revoked):
    if is_revoked:
        return "Annulirovan"
    if not expires_at:
        return "Bessrochno"
    try:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        if exp <= now:
            return "Istek"
        diff = (exp - now).days
        if diff == 0:
            hours = (exp - now).seconds // 3600
            return str(hours) + "h"
        return str(diff) + "d"
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
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    name = msg.from_user.first_name
    text = "<b>Nefrit VPN</b>\n\n"
    text = text + "Dobro pozhalovat, " + name + "!\n\n"
    text = text + "Bystry i nadezhny VPN servis.\n\n"
    text = text + "Vyberite deystvie:"
    await msg.answer(text, reply_markup=main_kb(is_admin(msg.from_user)), parse_mode="HTML")


@dp.callback_query(F.data == "back")
async def go_back(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit(cb.message, "<b>Nefrit VPN</b>\n\nGlavnoe menu", main_kb(is_admin(cb.from_user)))
    await cb.answer()


@dp.callback_query(F.data == "buy")
async def buy_menu(cb: types.CallbackQuery):
    text = "<b>Kupit podpisku</b>\n\n"
    text = text + "Vyberite tarif:\n\n"
    text = text + "1 nedelya - 5 zvezd\n"
    text = text + "1 mesyac - 10 zvezd\n"
    text = text + "1 god - 100 zvezd\n"
    text = text + "Navsegda - 300 zvezd\n\n"
    text = text + "Oplata cherez Telegram Stars"
    await safe_edit(cb.message, text, buy_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("pay_"))
async def process_payment(cb: types.CallbackQuery):
    plan = cb.data.replace("pay_", "")

    if plan not in PRICES:
        await cb.answer("Oshibka", show_alert=True)
        return

    price_info = PRICES[plan]
    stars = price_info["stars"]
    name = price_info["name"]

    await cb.answer()

    await bot.send_invoice(
        chat_id=cb.from_user.id,
        title="Nefrit VPN - " + name,
        description="Podpiska na VPN: " + name,
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
        await msg.answer("Oshibka obraotki platezha")
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
        await msg.answer("Oshibka sozdaniya podpiski")
        return

    user_uuid = info[1]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, path)
    sub_url = BASE_URL + "/sub/" + path

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        exp_str = "Deystvuet do: " + exp.strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Srok: Bessrochno"

    text = "<b>Oplata prinyata!</b>\n\n"
    text = text + "Spasibo za pokupku!\n\n"
    text = text + exp_str + "\n\n"
    text = text + "<b>Ssylka podpiski:</b>\n"
    text = text + "<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Config:</b>\n"
    text = text + "<code>" + link + "</code>\n\n"
    text = text + "<b>Prilozheniya:</b>\n"
    text = text + "Android: V2rayNG\n"
    text = text + "iOS: Streisand / V2Box\n"
    text = text + "Windows: V2rayN"

    await msg.answer(text, reply_markup=back_kb(), parse_mode="HTML")


@dp.callback_query(F.data == "activate")
async def activate(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(States.waiting_key)
    text = "<b>Vvedite kluch aktivatsii:</b>\n\n"
    text = text + "Primer: NEFRIT-A1B2C3D4E5F6G7H8"
    await safe_edit(cb.message, text, cancel_kb())
    await cb.answer()


@dp.message(States.waiting_key)
async def process_key(msg: types.Message, state: FSMContext):
    key = msg.text.strip().upper()
    username = msg.from_user.username or msg.from_user.first_name

    path, error = await activate_key(key, msg.from_user.id, username)
    await state.clear()

    if error:
        await safe_send(msg, "Oshibka: " + error, back_kb())
        return

    info = await get_user_info(msg.from_user.id)
    if not info:
        await safe_send(msg, "Oshibka", back_kb())
        return

    user_uuid = info[1]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, path)
    sub_url = BASE_URL + "/sub/" + path

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        exp_str = "Deystvuet do: " + exp.strftime("%d.%m.%Y %H:%M")
    else:
        exp_str = "Srok: Bessrochno"

    text = "<b>Podpiska aktivirovana!</b>\n\n"
    text = text + exp_str + "\n\n"
    text = text + "<b>Ssylka:</b>\n<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Config:</b>\n<code>" + link + "</code>"

    await safe_send(msg, text, back_kb())


@dp.callback_query(F.data == "mysub")
async def my_sub(cb: types.CallbackQuery):
    info = await get_user_info(cb.from_user.id)

    if not info:
        text = "<b>U vas net podpiski</b>\n\nKupite ili aktiviruyte kluch."
        await safe_edit(cb.message, text, back_kb())
        await cb.answer()
        return

    user_path = info[0]
    user_uuid = info[1]
    is_active = info[2]
    expires_at = info[3]

    link = generate_vless_link(user_uuid, user_path)
    sub_url = BASE_URL + "/sub/" + user_path

    status = "Aktivna" if is_active else "Neaktivna"

    if expires_at:
        exp = datetime.fromisoformat(expires_at)
        now = datetime.now()
        if exp > now:
            diff = (exp - now).days
            exp_str = exp.strftime("%d.%m.%Y") + " (" + str(diff) + " dn.)"
        else:
            exp_str = "Istek"
    else:
        exp_str = "Bessrochno"

    text = "<b>Vasha podpiska</b>\n\n"
    text = text + "Status: " + status + "\n"
    text = text + "Srok: " + exp_str + "\n\n"
    text = text + "<b>URL:</b>\n<code>" + sub_url + "</code>\n\n"
    text = text + "<b>Config:</b>\n<code>" + link + "</code>"

    await safe_edit(cb.message, text, back_kb())
    await cb.answer()


@dp.callback_query(F.data == "admin")
async def admin_panel(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    await state.clear()
    active, total, free_keys, total_keys, total_stars = await get_stats()

    xray_ok = xray_process is not None and xray_process.poll() is None
    xray_status = "OK" if xray_ok else "OFF"

    text = "<b>Admin panel</b>\n\n"
    text = text + "Polzovateley: " + str(active) + " / " + str(total) + "\n"
    text = text + "Kluchey: " + str(free_keys) + " / " + str(total_keys) + "\n"
    text = text + "Zarabotano zvezd: " + str(total_stars) + "\n"
    text = text + "Xray: " + xray_status

    await safe_edit(cb.message, text, admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "newkey")
async def new_key_menu(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    await state.set_state(States.waiting_days)
    text = "<b>Sozdanie klucha</b>\n\n"
    text = text + "Vyberite srok deystviya:"
    await safe_edit(cb.message, text, days_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("mkkey_"))
async def create_key_handler(cb: types.CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    val = cb.data.replace("mkkey_", "")

    if val == "0":
        days = None
        days_str = "Bessrochno"
    else:
        days = int(val)
        days_str = str(days) + " dney"

    await state.clear()
    key, key_id = await create_key(days)

    text = "<b>Kluch sozdan!</b>\n\n"
    text = text + "ID: #" + str(key_id) + "\n"
    text = text + "Kluch: <code>" + key + "</code>\n"
    text = text + "Srok: " + days_str

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.message(States.waiting_days)
async def process_days_manual(msg: types.Message, state: FSMContext):
    if not is_admin(msg.from_user):
        return

    try:
        days = int(msg.text.strip())
        if days <= 0:
            await safe_send(msg, "Vvedite polozhitelnoe chislo", back_admin_kb())
            return
    except:
        await safe_send(msg, "Vvedite chislo", back_admin_kb())
        return

    await state.clear()
    key, key_id = await create_key(days)

    text = "<b>Kluch sozdan!</b>\n\n"
    text = text + "ID: #" + str(key_id) + "\n"
    text = text + "Kluch: <code>" + key + "</code>\n"
    text = text + "Srok: " + str(days) + " dney"

    await safe_send(msg, text, back_admin_kb())


@dp.callback_query(F.data == "keys")
async def list_keys(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    keys = await get_keys_list()

    if not keys:
        text = "<b>Kluchey net</b>"
        await safe_edit(cb.message, text, back_admin_kb())
        await cb.answer()
        return

    text = "<b>Vse kluchi:</b>\n\nNazhmite dlya udaleniya:"

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
            user_str = "@" + username
        elif is_used:
            user_str = "?"
        else:
            user_str = "-"

        btn_text = "[" + status + "] #" + str(key_id) + " " + days_str + " " + user_str
        buttons.append([InlineKeyboardButton(text=btn_text, callback_data="keyinfo_" + str(key_id))])

    buttons.append([InlineKeyboardButton(text="Nazad", callback_data="admin")])

    await safe_edit(cb.message, text, InlineKeyboardMarkup(inline_keyboard=buttons))
    await cb.answer()


@dp.callback_query(F.data.startswith("keyinfo_"))
async def key_info(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    key_id = int(cb.data.replace("keyinfo_", ""))
    info = await get_key_info(key_id)

    if not info:
        await cb.answer("Kluch ne nayden", show_alert=True)
        return

    key = info[1]
    days = info[2]
    is_used = info[3]
    username = info[4]
    expires_at = info[5]
    is_revoked = info[6]

    if is_revoked:
        status = "Annulirovan"
    elif is_used:
        status = "Ispolzovan"
    else:
        status = "Svoboden"

    if days is None:
        days_str = "Bessrochno"
    else:
        days_str = str(days) + " dney"

    if username:
        user_str = "@" + username
    else:
        user_str = "-"

    exp_str = format_expiry(expires_at, is_revoked)

    text = "<b>Kluch #" + str(key_id) + "</b>\n\n"
    text = text + "Kluch: <code>" + key + "</code>\n"
    text = text + "Status: " + status + "\n"
    text = text + "Srok: " + days_str + "\n"
    text = text + "Polzovatel: " + user_str + "\n"
    text = text + "Ostalos: " + exp_str + "\n\n"

    if not is_revoked:
        text = text + "Udalit etot kluch?"
        await safe_edit(cb.message, text, confirm_revoke_kb(key_id))
    else:
        await safe_edit(cb.message, text, back_admin_kb())

    await cb.answer()


@dp.callback_query(F.data.startswith("confirmrev_"))
async def confirm_revoke(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    key_id = int(cb.data.replace("confirmrev_", ""))

    await revoke_key(key_id)

    text = "<b>Kluch #" + str(key_id) + " annulirovan!</b>\n\n"
    text = text + "Polzovatel poteryal dostup."

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "stats")
async def stats_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    active, total, free_keys, total_keys, total_stars = await get_stats()

    text = "<b>Statistika</b>\n\n"
    text = text + "<b>Polzovateli:</b>\n"
    text = text + "Aktivnyh: " + str(active) + "\n"
    text = text + "Vsego: " + str(total) + "\n\n"
    text = text + "<b>Kluchi:</b>\n"
    text = text + "Svobodnyh: " + str(free_keys) + "\n"
    text = text + "Vsego: " + str(total_keys) + "\n\n"
    text = text + "<b>Dohod:</b>\n"
    text = text + "Vsego zvezd: " + str(total_stars)

    await safe_edit(cb.message, text, back_admin_kb())
    await cb.answer()


@dp.callback_query(F.data == "restart_xray")
async def restart_xray_handler(cb: types.CallbackQuery):
    if not is_admin(cb.from_user):
        await cb.answer("Net dostupa", show_alert=True)
        return

    await cb.answer("Perezapusk...")
    await restart_xray()

    await safe_edit(cb.message, "<b>Xray perezapuschen!</b>", back_admin_kb())


async def run_bot():
    print("Starting bot...")
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
    print("=" * 40)
    print("NEFRIT VPN SERVER")
    print("=" * 40)

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
