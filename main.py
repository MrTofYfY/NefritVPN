import os
import json
import uuid
import base64
import asyncio
import secrets
import subprocess
from pathlib import Path
from datetime import datetime
from aiohttp import web
import aiosqlite
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

load_dotenv()

# ============== КОНФИГУРАЦИЯ ==============
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "mellfreezy")
BASE_URL = os.getenv("BASE_URL", "https://nefritvpn.onrender.com")
PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10000

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "vpn.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

SUPPORT_USERNAME = "mellfreezy"
CHANNEL_USERNAME = "nefrit_vpn"

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