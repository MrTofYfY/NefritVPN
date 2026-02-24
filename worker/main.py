import os
import json
import uuid
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime
from aiohttp import web, WSMsgType, ClientSession
import aiosqlite

PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001
SERVER_SECRET = os.getenv("SERVER_SECRET", "default-secret")
SERVER_NAME = os.getenv("SERVER_NAME", "Worker Server")

# Persistent storage для Render
if os.path.exists("/opt/render/project/src"):
    DATA_DIR = Path("/opt/render/project/src/data")
else:
    DATA_DIR = Path("data")

DATA_DIR.mkdir(exist_ok=True)
DB_PATH = DATA_DIR / "worker.db"
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

print(f"📁 Data directory: {DATA_DIR}")
print(f"📁 Database: {DB_PATH}")

xray_process = None
restart_lock = asyncio.Lock()


async def init_db():
    """Инициализация базы данных worker-сервера"""
    print("🔧 Initializing worker database...")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_uuid TEXT UNIQUE NOT NULL,
                path TEXT NOT NULL,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        await db.commit()
        
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        count = (await cursor.fetchone())[0]
        print(f"✅ Worker DB initialized. Users: {count}")


async def get_all_users():
    """Получить всех пользователей из БД"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute("SELECT user_uuid, path FROM users")
            return await cursor.fetchall()
    except Exception as e:
        print(f"❌ Error getting users: {e}")
        return []


async def add_user(user_uuid, user_path):
    """Добавить пользователя в БД"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO users (user_uuid, path, added_at) VALUES (?, ?, ?)",
                (user_uuid, user_path, datetime.now().isoformat())
            )
            await db.commit()
            print(f"✅ User added/updated: {user_uuid[:16]}...")
            return True
    except Exception as e:
        print(f"❌ Error adding user: {e}")
        return False


async def remove_user(user_uuid):
    """Удалить пользователя из БД"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                "SELECT id FROM users WHERE user_uuid = ?", (user_uuid,)
            )
            exists = await cursor.fetchone()
            
            if exists:
                await db.execute("DELETE FROM users WHERE user_uuid = ?", (user_uuid,))
                await db.commit()
                print(f"✅ User removed: {user_uuid[:16]}...")
                return True
            else:
                print(f"⚠️ User not found: {user_uuid[:16]}...")
                return False
    except Exception as e:
        print(f"❌ Error removing user: {e}")
        return False


async def generate_xray_config():
    """Генерация конфигурации Xray из БД"""
    users = await get_all_users()
    
    clients = []
    for user_uuid, path in users:
        clients.append({"id": user_uuid, "level": 0})
    
    if not clients:
        dummy_uuid = str(uuid.uuid4())
        clients.append({"id": dummy_uuid, "level": 0})
        print(f"⚠️ No users, using dummy UUID")
    
    config = {
        "log": {
            "loglevel": "warning"
        },
        "inbounds": [
            {
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
            }
        ],
        "outbounds": [
            {
                "protocol": "freedom",
                "tag": "direct"
            }
        ],
        "dns": {
            "servers": ["8.8.8.8", "1.1.1.1"]
        }
    }
    
    with open(XRAY_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"📝 Xray config: {len(clients)} clients")
    return len(clients)


def start_xray():
    """Запуск Xray"""
    global xray_process
    
    if not XRAY_CONFIG_PATH.exists():
        print("❌ Xray config not found!")
        return False
    
    try:
        if xray_process and xray_process.poll() is None:
            print("⚠️ Xray already running")
            return True
        
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        print(f"✅ Xray started with PID {xray_process.pid}")
        return True
        
    except FileNotFoundError:
        print("❌ Xray binary not found")
        return False
    except Exception as e:
        print(f"❌ Failed to start Xray: {e}")
        return False


def stop_xray():
    """Остановка Xray"""
    global xray_process
    
    if xray_process:
        try:
            xray_process.terminate()
            xray_process.wait(timeout=5)
            print("✅ Xray stopped")
        except subprocess.TimeoutExpired:
            xray_process.kill()
            print("⚠️ Xray killed")
        except Exception as e:
            print(f"❌ Error stopping Xray: {e}")
        finally:
            xray_process = None


async def restart_xray():
    """Безопасный перезапуск Xray"""
    async with restart_lock:
        print("🔄 Restarting Xray...")
        stop_xray()
        await generate_xray_config()
        await asyncio.sleep(1)
        start_xray()
        await asyncio.sleep(1)


# ============= WEB HANDLERS =============

async def handle_index(request):
    """Главная страница"""
    users = await get_all_users()
    xray_ok = xray_process is not None and xray_process.poll() is None
    
    return web.Response(
        text=f"{SERVER_NAME} | Users: {len(users)} | Xray: {'OK' if xray_ok else 'DOWN'}",
        content_type="text/html"
    )


async def handle_health(request):
    """Health check"""
    users = await get_all_users()
    xray_ok = xray_process is not None and xray_process.poll() is None
    
    return web.json_response({
        "status": "ok",
        "server": SERVER_NAME,
        "users": len(users),
        "xray": xray_ok
    })


async def handle_add_user(request):
    """API: Добавление пользователя (вызывается master сервером)"""
    try:
        data = await request.json()
    except:
        print("❌ Invalid JSON in add_user")
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    secret = data.get("secret")
    if secret != SERVER_SECRET:
        print(f"❌ Wrong secret: {secret}")
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    user_path = data.get("path", "")
    
    if not user_uuid:
        return web.json_response({"error": "Missing uuid"}, status=400)
    
    print(f"📥 API add_user: {user_uuid[:16]}...")
    
    success = await add_user(user_uuid, user_path)
    
    if success:
        await restart_xray()
        users = await get_all_users()
        return web.json_response({
            "success": True,
            "total_users": len(users)
        })
    else:
        return web.json_response({"error": "Failed"}, status=500)


async def handle_remove_user(request):
    """API: Удаление пользователя (вызывается master сервером)"""
    try:
        data = await request.json()
    except:
        print("❌ Invalid JSON in remove_user")
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    secret = data.get("secret")
    if secret != SERVER_SECRET:
        print(f"❌ Wrong secret")
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    
    if not user_uuid:
        return web.json_response({"error": "Missing uuid"}, status=400)
    
    print(f"📤 API remove_user: {user_uuid[:16]}...")
    
    success = await remove_user(user_uuid)
    
    await restart_xray()
    users = await get_all_users()
    
    return web.json_response({
        "success": success,
        "total_users": len(users)
    })


async def handle_sync(request):
    """API: Полная синхронизация пользователей"""
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    if data.get("secret") != SERVER_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    users_list = data.get("users", [])
    
    print(f"🔄 Full sync: {len(users_list)} users")
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users")
        
        for user in users_list:
            user_uuid = user.get("uuid")
            user_path = user.get("path", "")
            if user_uuid:
                await db.execute(
                    "INSERT INTO users (user_uuid, path, added_at) VALUES (?, ?, ?)",
                    (user_uuid, user_path, datetime.now().isoformat())
                )
        
        await db.commit()
    
    await restart_xray()
    
    users = await get_all_users()
    
    return web.json_response({
        "success": True,
        "total_users": len(users)
    })


async def handle_tunnel(request):
    """WebSocket туннель - основной VPN трафик"""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)
    
    # Проверяем Xray
    if not xray_process or xray_process.poll() is not None:
        print("⚠️ Xray not running, restarting...")
        await restart_xray()
        await asyncio.sleep(1)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    session = None
    ws_xray = None
    
    try:
        session = ClientSession()
        ws_xray = await session.ws_connect(
            f"http://127.0.0.1:{XRAY_PORT}/tunnel",
            timeout=30
        )
        
        async def forward(src, dst):
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
    
    except Exception as e:
        print(f"❌ Tunnel error: {e}")
    
    finally:
        if ws_xray and not ws_xray.closed:
            await ws_xray.close()
        if session:
            await session.close()
        if not ws_client.closed:
            await ws_client.close()
    
    return ws_client


# ============= BACKGROUND =============

async def health_checker():
    """Проверка Xray каждые 60 сек"""
    while True:
        await asyncio.sleep(60)
        try:
            if not xray_process or xray_process.poll() is not None:
                print("⚠️ Xray down, restarting...")
                await restart_xray()
        except Exception as e:
            print(f"❌ Health check error: {e}")


async def run_web():
    """Веб-сервер"""
    app = web.Application()
    
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/add_user", handle_add_user)
    app.router.add_post("/api/remove_user", handle_remove_user)
    app.router.add_post("/api/sync", handle_sync)
    app.router.add_get("/tunnel", handle_tunnel)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    
    print(f"🌐 {SERVER_NAME} on port {PORT}")
    
    while True:
        await asyncio.sleep(3600)


async def main():
    print("=" * 50)
    print(f"🔰 NEFRIT VPN WORKER: {SERVER_NAME}")
    print(f"📁 DB: {DB_PATH}")
    print("=" * 50)
    
    await init_db()
    await generate_xray_config()
    start_xray()
    await asyncio.sleep(2)
    
    await asyncio.gather(run_web(), health_checker())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye")
        stop_xray()
