import os
import json
import uuid
import asyncio
import subprocess
from pathlib import Path
from datetime import datetime
from aiohttp import web, WSMsgType, ClientSession

PORT = int(os.getenv("PORT", 8080))
XRAY_PORT = 10001
SERVER_SECRET = os.getenv("SERVER_SECRET", "default-secret")
SERVER_NAME = os.getenv("SERVER_NAME", "Worker Server")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
XRAY_CONFIG_PATH = DATA_DIR / "xray_config.json"

active_users = {}
xray_process = None
restart_lock = asyncio.Lock()


def generate_xray_config():
    """Генерация конфигурации Xray"""
    clients = []
    for user_uuid in active_users.keys():
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
    """Запуск Xray"""
    global xray_process
    if not XRAY_CONFIG_PATH.exists():
        generate_xray_config()
    
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"Xray started with PID {xray_process.pid}")
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
            print("Xray stopped")
        except subprocess.TimeoutExpired:
            xray_process.kill()
            print("Xray killed (timeout)")
        except Exception as e:
            print(f"Error stopping Xray: {e}")
        finally:
            xray_process = None


async def restart_xray():
    """Перезапуск Xray с блокировкой"""
    async with restart_lock:
        stop_xray()
        generate_xray_config()
        await asyncio.sleep(1)
        start_xray()
        await asyncio.sleep(1)


async def handle_index(request):
    """Главная страница"""
    return web.Response(
        text=f"{SERVER_NAME} - Active users: {len(active_users)}",
        content_type="text/html"
    )


async def handle_health(request):
    """Проверка здоровья сервера"""
    xray_ok = xray_process is not None and xray_process.poll() is None
    return web.json_response({
        "status": "ok",
        "server": SERVER_NAME,
        "users": len(active_users),
        "xray": xray_ok
    })


async def handle_add_user(request):
    """Добавление пользователя"""
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    if data.get("secret") != SERVER_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    user_path = data.get("path")
    
    if not user_uuid or not user_path:
        return web.json_response({"error": "Missing uuid or path"}, status=400)
    
    active_users[user_uuid] = {
        "path": user_path,
        "added_at": datetime.now().isoformat()
    }
    
    print(f"User added: {user_uuid}")
    await restart_xray()
    
    return web.json_response({
        "success": True,
        "total_users": len(active_users)
    })


async def handle_remove_user(request):
    """Удаление пользователя"""
    try:
        data = await request.json()
    except:
        return web.json_response({"error": "Invalid JSON"}, status=400)
    
    if data.get("secret") != SERVER_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    
    if not user_uuid:
        return web.json_response({"error": "Missing uuid"}, status=400)
    
    if user_uuid in active_users:
        del active_users[user_uuid]
        print(f"User removed: {user_uuid}")
        await restart_xray()
        return web.json_response({
            "success": True,
            "total_users": len(active_users)
        })
    
    return web.json_response({"error": "User not found"}, status=404)


async def handle_tunnel(request):
    """WebSocket туннель к Xray"""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return web.Response(text="WebSocket required", status=400)
    
    ws_client = web.WebSocketResponse()
    await ws_client.prepare(request)
    
    ws_xray = None
    session = None
    
    try:
        url = f"http://127.0.0.1:{XRAY_PORT}/tunnel"
        session = ClientSession()
        ws_xray = await session.ws_connect(url, timeout=30)
        
        async def forward(src, dst, name):
            """Пересылка данных между WebSocket"""
            try:
                async for msg in src:
                    if msg.type == WSMsgType.BINARY:
                        await dst.send_bytes(msg.data)
                    elif msg.type == WSMsgType.TEXT:
                        await dst.send_str(msg.data)
                    elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
                        break
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Forward error ({name}): {e}")
        
        await asyncio.gather(
            forward(ws_client, ws_xray, "client->xray"),
            forward(ws_xray, ws_client, "xray->client"),
            return_exceptions=True
        )
    
    except asyncio.TimeoutError:
        print("Xray connection timeout")
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


async def run_web():
    """Запуск веб-сервера"""
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/api/add_user", handle_add_user)
    app.router.add_post("/api/remove_user", handle_remove_user)
    app.router.add_get("/tunnel", handle_tunnel)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"{SERVER_NAME} running on port {PORT}")
    
    while True:
        await asyncio.sleep(3600)


async def health_checker():
    """Фоновая проверка состояния Xray"""
    while True:
        await asyncio.sleep(60)
        try:
            if xray_process and xray_process.poll() is not None:
                print("Xray process died! Restarting...")
                await restart_xray()
        except Exception as e:
            print(f"Health checker error: {e}")


async def main():
    print("=" * 50)
    print(f"NEFRIT VPN WORKER: {SERVER_NAME}")
    print("=" * 50)
    
    generate_xray_config()
    start_xray()
    await asyncio.sleep(3)
    
    await asyncio.gather(run_web(), health_checker())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down...")
        stop_xray()
