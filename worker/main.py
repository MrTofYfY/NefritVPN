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


def generate_xray_config():
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
    
    print("Xray config updated: " + str(len(clients)) + " clients")


def start_xray():
    global xray_process
    if not XRAY_CONFIG_PATH.exists():
        generate_xray_config()
    
    try:
        xray_process = subprocess.Popen(
            ["/usr/local/bin/xray", "run", "-config", str(XRAY_CONFIG_PATH)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print("Xray started PID: " + str(xray_process.pid))
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


def restart_xray():
    stop_xray()
    generate_xray_config()
    start_xray()


async def handle_index(request):
    text = SERVER_NAME + " - Active users: " + str(len(active_users))
    return web.Response(text=text, content_type="text/html")


async def handle_health(request):
    xray_ok = xray_process is not None and xray_process.poll() is None
    data = {
        "status": "ok",
        "server": SERVER_NAME,
        "users": len(active_users),
        "xray": xray_ok
    }
    return web.json_response(data)


async def handle_add_user(request):
    data = await request.json()
    
    if data.get("secret") != SERVER_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    user_path = data.get("path")
    
    if not user_uuid or not user_path:
        return web.json_response({"error": "Missing data"}, status=400)
    
    active_users[user_uuid] = {
        "path": user_path,
        "added_at": datetime.now().isoformat()
    }
    
    print("User added: " + user_path + " (" + user_uuid + ")")
    
    restart_xray()
    
    return web.json_response({
        "success": True,
        "message": "User " + user_path + " added",
        "total_users": len(active_users)
    })


async def handle_remove_user(request):
    data = await request.json()
    
    if data.get("secret") != SERVER_SECRET:
        return web.json_response({"error": "Unauthorized"}, status=401)
    
    user_uuid = data.get("uuid")
    
    if user_uuid in active_users:
        user_path = active_users[user_uuid]["path"]
        del active_users[user_uuid]
        
        print("User removed: " + user_path + " (" + user_uuid + ")")
        
        restart_xray()
        
        return web.json_response({
            "success": True,
            "message": "User removed",
            "total_users": len(active_users)
        })
    
    return web.json_response({"error": "User not found"}, status=404)


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
                
                await asyncio.gather(
                    fwd(ws_client, ws_xray),
                    fwd(ws_xray, ws_client),
                    return_exceptions=True
                )
    except:
        pass
    finally:
        if not ws_client.closed:
            await ws_client.close()
    
    return ws_client


async def run_web():
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
    print(SERVER_NAME + " on port " + str(PORT))
    
    while True:
        await asyncio.sleep(3600)


async def main():
    print("=" * 50)
    print("NEFRIT VPN WORKER: " + SERVER_NAME)
    print("=" * 50)
    
    generate_xray_config()
    start_xray()
    await asyncio.sleep(3)
    
    if xray_process and xray_process.poll() is None:
        print("Xray is running")
    else:
        print("Warning: Xray may not be running")
    
    await run_web()


if __name__ == "__main__":
    asyncio.run(main())
