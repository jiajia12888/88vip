import asyncio
import json
import base64
import uuid
from typing import Dict, List, Optional, Any
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from loguru import logger
import uvicorn
from redis import asyncio as aioredis
from datetime import datetime
import os
from fastapi.security import APIKeyCookie

# --- 配置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

logger.add("server.log", rotation="10 MB", retention="7 days", level="INFO", encoding="utf-8")

REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379
REDIS_PASSWORD = "xiaoxiao"
REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/2"
redis: aioredis.Redis = None
TIME_1 = 5
ADMIN_USERNAME = "admin"
SESSION_COOKIE_NAME = "wspanel_session_id"
SESSION_EXPIRATION_SECONDS = 86400
cookie_scheme = APIKeyCookie(name=SESSION_COOKIE_NAME, auto_error=False)

# --- 工具函数 ---
def encode_message(payload: dict) -> str:
    """JSON -> UTF-8 -> base64 (通用编码，支持特殊字符)"""
    try:
        json_str = json.dumps(payload, ensure_ascii=False)
        utf8_bytes = json_str.encode('utf-8')
        base64_str = base64.b64encode(utf8_bytes).decode('utf-8')
        return base64_str
    except Exception as e:
        logger.error(f"编码消息时出错: {e}")
        return ""


async def _generate_and_dispatch_config_for_user(user_id: str):
    """
    从Redis获取用户设置，生成配置负载，
    并将其发送给已连接的移动客户端（如果在线）。
    """
    account_str = await redis.hget("user_hao", user_id)
    if not account_str:
        logger.warning(f"尝试为不存在的用户调度配置: {user_id}")
        return

    account = json.loads(account_str)
    client_id = account.get("client_id")

    if not client_id or client_id not in manager.mobile_clients:
        logger.info(f"用户 {user_id} 不在线。跳过配置调度。")
        return

    # --- 核心配置生成逻辑 ---
    shill_plan_model_list = []
    forwarding_rules = account.get("forwarding_rules", [])
    send_enabled = account.get("send_enabled", False)
    forwarding_delay = account.get("forwarding_delay", 500)

    for rule in forwarding_rules:
        plan = {
            "planSourceList": [{
                "planSourceRoomId": rule.get("source_room", {}).get("id", ""),
                "planSourceSwitch": "1",
                "planSourceWechatId": ",".join([str(m.get("id")) for m in rule.get("source_members", [])]),
                "shillPlanSourceId": 0
            }],
            "planTargetRoomId": ",".join([str(d.get("id")) for d in rule.get("destinations", [])]),
            "planTargetSwitch": "1" if rule.get("enabled", False) and send_enabled else "0",
            "shillPlanTargetId": 0
        }
        shill_plan_model_list.append(plan)

    config_payload = {
        "type": 1,
        "data": {
            "code": "1000", "memo": "成功",
            "data": {
                "sendInterval": str(forwarding_delay),
                "shillActionModel": {
                    "foldSwitch": "0", "joinRoomSwitch": "1", "imgZipSwitch": "0",
                    "disturbSwitch": "0", "imgToEmojiSwitch": "1", "passFriendSwitch": "1"
                },
                "shillPlanModelList": shill_plan_model_list
            }
        }
    }

    if await manager.send_message_to_mobile(client_id, config_payload):
        logger.success(f"已成功向客户端 {client_id} (UserID: {user_id}) 下发配置。{config_payload}")
    else:
        logger.error(f"向客户端 {client_id} (UserID: {user_id}) 下发配置失败。")


def build_payload(item: dict, room_id: str) -> dict:
    t = item.get("type")
    req_id = str(uuid.uuid4())
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 文本和文件消息 (仅用于群发公告)
    if t == "text":
        return {"type": 5000, "requestId": req_id, "data": {"content": item["content"], "conversation_id": room_id}}
    if t == "image":
        return {"type": 5003, "requestId": req_id, "timestamp": ts, "data": {"path": item["path"], "conversation_id": room_id}}
    if t == "gif":
        return {"type": 5006, "requestId": req_id, "timestamp": ts, "data": {"path": item["path"], "conversation_id": room_id}}
    if t == "video":
        return {"type": 5004, "requestId": req_id, "timestamp": ts, "data": {"path": item["path"], "conversation_id": room_id}}
    
    raise ValueError(f"unsupported item type for broadcast: {t}")

# --- ConnectionManager ---
class ConnectionManager:
    def __init__(self):
        self.mobile_clients: Dict[str, WebSocket] = {}
        self.panel_viewers: Dict[str, WebSocket] = {}
        self.user_bindings: Dict[str, str] = {}

    async def broadcast_event_to_panels(self, event_type: str, payload: Any, target_user: Optional[str] = None):
        event_data = json.dumps({"timestamp": datetime.now().isoformat(), "type": event_type, "payload": payload}, ensure_ascii=False)
        recipients = []
        if ADMIN_USERNAME in self.panel_viewers:
            recipients.append(self.panel_viewers[ADMIN_USERNAME])
        if target_user and target_user != ADMIN_USERNAME and target_user in self.panel_viewers:
            recipients.append(self.panel_viewers[target_user])
        
        tasks = [viewer.send_text(event_data) for viewer in set(recipients) if viewer.client_state.name == 'CONNECTED']
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def connect_mobile(self, websocket: WebSocket, client_id: str, user_id: str, my_nick: str):
        self.mobile_clients[client_id] = websocket
        self.user_bindings[client_id] = user_id
        
        user_data = {"myNick": my_nick, "last_seen": datetime.now().isoformat(), "client_id": client_id}
        existing_data_str = await redis.hget("user_hao", user_id)
        if existing_data_str:
            existing_data = json.loads(existing_data_str)
            user_data.update({
                "admin_user": existing_data.get("admin_user"),
                "send_enabled": existing_data.get("send_enabled", False),
                "forwarding_rules": existing_data.get("forwarding_rules", []),
                "forwarding_delay": existing_data.get("forwarding_delay", 1000),
                "category": existing_data.get("category", "未分类"),
                "tags": existing_data.get("tags", [])
            })
        
        await redis.hset("user_hao", user_id, json.dumps(user_data, ensure_ascii=False))
        
        log_msg = f"客户端已连接: {client_id} (UserID: {user_id})"
        logger.success(f"[连接] {log_msg}")
        # --- 连接成功后调用辅助函数下发配置 ---
        await _generate_and_dispatch_config_for_user(user_id)
        
        await self.broadcast_event_to_panels('log', {"message": log_msg})
        await self.broadcast_event_to_panels('full_update', await get_all_admin_data())
        
        target_user = user_data.get("admin_user")
        if target_user:
            await self.broadcast_event_to_panels('user_update', await get_data_for_user(target_user), target_user=target_user)

    async def disconnect_mobile(self, client_id: str):
        if client_id in self.mobile_clients:
            user_id = self.user_bindings.pop(client_id, None)
            del self.mobile_clients[client_id]
            if user_id:
                await redis.delete(f"heartbeat:{user_id}")
                target_user = None
                user_info_str = await redis.hget("user_hao", user_id)
                if user_info_str:
                    target_user = json.loads(user_info_str).get("admin_user")
                
                log_msg = f"客户端已断开: {client_id} (UserID: {user_id or '未知'})"
                logger.warning(f"[断开] {log_msg}")
                await self.broadcast_event_to_panels('log', {"message": log_msg})
                await self.broadcast_event_to_panels('full_update', await get_all_admin_data())
                if target_user:
                    await self.broadcast_event_to_panels('user_update', await get_data_for_user(target_user), target_user=target_user)

    async def send_message_to_mobile(self, client_id: str, payload: dict) -> bool:
        if client_id in self.mobile_clients:
            try:
                encoded_payload = encode_message(payload)
                logger.success(f"准备向客户端 {client_id} 发送消息, 类型: {payload.get('type')}, 编码后长度: {len(encoded_payload)}")
                await self.mobile_clients[client_id].send_text(encoded_payload)
                return True
            except Exception as e:
                logger.error(f"向客户端 {client_id} 发送消息出错: {e}")
        return False

    async def connect_panel(self, websocket: WebSocket, username: str):
        await websocket.accept()
        self.panel_viewers[username] = websocket
        logger.info(f"Web面板用户 '{username}' 已连接WebSocket。")

    def disconnect_panel(self, username: str):
        if username in self.panel_viewers:
            del self.panel_viewers[username]
            logger.info(f"Web面板用户 '{username}' 已断开WebSocket。")

manager = ConnectionManager()

# --- 数据获取函数 ---
async def get_all_admin_data():
    all_users_from_db = await redis.hgetall("yonghu")
    all_accounts = await redis.hgetall("user_hao")
    online_user_ids = set(manager.user_bindings.values())
    unassigned, assigned = [], {}
    for user_id, acc_str in all_accounts.items():
        account = json.loads(acc_str)
        account.update({"user_id": user_id, "online": user_id in online_user_ids})
        assigned_to = account.get("admin_user")
        if assigned_to:
            assigned.setdefault(assigned_to, []).append(account)
        else:
            unassigned.append(account)
    return {"unassigned": unassigned, "assigned": assigned, "all_users": list(all_users_from_db.keys())}

async def get_data_for_user(username: str):
    if not username: return {"assigned_to_me": []}
    all_accounts = await redis.hgetall("user_hao")
    online_user_ids = set(manager.user_bindings.values())
    my_accounts = []
    for user_id, acc_str in all_accounts.items():
        account = json.loads(acc_str)
        if account.get("admin_user") == username:
            account.update({"user_id": user_id, "online": user_id in online_user_ids})
            my_accounts.append(account)
    return {"assigned_to_me": my_accounts}

# --- FastAPI App ---
app = FastAPI(title="WebSocket 控制服务")

@app.on_event("startup")
async def startup_event():
    global redis
    redis = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    if not await redis.hexists("yonghu", ADMIN_USERNAME):
        await redis.hset("yonghu", ADMIN_USERNAME, json.dumps({"password": "admin"}))
    logger.info("Redis 连接池已创建")

@app.on_event("shutdown")
async def shutdown_event():
    if redis: await redis.close()

# --- WebSocket 端点 ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, client_type: Optional[str] = None):
    # ... (WebSocket 连接逻辑无重大改动，保持原样) ...
    if client_type == 'viewer':
        session_id = websocket.cookies.get(SESSION_COOKIE_NAME)
        username = await redis.get(f"session:{session_id}") if session_id else None
        if not username:
            await websocket.close(); return
        await manager.connect_panel(websocket, username)
        try:
            while True: await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect_panel(username)
        return

    await websocket.accept()
    client_id = f"{websocket.client.host}:{websocket.client.port}"
    user_id = None
    try:
        info_request = {"type": 2000, "requestId": str(uuid.uuid4()), "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        await websocket.send_text(encode_message(info_request))
        
        async def wait_for_response():
            while True:
                data = await websocket.receive_text()
                try:
                    decoded_data = base64.b64decode(data).decode('utf-8')
                    msg = json.loads(decoded_data)
                except (json.JSONDecodeError, UnicodeDecodeError, Exception):
                    try:
                        msg = json.loads(data)
                    except json.JSONDecodeError:
                        if "heartBreak" in data: return {"type": "heartBreak"}
                        msg = {}
                if msg.get("type") != "heartBreak": return msg
                
        response_json = await asyncio.wait_for(wait_for_response(), timeout=10.0)
        data_part = response_json.get("data", {})
        
        # 兼容客户端回包 type=200 或 type=2000
        if response_json.get("type") in (200, 2000) and data_part.get("user_id"):
            user_id = str(data_part["user_id"])
            my_nick = str(response_json.get("myNick") or data_part.get("myNick", "未知昵称"))
            await manager.connect_mobile(websocket, client_id, user_id, my_nick)
        else:
            raise Exception("无效的客户端信息响应")

        while True:
            raw = await websocket.receive_text()
            msg = {}
            try:
                decoded_bytes = base64.b64decode(raw)
                try:
                    decoded_data = decoded_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    decoded_data = decoded_bytes.decode("gbk", errors="ignore")
                msg = json.loads(decoded_data)
            except Exception:
                try:
                    msg = json.loads(raw)
                except Exception:
                    if "heartBreak" in raw:
                        await redis.set(f"heartbeat:{user_id}", datetime.now().isoformat(), ex=15)
                    continue

            msg_type = msg.get("type")
            msg_data = msg.get("data", {})

            if msg_type == 2502:
                room_list = msg_data.get("room_list") or msg_data.get("roomList") or []
                await redis.hset("cached_rooms", user_id, json.dumps(room_list, ensure_ascii=False))
                account_str = await redis.hget("user_hao", user_id)
                if account_str:
                    target_user = json.loads(account_str).get("admin_user")
                    await manager.broadcast_event_to_panels('room_update', {"user_id": user_id, "rooms": room_list}, target_user=target_user)
            elif msg_type == 2503:
                req_id = msg.get("requestId")
                room_id = await redis.get(f"request_map:{req_id}") if req_id else None
                if room_id: await redis.delete(f"request_map:{req_id}")
                if not room_id: room_id = await redis.get(f"last_member_request:{user_id}")
                if room_id: await redis.delete(f"last_member_request:{user_id}")
                if not room_id:
                    room_id = (
                        msg_data.get("room_chat_id")
                        or msg_data.get("group_id")
                        or msg_data.get("room_id")
                        or msg_data.get("roomId")
                        or msg_data.get("groupId")
                    )

                member_list = msg_data.get("member_list") or msg_data.get("memberList") or msg_data.get("members") or []
                if room_id:
                    await redis.hset("cached_members", f"{user_id}:{room_id}", json.dumps(member_list, ensure_ascii=False))
                    account_str = await redis.hget("user_hao", user_id)
                    if account_str:
                        target_user = json.loads(account_str).get("admin_user")
                        broadcast_payload = {"user_id": user_id, "room_id": str(room_id), "members": member_list}
                        await manager.broadcast_event_to_panels('member_update', broadcast_payload, target_user=target_user)
                else:
                    logger.error(f"无法确定成员列表对应的 room_id。响应: {raw}")
    except Exception as e:
        logger.error(f"处理 {client_id} 连接时出错: {e}", exc_info=True)
    finally:
        await manager.disconnect_mobile(client_id)

# --- HTTP 页面路由 ---
@app.get("/", response_class=HTMLResponse)
async def route_root(): 
    await asyncio.sleep(TIME_1)
    return FileResponse(os.path.join(BASE_DIR, 'login.html'))

@app.get("/user", response_class=HTMLResponse)
async def get_user_panel(request: Request):
    await asyncio.sleep(TIME_1)
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_id or not await redis.get(f"session:{session_id}"):
        return RedirectResponse(url="/")
    return FileResponse(os.path.join(BASE_DIR, 'user_panel.html'))

@app.get("/admin", response_class=HTMLResponse)
async def get_admin_panel(request: Request):
    await asyncio.sleep(TIME_1)
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    username = await redis.get(f"session:{session_id}") if session_id else None
    if not username or username != ADMIN_USERNAME:
        return RedirectResponse(url="/")
    return FileResponse(os.path.join(BASE_DIR, 'admin_panel.html'))

@app.get("/debug", response_class=HTMLResponse)
async def get_debug_panel(request: Request):
    await asyncio.sleep(TIME_1)
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    username = await redis.get(f"session:{session_id}") if session_id else None
    if not username or username != ADMIN_USERNAME:
        return RedirectResponse(url="/")
    return FileResponse(os.path.join(BASE_DIR, 'debug_panel.html'))

# --- 用户认证依赖 ---
async def get_current_user(request: Request):
    session_id = request.cookies.get(SESSION_COOKIE_NAME)
    if session_id:
        username = await redis.get(f"session:{session_id}")
        if username:
            return {"username": username, "is_admin": username == ADMIN_USERNAME}
    raise HTTPException(status_code=401, detail="Not authenticated")

# --- HTTP API ---
@app.post("/api/register")
async def api_register(request: Request):
    await asyncio.sleep(TIME_1)
    data = await request.json()
    username, password = data.get("username"), data.get("password")
    if not username or not password:
        return {"success": False, "message": "用户名和密码不能为空"}
    if await redis.hexists("yonghu", username):
        return {"success": False, "message": "用户已存在"}
    await redis.hset("yonghu", username, json.dumps({"password": password}, ensure_ascii=False))
    return {"success": True, "message": "注册成功"}

@app.post("/api/login")
async def api_login(request: Request, response: Response):
    await asyncio.sleep(TIME_1)
    data = await request.json()
    username, password = data.get("username"), data.get("password")
    user_data_str = await redis.hget("yonghu", username)
    if user_data_str and json.loads(user_data_str).get("password") == password:
        session_id = str(uuid.uuid4())
        await redis.set(f"session:{session_id}", username, ex=SESSION_EXPIRATION_SECONDS)
        response.set_cookie(key=SESSION_COOKIE_NAME, value=session_id, httponly=True, max_age=SESSION_EXPIRATION_SECONDS)
        return {"success": True, "is_admin": username == ADMIN_USERNAME}
    return {"success": False, "message": "用户名或密码错误"}



@app.get("/api/data")
async def api_get_data(user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    if user_info["is_admin"]:
        return await get_all_admin_data()
    return await get_data_for_user(user_info["username"])

@app.post("/api/assign")
async def api_assign(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    if not user_info["is_admin"]: raise HTTPException(status_code=403)
    data = await request.json()
    target_user, account_id = data.get("target_user"), data.get("account_id")
    account_str = await redis.hget("user_hao", account_id)
    if account_str:
        account = json.loads(account_str)
        account["admin_user"] = target_user
        await redis.hset("user_hao", account_id, json.dumps(account, ensure_ascii=False))
        await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())
        await manager.broadcast_event_to_panels('user_update', await get_data_for_user(target_user), target_user=target_user)
        return {"success": True}
    return {"success": False}


@app.post("/api/unassign")
async def api_unassign(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    data = await request.json()
    account_id = data.get("account_id")
    account_str = await redis.hget("user_hao", account_id)
    if account_str:
        account = json.loads(account_str)
        previous_user = account.get("admin_user")
        if user_info["is_admin"] or user_info["username"] == previous_user:
            account.pop("admin_user", None)
            await redis.hset("user_hao", account_id, json.dumps(account, ensure_ascii=False))
            await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())
            if previous_user:
                await manager.broadcast_event_to_panels('user_update', await get_data_for_user(previous_user), target_user=previous_user)
            return {"success": True}
        else:
            raise HTTPException(status_code=403)
    return {"success": False}


@app.post("/api/request_rooms")
async def api_request_rooms(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    data = await request.json()
    user_id = data.get("user_id")
    force_refresh = data.get("force_refresh", False)
    auth_account_str = await redis.hget("user_hao", user_id)
    if not auth_account_str:
        return {"success": False, "message": "账号不存在"}
    auth_account = json.loads(auth_account_str)
    if not user_info["is_admin"] and auth_account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)
    cached_rooms_str = await redis.hget("cached_rooms", user_id)
    if cached_rooms_str and not force_refresh:
        return {"success": True, "cached": True, "rooms": json.loads(cached_rooms_str)}
    account_str = await redis.hget("user_hao", user_id)
    if not account_str: return {"success": False, "message": "账号不存在"}
    client_id = json.loads(account_str).get("client_id")
    if not client_id or client_id not in manager.mobile_clients:
        return {"success": False, "message": "该账号当前不在线"}
    req = {"type": 2502, "requestId": str(uuid.uuid4()), "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    await manager.send_message_to_mobile(client_id, req)
    return {"success": True, "cached": False, "message": "请求已发送"}


@app.post("/api/request_members")
async def api_request_members(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    data = await request.json()
    user_id, room_id, force_refresh = data.get("user_id"), data.get("room_id"), data.get("force_refresh", False)
    if not user_id or not room_id: raise HTTPException(status_code=400)
    auth_account_str = await redis.hget("user_hao", user_id)
    if not auth_account_str:
        return {"success": False, "message": "账号不存在"}
    auth_account = json.loads(auth_account_str)
    if not user_info["is_admin"] and auth_account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)
    cached_members_str = await redis.hget("cached_members", f"{user_id}:{room_id}")
    if cached_members_str and not force_refresh:
        return {"success": True, "cached": True, "members": json.loads(cached_members_str)}
    account_str = await redis.hget("user_hao", user_id)
    if not account_str: return {"success": False, "message": "账号不存在"}
    client_id = json.loads(account_str).get("client_id")
    if not client_id or client_id not in manager.mobile_clients:
        return {"success": False, "message": "该账号当前不在线"}
    req_id = str(uuid.uuid4())
    await redis.set(f"request_map:{req_id}", room_id, ex=60)
    await redis.set(f"last_member_request:{user_id}", room_id, ex=60)
    req = {"type": 2503, "requestId": req_id, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "data": {"room_chat_id": room_id}}
    await manager.send_message_to_mobile(client_id, req)
    return {"success": True, "cached": False, "message": "请求已发送"}


@app.post("/api/save_settings")
async def api_save_settings(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    data = await request.json()
    user_id = data.get("user_id")
    account_str = await redis.hget("user_hao", user_id)
    if not account_str: return {"success": False, "message": "账号不存在"}
    
    account = json.loads(account_str)
    if not user_info["is_admin"] and account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)
    
    account["send_enabled"] = data.get("send_enabled", False)
    account["forwarding_rules"] = data.get("forwarding_rules", [])
    account["forwarding_delay"] = data.get("forwarding_delay", 500)
    
    await redis.hset("user_hao", user_id, json.dumps(account, ensure_ascii=False))
    
    # --- 保存设置后，调用辅助函数下发最新配置 ---
    await _generate_and_dispatch_config_for_user(user_id)
    
    await manager.broadcast_event_to_panels('user_update', await get_data_for_user(user_info["username"]), target_user=user_info["username"])
    if user_info["is_admin"]:
        await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())
        
    return {"success": True, "message": "设置已保存并已尝试下发至客户端。"}


@app.post("/api/update_account_meta")
async def api_update_account_meta(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    """更新账号的分类和标签"""
    data = await request.json()
    user_id = data.get("user_id")
    
    account_str = await redis.hget("user_hao", user_id)
    if not account_str: return {"success": False, "message": "账号不存在"}
    
    account = json.loads(account_str)
    # 权限检查
    if not user_info["is_admin"] and account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)
        
    account["category"] = data.get("category", "未分类")
    account["tags"] = data.get("tags", [])
    
    await redis.hset("user_hao", user_id, json.dumps(account, ensure_ascii=False))
    
    # 广播更新到所有面板
    await manager.broadcast_event_to_panels('user_update', await get_data_for_user(user_info["username"]), target_user=user_info["username"])
    if user_info["is_admin"]:
        await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())
        
    return {"success": True}


@app.post("/api/batch_update_meta")
async def api_batch_update_meta(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    """批量更新账号的分类和标签"""
    data = await request.json()
    user_ids = data.get("user_ids", [])
    category = data.get("category")
    tags = data.get("tags")
    
    if not user_ids:
        return {"success": False, "message": "未选择任何账号"}

    for user_id in user_ids:
        account_str = await redis.hget("user_hao", user_id)
        if not account_str:
            logger.warning(f"批量更新时未找到账号: {user_id}")
            continue
            
        account = json.loads(account_str)
        # 权限检查
        if not user_info["is_admin"] and account.get("admin_user") != user_info["username"]:
            logger.warning(f"用户 {user_info['username']} 尝试批量更新不属于自己的账号 {user_id}")
            continue
        
        # 如果提供了新值，则更新
        if category is not None:
             account["category"] = category if category.strip() else "未分类"
        if tags is not None:
            account["tags"] = tags

        await redis.hset("user_hao", user_id, json.dumps(account, ensure_ascii=False))

    # 广播更新到所有面板
    await manager.broadcast_event_to_panels('user_update', await get_data_for_user(user_info["username"]), target_user=user_info["username"])
    if user_info["is_admin"]:
        await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())

    return {"success": True, "message": f"成功更新 {len(user_ids)} 个账号"}


@app.post("/api/broadcast_to_groups")
async def api_broadcast_to_groups(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    data = await request.json()
    message_item, user_id_to_send = data.get("message_item"), data.get("user_id")
    if not message_item or not user_id_to_send: raise HTTPException(400, "缺少参数")
    account_str = await redis.hget("user_hao", user_id_to_send)
    if not account_str: return {"success": False, "message": "账号不存在"}
    account = json.loads(account_str)
    if not user_info["is_admin"] and account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)
    client_id = account.get("client_id")
    if not client_id or client_id not in manager.mobile_clients:
        return {"success": False, "message": "账号不在线"}
    rules = account.get("forwarding_rules", [])
    all_destination_rooms = set()
    for rule in rules:
        if rule.get("enabled", True):
            for dest in rule.get("destinations", []):
                if dest_id := dest.get("id"):
                    all_destination_rooms.add(dest_id)
    if not all_destination_rooms:
        return {"success": False, "message": "该账号未设置任何转发规则或目标群聊"}
    delay_ms = account.get("forwarding_delay", 500)
    success_count = 0
    for room_id in all_destination_rooms:
        try:
            payload = build_payload(message_item, room_id)
            if await manager.send_message_to_mobile(client_id, payload):
                success_count += 1
                await asyncio.sleep(delay_ms / 1000.0)
        except Exception as e:
            logger.error(f"构建或发送公告失败: {e}")
    return {"success": True, "message": f"公告已成功发送到 {success_count} 个目标群聊。"}


@app.post("/api/batch_leave_groups")
async def api_batch_leave_groups(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    data = await request.json()
    user_id = data.get("user_id")
    room_ids = data.get("room_ids", [])
    delay_ms = data.get("delay_ms")

    if not user_id:
        raise HTTPException(status_code=400, detail="missing user_id")

    account_str = await redis.hget("user_hao", user_id)
    if not account_str:
        return {"success": False, "message": "账号不存在"}
    account = json.loads(account_str)

    if not user_info["is_admin"] and account.get("admin_user") != user_info["username"]:
        raise HTTPException(status_code=403)

    client_id = account.get("client_id")
    if not client_id or client_id not in manager.mobile_clients:
        return {"success": False, "message": "账号当前不在线"}

    if not room_ids:
        derived_ids = []
        for rule in account.get("forwarding_rules", []):
            for dest in rule.get("destinations", []):
                if dest_id := dest.get("id"):
                    derived_ids.append(str(dest_id))
        room_ids = derived_ids

    def _normalize_room_id(value: Any) -> str:
        rid = str(value).strip()
        if rid.startswith("R:"):
            rid = rid[2:]
        return rid

    unique_room_ids = list(dict.fromkeys([_normalize_room_id(rid) for rid in room_ids if str(rid).strip()]))
    if not unique_room_ids:
        return {"success": False, "message": "没有可退群的群聊"}

    try:
        delay_ms = int(delay_ms) if delay_ms is not None else int(account.get("forwarding_delay", 500))
    except (TypeError, ValueError):
        delay_ms = int(account.get("forwarding_delay", 500))
    delay_ms = max(0, delay_ms)

    sent_count = 0
    for idx, room_id in enumerate(unique_room_ids):
        payload = {
            "data": {"room_id": room_id},
            "requestId": str(uuid.uuid4()),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": 4001,
        }
        logger.info(f"[leave_group] user_id={user_id} client_id={client_id} payload={payload}")
        if await manager.send_message_to_mobile(client_id, payload):
            sent_count += 1
        if idx < len(unique_room_ids) - 1 and delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

    return {
        "success": True,
        "sent_count": sent_count,
        "total_count": len(unique_room_ids),
        "delay_ms": delay_ms,
        "message": f"已发送 {sent_count}/{len(unique_room_ids)} 个退群指令",
    }


@app.post("/api/send")
async def api_send_debug(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    # ... (此函数无改动) ...
    if not user_info or not user_info["is_admin"]:
        raise HTTPException(status_code=403)
    data = await request.json()
    user_id = data.get("user_id")
    payload = data.get("payload")
    if not user_id or not payload:
        raise HTTPException(status_code=400, detail="缺少 user_id 或 payload")
    account_str = await redis.hget("user_hao", user_id)
    if not account_str:
        return {"success": False, "message": "账号不存在"}
    client_id = json.loads(account_str).get("client_id")
    if not client_id or client_id not in manager.mobile_clients:
        return {"success": False, "message": "该账号不在线"}
    success = await manager.send_message_to_mobile(client_id, payload)
    if success:
        return {"success": True, "message": f"原始消息已发送至 {client_id}"}
    return {"success": False, "message": "客户端不在线或发送失败"}


@app.get("/api/logout")
async def api_logout(response: Response, session_id: Optional[str] = Depends(cookie_scheme)):
    await asyncio.sleep(TIME_1)
    if session_id:
        await redis.delete(f"session:{session_id}")
    response.delete_cookie(SESSION_COOKIE_NAME)
    return RedirectResponse(url="/")
    
@app.post("/api/batch_assign")
async def api_batch_assign(request: Request, user_info: dict = Depends(get_current_user)):
    await asyncio.sleep(TIME_1)
    """批量分配账号"""
    if not user_info["is_admin"]: raise HTTPException(status_code=403)
    data = await request.json()
    account_ids = data.get("account_ids", [])
    target_user = data.get("target_user")

    if not account_ids or not target_user:
        raise HTTPException(status_code=400, detail="缺少账号ID或目标用户")

    updated_count = 0
    for account_id in account_ids:
        account_str = await redis.hget("user_hao", account_id)
        if account_str:
            account = json.loads(account_str)
            account["admin_user"] = target_user
            await redis.hset("user_hao", account_id, json.dumps(account, ensure_ascii=False))
            updated_count += 1

    if updated_count > 0:
        await manager.broadcast_event_to_panels('full_update', await get_all_admin_data())

    return {"success": True, "message": f"成功分配 {updated_count} 个账号给 {target_user}"}

# --- 主程序入口 ---
if __name__ == "__main__":
    host = "0.0.0.0"; port = 11012
    logger.success(f"服务器即将启动，请访问 http://{host}:{port}/")
    uvicorn.run(app, host=host, port=port, reload=False)
