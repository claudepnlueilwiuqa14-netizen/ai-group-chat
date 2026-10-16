#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ai-group —— 多 AI 群聊服务
王承泽的「好几个 AI 在一个群聊里」：每个 AI 是一个成员，拥有独立人设与模型，
全部通过本机/服务器上的 key 池中转站（codex-free-gateway）出 token。
人类（王承泽）在群里发一句话，所有 AI 成员依次接话。

设计：
- 后端 FastAPI：GET / 返回群聊界面；WS /ws 处理实时消息。
- 每个 AI 成员独立 system prompt + 模型，调用 key 池 /v1/chat/completions（OpenAI 兼容，SSE 流式）。
- 群聊历史在所有成员间共享（带角色名），形成连贯的多轮讨论。
- 可选「自由讨论」模式：AI 们在人类发言后互相多聊几轮（默认关，省 token）。
"""
import asyncio
import json
import os
import time
import uuid
from pathlib import Path

import aiohttp
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse

BASE = Path(__file__).resolve().parent
MEMBERS = json.loads((BASE / "members.json").read_text(encoding="utf-8"))
# key 池中转站：本地 127.0.0.1:8789；服务器上同样部署一份，地址不变
LLM_BASE = os.getenv("LLM_BASE", "http://127.0.0.1:8789/v1").rstrip("/")
LLM_KEY = os.getenv("LLM_KEY", "pojia")
PORT = int(os.getenv("PORT", "8300"))

# 群聊共享历史：[{role:'system'|'user'|'assistant', name, content}]
HISTORY: list = []
HISTORY_LOCK = asyncio.Lock()
# 人类发言后 AI 之间额外互聊的轮数（0=不互聊，只各回一句）
AI_EXTRA_ROUNDS = int(os.getenv("AI_EXTRA_ROUNDS", "0"))

app = FastAPI()


def public_members():
    return [{"id": m["id"], "name": m["name"], "color": m["color"],
             "model": m["model"], "persona": m["persona"]} for m in MEMBERS]


@app.get("/")
async def index():
    return FileResponse(BASE / "static" / "index.html")


@app.get("/members")
async def members():
    return JSONResponse(public_members())


async def llm_stream(member: dict, history: list, ws: WebSocket):
    """调用 key 池，流式把该成员的回复通过 ws 广播出去。返回完整文本。"""
    messages = [{"role": "system", "content": member["persona"]}]
    for h in history:
        # 转发时把角色名带进 content，让模型知道谁说的
        if h["role"] == "system":
            continue
        prefix = f"【{h['name']}】" if h.get("name") else ""
        messages.append({"role": h["role"], "content": prefix + h["content"]})
    payload = {
        "model": member["model"],
        "messages": messages,
        "stream": True,
        "max_tokens": 800,
    }
    # temperature 仅当成员显式配置时才发送：部分模型（如 claude-sonnet-5-5）
    # 拒绝非默认温度，强制发送会导致上游 502 / 空回复
    if member.get("temperature") is not None:
        payload["temperature"] = member["temperature"]
    text = []
    last_err = ""
    # 上游额度瞬时空（All available accounts exhausted）属正常抖动，自动重试
    for attempt in range(3):
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{LLM_BASE}/chat/completions",
                json=payload,
                headers={"Authorization": f"Bearer {LLM_KEY}",
                          "Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                if resp.status != 200:
                    last_err = await resp.text()
                    # 末次失败直接报错返回；否则退避后重试
                    if attempt < 2:
                        await asyncio.sleep(2.5)
                        continue
                    await ws.send_json({"type": "error", "member": member["id"],
                                        "text": f"[{member['name']}] 调用失败 {resp.status}: {last_err[:200]}"})
                    return ""
                async for line in resp.content:
                    line = line.decode("utf-8").strip()
                    if not line or not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        break
                    try:
                        chunk = json.loads(data)
                    except Exception:
                        continue
                    # 某些帧 choices 为空（如末段 usage 统计），需防越界
                    choices = chunk.get("choices") or [{}]
                    delta = choices[0].get("delta", {}).get("content", "")
                    if delta:
                        text.append(delta)
                        await ws.send_json({"type": "token", "member": member["id"],
                                        "name": member["name"], "color": member["color"],
                                        "text": delta})
                break  # 流式成功，退出重试循环
    full = "".join(text)
    return full


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    await ws.send_json({"type": "members", "members": public_members()})
    await ws.send_json({"type": "history", "history": HISTORY})
    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            if msg.get("type") != "user":
                continue
            user_text = (msg.get("text") or "").strip()
            if not user_text:
                continue
            async with HISTORY_LOCK:
                HISTORY.append({"role": "user", "name": "王承泽", "content": user_text})
            await ws.send_json({"type": "user_msg", "name": "王承泽", "text": user_text})

            # 每个 AI 成员依次接话
            for member in MEMBERS:
                await ws.send_json({"type": "start", "member": member["id"],
                                    "name": member["name"], "color": member["color"]})
                async with HISTORY_LOCK:
                    snap = list(HISTORY)
                reply = await llm_stream(member, snap, ws)
                if reply:
                    async with HISTORY_LOCK:
                        HISTORY.append({"role": "assistant", "name": member["name"], "content": reply})
                    await ws.send_json({"type": "done", "member": member["id"]})

            # 可选：AI 之间自由讨论几轮
            for r in range(AI_EXTRA_ROUNDS):
                for member in MEMBERS:
                    await ws.send_json({"type": "start", "member": member["id"],
                                        "name": member["name"], "color": member["color"],
                                        "extra": True})
                    async with HISTORY_LOCK:
                        snap = list(HISTORY)
                    reply = await llm_stream(member, snap, ws)
                    if reply:
                        async with HISTORY_LOCK:
                            HISTORY.append({"role": "assistant", "name": member["name"], "content": reply})
                        await ws.send_json({"type": "done", "member": member["id"]})
    except WebSocketDisconnect:
        pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
