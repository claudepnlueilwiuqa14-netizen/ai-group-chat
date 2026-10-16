# 多 AI 群聊（ai-group-chat）

王承泽的「好几个 AI 在一个群里」：每个 AI 是一个成员，拥有独立人设与模型，
全部通过本地/服务器上的 key 池中转站（codex-free-gateway）出 token。
人类在群里发一句话，所有 AI 成员依次接话，共享同一段群聊历史，形成连贯多轮讨论。

## 架构

```
浏览器 ──wss://域名/ai/ws──> nginx ──> ai-group(8300) ──http──> key池(8789) ──> 985la 中转 ──> LLM
```

- 前端 WebSocket 连 `/ai/ws`（支持子路径部署，脚本按 pathname 自动推导前缀）
- 后端 FastAPI：GET `/` 返回群聊页，WS `/ws` 处理实时消息
- 成员逐个顺序接话（非并发），对上游额度更友好
- 上游瞬时空额自动重试 3 次（退避 2.5s），保证可用性

## 运行

```bash
pip install -r requirements.txt
# 可选环境变量：LLM_BASE（默认 http://127.0.0.1:8789/v1）、LLM_KEY、PORT（默认 8300）、AI_EXTRA_ROUNDS
python app.py
```

改 `members.json` 增删成员：每个成员含 `id / name / color / model / persona`，
`temperature` 可选（部分模型拒绝非默认温度，不填则用模型默认）。

## nginx 暴露（域名部署示例）

```nginx
location = /ai { return 301 /ai/; }
location /ai/ {
    proxy_pass http://127.0.0.1:8300/;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 300s;
    proxy_buffering off;
}
```

## systemd（服务器）

`/etc/systemd/system/ai-group.service`：`Environment=LLM_BASE=http://127.0.0.1:8789/v1`，
`PORT=8300`，`ExecStart=/usr/bin/python3 /home/ubuntu/ai-group/app.py`，`After=freellm.service`。
