# nanobot-obp

`nanobot-obp` 是给 nanobot 使用的轻量模型网关，全名可以理解为 OpenAI-compatible Balance Proxy。它把 DeepSeek、Gemini Web FastAPI、LongCat、MiniMax 等不同模型渠道收口到一个本地入口里，负责模型路由、超时降级、免费/付费成本统计，以及 OpenAI/Anthropic 两种调用格式的兼容。

## 它解决什么问题

- nanobot 不需要直接关心每个模型渠道的细节。
- 默认 nanobot、广州 nanobot 等不同来源可以走不同路由组。
- 日常任务优先走便宜或免费的模型，复杂任务再升级到高级模型。
- 主模型超时或失败时，可以自动降级到备用/应急模型。
- 能统计每个来源、模型、渠道、路由用了多少 token 和多少钱。
- 可以把 OpenAI 格式和 Anthropic 格式都统一接进来。

## 主要能力

- OpenAI 兼容入口：`/v1/chat/completions`
- Anthropic 兼容入口：`/anthropic/v1/messages` 和 `/v1/messages`
- 模型列表：`/v1/models`
- 管理接口：`/admin/channels`、`/admin/router`、`/admin/stats`
- 路由组：默认、高级、应急、备用
- 来源路由：按 `default-nanobot`、`guangzhou-nanobot` 等来源指定不同路由组
- 成本账本：区分付费、免费、总消耗
- Gemini Web FastAPI 串行保护：避免同一个 cookie 会话并发请求互相打架

## 快速启动

```bash
cp data/config.example.json data/config.json
cp data/router.example.json data/router.json
cargo run --release
```

默认监听 `0.0.0.0:8000`。线上建议只监听本机地址，或者放在反向代理/sidecar manager 后面，不要把管理接口裸露到公网。

## 配置文件

- `data/config.json`：真实渠道配置，里面有 API key，不要提交到 Git。
- `data/router.json`：真实路由配置，不要提交到 Git。
- `data/stats.json`：运行统计账本，不要提交到 Git。
- `data/*.example.json`：脱敏示例配置，可以提交。

## systemd 部署

```bash
cargo build --release
install -m 0755 target/release/obp-rs /usr/local/bin/obp-rs
cp deploy/systemd/obp-rs.service.example /etc/systemd/system/obp-rs.service
systemctl daemon-reload
systemctl enable --now obp-rs.service
```

如果部署目录不是 `/opt/nanobot-obp`，记得同步修改 systemd 文件里的 `WorkingDirectory` 和 `OBP_*_PATH`。

## 给客户端调用

OpenAI 兼容格式：

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek-v4-flash",
    "messages": [
      {"role": "user", "content": "你好"}
    ]
  }'
```

Anthropic 兼容格式：

```bash
curl http://127.0.0.1:8000/anthropic/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-3.5-flash",
    "max_tokens": 1024,
    "messages": [
      {"role": "user", "content": "你好"}
    ]
  }'
```

## 安全约定

下面这些文件和目录不能提交：

- `data/config.json`
- `data/router.json`
- `data/stats.json`
- `.env*`
- `backups/`
- `target/`

如果要新增真实渠道，请在网页控制台或本地 `data/config.json` 里配置真实 key；仓库里只保留脱敏示例。

## 开发检查

```bash
cargo fmt --check
cargo test --locked
```

GitHub Actions 也会自动跑这两项。
