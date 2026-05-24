# nanobot-obp

OBP (OpenAI-compatible Balance Proxy) is the lightweight model gateway used by nanobot. It provides one local gateway for model routing, fallback, usage accounting, OpenAI-compatible requests, Anthropic-compatible requests, and the small admin dashboard.

## Features

- OpenAI-compatible endpoint: `/v1/chat/completions`
- Anthropic-compatible endpoints: `/anthropic/v1/messages` and `/v1/messages`
- Model listing endpoint: `/v1/models`
- Admin endpoints: `/admin/channels`, `/admin/router`, `/admin/stats`
- Route profiles for default, pro, emergency, and backup models
- Source-aware routing for multiple nanobot instances
- Paid/free/total usage accounting by source, model, channel, and route
- Serial guard for Gemini Web FastAPI channels to avoid cookie-session races

## Local run

```bash
cp data/config.example.json data/config.json
cp data/router.example.json data/router.json
cargo run --release
```

Default listen address is `0.0.0.0:8000`. In production, keep the service private and expose it through the sidecar manager or a reverse proxy with authentication.

## Runtime files

- `data/config.json`: real channel config with API keys. Do not commit it.
- `data/router.json`: real routing config. Do not commit it.
- `data/stats.json`: runtime usage ledger. Do not commit it.
- `data/*.example.json`: safe examples that can be committed.

## Deploy with systemd

```bash
cargo build --release
install -m 0755 target/release/obp-rs /usr/local/bin/obp-rs
cp deploy/systemd/obp-rs.service.example /etc/systemd/system/obp-rs.service
systemctl daemon-reload
systemctl enable --now obp-rs.service
```

If the deploy directory is not `/opt/nanobot-obp`, update `WorkingDirectory` and the `OBP_*_PATH` environment variables in the systemd unit.

## Security rules

Never commit these files or directories:

- `data/config.json`
- `data/router.json`
- `data/stats.json`
- `.env*`
- `backups/`
- `target/`

Add or edit real channels in the dashboard or in local `data/config.json`. Keep only redacted examples in Git.
