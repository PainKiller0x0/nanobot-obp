mod config;
mod proxy;
mod stats;

use crate::config::{
    load_config, load_router_config, save_config, save_router_config, Channel, RouterConfig,
};
use crate::proxy::{handle_anthropic_proxy, handle_openai_proxy, ProxyState};
use crate::stats::{load_stats, pricing_snapshot, save_stats, UsageStats};
use axum::{
    extract::{Path, State},
    http::header,
    response::{Html, IntoResponse},
    routing::{get, post, put},
    Json, Router,
};
use reqwest::Client;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;
use std::{env, net::SocketAddr};
use tokio::sync::Mutex;

const CONFIG_PATH: &str = "data/config.json";
const ROUTER_PATH: &str = "data/router.json";
const STATS_PATH: &str = "data/stats.json";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    std::fs::create_dir_all("data").ok();

    let config_path = env::var("OBP_CONFIG_PATH").unwrap_or_else(|_| CONFIG_PATH.to_string());
    let router_path = env::var("OBP_ROUTER_PATH").unwrap_or_else(|_| ROUTER_PATH.to_string());
    let stats_path = env::var("OBP_STATS_PATH").unwrap_or_else(|_| STATS_PATH.to_string());
    let channels = load_config(&config_path);
    let router = load_router_config(&router_path);
    let stats = load_stats(&stats_path);
    let state = Arc::new(ProxyState {
        client: Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap(),
        channels: Mutex::new(channels),
        router: Mutex::new(router),
        stats: Mutex::new(stats),
        index: Mutex::new(0),
        config_path,
        router_path,
        stats_path,
        serial_channel_locks: Mutex::new(Default::default()),
    });

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/v1/chat/completions", post(handle_openai_proxy))
        .route("/v1/models", get(list_models))
        .route("/v1/messages", post(handle_anthropic_proxy))
        .route("/anthropic/v1/messages", post(handle_anthropic_proxy))
        .route("/admin/channels", get(get_channels).post(add_channel))
        .route("/admin/channels/test", post(test_channel))
        .route("/admin/stats", get(get_stats).delete(clear_stats))
        .route("/admin/router", get(get_router).put(update_router))
        .route(
            "/admin/channels/{id}",
            put(update_channel).delete(delete_channel),
        )
        .with_state(state);

    let host = env::var("OBP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port: u16 = env::var("OBP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8000);
    let addr: SocketAddr = format!("{}:{}", host, port).parse().unwrap();
    println!("OBP-RS listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn dashboard() -> impl IntoResponse {
    (
        [
            (header::CACHE_CONTROL, "no-store, max-age=0"),
            (header::PRAGMA, "no-cache"),
        ],
        Html(include_str!("index.html")),
    )
}

async fn list_models(State(state): State<Arc<ProxyState>>) -> Json<serde_json::Value> {
    let channels = state.channels.lock().await;
    let router = state.router.lock().await;
    let mut models = BTreeSet::new();
    for model in &router.external_allowed_models {
        let model = model.trim();
        if !model.is_empty() && model != "*" {
            models.insert(model.to_string());
        }
    }
    for ch in channels.iter().filter(|ch| ch.is_active()) {
        for model in ch.models.split(',').map(str::trim) {
            if !model.is_empty() && model != "*" {
                models.insert(model.to_string());
            }
        }
    }
    Json(serde_json::json!({
        "object": "list",
        "data": models.into_iter().map(|id| serde_json::json!({
            "id": id,
            "object": "model",
            "created": 0,
            "owned_by": "obp"
        })).collect::<Vec<_>>()
    }))
}

async fn get_channels(State(state): State<Arc<ProxyState>>) -> Json<serde_json::Value> {
    let channels = state.channels.lock().await;
    let router = state.router.lock().await;
    let stats = state.stats.lock().await;
    Json(serde_json::json!({
        "channels": redacted_channels(&channels),
        "router": &*router,
        "stats": &*stats,
        "pricing": pricing_snapshot(),
        "logs": &stats.recent,
    }))
}

fn redacted_channels(channels: &[Channel]) -> Vec<Channel> {
    channels
        .iter()
        .cloned()
        .map(|mut ch| {
            if !ch.key.trim().is_empty() {
                ch.key = "***".to_string();
            }
            ch
        })
        .collect()
}

async fn get_stats(State(state): State<Arc<ProxyState>>) -> Json<UsageStats> {
    let stats = state.stats.lock().await;
    Json(stats.clone())
}

async fn clear_stats(State(state): State<Arc<ProxyState>>) -> Json<serde_json::Value> {
    let mut stats = state.stats.lock().await;
    *stats = UsageStats::default();
    save_stats(&state.stats_path, &stats);
    Json(serde_json::json!({ "status": "ok" }))
}

async fn get_router(State(state): State<Arc<ProxyState>>) -> Json<RouterConfig> {
    let router = state.router.lock().await;
    Json(router.clone())
}

async fn update_router(
    State(state): State<Arc<ProxyState>>,
    Json(router): Json<RouterConfig>,
) -> Json<RouterConfig> {
    let router = router.normalized();
    let mut current = state.router.lock().await;
    *current = router.clone();
    save_router_config(&state.router_path, &router);
    Json(router)
}

async fn add_channel(
    State(state): State<Arc<ProxyState>>,
    Json(mut ch): Json<Channel>,
) -> Json<Channel> {
    let mut channels = state.channels.lock().await;
    ch.id = Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    );
    channels.push(ch.clone());
    save_config(&state.config_path, &channels);
    Json(ch)
}

async fn test_channel(
    State(state): State<Arc<ProxyState>>,
    Json(mut ch): Json<Channel>,
) -> Json<serde_json::Value> {
    if ch.key.trim().is_empty() || ch.key.trim() == "***" {
        if let Some(id) = ch.id {
            let channels = state.channels.lock().await;
            if let Some(saved) = channels.iter().find(|item| item.id == Some(id)) {
                ch.key = saved.key.clone();
            }
        }
    }

    let result = test_channel_once(&state.client, &ch).await;
    if let Some(id) = ch.id {
        let mut channels = state.channels.lock().await;
        if let Some(saved) = channels.iter_mut().find(|item| item.id == Some(id)) {
            let label = if result
                .get("ok")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                "ok"
            } else {
                "failed"
            };
            saved.last_test = Some(format!("{}: {}", now_label(), label));
            save_config(&state.config_path, &channels);
        }
    }
    Json(result)
}

async fn update_channel(
    State(state): State<Arc<ProxyState>>,
    Path(id): Path<u64>,
    Json(mut updated_ch): Json<Channel>,
) -> Json<serde_json::Value> {
    let mut channels = state.channels.lock().await;
    if let Some(ch) = channels.iter_mut().find(|c| c.id == Some(id)) {
        updated_ch.id = Some(id);
        if updated_ch.key.trim().is_empty() || updated_ch.key.trim() == "***" {
            updated_ch.key = ch.key.clone();
        }
        *ch = updated_ch;
        save_config(&state.config_path, &channels);
        return Json(serde_json::json!({ "status": "ok" }));
    }
    Json(serde_json::json!({ "status": "not_found" }))
}

async fn delete_channel(
    State(state): State<Arc<ProxyState>>,
    Path(id): Path<u64>,
) -> Json<serde_json::Value> {
    let mut channels = state.channels.lock().await;
    channels.retain(|c| c.id != Some(id));
    save_config(&state.config_path, &channels);
    Json(serde_json::json!({ "status": "ok" }))
}

async fn test_channel_once(client: &Client, ch: &Channel) -> serde_json::Value {
    let started = Instant::now();
    if ch.base.trim().is_empty() {
        return test_result(false, 0, "Base URL 不能为空", started);
    }
    if ch.key.trim().is_empty() || ch.key.trim() == "***" {
        return test_result(
            false,
            0,
            "API Key 不能为空；编辑已保存渠道时可以保留 ***",
            started,
        );
    }

    match ch.r#type.trim().to_lowercase().as_str() {
        "anthropic" => test_anthropic_channel(client, ch, started).await,
        "other" => test_other_channel(client, ch, started).await,
        _ => test_openai_channel(client, ch, started).await,
    }
}

async fn test_openai_channel(client: &Client, ch: &Channel, started: Instant) -> serde_json::Value {
    let model = ch.mapped_model("gpt-4o-mini", "gpt-4o-mini");
    let url = openai_chat_url(&ch.base);
    let res = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", ch.key))
        .json(&serde_json::json!({
            "model": model,
            "messages": [{"role":"user","content":"ping"}],
            "max_tokens": 16,
            "stream": false
        }))
        .send()
        .await;
    response_to_test_result(res, started, model, url).await
}

async fn test_anthropic_channel(
    client: &Client,
    ch: &Channel,
    started: Instant,
) -> serde_json::Value {
    let model =
        first_configured_model(&ch.models).unwrap_or_else(|| "claude-3-5-haiku-latest".to_string());
    let url = anthropic_messages_url(&ch.base);
    let mut req = client.post(&url).json(&serde_json::json!({
        "model": model,
        "messages": [{"role":"user","content":"ping"}],
        "max_tokens": 16
    }));
    if ch.base.to_lowercase().contains("anthropic.com") {
        req = req
            .header("x-api-key", &ch.key)
            .header("anthropic-version", "2023-06-01");
    } else {
        req = req.header("Authorization", format!("Bearer {}", ch.key));
    }
    let res = req.send().await;
    response_to_test_result(res, started, model, url).await
}

async fn test_other_channel(client: &Client, ch: &Channel, started: Instant) -> serde_json::Value {
    let url = ch.base.trim_end_matches('/').to_string();
    let res = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", ch.key))
        .header("x-api-key", &ch.key)
        .send()
        .await;
    response_to_test_result(res, started, "connectivity".to_string(), url).await
}

async fn response_to_test_result(
    res: Result<reqwest::Response, reqwest::Error>,
    started: Instant,
    model: String,
    url: String,
) -> serde_json::Value {
    match res {
        Ok(response) => {
            let status = response.status().as_u16();
            let ok = (200..400).contains(&status);
            let body = response.text().await.unwrap_or_default();
            let mut value = test_result(
                ok,
                status,
                &format!(
                    "{}；模型 {}；{}",
                    if ok { "测试通过" } else { "测试失败" },
                    model,
                    trim_for_display(&body)
                ),
                started,
            );
            if let Some(map) = value.as_object_mut() {
                map.insert("model".to_string(), serde_json::Value::String(model));
                map.insert("url".to_string(), serde_json::Value::String(url));
            }
            value
        }
        Err(err) => test_result(false, 0, &format!("请求失败：{}", err), started),
    }
}

fn test_result(ok: bool, status: u16, message: &str, started: Instant) -> serde_json::Value {
    serde_json::json!({
        "ok": ok,
        "status": status,
        "latency_ms": started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        "message": message,
    })
}

fn trim_for_display(text: &str) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    compact.chars().take(180).collect()
}

fn first_configured_model(models: &str) -> Option<String> {
    models
        .split(',')
        .map(str::trim)
        .find(|item| !item.is_empty() && *item != "*")
        .map(ToString::to_string)
}

fn openai_chat_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/chat/completions") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{}/chat/completions", base)
    } else {
        format!("{}/v1/chat/completions", base)
    }
}

fn anthropic_messages_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with("/messages") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{}/messages", base)
    } else {
        format!("{}/v1/messages", base)
    }
}

fn now_label() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|item| item.as_secs())
        .unwrap_or_default();
    secs.to_string()
}
