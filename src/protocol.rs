use crate::config::Channel;
use axum::http::StatusCode;
use reqwest::RequestBuilder;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApiProtocol {
    OpenAI,
    Anthropic,
}

impl ApiProtocol {
    pub(crate) fn from_channel(ch: &Channel) -> Option<Self> {
        match ch.r#type.trim().to_lowercase().as_str() {
            "" | "openai" | "openai-compatible" => Some(Self::OpenAI),
            "anthropic" | "anthropic-api" => Some(Self::Anthropic),
            _ => None,
        }
    }

    pub(crate) fn channel_match_rank(ch: &Channel, client_protocol: Self) -> u8 {
        match Self::from_channel(ch) {
            Some(upstream) if upstream == client_protocol => 0,
            Some(_) => 1,
            None => 2,
        }
    }

    pub(crate) fn target_url(self, base: &str) -> String {
        match self {
            Self::OpenAI => openai_chat_url(base),
            Self::Anthropic => anthropic_messages_url(base),
        }
    }

    pub(crate) fn apply_channel_auth(
        self,
        req: RequestBuilder,
        channel: &Channel,
    ) -> RequestBuilder {
        match self {
            Self::OpenAI => req.header("Authorization", format!("Bearer {}", channel.key)),
            Self::Anthropic => {
                if channel.base.to_lowercase().contains("anthropic.com") {
                    req.header("x-api-key", &channel.key)
                        .header("anthropic-version", "2023-06-01")
                } else {
                    req.header("Authorization", format!("Bearer {}", channel.key))
                }
            }
        }
    }
}

fn rewrite_model(body: &[u8], model: &str) -> Vec<u8> {
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    if let Some(obj) = value.as_object_mut() {
        obj.insert("model".to_string(), Value::String(model.to_string()));
        return serde_json::to_vec(&value).unwrap_or_else(|_| body.to_vec());
    }
    body.to_vec()
}

pub(crate) fn rewrite_body_for_upstream(
    body: &[u8],
    model: &str,
    client_protocol: ApiProtocol,
    upstream_protocol: ApiProtocol,
) -> Vec<u8> {
    match (client_protocol, upstream_protocol) {
        (ApiProtocol::OpenAI, ApiProtocol::OpenAI) => rewrite_openai_model(body, model),
        (ApiProtocol::Anthropic, ApiProtocol::Anthropic) => rewrite_model(body, model),
        (ApiProtocol::Anthropic, ApiProtocol::OpenAI) => anthropic_request_to_openai(body, model),
        (ApiProtocol::OpenAI, ApiProtocol::Anthropic) => openai_request_to_anthropic(body, model),
    }
}

fn rewrite_openai_model(body: &[u8], model: &str) -> Vec<u8> {
    let Ok(mut value) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    if let Some(obj) = value.as_object_mut() {
        obj.insert("model".to_string(), Value::String(model.to_string()));
        apply_openai_model_defaults(&mut value, model);
        return serde_json::to_vec(&value).unwrap_or_else(|_| body.to_vec());
    }
    body.to_vec()
}

fn anthropic_request_to_openai(body: &[u8], model: &str) -> Vec<u8> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return rewrite_openai_model(body, model);
    };
    let mut messages = Vec::new();
    if let Some(system) = value.get("system") {
        let system_text = content_to_text(system);
        if !system_text.is_empty() {
            messages.push(serde_json::json!({"role": "system", "content": system_text}));
        }
    }
    if let Some(items) = value.get("messages").and_then(Value::as_array) {
        for item in items {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = item.get("content").map(content_to_text).unwrap_or_default();
            messages.push(serde_json::json!({"role": role, "content": content}));
        }
    }

    let mut out = serde_json::json!({
        "model": model,
        "messages": messages,
    });
    copy_json_fields(
        &value,
        &mut out,
        &[
            ("max_tokens", "max_tokens"),
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("stream", "stream"),
            ("stop_sequences", "stop"),
        ],
    );
    apply_openai_model_defaults(&mut out, model);
    json_bytes_or(&out, rewrite_openai_model(body, model))
}

fn apply_openai_model_defaults(value: &mut Value, model: &str) {
    if !model.to_lowercase().contains("deepseek-v4-pro") {
        return;
    }
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    obj.entry("thinking".to_string())
        .or_insert_with(|| serde_json::json!({"type": "enabled"}));
    obj.entry("reasoning_effort".to_string())
        .or_insert_with(|| Value::String("high".to_string()));
}

fn openai_request_to_anthropic(body: &[u8], model: &str) -> Vec<u8> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return rewrite_model(body, model);
    };
    let mut messages = Vec::new();
    let mut system_parts = Vec::new();
    if let Some(items) = value.get("messages").and_then(Value::as_array) {
        for item in items {
            let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
            let content = item.get("content").map(content_to_text).unwrap_or_default();
            if role == "system" {
                if !content.is_empty() {
                    system_parts.push(content);
                }
                continue;
            }
            let anthropic_role = if role == "assistant" {
                "assistant"
            } else {
                "user"
            };
            messages.push(serde_json::json!({"role": anthropic_role, "content": content}));
        }
    }
    let max_tokens = value
        .get("max_tokens")
        .or_else(|| value.get("max_completion_tokens"))
        .cloned()
        .unwrap_or(Value::from(4096));
    let mut out = serde_json::json!({
        "model": model,
        "max_tokens": max_tokens,
        "messages": messages,
    });
    if !system_parts.is_empty() {
        out["system"] = Value::String(system_parts.join("\n\n"));
    }
    copy_json_fields(
        &value,
        &mut out,
        &[
            ("temperature", "temperature"),
            ("top_p", "top_p"),
            ("stream", "stream"),
            ("stop", "stop_sequences"),
        ],
    );
    json_bytes_or(&out, rewrite_model(body, model))
}

fn copy_json_fields(from: &Value, to: &mut Value, fields: &[(&str, &str)]) {
    let Some(obj) = to.as_object_mut() else {
        return;
    };
    for (src, dst) in fields {
        if let Some(value) = from.get(*src) {
            obj.insert((*dst).to_string(), value.clone());
        }
    }
}

fn json_bytes_or(value: &Value, fallback: Vec<u8>) -> Vec<u8> {
    serde_json::to_vec(value).unwrap_or(fallback)
}

fn content_to_text(value: &Value) -> String {
    if let Some(text) = value.as_str() {
        return text.to_string();
    }
    let Some(items) = value.as_array() else {
        return value.to_string();
    };
    items
        .iter()
        .filter_map(|item| {
            if let Some(text) = item.as_str() {
                return Some(text.to_string());
            }
            match item.get("type").and_then(Value::as_str) {
                Some("text") => item
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
                Some("image") | Some("image_url") => Some("[image]".to_string()),
                _ => item
                    .get("text")
                    .and_then(Value::as_str)
                    .map(ToString::to_string),
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn rewrite_response_for_client(
    body: &[u8],
    status: StatusCode,
    client_protocol: ApiProtocol,
    upstream_protocol: ApiProtocol,
) -> Vec<u8> {
    if client_protocol == upstream_protocol || !status.is_success() {
        return body.to_vec();
    }
    match (client_protocol, upstream_protocol) {
        (ApiProtocol::Anthropic, ApiProtocol::OpenAI) => openai_response_to_anthropic(body),
        (ApiProtocol::OpenAI, ApiProtocol::Anthropic) => anthropic_response_to_openai(body),
        _ => body.to_vec(),
    }
}

fn openai_response_to_anthropic(body: &[u8]) -> Vec<u8> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first());
    let message = choice.and_then(|item| item.get("message"));
    let text = message
        .and_then(|msg| msg.get("content"))
        .map(content_to_text)
        .unwrap_or_default();
    let reasoning = message
        .and_then(|msg| msg.get("reasoning_content"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let mut content = Vec::new();
    if !reasoning.is_empty() {
        content.push(serde_json::json!({"type": "thinking", "thinking": reasoning}));
    }
    content.push(serde_json::json!({"type": "text", "text": text}));
    let usage = value.get("usage").cloned().unwrap_or(Value::Null);
    let input_tokens = first_u64_in_value(&usage, &[&["prompt_tokens"], &["input_tokens"]]);
    let output_tokens = first_u64_in_value(&usage, &[&["completion_tokens"], &["output_tokens"]]);
    let cache_read_input_tokens = first_u64_in_value(
        &usage,
        &[
            &["prompt_tokens_details", "cached_tokens"],
            &["input_tokens_details", "cached_tokens"],
            &["cache_read_input_tokens"],
        ],
    );
    let out = serde_json::json!({
        "id": value.get("id").cloned().unwrap_or_else(|| Value::String(format!("msg_{}", now_secs()))),
        "type": "message",
        "role": "assistant",
        "model": value.get("model").cloned().unwrap_or_else(|| Value::String("unknown".to_string())),
        "content": content,
        "stop_reason": mapped_reason(
            choice.and_then(|item| item.get("finish_reason")).and_then(Value::as_str),
            &[("length", "max_tokens"), ("tool_calls", "tool_use")],
            "end_turn",
        ),
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": cache_read_input_tokens,
        }
    });
    json_bytes_or(&out, body.to_vec())
}

fn anthropic_response_to_openai(body: &[u8]) -> Vec<u8> {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return body.to_vec();
    };
    let content = value
        .get("content")
        .map(content_to_text)
        .unwrap_or_default();
    let usage = value.get("usage").cloned().unwrap_or(Value::Null);
    let prompt_tokens = first_u64_in_value(&usage, &[&["input_tokens"], &["prompt_tokens"]]);
    let completion_tokens =
        first_u64_in_value(&usage, &[&["output_tokens"], &["completion_tokens"]]);
    let cached_tokens = first_u64_in_value(
        &usage,
        &[
            &["cache_read_input_tokens"],
            &["cached_tokens"],
            &["prompt_tokens_details", "cached_tokens"],
        ],
    );
    let out = serde_json::json!({
        "id": value.get("id").cloned().unwrap_or_else(|| Value::String(format!("chatcmpl-{}", now_secs()))),
        "object": "chat.completion",
        "created": now_secs(),
        "model": value.get("model").cloned().unwrap_or_else(|| Value::String("unknown".to_string())),
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": mapped_reason(
                value.get("stop_reason").and_then(Value::as_str),
                &[("max_tokens", "length"), ("tool_use", "tool_calls")],
                "stop",
            ),
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens.saturating_add(completion_tokens),
            "prompt_tokens_details": {"cached_tokens": cached_tokens},
        }
    });
    json_bytes_or(&out, body.to_vec())
}

fn first_u64_in_value(value: &Value, paths: &[&[&str]]) -> u64 {
    for path in paths {
        let mut cur = value;
        for key in *path {
            let Some(next) = cur.get(*key) else {
                cur = &Value::Null;
                break;
            };
            cur = next;
        }
        if let Some(n) = cur.as_u64() {
            return n;
        }
    }
    0
}

fn mapped_reason(reason: Option<&str>, mappings: &[(&str, &str)], default: &str) -> Value {
    Value::String(
        mappings
            .iter()
            .find_map(|(from, to)| (reason == Some(*from)).then_some(*to))
            .unwrap_or(default)
            .to_string(),
    )
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn openai_chat_url(base: &str) -> String {
    endpoint_url(base, "chat/completions")
}

fn anthropic_messages_url(base: &str) -> String {
    endpoint_url(base, "messages")
}

fn endpoint_url(base: &str, endpoint: &str) -> String {
    let base = base.trim_end_matches('/');
    if base.ends_with(endpoint) {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{}/{}", base, endpoint)
    } else {
        format!("{}/v1/{}", base, endpoint)
    }
}
