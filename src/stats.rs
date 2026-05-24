use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const RECENT_LIMIT: usize = 200;
const SHANGHAI_OFFSET_SECS: i64 = 8 * 60 * 60;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
pub struct TokenUsage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
}

impl TokenUsage {
    pub fn from_response_bytes(bytes: &[u8]) -> Self {
        let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
            return Self::default();
        };
        let usage = value.get("usage");
        let prompt = first_u64(
            usage,
            &[
                &["prompt_tokens"],
                &["input_tokens"],
                &["prompt_cache_hit_tokens"],
            ],
        );
        let miss = first_u64(usage, &[&["prompt_cache_miss_tokens"]]);
        let cached = first_u64(
            usage,
            &[
                &["prompt_tokens_details", "cached_tokens"],
                &["input_tokens_details", "cached_tokens"],
                &["cached_tokens"],
                &["cache_read_input_tokens"],
                &["prompt_cache_hit_tokens"],
            ],
        );
        let completion = first_u64(usage, &[&["completion_tokens"], &["output_tokens"]]);
        let prompt_tokens = if prompt == 0 && (cached > 0 || miss > 0) {
            cached.saturating_add(miss)
        } else {
            prompt
        };
        let total = first_u64(usage, &[&["total_tokens"]]);
        Self {
            prompt_tokens,
            cached_tokens: cached.min(prompt_tokens),
            completion_tokens: completion,
            total_tokens: if total == 0 {
                prompt_tokens.saturating_add(completion)
            } else {
                total
            },
        }
    }

    pub fn uncached_prompt_tokens(&self) -> u64 {
        self.prompt_tokens.saturating_sub(self.cached_tokens)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestLog {
    pub ts: i64,
    pub day: String,
    pub month: String,
    pub time: String,
    #[serde(default = "default_request_id")]
    pub request_id: String,
    #[serde(default = "default_source")]
    pub source: String,
    pub channel: String,
    pub channel_id: Option<u64>,
    pub requested_model: String,
    pub model: String,
    pub route: String,
    pub route_reason: String,
    pub status: u16,
    pub latency_ms: u64,
    pub latency: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_chunk_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_text_ms: Option<u64>,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub uncached_prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cost_cny: f64,
}

impl RequestLog {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: String,
        source: String,
        channel_id: Option<u64>,
        channel: String,
        requested_model: String,
        actual_model: String,
        route: String,
        route_reason: String,
        status: u16,
        latency_ms: u64,
        usage: TokenUsage,
        first_chunk_ms: Option<u64>,
        first_text_ms: Option<u64>,
    ) -> Self {
        let ts = now_unix_secs();
        let (day, time) = shanghai_strings(ts);
        let month = day.get(0..7).unwrap_or("").to_string();
        let cost_cny = estimate_cost_cny(&actual_model, usage);
        Self {
            ts,
            day,
            month,
            time,
            request_id: normalize_key(request_id, "unknown-request"),
            source: normalize_key(source, "unknown-source"),
            channel,
            channel_id,
            requested_model: normalize_key(requested_model, "unknown"),
            model: normalize_key(actual_model, "unknown"),
            route: normalize_key(route, "default"),
            route_reason,
            status,
            latency_ms,
            latency: format!("{}ms", latency_ms),
            first_chunk_ms,
            first_text_ms,
            prompt_tokens: usage.prompt_tokens,
            cached_tokens: usage.cached_tokens,
            uncached_prompt_tokens: usage.uncached_prompt_tokens(),
            completion_tokens: usage.completion_tokens,
            total_tokens: usage.total_tokens,
            cost_cny,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UsageBucket {
    #[serde(default)]
    pub requests: u64,
    #[serde(default)]
    pub success: u64,
    #[serde(default)]
    pub errors: u64,
    #[serde(default)]
    pub latency_ms: u64,
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
    #[serde(default)]
    pub uncached_prompt_tokens: u64,
    #[serde(default)]
    pub completion_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cost_cny: f64,
}

impl UsageBucket {
    fn add(&mut self, log: &RequestLog) {
        self.requests = self.requests.saturating_add(1);
        if (200..400).contains(&log.status) {
            self.success = self.success.saturating_add(1);
        } else {
            self.errors = self.errors.saturating_add(1);
        }
        self.latency_ms = self.latency_ms.saturating_add(log.latency_ms);
        self.prompt_tokens = self.prompt_tokens.saturating_add(log.prompt_tokens);
        self.cached_tokens = self.cached_tokens.saturating_add(log.cached_tokens);
        self.uncached_prompt_tokens = self
            .uncached_prompt_tokens
            .saturating_add(log.uncached_prompt_tokens);
        self.completion_tokens = self.completion_tokens.saturating_add(log.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(log.total_tokens);
        self.cost_cny += log.cost_cny;
    }

    fn add_bucket(&mut self, bucket: &UsageBucket) {
        self.requests = self.requests.saturating_add(bucket.requests);
        self.success = self.success.saturating_add(bucket.success);
        self.errors = self.errors.saturating_add(bucket.errors);
        self.latency_ms = self.latency_ms.saturating_add(bucket.latency_ms);
        self.prompt_tokens = self.prompt_tokens.saturating_add(bucket.prompt_tokens);
        self.cached_tokens = self.cached_tokens.saturating_add(bucket.cached_tokens);
        self.uncached_prompt_tokens = self
            .uncached_prompt_tokens
            .saturating_add(bucket.uncached_prompt_tokens);
        self.completion_tokens = self
            .completion_tokens
            .saturating_add(bucket.completion_tokens);
        self.total_tokens = self.total_tokens.saturating_add(bucket.total_tokens);
        self.cost_cny += bucket.cost_cny;
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UsageBreakdown {
    #[serde(default)]
    pub total: UsageBucket,
    #[serde(default)]
    pub by_day: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_month: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_channel: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_source: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_source_day: BTreeMap<String, BTreeMap<String, UsageBucket>>,
    #[serde(default)]
    pub by_source_month: BTreeMap<String, BTreeMap<String, UsageBucket>>,
    #[serde(default)]
    pub by_model: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_route: BTreeMap<String, UsageBucket>,
}

impl UsageBreakdown {
    fn add(&mut self, log: &RequestLog) {
        self.total.add(log);
        self.by_day.entry(log.day.clone()).or_default().add(log);
        self.by_month.entry(log.month.clone()).or_default().add(log);
        self.by_channel
            .entry(normalize_key(log.channel.clone(), "unknown-channel"))
            .or_default()
            .add(log);
        let source_key = normalize_key(log.source.clone(), "unknown-source");
        self.by_source
            .entry(source_key.clone())
            .or_default()
            .add(log);
        self.by_source_day
            .entry(source_key.clone())
            .or_default()
            .entry(log.day.clone())
            .or_default()
            .add(log);
        self.by_source_month
            .entry(source_key)
            .or_default()
            .entry(log.month.clone())
            .or_default()
            .add(log);
        self.by_model
            .entry(normalize_key(log.model.clone(), "unknown-model"))
            .or_default()
            .add(log);
        self.by_route
            .entry(normalize_key(log.route.clone(), "unknown-route"))
            .or_default()
            .add(log);
    }

    fn add_model_bucket(&mut self, model: &str, bucket: &UsageBucket, month: Option<&str>) {
        self.total.add_bucket(bucket);
        self.by_model.insert(model.to_string(), bucket.clone());
        if let Some(month) = month {
            self.by_month
                .entry(month.to_string())
                .or_default()
                .add_bucket(bucket);
        }
    }

    fn backfill_single_source(&mut self, source: &str, month: Option<&str>) {
        if !bucket_has_usage(&self.total) {
            return;
        }
        self.by_source
            .insert(source.to_string(), self.total.clone());
        if let Some(month) = month {
            if let Some(bucket) = self.by_month.get(month).cloned() {
                self.by_source_month
                    .entry(source.to_string())
                    .or_default()
                    .insert(month.to_string(), bucket);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct UsageStats {
    #[serde(default)]
    pub total: UsageBucket,
    #[serde(default)]
    pub paid: UsageBreakdown,
    #[serde(default)]
    pub free: UsageBreakdown,
    #[serde(default)]
    pub by_day: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_month: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_channel: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_source: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_source_day: BTreeMap<String, BTreeMap<String, UsageBucket>>,
    #[serde(default)]
    pub by_source_month: BTreeMap<String, BTreeMap<String, UsageBucket>>,
    #[serde(default)]
    pub by_model: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub by_route: BTreeMap<String, UsageBucket>,
    #[serde(default)]
    pub recent: Vec<RequestLog>,
}

impl UsageStats {
    pub fn backfill_default_source(&mut self, default_source: &str) {
        for log in &mut self.recent {
            if log.source.trim().is_empty() || log.source == "unknown-source" {
                log.source = default_source.to_string();
            }
        }

        let source_requests: u64 = self.by_source.values().map(|bucket| bucket.requests).sum();
        if source_requests >= self.total.requests && !self.by_source.contains_key("unknown-source")
        {
            return;
        }

        self.by_source = backfill_source_map(&self.by_source, &self.total, default_source);
        self.by_source_day =
            backfill_source_nested_map(&self.by_source_day, &self.by_day, default_source);
        self.by_source_month =
            backfill_source_nested_map(&self.by_source_month, &self.by_month, default_source);
    }

    pub fn record(&mut self, log: RequestLog) {
        self.total.add(&log);
        self.by_day.entry(log.day.clone()).or_default().add(&log);
        self.by_month
            .entry(log.month.clone())
            .or_default()
            .add(&log);
        self.by_channel
            .entry(normalize_key(log.channel.clone(), "unknown-channel"))
            .or_default()
            .add(&log);
        let source_key = normalize_key(log.source.clone(), "unknown-source");
        self.by_source
            .entry(source_key.clone())
            .or_default()
            .add(&log);
        self.by_source_day
            .entry(source_key.clone())
            .or_default()
            .entry(log.day.clone())
            .or_default()
            .add(&log);
        self.by_source_month
            .entry(source_key)
            .or_default()
            .entry(log.month.clone())
            .or_default()
            .add(&log);
        self.by_model
            .entry(normalize_key(log.model.clone(), "unknown-model"))
            .or_default()
            .add(&log);
        self.by_route
            .entry(normalize_key(log.route.clone(), "unknown-route"))
            .or_default()
            .add(&log);

        if is_paid_usage(&log.model, log.cost_cny) {
            self.paid.add(&log);
        } else {
            self.free.add(&log);
        }

        self.recent.push(log);
        if self.recent.len() > RECENT_LIMIT {
            let remove = self.recent.len() - RECENT_LIMIT;
            self.recent.drain(0..remove);
        }
    }

    pub fn rebuild_billing_if_empty(&mut self) {
        let billing_requests = self
            .paid
            .total
            .requests
            .saturating_add(self.free.total.requests);
        let model_requests: u64 = self.by_model.values().map(|bucket| bucket.requests).sum();
        let expected_requests = self.total.requests.max(model_requests);
        let source_needs_backfill = !self.by_source.is_empty()
            && (bucket_has_usage(&self.paid.total) || bucket_has_usage(&self.free.total))
            && (self.paid.by_source.is_empty() || self.free.by_source.is_empty());
        if billing_requests >= expected_requests && !source_needs_backfill {
            return;
        }

        self.paid = UsageBreakdown::default();
        self.free = UsageBreakdown::default();

        let month_hint = if self.by_month.len() == 1 {
            self.by_month.keys().next().cloned()
        } else {
            None
        };
        for (model, bucket) in &self.by_model {
            if is_paid_usage(model, bucket.cost_cny) {
                self.paid
                    .add_model_bucket(model, bucket, month_hint.as_deref());
            } else {
                self.free
                    .add_model_bucket(model, bucket, month_hint.as_deref());
            }
        }
        let legacy_source = if self.by_source.len() == 1 {
            self.by_source.keys().next().cloned()
        } else if self.by_source.len() > 1 {
            Some("历史来源未拆分".to_string())
        } else {
            None
        };
        if let Some(source) = legacy_source {
            self.paid
                .backfill_single_source(&source, month_hint.as_deref());
            self.free
                .backfill_single_source(&source, month_hint.as_deref());
        }
    }

    pub fn current_month_cost(&self) -> f64 {
        let (day, _) = shanghai_strings(now_unix_secs());
        let month = day.get(0..7).unwrap_or("");
        self.by_month
            .get(month)
            .map(|bucket| bucket.cost_cny)
            .unwrap_or(0.0)
    }
}

fn is_paid_usage(model: &str, cost_cny: f64) -> bool {
    cost_cny > 0.000_000_1 || price_for_model(model).is_paid()
}

pub fn estimate_cost_cny(model: &str, usage: TokenUsage) -> f64 {
    let price = price_for_model(model);
    usage.cached_tokens as f64 / 1_000_000.0 * price.cached_input
        + usage.uncached_prompt_tokens() as f64 / 1_000_000.0 * price.input
        + usage.completion_tokens as f64 / 1_000_000.0 * price.output
}

#[derive(Debug, Serialize, Clone, Copy)]
pub struct ModelPrice {
    pub cached_input: f64,
    pub input: f64,
    pub output: f64,
}

impl ModelPrice {
    fn is_paid(&self) -> bool {
        self.cached_input > 0.0 || self.input > 0.0 || self.output > 0.0
    }
}

pub fn pricing_snapshot() -> BTreeMap<String, ModelPrice> {
    let mut data = BTreeMap::new();
    data.insert(
        "deepseek-v4-flash".to_string(),
        ModelPrice {
            cached_input: 0.02,
            input: 1.0,
            output: 2.0,
        },
    );
    data.insert(
        "deepseek-v4-pro-discount".to_string(),
        ModelPrice {
            cached_input: 0.025,
            input: 3.0,
            output: 6.0,
        },
    );
    data.insert(
        "deepseek-v4-pro-full".to_string(),
        ModelPrice {
            cached_input: 0.1,
            input: 12.0,
            output: 24.0,
        },
    );
    data
}

fn price_for_model(model: &str) -> ModelPrice {
    let key = model.to_lowercase();
    if key.contains("deepseek") && key.contains("pro") {
        return ModelPrice {
            cached_input: 0.025,
            input: 3.0,
            output: 6.0,
        };
    }
    if key.contains("deepseek") || key.contains("v4-flash") {
        return ModelPrice {
            cached_input: 0.02,
            input: 1.0,
            output: 2.0,
        };
    }
    ModelPrice {
        cached_input: 0.0,
        input: 0.0,
        output: 0.0,
    }
}

pub fn load_stats<P: AsRef<Path>>(path: P) -> UsageStats {
    if !path.as_ref().exists() {
        return UsageStats::default();
    }
    let data = fs::read_to_string(path).unwrap_or_default();
    let mut stats: UsageStats = serde_json::from_str(&data).unwrap_or_default();
    stats.backfill_default_source("default-nanobot");
    stats.rebuild_billing_if_empty();
    stats
}

pub fn save_stats<P: AsRef<Path>>(path: P, stats: &UsageStats) {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let data = serde_json::to_string_pretty(stats).unwrap_or_default();
    let tmp = path.with_extension("json.tmp");
    if fs::write(&tmp, &data).is_ok() && fs::rename(&tmp, path).is_ok() {
        return;
    }
    let _ = fs::write(path, data);
}

fn backfill_source_map(
    existing: &BTreeMap<String, UsageBucket>,
    total: &UsageBucket,
    default_source: &str,
) -> BTreeMap<String, UsageBucket> {
    let mut out = BTreeMap::new();
    let mut preserved = Vec::new();
    for (source, bucket) in existing {
        if source == default_source || source == "unknown-source" {
            continue;
        }
        if bucket_has_usage(bucket) {
            out.insert(source.clone(), bucket.clone());
            preserved.push(bucket.clone());
        }
    }
    let default_bucket = subtract_buckets(total, &preserved);
    if bucket_has_usage(&default_bucket) {
        out.insert(default_source.to_string(), default_bucket);
    }
    out
}

fn backfill_source_nested_map(
    existing: &BTreeMap<String, BTreeMap<String, UsageBucket>>,
    totals: &BTreeMap<String, UsageBucket>,
    default_source: &str,
) -> BTreeMap<String, BTreeMap<String, UsageBucket>> {
    let mut out: BTreeMap<String, BTreeMap<String, UsageBucket>> = BTreeMap::new();
    for (period, total) in totals {
        let mut preserved = Vec::new();
        for (source, periods) in existing {
            if source == default_source || source == "unknown-source" {
                continue;
            }
            let Some(bucket) = periods.get(period) else {
                continue;
            };
            if bucket_has_usage(bucket) {
                out.entry(source.clone())
                    .or_default()
                    .insert(period.clone(), bucket.clone());
                preserved.push(bucket.clone());
            }
        }
        let default_bucket = subtract_buckets(total, &preserved);
        if bucket_has_usage(&default_bucket) {
            out.entry(default_source.to_string())
                .or_default()
                .insert(period.clone(), default_bucket);
        }
    }
    out
}

fn subtract_buckets(total: &UsageBucket, subtracts: &[UsageBucket]) -> UsageBucket {
    let mut out = total.clone();
    for item in subtracts {
        out.requests = out.requests.saturating_sub(item.requests);
        out.success = out.success.saturating_sub(item.success);
        out.errors = out.errors.saturating_sub(item.errors);
        out.latency_ms = out.latency_ms.saturating_sub(item.latency_ms);
        out.prompt_tokens = out.prompt_tokens.saturating_sub(item.prompt_tokens);
        out.cached_tokens = out.cached_tokens.saturating_sub(item.cached_tokens);
        out.uncached_prompt_tokens = out
            .uncached_prompt_tokens
            .saturating_sub(item.uncached_prompt_tokens);
        out.completion_tokens = out.completion_tokens.saturating_sub(item.completion_tokens);
        out.total_tokens = out.total_tokens.saturating_sub(item.total_tokens);
        out.cost_cny = (out.cost_cny - item.cost_cny).max(0.0);
    }
    out
}

fn bucket_has_usage(bucket: &UsageBucket) -> bool {
    bucket.requests > 0
        || bucket.total_tokens > 0
        || bucket.prompt_tokens > 0
        || bucket.completion_tokens > 0
        || bucket.cost_cny > 0.0
}

fn first_u64(root: Option<&Value>, paths: &[&[&str]]) -> u64 {
    for path in paths {
        let mut current = root;
        for key in *path {
            current = current.and_then(|v| v.get(*key));
        }
        if let Some(value) = current.and_then(Value::as_u64) {
            return value;
        }
    }
    0
}

fn default_request_id() -> String {
    "unknown-request".to_string()
}

fn default_source() -> String {
    "unknown-source".to_string()
}

fn normalize_key(value: String, fallback: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        fallback.to_string()
    } else {
        trimmed.to_string()
    }
}

fn now_unix_secs() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => 0,
    }
}

fn shanghai_strings(ts: i64) -> (String, String) {
    let adjusted = ts + SHANGHAI_OFFSET_SECS;
    let days = adjusted.div_euclid(86_400);
    let secs = adjusted.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = secs / 3_600;
    let minute = (secs % 3_600) / 60;
    let second = secs % 60;
    let day_key = format!("{:04}-{:02}-{:02}", year, month, day);
    let time = format!("{} {:02}:{:02}:{:02}", day_key, hour, minute, second);
    (day_key, time)
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(prompt: u64, cached: u64, completion: u64) -> TokenUsage {
        TokenUsage {
            prompt_tokens: prompt,
            cached_tokens: cached,
            completion_tokens: completion,
            total_tokens: prompt + completion,
        }
    }

    #[test]
    fn record_splits_paid_and_free_usage() {
        let mut stats = UsageStats::default();
        stats.record(RequestLog::new(
            "test-request-1".to_string(),
            "default-nanobot".to_string(),
            None,
            "DeepSeek".to_string(),
            "deepseek-v4-flash".to_string(),
            "deepseek-v4-flash".to_string(),
            "default".to_string(),
            "test".to_string(),
            200,
            100,
            usage(1_000, 100, 200),
            None,
            None,
        ));
        stats.record(RequestLog::new(
            "test-request-2".to_string(),
            "default-nanobot".to_string(),
            None,
            "LongCat".to_string(),
            "deepseek-v4-flash".to_string(),
            "LongCat-Flash-Chat".to_string(),
            "emergency".to_string(),
            "test".to_string(),
            200,
            100,
            usage(2_000, 0, 100),
            None,
            None,
        ));

        assert_eq!(stats.total.requests, 2);
        assert_eq!(stats.paid.total.requests, 1);
        assert_eq!(stats.free.total.requests, 1);
        assert_eq!(stats.paid.total.total_tokens, 1_200);
        assert_eq!(stats.free.total.total_tokens, 2_100);
        assert!(stats.paid.total.cost_cny > 0.0);
        assert_eq!(stats.free.total.cost_cny, 0.0);
    }

    #[test]
    fn rebuild_billing_from_model_totals_for_legacy_stats() {
        let mut stats = UsageStats::default();
        let mut paid = UsageBucket::default();
        paid.requests = 3;
        paid.success = 3;
        paid.total_tokens = 300;
        paid.cost_cny = 0.12;
        let mut free = UsageBucket::default();
        free.requests = 5;
        free.success = 5;
        free.total_tokens = 500;

        stats
            .by_model
            .insert("deepseek-v4-flash".to_string(), paid.clone());
        stats
            .by_model
            .insert("LongCat-Flash-Chat".to_string(), free.clone());
        let mut month = UsageBucket::default();
        month.add_bucket(&paid);
        month.add_bucket(&free);
        stats.by_month.insert("2026-05".to_string(), month);

        stats.rebuild_billing_if_empty();

        assert_eq!(stats.paid.total.requests, 3);
        assert_eq!(stats.free.total.requests, 5);
        assert_eq!(
            stats.paid.by_month.get("2026-05").map(|b| b.total_tokens),
            Some(300)
        );
        assert_eq!(
            stats.free.by_month.get("2026-05").map(|b| b.total_tokens),
            Some(500)
        );
    }

    #[test]
    fn rebuild_billing_replaces_incomplete_partial_billing() {
        let mut stats = UsageStats::default();
        stats.total.requests = 6;
        let mut paid = UsageBucket::default();
        paid.requests = 2;
        paid.success = 2;
        paid.total_tokens = 200;
        paid.cost_cny = 0.2;
        let mut free = UsageBucket::default();
        free.requests = 4;
        free.success = 4;
        free.total_tokens = 400;
        stats
            .by_model
            .insert("deepseek-v4-flash".to_string(), paid.clone());
        stats
            .by_model
            .insert("LongCat-Flash-Chat".to_string(), free);
        stats.paid.total.requests = 1;

        stats.rebuild_billing_if_empty();

        assert_eq!(stats.paid.total.requests, 2);
        assert_eq!(stats.free.total.requests, 4);
    }

    #[test]
    fn rebuild_billing_uses_synthetic_source_for_multi_source_legacy_stats() {
        let mut stats = UsageStats::default();
        let mut paid = UsageBucket::default();
        paid.requests = 2;
        paid.success = 2;
        paid.total_tokens = 200;
        paid.cost_cny = 0.2;
        let mut free = UsageBucket::default();
        free.requests = 4;
        free.success = 4;
        free.total_tokens = 400;
        stats
            .by_model
            .insert("deepseek-v4-flash".to_string(), paid.clone());
        stats
            .by_model
            .insert("LongCat-Flash-Chat".to_string(), free.clone());
        let mut total = UsageBucket::default();
        total.add_bucket(&paid);
        total.add_bucket(&free);
        stats.by_month.insert("2026-05".to_string(), total.clone());
        stats
            .by_source
            .insert("default-nanobot".to_string(), total.clone());
        stats.by_source.insert("codex-test".to_string(), total);

        stats.rebuild_billing_if_empty();

        assert_eq!(
            stats
                .paid
                .by_source_month
                .get("历史来源未拆分")
                .and_then(|periods| periods.get("2026-05"))
                .map(|bucket| bucket.total_tokens),
            Some(200)
        );
    }

    #[test]
    fn rebuild_billing_keeps_single_source_visible_for_legacy_stats() {
        let mut stats = UsageStats::default();
        let mut paid = UsageBucket::default();
        paid.requests = 2;
        paid.success = 2;
        paid.total_tokens = 200;
        paid.cost_cny = 0.2;
        let mut free = UsageBucket::default();
        free.requests = 4;
        free.success = 4;
        free.total_tokens = 400;
        stats
            .by_model
            .insert("deepseek-v4-flash".to_string(), paid.clone());
        stats
            .by_model
            .insert("LongCat-Flash-Chat".to_string(), free.clone());
        let mut total = UsageBucket::default();
        total.add_bucket(&paid);
        total.add_bucket(&free);
        stats.by_month.insert("2026-05".to_string(), total.clone());
        stats.by_source.insert("default-nanobot".to_string(), total);

        stats.rebuild_billing_if_empty();

        assert_eq!(
            stats
                .paid
                .by_source_month
                .get("default-nanobot")
                .and_then(|periods| periods.get("2026-05"))
                .map(|bucket| bucket.total_tokens),
            Some(200)
        );
        assert_eq!(
            stats
                .free
                .by_source_month
                .get("default-nanobot")
                .and_then(|periods| periods.get("2026-05"))
                .map(|bucket| bucket.total_tokens),
            Some(400)
        );
    }
}
