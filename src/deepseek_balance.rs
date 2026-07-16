use crate::config::Channel;
use crate::stats::{UsageBucket, UsageStats};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const MAX_SNAPSHOTS: usize = 500;
const SHANGHAI_OFFSET_SECS: i64 = 8 * 60 * 60;

#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DeepSeekBalanceQuery {
    pub refresh: bool,
}

impl Default for DeepSeekBalanceQuery {
    fn default() -> Self {
        Self { refresh: false }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DeepSeekBalanceStore {
    #[serde(default)]
    pub snapshots: Vec<DeepSeekBalanceSnapshot>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeepSeekBalanceSnapshot {
    pub ts: i64,
    pub day: String,
    pub time: String,
    pub account_id: String,
    pub channel_id: Option<u64>,
    pub channel: String,
    pub is_available: bool,
    pub currency: String,
    pub total_balance: f64,
    pub granted_balance: f64,
    pub topped_up_balance: f64,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct DeepSeekBalanceReport {
    pub ok: bool,
    pub refreshed: bool,
    pub message: String,
    pub snapshot_count: usize,
    pub accounts: Vec<DeepSeekAccountReport>,
    pub local_usage: DeepSeekLocalUsage,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct DeepSeekAccountReport {
    pub account_id: String,
    pub channel_id: Option<u64>,
    pub channel: String,
    pub is_available: bool,
    pub currency: String,
    pub total_balance: f64,
    pub granted_balance: f64,
    pub topped_up_balance: f64,
    pub checked_at: i64,
    pub checked_time: String,
    pub spend_since_first: Option<f64>,
    pub spend_since_previous: Option<f64>,
    pub first_checked_time: Option<String>,
    pub previous_checked_time: Option<String>,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct DeepSeekLocalUsage {
    pub month: String,
    pub current_month: UsageBucket,
    pub total: UsageBucket,
}

pub async fn deepseek_balance_report(
    client: &Client,
    channels: &[Channel],
    stats: &UsageStats,
    path: &str,
    refresh: bool,
) -> DeepSeekBalanceReport {
    let mut store = load_store(path);
    let mut errors = Vec::new();
    let mut refreshed_any = false;

    if refresh {
        let accounts = select_deepseek_accounts(channels);
        if accounts.is_empty() {
            errors.push("没有找到可用于查询余额的 DeepSeek 渠道".to_string());
        }
        for account in accounts {
            match fetch_balance(client, &account).await {
                Ok(mut snapshots) => {
                    refreshed_any = true;
                    store.snapshots.append(&mut snapshots);
                }
                Err(err) => errors.push(format!("{}: {}", account.channel, err)),
            }
        }
        trim_snapshots(&mut store.snapshots);
        save_store(path, &store);
    }

    let accounts = account_reports(&store.snapshots);
    let has_snapshots = !accounts.is_empty();
    let message = if errors.is_empty() {
        if refreshed_any {
            "DeepSeek 余额已刷新".to_string()
        } else if has_snapshots {
            "显示最近一次 DeepSeek 余额快照".to_string()
        } else {
            "还没有 DeepSeek 余额快照，点刷新后开始记录".to_string()
        }
    } else {
        errors.join("；")
    };

    DeepSeekBalanceReport {
        ok: errors.is_empty() && (has_snapshots || refreshed_any),
        refreshed: refreshed_any,
        message,
        snapshot_count: store.snapshots.len(),
        accounts,
        local_usage: DeepSeekLocalUsage {
            month: shanghai_month(now_unix_secs()),
            current_month: stats.deepseek_current_month_usage(),
            total: stats.deepseek_total_usage(),
        },
    }
}

#[derive(Debug, Clone)]
struct DeepSeekAccount {
    account_id: String,
    channel_id: Option<u64>,
    channel: String,
    key: String,
    balance_url: String,
}

fn select_deepseek_accounts(channels: &[Channel]) -> Vec<DeepSeekAccount> {
    let mut seen = BTreeSet::new();
    let mut accounts = Vec::new();
    for ch in channels
        .iter()
        .filter(|ch| ch.is_active() && is_deepseek_channel(ch))
    {
        let key = ch.key.trim();
        if key.is_empty() || key == "***" {
            continue;
        }
        let account_id = format!("deepseek-{}", stable_hash(key));
        if !seen.insert(account_id.clone()) {
            continue;
        }
        accounts.push(DeepSeekAccount {
            account_id,
            channel_id: ch.id,
            channel: if ch.name.trim().is_empty() {
                "DeepSeek".to_string()
            } else {
                ch.name.clone()
            },
            key: key.to_string(),
            balance_url: deepseek_balance_url(&ch.base),
        });
    }
    accounts
}

fn is_deepseek_channel(ch: &Channel) -> bool {
    let base = ch.base.to_lowercase();
    let group = ch.group.to_lowercase();
    let cost_model = ch.cost_model.to_lowercase();
    let name = ch.name.to_lowercase();
    base.contains("api.deepseek.com")
        || group == "deepseek"
        || cost_model.contains("deepseek")
        || name.contains("deepseek")
}

fn deepseek_balance_url(base: &str) -> String {
    let mut base = base.trim().trim_end_matches('/').to_string();
    if base.ends_with("/v1") {
        base.truncate(base.len().saturating_sub(3));
        base = base.trim_end_matches('/').to_string();
    }
    if base.is_empty() {
        base = "https://api.deepseek.com".to_string();
    }
    format!("{}/user/balance", base)
}

async fn fetch_balance(
    client: &Client,
    account: &DeepSeekAccount,
) -> Result<Vec<DeepSeekBalanceSnapshot>, String> {
    let response = client
        .get(&account.balance_url)
        .bearer_auth(&account.key)
        .send()
        .await
        .map_err(|err| format!("请求失败：{}", err))?;
    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|err| format!("读取响应失败：{}", err))?;
    if !status.is_success() {
        let text = String::from_utf8_lossy(&body);
        return Err(format!("HTTP {} {}", status.as_u16(), text.trim()));
    }
    let value: Value =
        serde_json::from_slice(&body).map_err(|err| format!("JSON 解析失败：{}", err))?;
    Ok(parse_balance_response(account, &value))
}

fn parse_balance_response(
    account: &DeepSeekAccount,
    value: &Value,
) -> Vec<DeepSeekBalanceSnapshot> {
    let is_available = value
        .get("is_available")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let ts = now_unix_secs();
    let (day, time) = shanghai_strings(ts);
    value
        .get("balance_infos")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|item| DeepSeekBalanceSnapshot {
            ts,
            day: day.clone(),
            time: time.clone(),
            account_id: account.account_id.clone(),
            channel_id: account.channel_id,
            channel: account.channel.clone(),
            is_available,
            currency: item
                .get("currency")
                .and_then(Value::as_str)
                .unwrap_or("CNY")
                .to_string(),
            total_balance: number_from_value(item.get("total_balance")),
            granted_balance: number_from_value(item.get("granted_balance")),
            topped_up_balance: number_from_value(item.get("topped_up_balance")),
        })
        .collect()
}

fn account_reports(snapshots: &[DeepSeekBalanceSnapshot]) -> Vec<DeepSeekAccountReport> {
    let mut grouped: BTreeMap<(String, String), Vec<&DeepSeekBalanceSnapshot>> = BTreeMap::new();
    for snapshot in snapshots {
        grouped
            .entry((snapshot.account_id.clone(), snapshot.currency.clone()))
            .or_default()
            .push(snapshot);
    }

    grouped
        .into_iter()
        .filter_map(|((_account_id, _currency), mut items)| {
            items.sort_by_key(|item| item.ts);
            let current = (*items.last()?).clone();
            let previous = items.iter().rev().nth(1).map(|item| (*item).clone());
            let first = (*items.first()?).clone();
            Some(DeepSeekAccountReport {
                account_id: current.account_id.clone(),
                channel_id: current.channel_id,
                channel: current.channel.clone(),
                is_available: current.is_available,
                currency: current.currency.clone(),
                total_balance: current.total_balance,
                granted_balance: current.granted_balance,
                topped_up_balance: current.topped_up_balance,
                checked_at: current.ts,
                checked_time: current.time.clone(),
                spend_since_first: positive_delta(first.total_balance, current.total_balance),
                spend_since_previous: previous
                    .as_ref()
                    .and_then(|item| positive_delta(item.total_balance, current.total_balance)),
                first_checked_time: Some(first.time.clone()),
                previous_checked_time: previous.map(|item| item.time),
            })
        })
        .collect()
}

fn positive_delta(before: f64, after: f64) -> Option<f64> {
    let delta = before - after;
    if delta.abs() < 0.000_001 {
        Some(0.0)
    } else {
        Some(delta)  // 负数表示充值导致余额增加，不截断
    }
}

fn number_from_value(value: Option<&Value>) -> f64 {
    match value {
        Some(Value::Number(number)) => number.as_f64().unwrap_or(0.0),
        Some(Value::String(text)) => text.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn load_store<P: AsRef<Path>>(path: P) -> DeepSeekBalanceStore {
    let path = path.as_ref();
    if !path.exists() {
        return DeepSeekBalanceStore::default();
    }
    fs::read_to_string(path)
        .ok()
        .and_then(|data| serde_json::from_str(&data).ok())
        .unwrap_or_default()
}

fn save_store<P: AsRef<Path>>(path: P, store: &DeepSeekBalanceStore) {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let Ok(data) = serde_json::to_string_pretty(store) else {
        return;
    };
    let tmp = path.with_extension("json.tmp");
    if fs::write(&tmp, &data).is_ok() && fs::rename(&tmp, path).is_ok() {
        return;
    }
    let _ = fs::write(path, data);
}

fn trim_snapshots(snapshots: &mut Vec<DeepSeekBalanceSnapshot>) {
    snapshots.sort_by_key(|item| item.ts);
    if snapshots.len() > MAX_SNAPSHOTS {
        let remove = snapshots.len() - MAX_SNAPSHOTS;
        snapshots.drain(0..remove);
    }
}

fn stable_hash(text: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    text.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn now_unix_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn shanghai_month(ts: i64) -> String {
    let (day, _) = shanghai_strings(ts);
    day.get(0..7).unwrap_or("").to_string()
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

    fn account() -> DeepSeekAccount {
        DeepSeekAccount {
            account_id: "deepseek-test".to_string(),
            channel_id: Some(1),
            channel: "DeepSeek".to_string(),
            key: "sk-test".to_string(),
            balance_url: "https://api.deepseek.com/user/balance".to_string(),
        }
    }

    #[test]
    fn parses_official_balance_response() {
        let value = serde_json::json!({
            "is_available": true,
            "balance_infos": [{
                "currency": "CNY",
                "total_balance": "110.00",
                "granted_balance": "10.00",
                "topped_up_balance": "100.00"
            }]
        });
        let snapshots = parse_balance_response(&account(), &value);
        assert_eq!(snapshots.len(), 1);
        assert!(snapshots[0].is_available);
        assert_eq!(snapshots[0].currency, "CNY");
        assert_eq!(snapshots[0].total_balance, 110.0);
        assert_eq!(snapshots[0].topped_up_balance, 100.0);
    }

    #[test]
    fn normalizes_deepseek_balance_url() {
        assert_eq!(
            deepseek_balance_url("https://api.deepseek.com/v1"),
            "https://api.deepseek.com/user/balance"
        );
        assert_eq!(
            deepseek_balance_url("https://api.deepseek.com"),
            "https://api.deepseek.com/user/balance"
        );
    }

    #[test]
    fn deduplicates_channels_with_same_key() {
        let channels = vec![
            Channel {
                name: "DeepSeek Flash".to_string(),
                key: "sk-same".to_string(),
                base: "https://api.deepseek.com".to_string(),
                group: "deepseek".to_string(),
                ..Channel::default()
            },
            Channel {
                name: "DeepSeek Pro".to_string(),
                key: "sk-same".to_string(),
                base: "https://api.deepseek.com".to_string(),
                models: "deepseek-v4-pro".to_string(),
                ..Channel::default()
            },
        ];
        assert_eq!(select_deepseek_accounts(&channels).len(), 1);
    }

    #[test]
    fn ignores_compat_model_aliases_on_non_deepseek_channels() {
        let channels = vec![Channel {
            name: "Gemini Fallback".to_string(),
            key: "sk-gemini".to_string(),
            base: "http://127.0.0.1:8000/v1".to_string(),
            models: "gemini-flash,deepseek-v4-flash".to_string(),
            group: "gemini".to_string(),
            cost_model: "free-gemini-web".to_string(),
            ..Channel::default()
        }];
        assert_eq!(select_deepseek_accounts(&channels).len(), 0);
    }
}
