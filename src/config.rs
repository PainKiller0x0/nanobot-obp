use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct Channel {
    pub id: Option<u64>,
    pub name: String,
    pub r#type: String,
    pub key: String,
    pub base: String,
    pub models: String,
    pub model_mapping: String,
    pub status: String,
    pub requests: u64,
    pub last_test: Option<String>,
    pub fail_count: u32,
    pub disabled_until: Option<u64>,
    pub role: String,
    pub group: String,
    pub priority: u32,
    pub cost_model: String,
}

impl Default for Channel {
    fn default() -> Self {
        Self {
            id: None,
            name: String::new(),
            r#type: "openai".to_string(),
            key: String::new(),
            base: String::new(),
            models: "*".to_string(),
            model_mapping: String::new(),
            status: "active".to_string(),
            requests: 0,
            last_test: None,
            fail_count: 0,
            disabled_until: None,
            role: "default".to_string(),
            group: String::new(),
            priority: 100,
            cost_model: String::new(),
        }
    }
}

impl Channel {
    pub fn is_active(&self) -> bool {
        let status = self.status.trim();
        if status.is_empty() || status.eq_ignore_ascii_case("active") {
            return true;
        }
        if status.eq_ignore_ascii_case("cooldown") || status.eq_ignore_ascii_case("error") {
            return self.disabled_until.unwrap_or(0) <= unix_now_secs();
        }
        false
    }

    pub fn role_key(&self) -> String {
        let role = self.role.trim();
        if role.is_empty() {
            "default".to_string()
        } else {
            role.to_lowercase()
        }
    }

    pub fn group_key(&self) -> String {
        self.group.trim().to_lowercase()
    }

    pub fn supports_model(&self, model: &str) -> bool {
        let models = self.models.trim();
        if models.is_empty() || models == "*" {
            return true;
        }
        let target = model.trim().to_lowercase();
        self.model_set().contains(&target)
    }

    pub fn model_match_rank(&self, desired_model: &str, requested_model: &str) -> (u8, u8) {
        (
            self.single_model_match_rank(desired_model),
            self.single_model_match_rank(requested_model),
        )
    }

    fn single_model_match_rank(&self, model: &str) -> u8 {
        let target = model.trim().to_lowercase();
        if target.is_empty() {
            return 9;
        }
        let models = self.models.trim();
        if !models.is_empty() && models != "*" && self.model_set().contains(&target) {
            return 0;
        }
        if lookup_mapping(self.mapping_value().as_ref(), model).is_some() {
            return 1;
        }
        if models.is_empty() || models == "*" {
            return 3;
        }
        9
    }

    pub fn mapped_model(&self, requested_model: &str, desired_model: &str) -> String {
        let mapping = self.mapping_value();
        if let Some(mapped) = lookup_mapping(mapping.as_ref(), desired_model) {
            return mapped;
        }
        if let Some(mapped) = lookup_mapping(mapping.as_ref(), requested_model) {
            return mapped;
        }
        if !desired_model.trim().is_empty() && self.supports_model(desired_model) {
            return desired_model.to_string();
        }
        if !requested_model.trim().is_empty() && self.supports_model(requested_model) {
            return requested_model.to_string();
        }
        self.first_model()
            .unwrap_or_else(|| desired_model.to_string())
    }

    fn model_set(&self) -> BTreeSet<String> {
        self.models
            .split(',')
            .map(|item| item.trim().to_lowercase())
            .filter(|item| !item.is_empty())
            .collect()
    }

    fn first_model(&self) -> Option<String> {
        self.models
            .split(',')
            .map(str::trim)
            .find(|item| !item.is_empty() && *item != "*")
            .map(ToString::to_string)
    }

    fn mapping_value(&self) -> Option<Value> {
        if self.model_mapping.trim().is_empty() {
            return None;
        }
        serde_json::from_str::<Value>(&self.model_mapping).ok()
    }
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn lookup_mapping(mapping: Option<&Value>, model: &str) -> Option<String> {
    let map = mapping?.as_object()?;
    let exact = map.get(model).and_then(Value::as_str);
    if let Some(value) = exact {
        return Some(value.to_string());
    }
    let target = model.to_lowercase();
    map.iter()
        .find(|(key, _)| key.to_lowercase() == target)
        .and_then(|(_, value)| value.as_str())
        .map(ToString::to_string)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RouteProfile {
    pub default_model: String,
    pub pro_model: String,
    pub emergency_model: String,
    pub backup_model: String,
    pub default_group: String,
    pub pro_group: String,
    pub emergency_group: String,
    pub backup_group: String,
}

impl RouteProfile {
    pub fn default_stack() -> Self {
        Self {
            default_model: "deepseek-v4-flash".to_string(),
            pro_model: "deepseek-v4-pro".to_string(),
            emergency_model: "LongCat-Flash-Chat".to_string(),
            backup_model: "MiniMax-M2.7".to_string(),
            default_group: "deepseek".to_string(),
            pro_group: "deepseek".to_string(),
            emergency_group: "longcat".to_string(),
            backup_group: "minimax".to_string(),
        }
    }

    pub fn gemini_stack() -> Self {
        Self {
            default_model: "gemini-flash".to_string(),
            pro_model: "gemini-pro".to_string(),
            emergency_model: "LongCat-Flash-Chat".to_string(),
            backup_model: "gemini-flash".to_string(),
            default_group: "gemini".to_string(),
            pro_group: "gemini".to_string(),
            emergency_group: "longcat".to_string(),
            backup_group: "gemini".to_string(),
        }
    }

    pub fn apply_to(&self, router: &mut RouterConfig) {
        router.default_model = self.default_model.clone();
        router.pro_model = self.pro_model.clone();
        router.emergency_model = self.emergency_model.clone();
        router.backup_model = self.backup_model.clone();
        router.default_group = self.default_group.clone();
        router.pro_group = self.pro_group.clone();
        router.emergency_group = self.emergency_group.clone();
        router.backup_group = self.backup_group.clone();
    }
}

impl Default for RouteProfile {
    fn default() -> Self {
        Self::default_stack()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RouteRule {
    pub name: String,
    pub enabled: bool,
    pub priority: u32,
    pub role: String,
    pub model: String,
    pub group: String,
    pub reason: String,
    pub requested_models: Vec<String>,
    pub source_patterns: Vec<String>,
    pub hint_patterns: Vec<String>,
    pub latest_text_patterns: Vec<String>,
    pub task_text_patterns: Vec<String>,
    pub any_text_patterns: Vec<String>,
    pub min_monthly_cost_rmb: f64,
}

impl Default for RouteRule {
    fn default() -> Self {
        Self {
            name: String::new(),
            enabled: true,
            priority: 100,
            role: "default".to_string(),
            model: String::new(),
            group: String::new(),
            reason: String::new(),
            requested_models: Vec::new(),
            source_patterns: Vec::new(),
            hint_patterns: Vec::new(),
            latest_text_patterns: Vec::new(),
            task_text_patterns: Vec::new(),
            any_text_patterns: Vec::new(),
            min_monthly_cost_rmb: 0.0,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(default)]
pub struct RouterConfig {
    pub enabled: bool,
    pub dry_run: bool,
    pub external_enabled: bool,
    pub external_allowed_models: Vec<String>,
    pub default_model: String,
    pub pro_model: String,
    pub emergency_model: String,
    pub backup_model: String,
    pub default_group: String,
    pub pro_group: String,
    pub emergency_group: String,
    pub backup_group: String,
    // Client-side model names that mean "normal/default" or "complex/pro".
    // They let OBP route old nanobot configs without editing every client.
    pub default_alias_models: Vec<String>,
    pub pro_alias_models: Vec<String>,
    pub route_profiles: BTreeMap<String, RouteProfile>,
    pub source_route_profiles: BTreeMap<String, String>,
    pub route_rules: Vec<RouteRule>,
    // Legacy compatibility fields. Routing no longer upgrades to Pro only
    // because the prompt or message history is long.
    pub pro_prompt_chars: usize,
    pub pro_message_count: usize,
    pub monthly_warn_rmb: f64,
    pub monthly_downgrade_rmb: f64,
    pub monthly_hard_limit_rmb: f64,
    pub retry_statuses: Vec<u16>,
    pub pro_keywords: Vec<String>,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dry_run: false,
            external_enabled: true,
            external_allowed_models: vec![
                "deepseek-v4-flash".to_string(),
                "deepseek-v4-pro".to_string(),
                "MiniMax-M2.7".to_string(),
                "LongCat-Flash-Chat".to_string(),
            ],
            default_model: "deepseek-v4-flash".to_string(),
            pro_model: "deepseek-v4-pro".to_string(),
            emergency_model: "LongCat-Flash-Chat".to_string(),
            backup_model: "coding-plan".to_string(),
            default_group: "deepseek".to_string(),
            pro_group: "deepseek".to_string(),
            emergency_group: "longcat".to_string(),
            backup_group: String::new(),
            default_alias_models: vec![
                "deepseek-v4-flash".to_string(),
                "gpt-4o-mini".to_string(),
                "gpt-3.5-turbo".to_string(),
            ],
            pro_alias_models: vec![
                "deepseek-v4-pro".to_string(),
                "deepseek-reasoner".to_string(),
                "gpt-4o".to_string(),
            ],
            route_profiles: default_route_profiles(),
            source_route_profiles: default_source_route_profiles(),
            route_rules: default_route_rules(),
            pro_prompt_chars: 0,
            pro_message_count: 0,
            monthly_warn_rmb: 10.0,
            monthly_downgrade_rmb: 20.0,
            monthly_hard_limit_rmb: 30.0,
            retry_statuses: vec![408, 409, 425, 429, 500, 502, 503, 504, 529],
            pro_keywords: vec![
                "深度分析".to_string(),
                "深入分析".to_string(),
                "架构".to_string(),
                "architecture".to_string(),
                "review".to_string(),
                "code review".to_string(),
                "代码审查".to_string(),
                "重构".to_string(),
                "refactor".to_string(),
                "取舍".to_string(),
                "迁移".to_string(),
                "migration".to_string(),
                "全盘".to_string(),
                "排障".to_string(),
                "root cause".to_string(),
                "压缩".to_string(),
                "上下文压缩".to_string(),
                "反思".to_string(),
            ],
        }
    }
}

impl RouterConfig {
    pub fn normalized(mut self) -> Self {
        self.ensure_defaults();
        self
    }

    pub fn ensure_defaults(&mut self) {
        if self.default_alias_models.is_empty() {
            self.default_alias_models = RouterConfig::default().default_alias_models;
        }
        if self.pro_alias_models.is_empty() {
            self.pro_alias_models = RouterConfig::default().pro_alias_models;
        }
        if self.route_profiles.is_empty() {
            self.route_profiles = default_route_profiles();
        } else {
            self.route_profiles
                .entry("default".to_string())
                .or_insert_with(RouteProfile::default_stack);
            self.route_profiles
                .entry("gemini".to_string())
                .or_insert_with(RouteProfile::gemini_stack);
        }
        if self.source_route_profiles.is_empty() {
            self.source_route_profiles = default_source_route_profiles();
        }
        if self.route_rules.is_empty() {
            self.route_rules = default_route_rules();
        }
    }

    pub fn profile_name_for_source(&self, source: &str) -> String {
        self.source_route_profiles
            .get(source)
            .or_else(|| self.source_route_profiles.get("*"))
            .cloned()
            .unwrap_or_else(|| "default".to_string())
    }

    pub fn effective_for_source(&self, source: &str) -> Self {
        let mut router = self.clone().normalized();
        let profile_name = router.profile_name_for_source(source);
        if let Some(profile) = router.route_profiles.get(&profile_name).cloned() {
            profile.apply_to(&mut router);
        }
        router
    }
}

fn default_route_profiles() -> BTreeMap<String, RouteProfile> {
    BTreeMap::from([
        ("default".to_string(), RouteProfile::default_stack()),
        ("gemini".to_string(), RouteProfile::gemini_stack()),
    ])
}

fn default_source_route_profiles() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("default-nanobot".to_string(), "gemini".to_string()),
        ("guangzhou-nanobot".to_string(), "default".to_string()),
    ])
}

fn default_route_rules() -> Vec<RouteRule> {
    vec![
        RouteRule {
            name: "free-health-and-memory".to_string(),
            priority: 10,
            role: "emergency".to_string(),
            model: "LongCat-Flash-Chat".to_string(),
            group: "longcat".to_string(),
            reason: "free task pattern matched".to_string(),
            hint_patterns: vec![
                "heartbeat".to_string(),
                "healthcheck".to_string(),
                "self_check".to_string(),
                "self-check".to_string(),
            ],
            latest_text_patterns: vec![
                "heartbeat.md".to_string(),
                "heartbeat agent".to_string(),
                "heartbeat tool".to_string(),
                "\"name\":\"heartbeat\"".to_string(),
                "\"name\": \"heartbeat\"".to_string(),
            ],
            task_text_patterns: vec![
                "extract key facts from this conversation".to_string(),
                "only output items matching these categories".to_string(),
                "output as concise bullet points".to_string(),
            ],
            ..RouteRule::default()
        },
        RouteRule {
            name: "complex-work-to-pro".to_string(),
            priority: 80,
            role: "pro".to_string(),
            reason: "complex task pattern matched".to_string(),
            hint_patterns: vec![
                "compact".to_string(),
                "compression".to_string(),
                "summarize".to_string(),
                "summary".to_string(),
                "memory".to_string(),
                "reflection".to_string(),
                "review".to_string(),
                "code_review".to_string(),
                "architecture".to_string(),
                "migration".to_string(),
                "reasoning".to_string(),
                "analysis".to_string(),
                "diagnose".to_string(),
                "root_cause".to_string(),
            ],
            any_text_patterns: vec![
                "context compression".to_string(),
                "memory consolidation".to_string(),
                "code review".to_string(),
                "review existing".to_string(),
                "root cause".to_string(),
                "上下文压缩".to_string(),
                "压缩上下文".to_string(),
                "代码审查".to_string(),
                "根因".to_string(),
                "排障".to_string(),
            ],
            ..RouteRule::default()
        },
    ]
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Vec<Channel> {
    if !path.as_ref().exists() {
        return Vec::new();
    }
    let data = fs::read_to_string(path).unwrap_or_default();
    let value = serde_json::from_str::<Value>(&data).unwrap_or(Value::Null);
    if let Ok(channels) = serde_json::from_value::<Vec<Channel>>(value.clone()) {
        return channels;
    }
    value
        .get("channels")
        .cloned()
        .and_then(|v| serde_json::from_value::<Vec<Channel>>(v).ok())
        .unwrap_or_default()
}

pub fn save_config<P: AsRef<Path>>(path: P, channels: &[Channel]) {
    if let Some(parent) = path.as_ref().parent() {
        let _ = fs::create_dir_all(parent);
    }
    let data = serde_json::to_string_pretty(channels).unwrap_or_default();
    let _ = fs::write(path, data);
}

pub fn load_router_config<P: AsRef<Path>>(path: P) -> RouterConfig {
    if !path.as_ref().exists() {
        return RouterConfig::default();
    }
    let data = fs::read_to_string(path).unwrap_or_default();
    serde_json::from_str::<RouterConfig>(&data)
        .unwrap_or_default()
        .normalized()
}

pub fn save_router_config<P: AsRef<Path>>(path: P, router: &RouterConfig) {
    if let Some(parent) = path.as_ref().parent() {
        let _ = fs::create_dir_all(parent);
    }
    let data = serde_json::to_string_pretty(router).unwrap_or_default();
    let _ = fs::write(path, data);
}
