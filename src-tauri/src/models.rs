use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppConfig {
    pub gateway: GatewayConfig,
    pub providers: Vec<ProviderConfig>,
    pub local_auth_token: String,
    pub routing: RoutingConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            gateway: GatewayConfig::default(),
            providers: vec![
                ProviderConfig::template("ciyuanshen", "词原神", "https://ciyuanshen.top/v1"),
                ProviderConfig::template("uocode", "Uocode", "https://uocode.com/v1"),
            ],
            local_auth_token: format!("apidev-{}", uuid::Uuid::new_v4()),
            routing: RoutingConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn normalize_balance_auth(&mut self) -> bool {
        let mut changed = false;
        for provider in &mut self.providers {
            changed |= provider.normalize_balance_auth();
        }
        changed
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GatewayConfig {
    pub host: String,
    pub port: u16,
    pub require_local_token: bool,
    pub request_timeout_secs: u64,
    pub stream_idle_timeout_secs: u64,
    pub max_request_body_mb: u64,
    /// Soft cap for Codex-like request bodies. When exceeded, the gateway returns a
    /// Codex-compatible context_length_exceeded signal instead of forwarding the
    /// request upstream, nudging the client to compact context before retrying.
    /// Set to 0 to disable the soft context/body guard.
    pub codex_context_body_limit_mb: u64,
    /// Token threshold advertised to Codex so it auto-compacts before requests
    /// grow into oversized context payloads.
    pub codex_auto_compact_token_limit: i64,
}

pub const DEFAULT_CODEX_CONTEXT_BODY_LIMIT_MB: u64 = 32;
pub const DEFAULT_CODEX_AUTO_COMPACT_TOKEN_LIMIT: i64 = 120_000;

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 14555,
            require_local_token: false,
            request_timeout_secs: 300,
            stream_idle_timeout_secs: 300,
            max_request_body_mb: 512,
            codex_context_body_limit_mb: DEFAULT_CODEX_CONTEXT_BODY_LIMIT_MB,
            codex_auto_compact_token_limit: DEFAULT_CODEX_AUTO_COMPACT_TOKEN_LIMIT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConfig {
    #[serde(default = "default_true")]
    pub auto_round_robin: bool,
    #[serde(default = "default_true")]
    pub auto_failover: bool,
    pub max_attempts_per_request: usize,
    pub cooldown_secs: i64,
    #[serde(default = "default_auth_failure_threshold")]
    pub auth_failure_threshold: u32,
    #[serde(default = "default_probe_interval_secs")]
    pub probe_interval_secs: i64,
    #[serde(default = "default_max_cooldown_secs")]
    pub max_cooldown_secs: i64,
    pub selected_provider_id: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_auth_failure_threshold() -> u32 {
    2
}

fn default_probe_interval_secs() -> i64 {
    300
}

fn default_max_cooldown_secs() -> i64 {
    600
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            auto_round_robin: true,
            auto_failover: true,
            max_attempts_per_request: 4,
            cooldown_secs: 60,
            auth_failure_threshold: 2,
            probe_interval_secs: 300,
            max_cooldown_secs: 600,
            selected_provider_id: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    pub base_url: String,
    pub api_key: String,
    pub enabled: bool,
    pub timeout_secs: u64,
    pub headers: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub quota: Option<QuotaConfig>,
    pub balance_auth: Option<BalanceAuthConfig>,
}

impl ProviderConfig {
    pub fn template(id: &str, name: &str, base_url: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            base_url: base_url.to_string(),
            api_key: String::new(),
            enabled: false,
            timeout_secs: 300,
            headers: BTreeMap::new(),
            query: BTreeMap::new(),
            quota: None,
            balance_auth: Some(BalanceAuthConfig::default()),
        }
    }

    pub fn effective_balance_auth(&self) -> BalanceAuthConfig {
        self.balance_auth.clone().unwrap_or_else(|| {
            if self.quota.is_some() {
                BalanceAuthConfig::quota_api()
            } else {
                BalanceAuthConfig::default()
            }
        })
    }

    pub fn normalize_balance_auth(&mut self) -> bool {
        if self.balance_auth.is_none() {
            self.balance_auth = Some(self.effective_balance_auth());
            return true;
        }
        false
    }
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self::template(
            &uuid::Uuid::new_v4().to_string(),
            "New Provider",
            "https://api.openai.com/v1",
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BalanceAuthMode {
    Disabled,
    QuotaApi,
    NewapiLogin,
    Sub2apiLogin,
}

impl Default for BalanceAuthMode {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BalanceAuthConfig {
    pub mode: BalanceAuthMode,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl BalanceAuthConfig {
    pub fn quota_api() -> Self {
        Self {
            mode: BalanceAuthMode::QuotaApi,
            ..Default::default()
        }
    }
}

impl Default for BalanceAuthConfig {
    fn default() -> Self {
        Self {
            mode: BalanceAuthMode::Disabled,
            username: None,
            password: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QuotaConfig {
    pub url: String,
    pub method: String,
    pub headers: BTreeMap<String, String>,
    pub balance_json_path: Option<String>,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            method: "GET".to_string(),
            headers: BTreeMap::new(),
            balance_json_path: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealth {
    Healthy,
    Degraded,
    CoolingDown,
    AuthFailed,
    Disabled,
    Unknown,
}

impl Default for ProviderHealth {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderState {
    pub provider_id: String,
    pub health: ProviderHealth,
    pub last_checked_at: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub next_probe_at: Option<DateTime<Utc>>,
    pub consecutive_failures: u32,
    pub auth_failure_count: u32,
    pub transient_failure_count: u32,
    pub error_kind: Option<String>,
    pub last_error: Option<String>,
    pub last_latency_ms: Option<u128>,
    pub last_status: Option<u16>,
    pub remaining_hint: Option<String>,
}

impl Default for ProviderState {
    fn default() -> Self {
        Self {
            provider_id: String::new(),
            health: ProviderHealth::Unknown,
            last_checked_at: None,
            last_success_at: None,
            cooldown_until: None,
            next_probe_at: None,
            consecutive_failures: 0,
            auth_failure_count: 0,
            transient_failure_count: 0,
            error_kind: None,
            last_error: None,
            last_latency_ms: None,
            last_status: None,
            remaining_hint: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeState {
    pub providers: BTreeMap<String, ProviderState>,
    pub response_routes: BTreeMap<String, String>,
}

impl RuntimeState {
    /// Normalize runtime state written by older builds.
    ///
    /// Previous versions treated every 401/403 as `auth_failed`.  The new
    /// routing policy only reserves that state for clear authentication
    /// failures, so legacy non-auth failures are downgraded back to
    /// `degraded` and get a coarse `error_kind` for the UI.
    pub fn normalize_legacy_provider_states(&mut self) -> bool {
        let mut changed = false;
        for state in self.providers.values_mut() {
            if state.health != ProviderHealth::AuthFailed {
                continue;
            }
            let kind = state.error_kind.clone().unwrap_or_else(|| {
                legacy_error_kind(state.last_status, state.last_error.as_deref()).to_string()
            });
            if state.error_kind.as_deref() != Some(kind.as_str()) {
                state.error_kind = Some(kind.clone());
                changed = true;
            }
            if !matches!(kind.as_str(), "auth_failed" | "auth_suspect") {
                state.health = ProviderHealth::Degraded;
                state.cooldown_until = None;
                state.next_probe_at = None;
                changed = true;
            }
        }
        changed
    }
}

fn legacy_error_kind(status: Option<u16>, error: Option<&str>) -> &'static str {
    let lower = error.unwrap_or_default().to_ascii_lowercase();
    let status = status.or_else(|| extract_legacy_status_code(&lower));
    if matches!(status, Some(401)) || looks_like_legacy_auth_error(&lower) {
        return "auth_failed";
    }
    if matches!(status, Some(403)) || looks_like_legacy_permission_error(&lower) {
        return "permission";
    }
    if matches!(status, Some(402)) || looks_like_legacy_quota_error(&lower) {
        return "quota";
    }
    if matches!(status, Some(429)) || looks_like_legacy_rate_limit_error(&lower) {
        return "rate_limit";
    }
    if matches!(status, Some(s) if s >= 500) {
        return "upstream_5xx";
    }
    if looks_like_legacy_network_error(&lower) {
        return "network";
    }
    "unknown"
}

fn extract_legacy_status_code(lower: &str) -> Option<u16> {
    lower
        .split(|ch: char| !ch.is_ascii_digit())
        .filter_map(|part| part.parse::<u16>().ok())
        .find(|code| (400..=599).contains(code))
}

fn looks_like_legacy_auth_error(lower: &str) -> bool {
    lower.contains("invalid_api_key")
        || lower.contains("invalid api key")
        || lower.contains("incorrect api key")
        || lower.contains("api key is invalid")
        || lower.contains("unauthorized")
        || lower.contains("invalid token")
        || lower.contains("token is invalid")
        || lower.contains("token has expired")
        || lower.contains("invalid credentials")
        || lower.contains("认证失败")
        || lower.contains("鉴权失败")
        || lower.contains("未授权")
}

fn looks_like_legacy_permission_error(lower: &str) -> bool {
    lower.contains("permission_error")
        || lower.contains("permission denied")
        || lower.contains("not enabled")
        || lower.contains("not allowed")
        || lower.contains("group")
        || lower.contains("分组")
        || lower.contains("权限")
}

fn looks_like_legacy_quota_error(lower: &str) -> bool {
    lower.contains("insufficient_quota")
        || lower.contains("quota exceeded")
        || lower.contains("insufficient balance")
        || lower.contains("balance is not enough")
        || lower.contains("额度不足")
        || lower.contains("余额不足")
}

fn looks_like_legacy_rate_limit_error(lower: &str) -> bool {
    lower.contains("rate limit") || lower.contains("too many requests") || lower.contains("限流")
}

fn looks_like_legacy_network_error(lower: &str) -> bool {
    lower.contains("timeout")
        || lower.contains("timed out")
        || lower.contains("connection")
        || lower.contains("connect")
        || lower.contains("dns")
        || lower.contains("network")
        || lower.contains("tcp")
        || lower.contains("tls")
        || lower.contains("网络")
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModelsCache {
    pub refreshed_at: Option<DateTime<Utc>>,
    pub providers: BTreeMap<String, ProviderModels>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderModels {
    pub provider_id: String,
    pub fetched_at: Option<DateTime<Utc>>,
    pub models: Vec<ModelInfo>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ModelInfo {
    pub id: String,
    #[serde(default)]
    pub object: Option<String>,
    #[serde(default)]
    pub created: Option<i64>,
    #[serde(default)]
    pub owned_by: Option<String>,
    #[serde(default)]
    pub raw: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RequestLogEntry {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub method: String,
    pub path: String,
    pub model: Option<String>,
    pub provider_id: Option<String>,
    pub provider_name: Option<String>,
    pub status: Option<u16>,
    pub latency_ms: u128,
    pub attempts: usize,
    pub streamed: bool,
    pub body_size_bytes: Option<u64>,
    pub failover_reason: Option<String>,
    pub local_rejected: bool,
    pub error_kind: Option<String>,
    pub error: Option<String>,
    pub usage: Option<Value>,
}

impl Default for RequestLogEntry {
    fn default() -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            method: String::new(),
            path: String::new(),
            model: None,
            provider_id: None,
            provider_name: None,
            status: None,
            latency_ms: 0,
            attempts: 0,
            streamed: false,
            body_size_bytes: None,
            failover_reason: None,
            local_rejected: false,
            error_kind: None,
            error: None,
            usage: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewayStatus {
    pub running: bool,
    pub bind_url: String,
    pub provider_count: usize,
    pub enabled_provider_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderView {
    pub config: ProviderConfig,
    pub state: ProviderState,
    pub model_count: usize,
    pub gpt_model_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderExportResult {
    pub ok: bool,
    pub path: String,
    pub count: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProviderImportResult {
    pub ok: bool,
    pub imported: usize,
    pub updated: usize,
    pub skipped: usize,
    pub total: usize,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TestResult {
    pub ok: bool,
    pub provider_id: String,
    pub model: Option<String>,
    pub status: Option<u16>,
    pub latency_ms: u128,
    pub error: Option<String>,
    pub response_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuotaResult {
    pub provider_id: String,
    pub ok: bool,
    pub balance: Option<String>,
    pub health_hint: Option<String>,
    pub status: Option<u16>,
    pub error: Option<String>,
    pub raw: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewaySelfCheckItem {
    pub name: String,
    pub ok: bool,
    pub status: Option<u16>,
    pub latency_ms: u128,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GatewaySelfCheckResult {
    pub ok: bool,
    pub message: String,
    pub checks: Vec<GatewaySelfCheckItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexConfigResult {
    pub ok: bool,
    pub config_path: String,
    pub backup_path: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ConfigureCodexRequest {
    pub model: String,
    pub provider_name: Option<String>,
    pub auto_compact_token_limit: Option<i64>,
}
