use crate::{
    gateway::{is_gpt_model, GatewayManager},
    models::{
        AppConfig, GatewaySelfCheckItem, GatewaySelfCheckResult, GatewayStatus, ProviderConfig,
        ProviderImportResult, ProviderState, ProviderView,
    },
    quota,
    shared::{
        err_string, filter_supported_models_cache, finish_self_check, first_cached_gpt_model,
        parse_provider_import, validate_provider,
    },
    storage::{sqlite_get_raw_app_value, sqlite_set_raw_app_value, Storage},
};
use anyhow::{Context, Result};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Instant};
use tokio::{net::TcpListener, sync::RwLock};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

const ADMIN_HASH_KEY: &str = "admin_password_hash";
const DEFAULT_WEB_PORT: u16 = 14556;

#[derive(Clone)]
struct WebState {
    storage: Storage,
    gateway: GatewayManager,
    db_path: PathBuf,
    auth: Arc<RwLock<WebAuthState>>,
}

#[derive(Clone)]
struct WebAuthState {
    password_hash: String,
    session_token: String,
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    ok: bool,
    token: String,
}

#[derive(Debug, Deserialize)]
struct ImportProvidersRequest {
    raw: String,
}

#[derive(Debug, Deserialize)]
struct TestModelRequest {
    provider_id: String,
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LogsQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SelfCheckRequest {
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CodexSnippetQuery {
    model: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CodexSnippetResponse {
    pub model: String,
    pub base_url: String,
    pub bearer_token: String,
    pub auto_compact_token_limit: i64,
    pub config_toml: String,
    pub configure_script: String,
    pub download_name: String,
}

#[derive(Debug, Serialize)]
struct StoragePathResponse {
    db_path: String,
    data_dir: String,
}

pub async fn run_web_server(host: String, port: u16) -> Result<()> {
    let db_path = web_db_path()?;
    let first_run = !db_path.exists();
    let storage = Storage::load_sqlite(db_path.clone()).await?;
    let auth = init_admin_auth(&db_path)?;
    let gateway = GatewayManager::new(storage.clone());

    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .with_context(|| format!("parse web listen address {host}:{port}"))?;
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("bind Other Model Web on {addr}"))?;
    let local_addr = listener.local_addr()?;
    let base_url = format!("http://{}:{}/v1", local_addr.ip(), local_addr.port());
    storage
        .update_config(|cfg| {
            cfg.gateway.host = local_addr.ip().to_string();
            cfg.gateway.port = local_addr.port();
            if first_run {
                cfg.gateway.require_local_token = true;
            }
        })
        .await?;
    gateway.mark_running_at(base_url.clone()).await;

    let state = WebState {
        storage,
        gateway: gateway.clone(),
        db_path,
        auth: Arc::new(RwLock::new(auth)),
    };
    let static_dir = web_static_dir();
    let static_service =
        ServeDir::new(&static_dir).not_found_service(ServeFile::new(static_dir.join("index.html")));
    let app = Router::new()
        .merge(gateway.router())
        .merge(api_router(state))
        .fallback_service(static_service)
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        );

    eprintln!("Other Model Web admin UI: http://{}", local_addr);
    eprintln!("Other Model Web gateway: {base_url}");
    eprintln!("Other Model Web SQLite: {}", web_db_path()?.display());
    axum::serve(listener, app).await?;
    Ok(())
}

pub fn default_web_host() -> String {
    "127.0.0.1".to_string()
}

pub fn default_web_port() -> u16 {
    DEFAULT_WEB_PORT
}

fn api_router(state: WebState) -> Router {
    Router::new()
        .route("/api/auth/login", post(login_handler))
        .route("/api/auth/logout", post(logout_handler))
        .route("/api/status", get(status_handler))
        .route("/api/gateway/start", post(start_gateway_handler))
        .route("/api/gateway/stop", post(stop_gateway_handler))
        .route(
            "/api/config",
            get(get_config_handler).post(save_config_handler),
        )
        .route(
            "/api/providers",
            get(list_providers_handler).post(create_provider_handler),
        )
        .route(
            "/api/providers/:provider_id",
            put(update_provider_handler).delete(delete_provider_handler),
        )
        .route("/api/providers/export", get(export_providers_handler))
        .route("/api/providers/import", post(import_providers_handler))
        .route("/api/models/discover", post(discover_models_handler))
        .route("/api/models/cache", get(models_cache_handler))
        .route("/api/models/test", post(test_model_handler))
        .route("/api/quota/:provider_id", get(quota_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/self-check", post(self_check_handler))
        .route("/api/codex/snippet", get(codex_snippet_handler))
        .route("/api/storage/path", get(storage_path_handler))
        .with_state(state)
}

async fn login_handler(State(state): State<WebState>, Json(req): Json<LoginRequest>) -> Response {
    let ok = {
        let auth = state.auth.read().await;
        verify_password(&auth.password_hash, &req.password)
    };
    if !ok {
        return api_error(StatusCode::UNAUTHORIZED, "invalid admin password");
    }
    let token = format!("om-session-{}", uuid::Uuid::new_v4());
    {
        let mut auth = state.auth.write().await;
        auth.session_token = token.clone();
    }
    Json(LoginResponse { ok: true, token }).into_response()
}

async fn logout_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    state.auth.write().await.session_token = format!("om-session-{}", uuid::Uuid::new_v4());
    Json(json!({ "ok": true })).into_response()
}

async fn status_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(state.gateway.status().await).into_response()
}

async fn start_gateway_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let status = state.gateway.status().await;
    Json(status).into_response()
}

async fn stop_gateway_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    // Web 自托管版本的管理后台和 /v1 网关共用同一个 HTTP 服务，不能在 UI 内单独停止。
    Json(state.gateway.status().await).into_response()
}

async fn get_config_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(state.storage.config().await).into_response()
}

async fn save_config_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(mut config): Json<AppConfig>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    config.normalize_balance_auth();
    if let Err(err) = state.storage.set_config(config.clone()).await {
        return api_error(StatusCode::BAD_REQUEST, &err_string(err));
    }
    Json(config).into_response()
}

async fn list_providers_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(provider_views(&state.storage).await).into_response()
}

async fn create_provider_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(mut provider): Json<ProviderConfig>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    if provider.id.trim().is_empty() {
        provider.id = uuid::Uuid::new_v4().to_string();
    }
    provider.normalize_balance_auth();
    if let Err(err) = validate_provider(&provider) {
        return api_error(StatusCode::BAD_REQUEST, &err.to_string());
    }
    if let Err(err) = state
        .storage
        .update_config(|cfg| cfg.providers.push(provider.clone()))
        .await
    {
        return api_error(StatusCode::INTERNAL_SERVER_ERROR, &err_string(err));
    }
    Json(provider).into_response()
}

async fn update_provider_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(provider_id): Path<String>,
    Json(mut provider): Json<ProviderConfig>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    provider.id = provider_id;
    provider.normalize_balance_auth();
    if let Err(err) = validate_provider(&provider) {
        return api_error(StatusCode::BAD_REQUEST, &err.to_string());
    }
    let found = state
        .storage
        .update_config(|cfg| {
            if let Some(existing) = cfg.providers.iter_mut().find(|p| p.id == provider.id) {
                *existing = provider.clone();
                true
            } else {
                false
            }
        })
        .await;
    match found {
        Ok(true) => Json(provider).into_response(),
        Ok(false) => api_error(StatusCode::NOT_FOUND, "provider not found"),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &err_string(err)),
    }
}

async fn delete_provider_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(provider_id): Path<String>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    match state
        .storage
        .update_config(|cfg| {
            let before = cfg.providers.len();
            cfg.providers.retain(|p| p.id != provider_id);
            before != cfg.providers.len()
        })
        .await
    {
        Ok(deleted) => Json(deleted).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &err_string(err)),
    }
}

async fn export_providers_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let cfg = state.storage.config().await;
    let stamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let filename = format!("other-model-providers-{stamp}.json");
    let payload = json!({
        "schema": "other_model.providers.v1",
        "exported_at": Utc::now(),
        "providers": cfg.providers,
    });
    let raw = match serde_json::to_string_pretty(&payload) {
        Ok(raw) => raw,
        Err(err) => return api_error(StatusCode::INTERNAL_SERVER_ERROR, &err.to_string()),
    };
    (
        [
            (
                header::CONTENT_TYPE,
                "application/json; charset=utf-8".to_string(),
            ),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        raw,
    )
        .into_response()
}

async fn import_providers_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(req): Json<ImportProvidersRequest>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    match import_providers_inner(&state.storage, &req.raw).await {
        Ok(result) => Json(result).into_response(),
        Err(err) => api_error(StatusCode::BAD_REQUEST, &err.to_string()),
    }
}

async fn discover_models_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    match state.gateway.discover_models().await {
        Ok(cache) => Json(cache).into_response(),
        Err(err) => api_error(StatusCode::BAD_GATEWAY, &err.to_string()),
    }
}

async fn models_cache_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(filter_supported_models_cache(
        state.storage.models_cache().await,
    ))
    .into_response()
}

async fn test_model_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(req): Json<TestModelRequest>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(
        state
            .gateway
            .test_provider_model(req.provider_id, req.model)
            .await,
    )
    .into_response()
}

async fn quota_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Path(provider_id): Path<String>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(quota::get_quota(state.storage.clone(), provider_id).await).into_response()
}

async fn logs_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<LogsQuery>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    match state.storage.read_logs(query.limit.unwrap_or(200)).await {
        Ok(logs) => Json(logs).into_response(),
        Err(err) => api_error(StatusCode::INTERNAL_SERVER_ERROR, &err_string(err)),
    }
}

async fn self_check_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(req): Json<SelfCheckRequest>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(run_web_self_check(&state, req.model).await).into_response()
}

async fn codex_snippet_handler(
    State(state): State<WebState>,
    headers: HeaderMap,
    Query(query): Query<CodexSnippetQuery>,
) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    let model = query
        .model
        .filter(|item| is_gpt_model(item))
        .unwrap_or_else(|| "gpt-5.5".to_string());
    Json(codex_snippet(
        &state.gateway.status().await,
        &state.storage.config().await,
        model,
    ))
    .into_response()
}

async fn storage_path_handler(State(state): State<WebState>, headers: HeaderMap) -> Response {
    if let Err(resp) = require_admin(&state, &headers).await {
        return resp;
    }
    Json(StoragePathResponse {
        db_path: state.db_path.display().to_string(),
        data_dir: state.storage.dir().display().to_string(),
    })
    .into_response()
}

async fn require_admin(state: &WebState, headers: &HeaderMap) -> Result<(), Response> {
    let Some(token) = bearer_token(headers) else {
        return Err(api_error(StatusCode::UNAUTHORIZED, "admin login required"));
    };
    let expected = state.auth.read().await.session_token.clone();
    if token == expected {
        Ok(())
    } else {
        Err(api_error(StatusCode::UNAUTHORIZED, "admin login required"))
    }
}

fn bearer_token(headers: &HeaderMap) -> Option<String> {
    let raw = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())?;
    raw.strip_prefix("Bearer ")
        .map(str::to_string)
        .filter(|value| !value.trim().is_empty())
}

fn api_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "message": message,
                "type": "other_model_web_error"
            }
        })),
    )
        .into_response()
}

fn init_admin_auth(db_path: &PathBuf) -> Result<WebAuthState> {
    let password_from_env = std::env::var("OTHER_MODEL_ADMIN_PASSWORD")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if let Some(password) = password_from_env {
        let hash = hash_password(&password);
        sqlite_set_raw_app_value(db_path, ADMIN_HASH_KEY, &hash)?;
        eprintln!("Other Model Web admin password loaded from OTHER_MODEL_ADMIN_PASSWORD.");
        return Ok(WebAuthState {
            password_hash: hash,
            session_token: format!("om-session-{}", uuid::Uuid::new_v4()),
        });
    }

    if let Some(existing) = sqlite_get_raw_app_value(db_path, ADMIN_HASH_KEY)? {
        eprintln!(
            "Other Model Web admin password already exists. Set OTHER_MODEL_ADMIN_PASSWORD to reset it."
        );
        return Ok(WebAuthState {
            password_hash: existing,
            session_token: format!("om-session-{}", uuid::Uuid::new_v4()),
        });
    }

    let initial_password = format!("om-{}", uuid::Uuid::new_v4());
    let hash = hash_password(&initial_password);
    sqlite_set_raw_app_value(db_path, ADMIN_HASH_KEY, &hash)?;
    eprintln!("============================================================");
    eprintln!("Other Model Web initial admin password:");
    eprintln!("{initial_password}");
    eprintln!("Please save it now. You can reset it with OTHER_MODEL_ADMIN_PASSWORD.");
    eprintln!("============================================================");
    Ok(WebAuthState {
        password_hash: hash,
        session_token: format!("om-session-{}", uuid::Uuid::new_v4()),
    })
}

fn hash_password(password: &str) -> String {
    let salt = uuid::Uuid::new_v4().to_string();
    let digest = password_digest(&salt, password);
    format!("{salt}:{digest}")
}

fn verify_password(stored: &str, password: &str) -> bool {
    let Some((salt, expected)) = stored.split_once(':') else {
        return false;
    };
    password_digest(salt, password) == expected
}

fn password_digest(salt: &str, password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(salt.as_bytes());
    hasher.update(b":");
    hasher.update(password.as_bytes());
    to_hex(&hasher.finalize())
}

fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

async fn provider_views(storage: &Storage) -> Vec<ProviderView> {
    let cfg = storage.config().await;
    let runtime = storage.runtime_state().await;
    let cache = storage.models_cache().await;
    cfg.providers
        .into_iter()
        .map(|provider| {
            let st = runtime
                .providers
                .get(&provider.id)
                .cloned()
                .unwrap_or_else(|| ProviderState {
                    provider_id: provider.id.clone(),
                    ..Default::default()
                });
            let models = cache
                .providers
                .get(&provider.id)
                .map(|m| {
                    m.models
                        .iter()
                        .filter(|model| is_gpt_model(&model.id))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            ProviderView {
                config: provider,
                state: st,
                model_count: models.len(),
                gpt_model_count: models.len(),
            }
        })
        .collect()
}

async fn import_providers_inner(storage: &Storage, raw: &str) -> Result<ProviderImportResult> {
    let providers = parse_provider_import(raw)?;
    let total = providers.len();
    let mut skipped = 0usize;
    let mut imported = 0usize;
    let mut updated = 0usize;
    let mut seen = HashSet::<String>::new();

    storage
        .update_config(|cfg| {
            for mut provider in providers {
                if provider.id.trim().is_empty() {
                    provider.id = uuid::Uuid::new_v4().to_string();
                }
                provider.id = provider.id.trim().to_string();
                if seen.contains(&provider.id) || validate_provider(&provider).is_err() {
                    skipped += 1;
                    continue;
                }
                seen.insert(provider.id.clone());
                if let Some(existing) = cfg.providers.iter_mut().find(|p| p.id == provider.id) {
                    *existing = provider;
                    updated += 1;
                } else {
                    cfg.providers.push(provider);
                    imported += 1;
                }
            }
        })
        .await?;

    Ok(ProviderImportResult {
        ok: true,
        imported,
        updated,
        skipped,
        total,
        message: format!("导入完成：新增 {imported} 个，更新 {updated} 个，跳过 {skipped} 个。"),
    })
}

async fn run_web_self_check(state: &WebState, model: Option<String>) -> GatewaySelfCheckResult {
    let mut checks = Vec::<GatewaySelfCheckItem>::new();
    let status = state.gateway.status().await;
    checks.push(GatewaySelfCheckItem {
        name: "Web 服务 / 网关状态".to_string(),
        ok: status.running,
        status: None,
        latency_ms: 0,
        details: Some(status.bind_url.clone()),
    });

    let cfg = state.storage.config().await;
    let client = match reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(
            cfg.gateway.request_timeout_secs.max(30),
        ))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            checks.push(GatewaySelfCheckItem {
                name: "HTTP Client".to_string(),
                ok: false,
                status: None,
                latency_ms: 0,
                details: Some(err.to_string()),
            });
            return finish_self_check(checks);
        }
    };

    let auth_header = cfg
        .gateway
        .require_local_token
        .then(|| format!("Bearer {}", cfg.local_auth_token));
    let base = status.bind_url.trim_end_matches('/').to_string();
    let root = base.trim_end_matches("/v1").to_string();

    let start = Instant::now();
    match client.get(format!("{root}/health")).send().await {
        Ok(resp) => checks.push(GatewaySelfCheckItem {
            name: "健康检查".to_string(),
            ok: resp.status().is_success(),
            status: Some(resp.status().as_u16()),
            latency_ms: start.elapsed().as_millis(),
            details: Some("GET /health".to_string()),
        }),
        Err(err) => checks.push(GatewaySelfCheckItem {
            name: "健康检查".to_string(),
            ok: false,
            status: None,
            latency_ms: start.elapsed().as_millis(),
            details: Some(err.to_string()),
        }),
    }

    let start = Instant::now();
    let mut req = client.get(format!("{base}/models"));
    if let Some(auth) = &auth_header {
        req = req.header(reqwest::header::AUTHORIZATION, auth);
    }
    match req.send().await {
        Ok(resp) => {
            let status_code = resp.status();
            let text = resp.text().await.unwrap_or_default();
            checks.push(GatewaySelfCheckItem {
                name: "模型列表".to_string(),
                ok: status_code.is_success(),
                status: Some(status_code.as_u16()),
                latency_ms: start.elapsed().as_millis(),
                details: Some(text.chars().take(300).collect()),
            });
        }
        Err(err) => checks.push(GatewaySelfCheckItem {
            name: "模型列表".to_string(),
            ok: false,
            status: None,
            latency_ms: start.elapsed().as_millis(),
            details: Some(err.to_string()),
        }),
    }

    let selected_model = if let Some(item) = model.filter(|item| is_gpt_model(item)) {
        item
    } else {
        let cache = state.storage.models_cache().await;
        first_cached_gpt_model(&cache).unwrap_or_else(|| "gpt-5.5".to_string())
    };
    let small_body = json!({
        "model": selected_model,
        "input": "ping",
        "max_output_tokens": 16,
        "stream": false
    });
    let start = Instant::now();
    let mut req = client.post(format!("{base}/responses")).json(&small_body);
    if let Some(auth) = &auth_header {
        req = req.header(reqwest::header::AUTHORIZATION, auth);
    }
    match req.send().await {
        Ok(resp) => {
            let status_code = resp.status();
            let text = resp.text().await.unwrap_or_default();
            checks.push(GatewaySelfCheckItem {
                name: "Responses 小请求".to_string(),
                ok: status_code.is_success(),
                status: Some(status_code.as_u16()),
                latency_ms: start.elapsed().as_millis(),
                details: Some(text.chars().take(500).collect()),
            });
        }
        Err(err) => checks.push(GatewaySelfCheckItem {
            name: "Responses 小请求".to_string(),
            ok: false,
            status: None,
            latency_ms: start.elapsed().as_millis(),
            details: Some(err.to_string()),
        }),
    }

    checks.push(GatewaySelfCheckItem {
        name: "Failover 配置".to_string(),
        ok: !cfg.routing.auto_failover || status.enabled_provider_count > 1,
        status: None,
        latency_ms: 0,
        details: Some(format!(
            "auto_failover={}，启用供应商={}",
            cfg.routing.auto_failover, status.enabled_provider_count
        )),
    });

    finish_self_check(checks)
}

fn codex_snippet(status: &GatewayStatus, cfg: &AppConfig, model: String) -> CodexSnippetResponse {
    let provider_name = "other_model_web";
    let base_url = status.bind_url.clone();
    let token = cfg.local_auth_token.clone();
    let auto_compact_token_limit = cfg.gateway.codex_auto_compact_token_limit;
    let config_toml = format!(
        r#"model = "{model}"
model_provider = "{provider_name}"
model_auto_compact_token_limit = {auto_compact_token_limit}

[model_providers.{provider_name}]
name = "Other Model Web"
base_url = "{base_url}"
wire_api = "responses"
supports_websockets = false
experimental_bearer_token = "{token}"

[shell_environment_policy.set]
NO_PROXY = "localhost,127.0.0.1,::1"
no_proxy = "localhost,127.0.0.1,::1"
"#
    );
    let configure_script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p "$HOME/.codex"
CONFIG="$HOME/.codex/config.toml"
if [ -f "$CONFIG" ]; then
  cp "$CONFIG" "$CONFIG.other-model-web-bak-$(date +%Y%m%d-%H%M%S)"
fi
cat > "$CONFIG" <<'EOF'
{config_toml}
EOF
echo "Codex config written to $CONFIG"
echo "Restart your Codex CLI/terminal before testing."
"#
    );
    CodexSnippetResponse {
        model,
        base_url,
        bearer_token: token,
        auto_compact_token_limit,
        config_toml,
        configure_script,
        download_name: "configure-codex.sh".to_string(),
    }
}

fn web_db_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("OTHER_MODEL_DB") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    Ok(std::env::current_dir()?
        .join("data")
        .join("other-model.sqlite"))
}

fn web_static_dir() -> PathBuf {
    if let Ok(path) = std::env::var("OTHER_MODEL_WEB_DIST") {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            return PathBuf::from(trimmed);
        }
    }
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    for candidate in [
        cwd.join("dist"),
        cwd.join("../dist"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../dist"),
    ] {
        if candidate.join("index.html").exists() {
            return candidate;
        }
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../dist")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_roundtrips() {
        let hash = hash_password("secret");
        assert!(verify_password(&hash, "secret"));
        assert!(!verify_password(&hash, "wrong"));
    }

    #[tokio::test]
    async fn codex_snippet_contains_web_gateway_settings() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::load_sqlite(dir.path().join("test.sqlite"))
            .await
            .unwrap();
        let gateway = GatewayManager::new(storage.clone());
        gateway
            .mark_running_at("http://127.0.0.1:14556/v1".to_string())
            .await;
        let snippet = codex_snippet(
            &gateway.status().await,
            &storage.config().await,
            "gpt-5.5".to_string(),
        );
        assert!(snippet
            .config_toml
            .contains("base_url = \"http://127.0.0.1:14556/v1\""));
        assert!(snippet.config_toml.contains("wire_api = \"responses\""));
        assert!(snippet.config_toml.contains("experimental_bearer_token"));
        assert!(snippet.configure_script.contains("other-model-web-bak"));
    }
}
