use crate::models::{
    GatewayStatus, ModelInfo, ModelsCache, ProviderConfig, ProviderHealth, ProviderModels,
    ProviderState, RequestLogEntry,
};
use crate::storage::Storage;
use anyhow::{anyhow, Context, Result};
use async_stream::stream;
use axum::{
    body::{to_bytes, Body, Bytes},
    extract::{Path, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post},
    Json, Router,
};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock},
};
use tower_http::cors::{Any, CorsLayer};

#[derive(Clone)]
pub struct GatewayManager {
    inner: Arc<GatewayInner>,
}

struct GatewayInner {
    storage: Storage,
    rr_counter: AtomicUsize,
    running: AtomicBool,
    shutdown_tx: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    bind_url: RwLock<String>,
}

#[derive(Clone)]
struct GatewayAppState {
    manager: GatewayManager,
}

#[derive(Debug)]
struct UpstreamAttempt {
    status: Option<StatusCode>,
    latency_ms: u128,
    body: Option<Bytes>,
    headers: HeaderMap,
    error: Option<String>,
}

struct UpstreamStreamResult {
    provider: ProviderConfig,
    response: reqwest::Response,
    status: StatusCode,
    headers: HeaderMap,
    latency_ms: u128,
}

impl GatewayManager {
    pub fn new(storage: Storage) -> Self {
        Self {
            inner: Arc::new(GatewayInner {
                storage,
                rr_counter: AtomicUsize::new(0),
                running: AtomicBool::new(false),
                shutdown_tx: Mutex::new(None),
                bind_url: RwLock::new(String::new()),
            }),
        }
    }

    pub fn storage(&self) -> Storage {
        self.inner.storage.clone()
    }

    pub async fn start(&self) -> Result<GatewayStatus> {
        if self.inner.running.load(Ordering::SeqCst) {
            return Ok(self.status().await);
        }

        let cfg = self.inner.storage.config().await;
        let addr: SocketAddr = format!("{}:{}", cfg.gateway.host, cfg.gateway.port).parse()?;
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("bind gateway on {}", addr))?;
        let local_addr = listener.local_addr()?;
        let bind_url = format!("http://{}:{}/v1", local_addr.ip(), local_addr.port());
        *self.inner.bind_url.write().await = bind_url;

        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/v1/models", get(list_models_handler))
            .route("/v1/models/:model", get(get_model_handler))
            .route("/v1/responses", post(responses_handler))
            .route("/v1/chat/completions", post(chat_completions_handler))
            .route("/v1/*path", any(proxy_handler))
            .layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any),
            )
            .with_state(GatewayAppState {
                manager: self.clone(),
            });

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        *self.inner.shutdown_tx.lock().await = Some(tx);
        self.inner.running.store(true, Ordering::SeqCst);
        let manager = self.clone();
        tokio::spawn(async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = rx.await;
                })
                .await;
            if let Err(err) = result {
                eprintln!("gateway server error: {err}");
            }
            manager.inner.running.store(false, Ordering::SeqCst);
        });

        Ok(self.status().await)
    }

    pub async fn stop(&self) -> Result<GatewayStatus> {
        if let Some(tx) = self.inner.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        self.inner.running.store(false, Ordering::SeqCst);
        Ok(self.status().await)
    }

    pub async fn status(&self) -> GatewayStatus {
        let cfg = self.inner.storage.config().await;
        let bind_url = {
            let current = self.inner.bind_url.read().await.clone();
            if current.is_empty() {
                format!("http://{}:{}/v1", cfg.gateway.host, cfg.gateway.port)
            } else {
                current
            }
        };
        GatewayStatus {
            running: self.inner.running.load(Ordering::SeqCst),
            bind_url,
            provider_count: cfg.providers.len(),
            enabled_provider_count: cfg
                .providers
                .iter()
                .filter(|p| p.enabled && !p.api_key.trim().is_empty())
                .count(),
        }
    }

    pub async fn discover_models(&self) -> Result<ModelsCache> {
        let cfg = self.inner.storage.config().await;
        let mut cache = ModelsCache {
            refreshed_at: Some(Utc::now()),
            providers: BTreeMap::new(),
        };

        for provider in cfg
            .providers
            .iter()
            .filter(|p| p.enabled && !p.api_key.trim().is_empty())
        {
            let fetched_at = Utc::now();
            match self.fetch_provider_models(provider).await {
                Ok(models) => {
                    self.record_provider_success(provider, Some(200), None, None)
                        .await?;
                    cache.providers.insert(
                        provider.id.clone(),
                        ProviderModels {
                            provider_id: provider.id.clone(),
                            fetched_at: Some(fetched_at),
                            models,
                            error: None,
                        },
                    );
                }
                Err(err) => {
                    self.record_provider_failure(provider, None, err.to_string())
                        .await?;
                    cache.providers.insert(
                        provider.id.clone(),
                        ProviderModels {
                            provider_id: provider.id.clone(),
                            fetched_at: Some(fetched_at),
                            models: Vec::new(),
                            error: Some(err.to_string()),
                        },
                    );
                }
            }
        }

        self.inner.storage.set_models_cache(cache.clone()).await?;
        Ok(cache)
    }

    pub async fn fetch_provider_models(&self, provider: &ProviderConfig) -> Result<Vec<ModelInfo>> {
        let url = join_url(&provider.base_url, "models");
        let start = Instant::now();
        let resp = self
            .client_for_provider(provider)
            .await?
            .get(url)
            .bearer_auth(&provider.api_key)
            .send()
            .await?;
        let status = resp.status();
        let value: Value = resp.json().await.unwrap_or_else(|_| json!({}));
        let latency = start.elapsed().as_millis();
        if !status.is_success() {
            return Err(anyhow!(
                "models request failed with {}: {}",
                status.as_u16(),
                compact_json(&value)
            ));
        }
        let data = value
            .get("data")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let models = data
            .into_iter()
            .filter_map(|raw| {
                let id = raw.get("id")?.as_str()?.to_string();
                Some(ModelInfo {
                    id,
                    object: raw
                        .get("object")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    created: raw.get("created").and_then(|v| v.as_i64()),
                    owned_by: raw
                        .get("owned_by")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    raw,
                })
            })
            .collect::<Vec<_>>();
        self.record_provider_success(
            provider,
            Some(status.as_u16()),
            Some(latency),
            rate_limit_hint(&HeaderMap::new()),
        )
        .await?;
        Ok(models)
    }

    pub async fn test_provider_model(
        &self,
        provider_id: String,
        model: Option<String>,
    ) -> crate::models::TestResult {
        let cfg = self.inner.storage.config().await;
        let Some(provider) = cfg.providers.into_iter().find(|p| p.id == provider_id) else {
            return crate::models::TestResult {
                ok: false,
                provider_id,
                error: Some("provider not found".to_string()),
                ..Default::default()
            };
        };
        let model = model.or_else(|| Some("gpt-5.4".to_string()));
        let start = Instant::now();
        let body = json!({
            "model": model.clone().unwrap_or_default(),
            "input": "ping",
            "max_output_tokens": 16,
            "stream": false
        });
        let url = join_url(&provider.base_url, "responses");
        match self
            .client_for_provider(&provider)
            .await
            .map(|c| c.post(url).bearer_auth(&provider.api_key).json(&body))
        {
            Ok(req) => match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    let ok = status.is_success();
                    if ok {
                        let _ = self
                            .record_provider_success(
                                &provider,
                                Some(status.as_u16()),
                                Some(start.elapsed().as_millis()),
                                None,
                            )
                            .await;
                    } else {
                        let _ = self
                            .record_provider_failure(
                                &provider,
                                Some(status.as_u16()),
                                text.chars().take(300).collect(),
                            )
                            .await;
                    }
                    crate::models::TestResult {
                        ok,
                        provider_id: provider.id,
                        model,
                        status: Some(status.as_u16()),
                        latency_ms: start.elapsed().as_millis(),
                        error: if ok {
                            None
                        } else {
                            Some(text.chars().take(600).collect())
                        },
                        response_preview: if ok {
                            Some(text.chars().take(600).collect())
                        } else {
                            None
                        },
                    }
                }
                Err(err) => {
                    let _ = self
                        .record_provider_failure(&provider, None, err.to_string())
                        .await;
                    crate::models::TestResult {
                        ok: false,
                        provider_id: provider.id,
                        model,
                        latency_ms: start.elapsed().as_millis(),
                        error: Some(err.to_string()),
                        ..Default::default()
                    }
                }
            },
            Err(err) => crate::models::TestResult {
                ok: false,
                provider_id: provider.id,
                model,
                error: Some(err.to_string()),
                ..Default::default()
            },
        }
    }

    async fn client_for_provider(&self, provider: &ProviderConfig) -> Result<Client> {
        let cfg = self.inner.storage.config().await;
        let timeout_secs = provider
            .timeout_secs
            .max(cfg.gateway.request_timeout_secs)
            .max(1);
        Ok(Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(timeout_secs))
            .no_proxy()
            .build()?)
    }

    async fn stream_client_for_provider(&self, _provider: &ProviderConfig) -> Result<Client> {
        let cfg = self.inner.storage.config().await;
        Ok(Client::builder()
            .pool_idle_timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(15))
            .read_timeout(Duration::from_secs(
                cfg.gateway.stream_idle_timeout_secs.max(1),
            ))
            .no_proxy()
            .build()?)
    }

    async fn enabled_providers(&self) -> Vec<ProviderConfig> {
        let cfg = self.inner.storage.config().await;
        let state = self.inner.storage.runtime_state().await;
        let now = Utc::now();
        cfg.providers
            .into_iter()
            .filter(|p| p.enabled && !p.api_key.trim().is_empty())
            .filter(|p| {
                if let Some(st) = state.providers.get(&p.id) {
                    if st.health == ProviderHealth::Disabled
                        || st.health == ProviderHealth::AuthFailed
                    {
                        return false;
                    }
                    if let Some(until) = st.cooldown_until {
                        return until <= now;
                    }
                }
                true
            })
            .collect()
    }

    async fn ordered_providers(
        &self,
        forced_provider_id: Option<String>,
        auto_round_robin: bool,
        selected_provider_id: Option<String>,
    ) -> Vec<ProviderConfig> {
        let providers = self.enabled_providers().await;
        if providers.is_empty() {
            return providers;
        }
        if let Some(id) = forced_provider_id {
            if let Some(provider) = providers.iter().find(|p| p.id == id) {
                return vec![provider.clone()];
            }
        }
        if !auto_round_robin {
            if let Some(id) = selected_provider_id
                .as_deref()
                .map(str::trim)
                .filter(|id| !id.is_empty())
            {
                if let Some(index) = providers.iter().position(|p| p.id == id) {
                    return providers
                        .iter()
                        .cycle()
                        .skip(index)
                        .take(providers.len())
                        .cloned()
                        .collect();
                }
            }
        }
        let start = if auto_round_robin {
            self.inner.rr_counter.fetch_add(1, Ordering::SeqCst) % providers.len()
        } else {
            0
        };
        providers
            .iter()
            .cycle()
            .skip(start)
            .take(providers.len())
            .cloned()
            .collect()
    }

    async fn proxy_http_request(
        &self,
        method: Method,
        path: String,
        request: Request<Body>,
        forced_provider_id: Option<String>,
    ) -> Response {
        let started = Instant::now();
        let headers = request.headers().clone();
        if let Err(resp) = self.authorize_local(&headers).await {
            let mut log = RequestLogEntry {
                method: method.to_string(),
                path: format!("/v1/{path}"),
                status: Some(StatusCode::UNAUTHORIZED.as_u16()),
                latency_ms: started.elapsed().as_millis(),
                body_size_bytes: content_length(&headers),
                local_rejected: true,
                error: Some("local gateway authorization failed".to_string()),
                ..Default::default()
            };
            log.streamed = false;
            let _ = self.inner.storage.append_log(&log).await;
            return resp;
        }

        let cfg = self.inner.storage.config().await;
        let limit = max_request_body_bytes(cfg.gateway.max_request_body_mb);
        if let Some(size) = content_length(&headers) {
            if size > limit as u64 {
                return self
                    .reject_large_body(method, path, headers, started, size, limit, None)
                    .await;
            }
        }

        match to_bytes(request.into_body(), limit).await {
            Ok(body) => {
                self.proxy_request(method, path, headers, body, forced_provider_id)
                    .await
            }
            Err(err) => {
                let err_text = err.to_string();
                let status = if err_text.contains("length limit exceeded") {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::BAD_REQUEST
                };
                if status == StatusCode::PAYLOAD_TOO_LARGE {
                    let size = content_length(&headers).unwrap_or(limit as u64 + 1);
                    self.reject_large_body(
                        method,
                        path,
                        headers,
                        started,
                        size,
                        limit,
                        Some(err_text),
                    )
                    .await
                } else {
                    let mut log = RequestLogEntry {
                        method: method.to_string(),
                        path: format!("/v1/{path}"),
                        status: Some(status.as_u16()),
                        latency_ms: started.elapsed().as_millis(),
                        body_size_bytes: content_length(&headers),
                        local_rejected: true,
                        error: Some(format!("failed to read request body: {err_text}")),
                        ..Default::default()
                    };
                    log.streamed = false;
                    let _ = self.inner.storage.append_log(&log).await;
                    json_error(status, &format!("failed to read request body: {err_text}"))
                }
            }
        }
    }

    async fn reject_large_body(
        &self,
        method: Method,
        path: String,
        _headers: HeaderMap,
        started: Instant,
        size: u64,
        limit: usize,
        source_error: Option<String>,
    ) -> Response {
        let message = format!(
            "local gateway request body is too large: received at least {} bytes, limit is {} bytes ({} MB). Increase Settings → Max request body MB if needed.",
            size,
            limit,
            limit / 1024 / 1024
        );
        let mut log = RequestLogEntry {
            method: method.to_string(),
            path: format!("/v1/{path}"),
            status: Some(StatusCode::PAYLOAD_TOO_LARGE.as_u16()),
            latency_ms: started.elapsed().as_millis(),
            body_size_bytes: Some(size),
            local_rejected: true,
            error: Some(match source_error {
                Some(err) => format!("{message}; extractor error: {err}"),
                None => message.clone(),
            }),
            ..Default::default()
        };
        log.streamed = false;
        let _ = self.inner.storage.append_log(&log).await;
        json_error(StatusCode::PAYLOAD_TOO_LARGE, &message)
    }

    async fn proxy_request(
        &self,
        method: Method,
        path: String,
        headers: HeaderMap,
        body: Bytes,
        forced_provider_id: Option<String>,
    ) -> Response {
        let started = Instant::now();
        let model = extract_model_from_body(&body);
        let streamed = is_stream_request(&body);
        let mut log = RequestLogEntry {
            method: method.to_string(),
            path: format!("/v1/{path}"),
            model,
            streamed,
            body_size_bytes: Some(body.len() as u64),
            ..Default::default()
        };
        eprintln!(
            "apidev gateway request id={} method={} path=/v1/{} streamed={} model={:?} auth_present={}",
            log.id,
            method,
            path,
            streamed,
            log.model,
            headers.get(http::header::AUTHORIZATION).is_some()
        );
        if let Err(resp) = self.authorize_local(&headers).await {
            log.error = Some("local gateway authorization failed".to_string());
            log.status = Some(StatusCode::UNAUTHORIZED.as_u16());
            log.local_rejected = true;
            log.latency_ms = started.elapsed().as_millis();
            let _ = self.inner.storage.append_log(&log).await;
            eprintln!("apidev gateway auth failed id={}", log.id);
            return resp;
        }
        let cfg = self.inner.storage.config().await;
        let providers = self
            .ordered_providers(
                forced_provider_id,
                cfg.routing.auto_round_robin,
                cfg.routing.selected_provider_id.clone(),
            )
            .await;
        if providers.is_empty() {
            log.error = Some("no enabled providers".to_string());
            log.latency_ms = started.elapsed().as_millis();
            let _ = self.inner.storage.append_log(&log).await;
            return json_error(StatusCode::BAD_GATEWAY, "no enabled upstream providers");
        }
        let max_attempts = if cfg.routing.auto_failover {
            cfg.routing
                .max_attempts_per_request
                .max(1)
                .min(providers.len())
        } else {
            1
        };
        let mut last_error = None;
        let auto_failover = cfg.routing.auto_failover;

        if streamed {
            return self
                .proxy_stream_request(
                    providers,
                    max_attempts,
                    method,
                    path,
                    headers,
                    body,
                    log,
                    started,
                    auto_failover,
                )
                .await;
        }

        for provider in providers.into_iter().take(max_attempts) {
            log.attempts += 1;
            match self
                .send_upstream(&provider, &method, &path, &headers, body.clone())
                .await
            {
                Ok(attempt) => {
                    log.provider_id = Some(provider.id.clone());
                    log.provider_name = Some(provider.name.clone());
                    log.status = attempt.status.map(|s| s.as_u16());
                    log.latency_ms = started.elapsed().as_millis();
                    log.error = attempt.error.clone();
                    if let Some(status) = attempt.status {
                        if auto_failover && should_failover_status(status, attempt.error.as_deref())
                        {
                            let err = attempt.error.clone().unwrap_or_else(|| {
                                format!("upstream returned {}", status.as_u16())
                            });
                            log.failover_reason = Some(err.clone());
                            let _ = self
                                .record_provider_failure(
                                    &provider,
                                    Some(status.as_u16()),
                                    err.clone(),
                                )
                                .await;
                            last_error = Some(err);
                            continue;
                        }
                        if status.is_success() {
                            log.error = None;
                            let _ = self
                                .record_provider_success(
                                    &provider,
                                    Some(status.as_u16()),
                                    Some(attempt.latency_ms),
                                    rate_limit_hint(&attempt.headers),
                                )
                                .await;
                        } else {
                            let err = attempt.error.clone().unwrap_or_else(|| {
                                format!("upstream returned {}", status.as_u16())
                            });
                            let _ = self
                                .record_provider_failure(&provider, Some(status.as_u16()), err)
                                .await;
                        }
                    }

                    if let Some(bytes) = attempt.body {
                        if let Ok(value) = serde_json::from_slice::<Value>(&bytes) {
                            if let Some(id) = value.get("id").and_then(|v| v.as_str()) {
                                let _ = self
                                    .remember_response_route(id.to_string(), provider.id.clone())
                                    .await;
                            }
                            log.usage = value.get("usage").cloned();
                        }
                        let status = attempt.status.unwrap_or(StatusCode::OK);
                        let headers =
                            sanitize_response_headers(attempt.headers.clone(), bytes.len());
                        let _ = self.inner.storage.append_log(&log).await;
                        return (status, headers, bytes).into_response();
                    }
                }
                Err(err) => {
                    let err_text = err.to_string();
                    let _ = self
                        .record_provider_failure(&provider, None, err_text.clone())
                        .await;
                    last_error = Some(err_text);
                    continue;
                }
            }
        }

        log.error = last_error.clone();
        log.latency_ms = started.elapsed().as_millis();
        let _ = self.inner.storage.append_log(&log).await;
        json_error(
            StatusCode::BAD_GATEWAY,
            &last_error.unwrap_or_else(|| "all upstream providers failed".to_string()),
        )
    }

    async fn proxy_stream_request(
        &self,
        providers: Vec<ProviderConfig>,
        max_attempts: usize,
        method: Method,
        path: String,
        headers: HeaderMap,
        body: Bytes,
        mut log: RequestLogEntry,
        started: Instant,
        auto_failover: bool,
    ) -> Response {
        let mut last_error = None;
        for provider in providers.into_iter().take(max_attempts) {
            log.attempts += 1;
            eprintln!(
                "apidev stream attempt id={} provider={} path=/v1/{}",
                log.id, provider.name, path
            );
            match self
                .open_stream_upstream(&provider, &method, &path, &headers, body.clone())
                .await
            {
                Ok(opened) => {
                    eprintln!(
                        "apidev stream opened id={} provider={} status={} content_type={:?}",
                        log.id,
                        opened.provider.name,
                        opened.status.as_u16(),
                        opened.headers.get(http::header::CONTENT_TYPE)
                    );
                    if !opened.status.is_success() {
                        let status = opened.status;
                        let headers = opened.headers.clone();
                        let text = opened.response.text().await.unwrap_or_default();
                        let err = format!(
                            "upstream {} returned {} before stream output: {}",
                            provider.name,
                            status.as_u16(),
                            text.chars().take(700).collect::<String>()
                        );
                        let _ = self
                            .record_provider_failure(&provider, Some(status.as_u16()), err.clone())
                            .await;
                        if auto_failover && should_failover_status(status, Some(&text)) {
                            log.failover_reason = Some(err.clone());
                            last_error = Some(err);
                            continue;
                        }
                        let mut stream_log = log.clone();
                        stream_log.provider_id = Some(provider.id.clone());
                        stream_log.provider_name = Some(provider.name.clone());
                        stream_log.status = Some(status.as_u16());
                        stream_log.latency_ms = started.elapsed().as_millis();
                        stream_log.error = Some(err);
                        let _ = self.inner.storage.append_log(&stream_log).await;
                        let body = Bytes::from(text);
                        let headers = sanitize_response_headers(headers, body.len());
                        return (status, headers, body).into_response();
                    }
                    let mut stream_log = log.clone();
                    stream_log.provider_id = Some(opened.provider.id.clone());
                    stream_log.provider_name = Some(opened.provider.name.clone());
                    stream_log.status = Some(opened.status.as_u16());
                    stream_log.latency_ms = started.elapsed().as_millis();
                    return self
                        .stream_opened_upstream(opened, stream_log, started)
                        .await;
                }
                Err(err) => {
                    let err_text = format!("{}: {}", provider.name, err);
                    eprintln!("apidev stream open error id={} {}", log.id, err_text);
                    let _ = self
                        .record_provider_failure(&provider, None, err_text.clone())
                        .await;
                    last_error = Some(err_text);
                    continue;
                }
            }
        }
        log.error = last_error.clone();
        log.latency_ms = started.elapsed().as_millis();
        let _ = self.inner.storage.append_log(&log).await;
        {
            let msg = last_error.unwrap_or_else(|| {
                "all upstream providers failed before stream output".to_string()
            });
            eprintln!("apidev stream 502 id={} error={}", log.id, msg);
            json_error(StatusCode::BAD_GATEWAY, &msg)
        }
    }

    async fn open_stream_upstream(
        &self,
        provider: &ProviderConfig,
        method: &Method,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<UpstreamStreamResult> {
        let start = Instant::now();
        let client = self.stream_client_for_provider(provider).await?;
        let url = join_url(&provider.base_url, path);
        let mut req = client
            .request(method.clone(), url)
            .bearer_auth(&provider.api_key);
        req = apply_provider_headers(req, provider, headers);
        for (k, v) in &provider.query {
            req = req.query(&[(k, v)]);
        }
        if method != Method::GET && method != Method::HEAD {
            req = req.body(body);
        }
        let response = req.send().await?;
        let status = response.status();
        let headers = to_axum_headers(response.headers());
        Ok(UpstreamStreamResult {
            provider: provider.clone(),
            response,
            status,
            headers,
            latency_ms: start.elapsed().as_millis(),
        })
    }

    async fn send_upstream(
        &self,
        provider: &ProviderConfig,
        method: &Method,
        path: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<UpstreamAttempt> {
        let start = Instant::now();
        let client = self.client_for_provider(provider).await?;
        let url = join_url(&provider.base_url, path);
        let mut req = client
            .request(method.clone(), url)
            .bearer_auth(&provider.api_key);
        req = apply_provider_headers(req, provider, headers);
        for (k, v) in &provider.query {
            req = req.query(&[(k, v)]);
        }
        if method != Method::GET && method != Method::HEAD {
            req = req.body(body);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let headers = to_axum_headers(resp.headers());
        let content_type = headers
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let streamed = content_type.contains("text/event-stream");
        if streamed && status.is_success() {
            Ok(UpstreamAttempt {
                status: Some(status),
                latency_ms: start.elapsed().as_millis(),
                body: None,
                headers,
                error: None,
            })
        } else {
            let bytes = resp.bytes().await.unwrap_or_default();
            let error = if status.is_success() {
                None
            } else {
                Some(String::from_utf8_lossy(&bytes).chars().take(700).collect())
            };
            Ok(UpstreamAttempt {
                status: Some(status),
                latency_ms: start.elapsed().as_millis(),
                body: Some(bytes),
                headers,
                error,
            })
        }
    }

    async fn stream_opened_upstream(
        &self,
        opened: UpstreamStreamResult,
        mut log: RequestLogEntry,
        started: Instant,
    ) -> Response {
        let status = opened.status;
        let headers = sanitize_stream_headers(opened.headers.clone());
        let manager = self.clone();
        let provider_for_stream = opened.provider.clone();
        let initial_latency = opened.latency_ms;
        let stream = stream! {
            let mut upstream = opened.response.bytes_stream();
            let mut keepalive = tokio::time::interval(Duration::from_secs(3));
            keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut first = true;
            let mut saw_error = false;
            loop {
                tokio::select! {
                    item = upstream.next() => {
                        let Some(item) = item else { break; };
                        match item {
                            Ok(chunk) => {
                                if chunk.is_empty() {
                                    continue;
                                }
                                if first {
                                    first = false;
                                    if status.is_success() {
                                        let _ = manager.record_provider_success(&provider_for_stream, Some(status.as_u16()), Some(initial_latency), None).await;
                                    }
                                }
                                if let Some(id) = extract_response_id_from_sse(&chunk) {
                                    let _ = manager.remember_response_route(id, provider_for_stream.id.clone()).await;
                                }
                                yield Ok::<Bytes, std::io::Error>(chunk);
                            }
                            Err(err) => {
                                saw_error = true;
                                eprintln!("apidev stream interrupted id={} provider={} error={}", log.id, provider_for_stream.name, err);
                                let err_msg = format!("stream interrupted: {err}");
                                let _ = manager.record_provider_degraded(&provider_for_stream, err_msg.clone()).await;
                                log.error = Some(err_msg.clone());
                                let msg = format!("event: error\ndata: {{\"error\":{{\"message\":\"upstream stream interrupted after output: {}\"}}}}\n\n", escape_json_string(&err.to_string()));
                                yield Ok::<Bytes, std::io::Error>(Bytes::from(msg));
                                break;
                            }
                        }
                    }
                    _ = keepalive.tick(), if first => {
                        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b": other-model keepalive\n\n"));
                    }
                }
            }
            log.status = Some(status.as_u16());
            log.latency_ms = started.elapsed().as_millis();
            if saw_error && log.error.is_none() {
                log.error = Some("stream interrupted".to_string());
            }
            let _ = manager.inner.storage.append_log(&log).await;
        };
        (status, headers, Body::from_stream(stream)).into_response()
    }

    async fn authorize_local(&self, headers: &HeaderMap) -> Result<(), Response> {
        let cfg = self.inner.storage.config().await;
        if !cfg.gateway.require_local_token {
            return Ok(());
        }
        let expected = format!("Bearer {}", cfg.local_auth_token);
        let got = headers
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if got == expected {
            Ok(())
        } else {
            Err(json_error(
                StatusCode::UNAUTHORIZED,
                "invalid local gateway token",
            ))
        }
    }

    async fn remember_response_route(
        &self,
        response_id: String,
        provider_id: String,
    ) -> Result<()> {
        self.inner
            .storage
            .update_runtime_state(|state| {
                state.response_routes.insert(response_id, provider_id);
            })
            .await
    }

    pub async fn provider_for_response_path(&self, path: &str) -> Option<String> {
        let parts: Vec<_> = path.split('/').collect();
        let id = if parts.len() >= 2 && parts[0] == "responses" {
            parts[1]
        } else {
            return None;
        };
        let state = self.inner.storage.runtime_state().await;
        state.response_routes.get(id).cloned()
    }

    async fn record_provider_success(
        &self,
        provider: &ProviderConfig,
        status: Option<u16>,
        latency_ms: Option<u128>,
        remaining_hint: Option<String>,
    ) -> Result<()> {
        let id = provider.id.clone();
        self.inner
            .storage
            .update_runtime_state(|state| {
                let entry = state
                    .providers
                    .entry(id.clone())
                    .or_insert_with(|| ProviderState {
                        provider_id: id.clone(),
                        ..Default::default()
                    });
                entry.health = if provider.enabled {
                    ProviderHealth::Healthy
                } else {
                    ProviderHealth::Disabled
                };
                entry.last_checked_at = Some(Utc::now());
                entry.cooldown_until = None;
                entry.consecutive_failures = 0;
                entry.last_error = None;
                entry.last_status = status;
                if let Some(ms) = latency_ms {
                    entry.last_latency_ms = Some(ms);
                }
                if remaining_hint.is_some() {
                    entry.remaining_hint = remaining_hint;
                }
            })
            .await
    }

    async fn record_provider_degraded(
        &self,
        provider: &ProviderConfig,
        error: String,
    ) -> Result<()> {
        let id = provider.id.clone();
        self.inner
            .storage
            .update_runtime_state(|state| {
                let entry = state
                    .providers
                    .entry(id.clone())
                    .or_insert_with(|| ProviderState {
                        provider_id: id.clone(),
                        ..Default::default()
                    });
                entry.last_checked_at = Some(Utc::now());
                entry.last_error = Some(error);
                entry.health = ProviderHealth::Degraded;
                entry.cooldown_until = None;
            })
            .await
    }

    async fn record_provider_failure(
        &self,
        provider: &ProviderConfig,
        status: Option<u16>,
        error: String,
    ) -> Result<()> {
        let id = provider.id.clone();
        let cooldown_secs = self.inner.storage.config().await.routing.cooldown_secs;
        self.inner
            .storage
            .update_runtime_state(|state| {
                let entry = state
                    .providers
                    .entry(id.clone())
                    .or_insert_with(|| ProviderState {
                        provider_id: id.clone(),
                        ..Default::default()
                    });
                entry.last_checked_at = Some(Utc::now());
                entry.consecutive_failures += 1;
                entry.last_error = Some(error.clone());
                entry.last_status = status;
                entry.health = match status {
                    Some(401) | Some(403) => ProviderHealth::AuthFailed,
                    Some(402) | Some(429) => ProviderHealth::CoolingDown,
                    Some(s) if s >= 500 => ProviderHealth::CoolingDown,
                    _ if entry.consecutive_failures >= 3 => ProviderHealth::CoolingDown,
                    _ => ProviderHealth::Degraded,
                };
                if matches!(entry.health, ProviderHealth::CoolingDown) {
                    entry.cooldown_until =
                        Some(Utc::now() + ChronoDuration::seconds(cooldown_secs.max(5)));
                }
            })
            .await
    }
}

async fn health_handler(State(state): State<GatewayAppState>) -> Json<Value> {
    Json(json!({ "ok": true, "status": state.manager.status().await }))
}

async fn list_models_handler(State(state): State<GatewayAppState>, headers: HeaderMap) -> Response {
    if let Err(resp) = state.manager.authorize_local(&headers).await {
        return resp;
    }
    let cache = state.manager.storage().models_cache().await;
    let mut seen = HashSet::new();
    let mut data = Vec::new();
    for provider in cache.providers.values() {
        for model in &provider.models {
            if seen.insert(model.id.clone()) {
                data.push(model.clone());
            }
        }
    }
    data.sort_by(|a, b| a.id.cmp(&b.id));
    let catalog_models: Vec<Value> = data.iter().map(codex_catalog_model).collect();
    Json(json!({
        "object": "list",
        "data": data,
        "models": catalog_models,
    }))
    .into_response()
}

async fn get_model_handler(
    State(state): State<GatewayAppState>,
    headers: HeaderMap,
    Path(model): Path<String>,
) -> Response {
    if let Err(resp) = state.manager.authorize_local(&headers).await {
        return resp;
    }
    let cache = state.manager.storage().models_cache().await;
    for provider in cache.providers.values() {
        for item in &provider.models {
            if item.id == model {
                return Json(item).into_response();
            }
        }
    }
    json_error(
        StatusCode::NOT_FOUND,
        "model not found in local cache; run discover models",
    )
}

async fn responses_handler(
    State(state): State<GatewayAppState>,
    request: Request<Body>,
) -> Response {
    state
        .manager
        .proxy_http_request(Method::POST, "responses".to_string(), request, None)
        .await
}

async fn chat_completions_handler(
    State(state): State<GatewayAppState>,
    request: Request<Body>,
) -> Response {
    state
        .manager
        .proxy_http_request(Method::POST, "chat/completions".to_string(), request, None)
        .await
}

async fn proxy_handler(State(state): State<GatewayAppState>, request: Request<Body>) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().trim_start_matches("/v1/").to_string();
    let forced = state.manager.provider_for_response_path(&path).await;
    state
        .manager
        .proxy_http_request(method, path, request, forced)
        .await
}

pub fn is_gpt_model(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    lower.starts_with("gpt") || lower.contains("/gpt") || lower.contains("gpt-")
}

pub fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn should_failover_status(status: StatusCode, body_preview: Option<&str>) -> bool {
    status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::PAYMENT_REQUIRED
        || status == StatusCode::UNAUTHORIZED
        || status == StatusCode::FORBIDDEN
        || status == StatusCode::PAYLOAD_TOO_LARGE
        || status.is_server_error()
        || (matches!(status, StatusCode::NOT_FOUND | StatusCode::BAD_REQUEST)
            && looks_like_model_routing_error(body_preview.unwrap_or_default()))
}

fn looks_like_model_routing_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    let has_model = lower.contains("model")
        || lower.contains("模型")
        || lower.contains("deployment")
        || lower.contains("engine");
    has_model
        && (lower.contains("not found")
            || lower.contains("does not exist")
            || lower.contains("not exist")
            || lower.contains("not supported")
            || lower.contains("unsupported")
            || lower.contains("不存在")
            || lower.contains("不支持")
            || lower.contains("无效"))
}

fn max_request_body_bytes(max_request_body_mb: u64) -> usize {
    let bytes = max_request_body_mb.max(1).saturating_mul(1024 * 1024);
    bytes.min(usize::MAX as u64) as usize
}

fn content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn extract_model_from_body(body: &[u8]) -> Option<String> {
    let value = serde_json::from_slice::<Value>(body).ok()?;
    value
        .get("model")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn is_stream_request(body: &[u8]) -> bool {
    let value = serde_json::from_slice::<Value>(body).ok();
    value
        .and_then(|v| v.get("stream").and_then(|s| s.as_bool()))
        .unwrap_or(false)
}

fn apply_provider_headers(
    mut req: reqwest::RequestBuilder,
    provider: &ProviderConfig,
    incoming: &HeaderMap,
) -> reqwest::RequestBuilder {
    for (name, value) in incoming.iter() {
        if is_hop_by_hop(name.as_str())
            || name == http::header::AUTHORIZATION
            || name == http::header::HOST
            || name.as_str().eq_ignore_ascii_case("content-length")
        {
            continue;
        }
        if let Ok(v) = value.to_str() {
            req = req.header(name.as_str(), v);
        }
    }
    for (name, value) in &provider.headers {
        req = req.header(name, value);
    }
    req
}

fn to_axum_headers(headers: &reqwest::header::HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers.iter() {
        if let Ok(header_name) = HeaderName::from_bytes(name.as_str().as_bytes()) {
            if let Ok(header_value) = HeaderValue::from_bytes(value.as_bytes()) {
                out.insert(header_name, header_value);
            }
        }
    }
    out
}

fn sanitize_response_headers(mut headers: HeaderMap, len: usize) -> HeaderMap {
    remove_hop_by_hop_headers(&mut headers);
    headers.remove(http::header::CONTENT_LENGTH);
    if let Ok(v) = HeaderValue::from_str(&len.to_string()) {
        headers.insert(http::header::CONTENT_LENGTH, v);
    }
    headers
}

fn sanitize_stream_headers(mut headers: HeaderMap) -> HeaderMap {
    remove_hop_by_hop_headers(&mut headers);
    headers.remove(http::header::CONTENT_LENGTH);
    headers.insert(
        http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    headers
}

fn remove_hop_by_hop_headers(headers: &mut HeaderMap) {
    let names: Vec<_> = headers.keys().cloned().collect();
    for name in names {
        if is_hop_by_hop(name.as_str()) {
            headers.remove(name);
        }
    }
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn json_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": { "message": message, "type": "other_model_gateway_error" } })),
    )
        .into_response()
}

fn compact_json(value: &Value) -> String {
    value.to_string().chars().take(700).collect()
}

fn rate_limit_hint(headers: &HeaderMap) -> Option<String> {
    for key in [
        "x-ratelimit-remaining-requests",
        "x-ratelimit-remaining-tokens",
        "x-request-cost",
        "x-remaining-credits",
    ] {
        if let Some(value) = headers.get(key).and_then(|v| v.to_str().ok()) {
            return Some(format!("{key}: {value}"));
        }
    }
    None
}

fn extract_response_id_from_sse(chunk: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(chunk).ok()?;
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data: ") {
            if data.trim() == "[DONE]" {
                continue;
            }
            if let Ok(value) = serde_json::from_str::<Value>(data) {
                if let Some(id) = value
                    .get("response")
                    .and_then(|r| r.get("id"))
                    .and_then(|v| v.as_str())
                {
                    return Some(id.to_string());
                }
                if let Some(id) = value.get("id").and_then(|v| v.as_str()) {
                    return Some(id.to_string());
                }
            }
        }
    }
    None
}

fn escape_json_string(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

fn codex_catalog_model(model: &ModelInfo) -> Value {
    json!({
        "slug": model.id,
        "display_name": model.id,
        "description": format!("Discovered by Other Model from upstream provider cache: {}", model.id),
        "base_instructions": "You are Codex, a coding agent based on GPT-5. You and the user share one workspace, and your job is to collaborate with them until their goal is handled.",
        "model_messages": {
            "instructions_template": "You are Codex, a coding agent based on GPT-5. You and the user share one workspace, and your job is to collaborate with them until their goal is handled.\n\n{{ personality }}",
            "instructions_variables": {
                "personality_default": "",
                "personality_friendly": "# Personality\n\nYou are warm, collaborative, and concise.",
                "personality_pragmatic": "# Personality\n\nYou are pragmatic, direct, and concise."
            }
        },
        "default_reasoning_level": "medium",
        "supported_reasoning_levels": [
            { "effort": "low", "description": "Fast responses with lighter reasoning" },
            { "effort": "medium", "description": "Balanced reasoning depth" },
            { "effort": "high", "description": "Greater reasoning depth" },
            { "effort": "xhigh", "description": "Extra high reasoning depth" }
        ],
        "shell_type": "shell_command",
        "visibility": "list",
        "minimal_client_version": "0.98.0",
        "supported_in_api": true,
        "availability_nux": null,
        "upgrade": null,
        "priority": 50,
        "prefer_websockets": false,
        "support_verbosity": true,
        "default_verbosity": "low",
        "apply_patch_tool_type": "freeform",
        "web_search_tool_type": "text_and_image",
        "input_modalities": ["text", "image"],
        "supports_image_detail_original": true,
        "truncation_policy": { "mode": "tokens", "limit": 10000 },
        "supports_parallel_tool_calls": true,
        "context_window": 272000,
        "max_context_window": 1000000,
        "effective_context_window_percent": 95,
        "experimental_supported_tools": [],
        "reasoning_summary_format": "experimental",
        "default_reasoning_summary": "none",
        "supports_search_tool": true,
        "additional_speed_tiers": [],
        "service_tiers": [],
        "supports_reasoning_summaries": true
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        models::{AppConfig, GatewayConfig, RoutingConfig},
        storage::Storage,
    };
    use http::header::AUTHORIZATION;
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    #[test]
    fn joins_urls_safely() {
        assert_eq!(
            join_url("https://example.com/v1/", "/models"),
            "https://example.com/v1/models"
        );
    }

    #[test]
    fn detects_gpt_models() {
        assert!(is_gpt_model("gpt-5.5"));
        assert!(is_gpt_model("openai/gpt-5.4"));
        assert!(!is_gpt_model("claude-sonnet"));
    }

    #[test]
    fn parses_stream_flag() {
        assert!(is_stream_request(br#"{"stream":true}"#));
        assert!(!is_stream_request(br#"{"stream":false}"#));
    }

    #[tokio::test]
    async fn round_robin_can_be_enabled_and_disabled() {
        let upstream_a = upstream_json(200, json!({"id": "resp_a", "object": "response"})).await;
        let upstream_b = upstream_json(200, json!({"id": "resp_b", "object": "response"})).await;

        let manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: true,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;

        for _ in 0..4 {
            let response = manager
                .proxy_request(
                    Method::POST,
                    "responses".to_string(),
                    auth_headers(),
                    Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                    None,
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        assert_eq!(request_count(&upstream_a).await, 2);
        assert_eq!(request_count(&upstream_b).await, 2);

        let fixed_manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;

        for _ in 0..3 {
            let response = fixed_manager
                .proxy_request(
                    Method::POST,
                    "responses".to_string(),
                    auth_headers(),
                    Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                    None,
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        assert_eq!(request_count(&upstream_a).await, 5);
        assert_eq!(request_count(&upstream_b).await, 2);
    }

    #[tokio::test]
    async fn failover_can_be_enabled_and_disabled() {
        let failing = upstream_json(500, json!({"error": {"message": "boom"}})).await;
        let healthy = upstream_json(200, json!({"id": "resp_ok", "object": "response"})).await;

        let failover_manager = test_manager(
            &failing,
            &healthy,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        let response = failover_manager
            .proxy_request(
                Method::POST,
                "responses".to_string(),
                auth_headers(),
                Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(request_count(&failing).await, 1);
        assert_eq!(request_count(&healthy).await, 1);

        let no_failover_manager = test_manager(
            &failing,
            &healthy,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: false,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        let response = no_failover_manager
            .proxy_request(
                Method::POST,
                "responses".to_string(),
                auth_headers(),
                Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(request_count(&failing).await, 2);
        assert_eq!(request_count(&healthy).await, 1);

        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("boom"));
    }

    #[tokio::test]
    async fn selected_provider_is_primary_when_round_robin_disabled() {
        let upstream_a = upstream_json(200, json!({"id": "resp_a", "object": "response"})).await;
        let upstream_b = upstream_json(200, json!({"id": "resp_b", "object": "response"})).await;

        let manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: Some("b".to_string()),
            },
        )
        .await;

        for _ in 0..3 {
            let response = manager
                .proxy_request(
                    Method::POST,
                    "responses".to_string(),
                    auth_headers(),
                    Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                    None,
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        assert_eq!(request_count(&upstream_a).await, 0);
        assert_eq!(request_count(&upstream_b).await, 3);
    }

    #[tokio::test]
    async fn round_robin_ignores_selected_provider() {
        let upstream_a = upstream_json(200, json!({"id": "resp_a", "object": "response"})).await;
        let upstream_b = upstream_json(200, json!({"id": "resp_b", "object": "response"})).await;

        let manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: true,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: Some("b".to_string()),
            },
        )
        .await;

        for _ in 0..4 {
            let response = manager
                .proxy_request(
                    Method::POST,
                    "responses".to_string(),
                    auth_headers(),
                    Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                    None,
                )
                .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        assert_eq!(request_count(&upstream_a).await, 2);
        assert_eq!(request_count(&upstream_b).await, 2);
    }

    #[tokio::test]
    async fn upstream_413_failovers_when_enabled() {
        let too_large =
            upstream_json(413, json!({"error": {"message": "payload too large"}})).await;
        let healthy = upstream_json(200, json!({"id": "resp_ok", "object": "response"})).await;

        let manager = test_manager(
            &too_large,
            &healthy,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        let response = manager
            .proxy_request(
                Method::POST,
                "responses".to_string(),
                auth_headers(),
                Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(request_count(&too_large).await, 1);
        assert_eq!(request_count(&healthy).await, 1);
    }

    #[tokio::test]
    async fn bad_request_only_failovers_for_model_routing_errors() {
        let invalid_body =
            upstream_json(400, json!({"error": {"message": "input is required"}})).await;
        let healthy = upstream_json(200, json!({"id": "resp_ok", "object": "response"})).await;
        let manager = test_manager(
            &invalid_body,
            &healthy,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        let response = manager
            .proxy_request(
                Method::POST,
                "responses".to_string(),
                auth_headers(),
                Bytes::from_static(br#"{"model":"gpt-test"}"#),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(request_count(&invalid_body).await, 1);
        assert_eq!(request_count(&healthy).await, 0);

        let model_missing = upstream_json(
            400,
            json!({"error": {"message": "model gpt-test does not exist"}}),
        )
        .await;
        let healthy = upstream_json(200, json!({"id": "resp_ok", "object": "response"})).await;
        let manager = test_manager(
            &model_missing,
            &healthy,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        let response = manager
            .proxy_request(
                Method::POST,
                "responses".to_string(),
                auth_headers(),
                Bytes::from_static(br#"{"model":"gpt-test","input":"ping"}"#),
                None,
            )
            .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(request_count(&model_missing).await, 1);
        assert_eq!(request_count(&healthy).await, 1);
    }

    #[tokio::test]
    async fn http_gateway_accepts_large_bodies_above_axum_default() {
        let upstream_a =
            upstream_json(200, json!({"id": "resp_large", "object": "response"})).await;
        let upstream_b = upstream_json(200, json!({"id": "resp_b", "object": "response"})).await;
        let manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        manager
            .inner
            .storage
            .update_config(|cfg| {
                cfg.gateway.require_local_token = false;
                cfg.gateway.max_request_body_mb = 16;
            })
            .await
            .unwrap();
        let status = manager.start().await.unwrap();
        let payload = json!({
            "model": "gpt-test",
            "input": "x".repeat(3 * 1024 * 1024),
            "max_output_tokens": 1
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/responses", status.bind_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(request_count(&upstream_a).await, 1);
        let _ = manager.stop().await;
    }

    #[tokio::test]
    async fn http_gateway_returns_structured_local_413_when_config_limit_exceeded() {
        let upstream_a =
            upstream_json(200, json!({"id": "resp_large", "object": "response"})).await;
        let upstream_b = upstream_json(200, json!({"id": "resp_b", "object": "response"})).await;
        let manager = test_manager(
            &upstream_a,
            &upstream_b,
            RoutingConfig {
                auto_round_robin: false,
                auto_failover: true,
                max_attempts_per_request: 2,
                cooldown_secs: 60,
                selected_provider_id: None,
            },
        )
        .await;
        manager
            .inner
            .storage
            .update_config(|cfg| {
                cfg.gateway.require_local_token = false;
                cfg.gateway.max_request_body_mb = 1;
            })
            .await
            .unwrap();
        let status = manager.start().await.unwrap();
        let payload = json!({
            "model": "gpt-test",
            "input": "x".repeat(2 * 1024 * 1024),
            "max_output_tokens": 1
        });
        let response = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap()
            .post(format!("{}/responses", status.bind_url))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
        let text = response.text().await.unwrap();
        assert!(text.contains("local gateway request body is too large"));
        assert_eq!(request_count(&upstream_a).await, 0);
        let _ = manager.stop().await;
    }

    async fn upstream_json(status: u16, body: Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/responses"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    async fn test_manager(
        upstream_a: &MockServer,
        upstream_b: &MockServer,
        routing: RoutingConfig,
    ) -> GatewayManager {
        let dir = std::env::temp_dir().join(format!("other-model-test-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let storage = Storage::load_from_dir(dir).await.unwrap();
        let mut cfg = AppConfig::default();
        cfg.gateway = GatewayConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            require_local_token: true,
            request_timeout_secs: 30,
            stream_idle_timeout_secs: 30,
            max_request_body_mb: 512,
        };
        cfg.local_auth_token = "test-token".to_string();
        cfg.routing = routing;
        cfg.providers = vec![
            ProviderConfig {
                id: "a".to_string(),
                name: "A".to_string(),
                base_url: format!("{}/v1", upstream_a.uri()),
                api_key: "upstream-a-key".to_string(),
                enabled: true,
                ..Default::default()
            },
            ProviderConfig {
                id: "b".to_string(),
                name: "B".to_string(),
                base_url: format!("{}/v1", upstream_b.uri()),
                api_key: "upstream-b-key".to_string(),
                enabled: true,
                ..Default::default()
            },
        ];
        storage.set_config(cfg).await.unwrap();
        GatewayManager::new(storage)
    }

    fn auth_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers
    }

    async fn request_count(server: &MockServer) -> usize {
        server.received_requests().await.unwrap().len()
    }
}
