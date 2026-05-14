use crate::{
    codex_config,
    gateway::{is_gpt_model, GatewayManager},
    models::{
        AppConfig, CodexConfigResult, ConfigureCodexRequest, GatewaySelfCheckItem,
        GatewaySelfCheckResult, GatewayStatus, ModelInfo, ModelsCache, ProviderConfig,
        ProviderExportResult, ProviderImportResult, ProviderState, ProviderView, QuotaResult,
        RequestLogEntry, TestResult,
    },
    quota,
    shared::{
        err_string, filter_supported_models_cache, finish_self_check, first_cached_gpt_model,
        parse_provider_import, validate_provider,
    },
    storage::Storage,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use std::{
    collections::{BTreeMap, HashSet},
    time::Instant,
};
use tauri::State;

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub gateway: GatewayManager,
}

#[tauri::command]
pub async fn start_gateway(state: State<'_, AppState>) -> Result<GatewayStatus, String> {
    state.gateway.start().await.map_err(err_string)
}

#[tauri::command]
pub async fn stop_gateway(state: State<'_, AppState>) -> Result<GatewayStatus, String> {
    state.gateway.stop().await.map_err(err_string)
}

#[tauri::command]
pub async fn get_gateway_status(state: State<'_, AppState>) -> Result<GatewayStatus, String> {
    Ok(state.gateway.status().await)
}

#[tauri::command]
pub async fn get_app_config(state: State<'_, AppState>) -> Result<AppConfig, String> {
    Ok(state.storage.config().await)
}

#[tauri::command]
pub async fn save_app_config(
    state: State<'_, AppState>,
    mut config: AppConfig,
) -> Result<AppConfig, String> {
    config.normalize_balance_auth();
    state
        .storage
        .set_config(config.clone())
        .await
        .map_err(err_string)?;
    Ok(config)
}

#[tauri::command]
pub async fn list_providers(state: State<'_, AppState>) -> Result<Vec<ProviderView>, String> {
    let cfg = state.storage.config().await;
    let runtime = state.storage.runtime_state().await;
    let cache = state.storage.models_cache().await;
    let views = cfg
        .providers
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
            let gpt_model_count = models.iter().filter(|m| is_gpt_model(&m.id)).count();
            ProviderView {
                config: provider,
                state: st,
                model_count: models.len(),
                gpt_model_count,
            }
        })
        .collect();
    Ok(views)
}

#[tauri::command]
pub async fn create_provider(
    state: State<'_, AppState>,
    mut provider: ProviderConfig,
) -> Result<ProviderConfig, String> {
    if provider.id.trim().is_empty() {
        provider.id = uuid::Uuid::new_v4().to_string();
    }
    provider.normalize_balance_auth();
    validate_provider(&provider).map_err(|e| e.to_string())?;
    state
        .storage
        .update_config(|cfg| cfg.providers.push(provider.clone()))
        .await
        .map_err(err_string)?;
    Ok(provider)
}

#[tauri::command]
pub async fn update_provider(
    state: State<'_, AppState>,
    mut provider: ProviderConfig,
) -> Result<ProviderConfig, String> {
    provider.normalize_balance_auth();
    validate_provider(&provider).map_err(|e| e.to_string())?;
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
        .await
        .map_err(err_string)?;
    if !found {
        return Err("provider not found".to_string());
    }
    Ok(provider)
}

#[tauri::command]
pub async fn delete_provider(
    state: State<'_, AppState>,
    provider_id: String,
) -> Result<bool, String> {
    state
        .storage
        .update_config(|cfg| {
            let before = cfg.providers.len();
            cfg.providers.retain(|p| p.id != provider_id);
            before != cfg.providers.len()
        })
        .await
        .map_err(err_string)
}

#[tauri::command]
pub async fn export_providers(
    state: State<'_, AppState>,
    directory: Option<String>,
) -> Result<ProviderExportResult, String> {
    export_providers_to_dir(state.storage.clone(), directory)
        .await
        .map_err(err_string)
}

pub(crate) async fn export_providers_to_dir(
    storage: Storage,
    directory: Option<String>,
) -> Result<ProviderExportResult> {
    let cfg = storage.config().await;
    let stamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let dir = directory
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| storage.dir());
    if !dir.exists() {
        return Err(anyhow!(
            "export directory does not exist: {}",
            dir.display()
        ));
    }
    if !dir.is_dir() {
        return Err(anyhow!("export path is not a directory: {}", dir.display()));
    }
    let path = dir.join(format!("other-model-providers-{stamp}.json"));
    let provider_count = cfg.providers.len();
    let payload = serde_json::json!({
        "schema": "other_model.providers.v1",
        "exported_at": Utc::now(),
        "providers": cfg.providers,
    });
    let raw = serde_json::to_string_pretty(&payload)?;
    tokio::fs::write(&path, raw).await?;
    Ok(ProviderExportResult {
        ok: true,
        path: path.display().to_string(),
        count: provider_count,
        message: format!(
            "已导出 {provider_count} 个供应商到 {}。导出文件包含明文 API Key，请妥善保管。",
            path.display()
        ),
    })
}

#[tauri::command]
pub async fn import_providers(
    state: State<'_, AppState>,
    raw: String,
) -> Result<ProviderImportResult, String> {
    let providers = parse_provider_import(&raw).map_err(|err| err.to_string())?;
    let total = providers.len();
    let mut skipped = 0usize;
    let mut imported = 0usize;
    let mut updated = 0usize;
    let mut seen = HashSet::<String>::new();

    state
        .storage
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
        .await
        .map_err(err_string)?;

    Ok(ProviderImportResult {
        ok: true,
        imported,
        updated,
        skipped,
        total,
        message: format!("导入完成：新增 {imported} 个，更新 {updated} 个，跳过 {skipped} 个。"),
    })
}

#[tauri::command]
pub async fn discover_models(state: State<'_, AppState>) -> Result<ModelsCache, String> {
    state.gateway.discover_models().await.map_err(err_string)
}

#[tauri::command]
pub async fn get_models_cache(state: State<'_, AppState>) -> Result<ModelsCache, String> {
    Ok(filter_supported_models_cache(
        state.storage.models_cache().await,
    ))
}

#[tauri::command]
pub async fn list_gpt_models(state: State<'_, AppState>) -> Result<Vec<ModelInfo>, String> {
    let cache = state.storage.models_cache().await;
    let mut map = BTreeMap::<String, ModelInfo>::new();
    for provider in cache.providers.values() {
        for model in &provider.models {
            if is_gpt_model(&model.id) {
                map.entry(model.id.clone()).or_insert_with(|| model.clone());
            }
        }
    }
    Ok(map.into_values().collect())
}

#[tauri::command]
pub async fn test_model(
    state: State<'_, AppState>,
    provider_id: String,
    model: Option<String>,
) -> Result<TestResult, String> {
    Ok(state.gateway.test_provider_model(provider_id, model).await)
}

#[tauri::command]
pub async fn test_provider(
    state: State<'_, AppState>,
    provider_id: String,
) -> Result<TestResult, String> {
    Ok(state.gateway.test_provider_model(provider_id, None).await)
}

#[tauri::command]
pub async fn get_quota(
    state: State<'_, AppState>,
    provider_id: String,
) -> Result<QuotaResult, String> {
    Ok(quota::get_quota(state.storage.clone(), provider_id).await)
}

#[tauri::command]
pub async fn get_logs(
    state: State<'_, AppState>,
    limit: Option<usize>,
) -> Result<Vec<RequestLogEntry>, String> {
    state
        .storage
        .read_logs(limit.unwrap_or(200))
        .await
        .map_err(err_string)
}

#[tauri::command]
pub async fn run_gateway_self_check(
    state: State<'_, AppState>,
    model: Option<String>,
) -> Result<GatewaySelfCheckResult, String> {
    let mut checks = Vec::<GatewaySelfCheckItem>::new();
    let mut status = state.gateway.status().await;
    if !status.running {
        let start = Instant::now();
        match state.gateway.start().await {
            Ok(next) => {
                status = next;
                checks.push(GatewaySelfCheckItem {
                    name: "启动网关".to_string(),
                    ok: true,
                    status: None,
                    latency_ms: start.elapsed().as_millis(),
                    details: Some(format!("已启动：{}", status.bind_url)),
                });
            }
            Err(err) => {
                checks.push(GatewaySelfCheckItem {
                    name: "启动网关".to_string(),
                    ok: false,
                    status: None,
                    latency_ms: start.elapsed().as_millis(),
                    details: Some(err.to_string()),
                });
                return Ok(finish_self_check(checks));
            }
        }
    }

    let cfg = state.storage.config().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(
            cfg.gateway.request_timeout_secs.max(30),
        ))
        .build()
        .map_err(|err| err.to_string())?;
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

    let selected_model = if let Some(model) = model.filter(|item| !item.trim().is_empty()) {
        Some(model)
    } else {
        let cache = state.storage.models_cache().await;
        first_cached_gpt_model(&cache)
    };
    if let Some(model) = selected_model {
        let small_body = serde_json::json!({
            "model": model.clone(),
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

        let large_input = "x".repeat(3 * 1024 * 1024);
        let large_body = serde_json::json!({
            "model": model.clone(),
            "input": large_input,
            "max_output_tokens": 1,
            "stream": false
        });
        let start = Instant::now();
        let mut req = client.post(format!("{base}/responses")).json(&large_body);
        if let Some(auth) = &auth_header {
            req = req.header(reqwest::header::AUTHORIZATION, auth);
        }
        match req.send().await {
            Ok(resp) => {
                let status_code = resp.status();
                let text = resp.text().await.unwrap_or_default();
                let local_limit_rejected = status_code == reqwest::StatusCode::PAYLOAD_TOO_LARGE
                    && text.contains("local gateway request body is too large");
                checks.push(GatewaySelfCheckItem {
                    name: "2MB+ 大请求通道".to_string(),
                    ok: !local_limit_rejected,
                    status: Some(status_code.as_u16()),
                    latency_ms: start.elapsed().as_millis(),
                    details: Some(if local_limit_rejected {
                        text.chars().take(500).collect()
                    } else {
                        "本地网关已接收大请求；后续状态来自上游供应商。".to_string()
                    }),
                });
            }
            Err(err) => checks.push(GatewaySelfCheckItem {
                name: "2MB+ 大请求通道".to_string(),
                ok: false,
                status: None,
                latency_ms: start.elapsed().as_millis(),
                details: Some(err.to_string()),
            }),
        }

        let stream_body = serde_json::json!({
            "model": model.clone(),
            "input": "stream ping",
            "max_output_tokens": 16,
            "stream": true
        });
        let start = Instant::now();
        let mut req = client.post(format!("{base}/responses")).json(&stream_body);
        if let Some(auth) = &auth_header {
            req = req.header(reqwest::header::AUTHORIZATION, auth);
        }
        match req.send().await {
            Ok(resp) => checks.push(GatewaySelfCheckItem {
                name: "SSE 流式请求".to_string(),
                ok: resp.status().is_success(),
                status: Some(resp.status().as_u16()),
                latency_ms: start.elapsed().as_millis(),
                details: Some("已成功打开流式响应。".to_string()),
            }),
            Err(err) => checks.push(GatewaySelfCheckItem {
                name: "SSE 流式请求".to_string(),
                ok: false,
                status: None,
                latency_ms: start.elapsed().as_millis(),
                details: Some(err.to_string()),
            }),
        }
    } else {
        checks.push(GatewaySelfCheckItem {
            name: "Responses 请求".to_string(),
            ok: true,
            status: None,
            latency_ms: 0,
            details: Some("跳过：未发现 GPT-5.4 / GPT-5.5，请先在供应商页查询模型。".to_string()),
        });
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

    Ok(finish_self_check(checks))
}

#[tauri::command]
pub async fn configure_codex(
    state: State<'_, AppState>,
    request: ConfigureCodexRequest,
) -> Result<CodexConfigResult, String> {
    let cfg = state.storage.config().await;
    let status = state.gateway.status().await;
    let url = status.bind_url;
    codex_config::configure_codex(request, url, cfg.local_auth_token)
        .await
        .map_err(err_string)
}

#[tauri::command]
pub async fn restore_codex_backup() -> Result<CodexConfigResult, String> {
    codex_config::restore_latest_backup()
        .await
        .map_err(err_string)
}

#[tauri::command]
pub async fn get_codex_config_path() -> Result<String, String> {
    codex_config::codex_config_path()
        .map(|path| path.display().to_string())
        .map_err(|err| err.to_string())
}

#[tauri::command]
pub async fn app_data_dir(state: State<'_, AppState>) -> Result<String, String> {
    Ok(state.storage.dir().display().to_string())
}

#[tauri::command]
pub async fn reset_default_templates(state: State<'_, AppState>) -> Result<AppConfig, String> {
    let mut cfg = state.storage.config().await;
    let mut existing: BTreeMap<String, ProviderConfig> = cfg
        .providers
        .iter()
        .map(|p| (p.id.clone(), p.clone()))
        .collect();
    for template in AppConfig::default().providers {
        existing.entry(template.id.clone()).or_insert(template);
    }
    cfg.providers = existing.into_values().collect();
    state
        .storage
        .set_config(cfg.clone())
        .await
        .map_err(err_string)?;
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_wrapped_provider_import() {
        let raw = r#"{
            "schema": "other_model.providers.v1",
            "providers": [{
                "id": "p1",
                "name": "Provider 1",
                "base_url": "https://example.com/v1",
                "api_key": "test-key",
                "enabled": true,
                "timeout_secs": 120,
                "headers": {},
                "query": {}
            }]
        }"#;
        let providers = parse_provider_import(raw).unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "p1");
        assert_eq!(providers[0].api_key, "test-key");
    }

    #[test]
    fn parses_array_provider_import() {
        let raw = r#"[{
            "id": "p2",
            "name": "Provider 2",
            "base_url": "https://example.org/v1",
            "api_key": "test-key-2",
            "enabled": false,
            "timeout_secs": 60,
            "headers": {},
            "query": {}
        }]"#;
        let providers = parse_provider_import(raw).unwrap();
        assert_eq!(providers.len(), 1);
        assert_eq!(providers[0].id, "p2");
        assert!(!providers[0].enabled);
    }

    #[tokio::test]
    async fn exports_providers_to_selected_directory() {
        let storage_dir = tempfile::tempdir().unwrap();
        let export_dir = tempfile::tempdir().unwrap();
        let storage = Storage::load_from_dir(storage_dir.path().to_path_buf())
            .await
            .unwrap();
        let result =
            export_providers_to_dir(storage, Some(export_dir.path().display().to_string()))
                .await
                .unwrap();
        assert!(result.ok);
        assert!(result.path.contains("other-model-providers-"));
        assert!(std::path::Path::new(&result.path).exists());
        assert_eq!(result.count, 2);
    }
}

pub fn invoke_handler() -> impl Fn(tauri::ipc::Invoke<tauri::Wry>) -> bool + Send + Sync + 'static {
    tauri::generate_handler![
        start_gateway,
        stop_gateway,
        get_gateway_status,
        get_app_config,
        save_app_config,
        list_providers,
        create_provider,
        update_provider,
        delete_provider,
        export_providers,
        import_providers,
        discover_models,
        get_models_cache,
        list_gpt_models,
        test_model,
        test_provider,
        get_quota,
        get_logs,
        run_gateway_self_check,
        configure_codex,
        restore_codex_backup,
        get_codex_config_path,
        app_data_dir,
        reset_default_templates
    ]
}
