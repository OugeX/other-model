use crate::{
    codex_config,
    gateway::{is_gpt_model, GatewayManager},
    models::{
        AppConfig, CodexConfigResult, ConfigureCodexRequest, GatewayStatus, ModelInfo, ModelsCache,
        ProviderConfig, ProviderExportResult, ProviderImportResult, ProviderState, ProviderView,
        QuotaResult, RequestLogEntry, TestResult,
    },
    quota,
    storage::Storage,
};
use anyhow::{anyhow, Result};
use chrono::Utc;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use tauri::State;

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub gateway: GatewayManager,
}

fn err_string(err: anyhow::Error) -> String {
    err.to_string()
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
    config: AppConfig,
) -> Result<AppConfig, String> {
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
                .map(|m| m.models.clone())
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
    provider: ProviderConfig,
) -> Result<ProviderConfig, String> {
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

async fn export_providers_to_dir(
    storage: Storage,
    directory: Option<String>,
) -> Result<ProviderExportResult> {
    let cfg = storage.config().await;
    let stamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let dir = directory
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| storage.dir());
    if !dir.exists() {
        return Err(anyhow!("export directory does not exist: {}", dir.display()));
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
    Ok(state.storage.models_cache().await)
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

fn validate_provider(provider: &ProviderConfig) -> Result<()> {
    if provider.name.trim().is_empty() {
        return Err(anyhow!("provider name is required"));
    }
    if provider.base_url.trim().is_empty() {
        return Err(anyhow!("base URL is required"));
    }
    if !provider.base_url.starts_with("http://") && !provider.base_url.starts_with("https://") {
        return Err(anyhow!("base URL must start with http:// or https://"));
    }
    Ok(())
}

fn parse_provider_import(raw: &str) -> Result<Vec<ProviderConfig>> {
    if raw.trim().is_empty() {
        return Err(anyhow!("import file is empty"));
    }
    if let Ok(providers) = serde_json::from_str::<Vec<ProviderConfig>>(raw) {
        return Ok(providers);
    }
    let value: Value = serde_json::from_str(raw)?;
    if let Some(providers) = value.get("providers") {
        return Ok(serde_json::from_value::<Vec<ProviderConfig>>(
            providers.clone(),
        )?);
    }
    if let Some(config) = value.get("config").and_then(|item| item.get("providers")) {
        return Ok(serde_json::from_value::<Vec<ProviderConfig>>(
            config.clone(),
        )?);
    }
    Err(anyhow!(
        "unsupported provider import format; expected an array or an object with providers"
    ))
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
        let result = export_providers_to_dir(
            storage,
            Some(export_dir.path().display().to_string()),
        )
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
        configure_codex,
        restore_codex_backup,
        get_codex_config_path,
        app_data_dir,
        reset_default_templates
    ]
}
