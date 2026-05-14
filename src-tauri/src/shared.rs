use crate::{
    gateway::is_gpt_model,
    models::{
        BalanceAuthMode, GatewaySelfCheckItem, GatewaySelfCheckResult, ModelsCache, ProviderConfig,
    },
};
use anyhow::{anyhow, Result};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) fn err_string(err: anyhow::Error) -> String {
    err.to_string()
}

pub(crate) fn validate_provider(provider: &ProviderConfig) -> Result<()> {
    if provider.name.trim().is_empty() {
        return Err(anyhow!("provider name is required"));
    }
    if provider.base_url.trim().is_empty() {
        return Err(anyhow!("base URL is required"));
    }
    if !provider.base_url.starts_with("http://") && !provider.base_url.starts_with("https://") {
        return Err(anyhow!("base URL must start with http:// or https://"));
    }
    let balance_auth = provider.effective_balance_auth();
    match balance_auth.mode {
        BalanceAuthMode::Disabled => {}
        BalanceAuthMode::QuotaApi => {
            if provider.quota.is_none() {
                return Err(anyhow!("quota_api mode requires quota config"));
            }
        }
        BalanceAuthMode::NewapiLogin | BalanceAuthMode::Sub2apiLogin => {
            if balance_auth
                .username
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
            {
                return Err(anyhow!("balance login username is required"));
            }
            if balance_auth
                .password
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .is_none()
            {
                return Err(anyhow!("balance login password is required"));
            }
        }
    }
    Ok(())
}

pub(crate) fn parse_provider_import(raw: &str) -> Result<Vec<ProviderConfig>> {
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

pub(crate) fn first_cached_gpt_model(cache: &ModelsCache) -> Option<String> {
    let mut models = BTreeMap::<String, ()>::new();
    for provider in cache.providers.values() {
        for model in &provider.models {
            if is_gpt_model(&model.id) {
                models.insert(model.id.clone(), ());
            }
        }
    }
    models.into_keys().next()
}

pub(crate) fn filter_supported_models_cache(mut cache: ModelsCache) -> ModelsCache {
    for provider in cache.providers.values_mut() {
        provider.models.retain(|model| is_gpt_model(&model.id));
    }
    cache
}

pub(crate) fn finish_self_check(checks: Vec<GatewaySelfCheckItem>) -> GatewaySelfCheckResult {
    let failed = checks.iter().filter(|item| !item.ok).count();
    GatewaySelfCheckResult {
        ok: failed == 0,
        message: if failed == 0 {
            format!("自检完成：{} 项全部通过。", checks.len())
        } else {
            format!(
                "自检完成：{} 项通过，{} 项失败。",
                checks.len() - failed,
                failed
            )
        },
        checks,
    }
}
