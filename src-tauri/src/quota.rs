use crate::{
    models::{ProviderConfig, QuotaResult},
    storage::Storage,
};
use reqwest::Client;
use serde_json::Value;

pub async fn get_quota(storage: Storage, provider_id: String) -> QuotaResult {
    let cfg = storage.config().await;
    let Some(provider) = cfg.providers.into_iter().find(|p| p.id == provider_id) else {
        return QuotaResult {
            provider_id,
            ok: false,
            error: Some("provider not found".to_string()),
            ..Default::default()
        };
    };
    query_quota(provider).await
}

async fn query_quota(provider: ProviderConfig) -> QuotaResult {
    let Some(quota) = provider.quota.clone() else {
        return QuotaResult {
            provider_id: provider.id,
            ok: true,
            health_hint: Some(
                "No quota adapter configured; using health and rate-limit hints only.".to_string(),
            ),
            ..Default::default()
        };
    };
    if quota.url.trim().is_empty() {
        return QuotaResult {
            provider_id: provider.id,
            ok: false,
            error: Some("quota URL is empty".to_string()),
            ..Default::default()
        };
    }
    let client = match Client::builder()
        .timeout(std::time::Duration::from_secs(provider.timeout_secs.max(1)))
        .build()
    {
        Ok(client) => client,
        Err(err) => {
            return QuotaResult {
                provider_id: provider.id,
                ok: false,
                error: Some(err.to_string()),
                ..Default::default()
            }
        }
    };
    let mut req = if quota.method.eq_ignore_ascii_case("POST") {
        client.post(&quota.url)
    } else {
        client.get(&quota.url)
    };
    req = req.bearer_auth(&provider.api_key);
    for (k, v) in quota.headers {
        req = req.header(k, v);
    }
    match req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let raw: Value = resp.json().await.unwrap_or(Value::Null);
            let balance = quota
                .balance_json_path
                .as_deref()
                .and_then(|path| json_path(&raw, path))
                .map(value_to_string);
            QuotaResult {
                provider_id: provider.id,
                ok: (200..300).contains(&status),
                balance,
                status: Some(status),
                raw: Some(raw),
                error: if (200..300).contains(&status) {
                    None
                } else {
                    Some(format!("quota endpoint returned {status}"))
                },
                ..Default::default()
            }
        }
        Err(err) => QuotaResult {
            provider_id: provider.id,
            ok: false,
            error: Some(err.to_string()),
            ..Default::default()
        },
    }
}

fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path
        .trim_start_matches('$')
        .trim_start_matches('.')
        .split('.')
    {
        if segment.is_empty() {
            continue;
        }
        current = current.get(segment)?;
    }
    Some(current)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}
