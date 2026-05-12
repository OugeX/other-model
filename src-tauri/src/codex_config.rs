use crate::models::{CodexConfigResult, ConfigureCodexRequest};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::path::{Path, PathBuf};
use toml_edit::{value, DocumentMut, Item, Table};

pub async fn configure_codex(
    req: ConfigureCodexRequest,
    gateway_base_url: String,
    token: String,
) -> Result<CodexConfigResult> {
    if req.model.trim().is_empty() {
        return Err(anyhow!("model is required"));
    }
    let path = codex_config_path()?;
    configure_codex_at_path(&path, req, gateway_base_url, token).await
}

pub async fn configure_codex_at_path(
    path: &Path,
    req: ConfigureCodexRequest,
    gateway_base_url: String,
    token: String,
) -> Result<CodexConfigResult> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let original = if path.exists() {
        tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("read {}", path.display()))?
    } else {
        String::new()
    };
    let backup_path = if path.exists() {
        let stamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
        let backup = path.with_file_name(format!("config.toml.other-model-bak-{stamp}"));
        tokio::fs::write(&backup, &original).await?;
        Some(backup)
    } else {
        None
    };
    let mut doc = if original.trim().is_empty() {
        DocumentMut::new()
    } else {
        original
            .parse::<DocumentMut>()
            .with_context(|| format!("parse {}", path.display()))?
    };

    let provider_name = existing_or_requested_provider(&doc, req.provider_name.as_deref());
    doc["model"] = value(req.model.trim());
    doc["model_provider"] = value(provider_name.as_str());

    if !doc.as_table().contains_key("model_providers") || !doc["model_providers"].is_table() {
        doc["model_providers"] = Item::Table(Table::new());
    }
    if !doc["model_providers"]
        .as_table()
        .unwrap()
        .contains_key(&provider_name)
        || !doc["model_providers"][&provider_name].is_table()
    {
        doc["model_providers"][&provider_name] = Item::Table(Table::new());
    }

    let provider_table = doc["model_providers"][&provider_name]
        .as_table_mut()
        .expect("provider table");
    provider_table["name"] = value("Other Model");
    provider_table["base_url"] = value(gateway_base_url);
    provider_table["wire_api"] = value("responses");
    provider_table["supports_websockets"] = value(false);
    provider_table["experimental_bearer_token"] = value(token);

    tokio::fs::write(path, doc.to_string())
        .await
        .with_context(|| format!("write {}", path.display()))?;
    Ok(CodexConfigResult {
        ok: true,
        config_path: path.display().to_string(),
        backup_path: backup_path.map(|p| p.display().to_string()),
        message: format!(
            "写入成功：已将现有 provider '{provider_name}' 替换为 Other Model，默认模型为 '{}'.",
            req.model
        ),
    })
}

fn existing_or_requested_provider(doc: &DocumentMut, requested: Option<&str>) -> String {
    if let Some(requested) = requested
        .map(str::trim)
        .filter(|s| !s.is_empty() && *s != "other_model_gateway")
    {
        return requested.to_string();
    }
    doc.get("model_provider")
        .and_then(|item| item.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("other_model_gateway")
        .to_string()
}

pub async fn restore_latest_backup() -> Result<CodexConfigResult> {
    let path = codex_config_path()?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("invalid Codex config path"))?;
    let mut entries = tokio::fs::read_dir(parent).await?;
    let mut backups: Vec<PathBuf> = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let p = entry.path();
        if p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("config.toml.other-model-bak-"))
            .unwrap_or(false)
        {
            backups.push(p);
        }
    }
    backups.sort();
    let Some(latest) = backups.pop() else {
        return Err(anyhow!("no Other Model Codex backup found"));
    };
    let content = tokio::fs::read_to_string(&latest).await?;
    tokio::fs::write(&path, content).await?;
    Ok(CodexConfigResult {
        ok: true,
        config_path: path.display().to_string(),
        backup_path: Some(latest.display().to_string()),
        message: "Restored latest Other Model Codex backup.".to_string(),
    })
}

pub fn codex_config_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("could not find home directory"))?;
    Ok(home.join(".codex").join("config.toml"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn configures_codex_by_replacing_active_provider() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(
            &path,
            r#"model = "gpt-5.4"
model_provider = "cliproxy"
sandbox_mode = "danger-full-access"

[model_providers.cliproxy]
name = "CLIProxyAPI Server"
base_url = "https://ciyuanshen.top/v1"
wire_api = "responses"
experimental_bearer_token = "old-token"
"#,
        )
        .await
        .unwrap();
        let result = configure_codex_at_path(
            &path,
            ConfigureCodexRequest {
                model: "gpt-5.5".to_string(),
                provider_name: None,
            },
            "http://127.0.0.1:14555/v1".to_string(),
            "local-token".to_string(),
        )
        .await
        .unwrap();
        assert!(result.backup_path.is_some());
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("sandbox_mode"));
        assert!(content.contains("model = \"gpt-5.5\""));
        assert!(content.contains("model_provider = \"cliproxy\""));
        assert!(content.contains("[model_providers.cliproxy]"));
        assert!(!content.contains("[model_providers.other_model_gateway]"));
        assert!(content.contains("base_url = \"http://127.0.0.1:14555/v1\""));
        assert!(content.contains("experimental_bearer_token = \"local-token\""));
    }

    #[tokio::test]
    async fn creates_provider_when_no_active_provider_exists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, "sandbox_mode = \"danger-full-access\"\n")
            .await
            .unwrap();
        configure_codex_at_path(
            &path,
            ConfigureCodexRequest {
                model: "gpt-5.5".to_string(),
                provider_name: None,
            },
            "http://127.0.0.1:14555/v1".to_string(),
            "local-token".to_string(),
        )
        .await
        .unwrap();
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("model_provider = \"other_model_gateway\""));
        assert!(content.contains("[model_providers.other_model_gateway]"));
    }
}
