use crate::models::{CodexConfigResult, ConfigureCodexRequest};
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use std::path::{Path, PathBuf};
use toml_edit::{value, DocumentMut, Item, Table};

const LOCAL_NO_PROXY: &[&str] = &["localhost", "127.0.0.1", "::1"];
const ENV_SCRIPT_NAME: &str = "other-model-env.sh";
const PROFILE_BLOCK_START: &str = "# >>> Other Model local gateway proxy bypass >>>";
const PROFILE_BLOCK_END: &str = "# <<< Other Model local gateway proxy bypass <<<";

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
    if let Some(limit) = req.auto_compact_token_limit.filter(|limit| *limit > 0) {
        doc["model_auto_compact_token_limit"] = value(limit);
    }

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

    ensure_shell_environment_policy(&mut doc);
    let proxy_bypass = install_proxy_bypass_if_standard_codex_path(path).await?;

    tokio::fs::write(path, doc.to_string())
        .await
        .with_context(|| format!("write {}", path.display()))?;
    let proxy_note = proxy_bypass
        .as_ref()
        .map(|bypass| {
            format!(
                " 已写入本地代理绕过 NO_PROXY={}；若当前 Codex/Terminal 已经打开，请重启后生效。",
                bypass.no_proxy_value
            )
        })
        .unwrap_or_default();
    Ok(CodexConfigResult {
        ok: true,
        config_path: path.display().to_string(),
        backup_path: backup_path.map(|p| p.display().to_string()),
        message: format!(
            "写入成功：已将现有 provider '{provider_name}' 替换为 Other Model，默认模型为 '{}'.{proxy_note}",
            req.model
        ),
    })
}

struct ProxyBypassInstall {
    no_proxy_value: String,
}

fn ensure_shell_environment_policy(doc: &mut DocumentMut) {
    if !doc.as_table().contains_key("shell_environment_policy")
        || !doc["shell_environment_policy"].is_table()
    {
        doc["shell_environment_policy"] = Item::Table(Table::new());
    }
    let table = doc["shell_environment_policy"]
        .as_table_mut()
        .expect("shell environment table");
    if !table.contains_key("set") || !table["set"].is_table() {
        table["set"] = Item::Table(Table::new());
    }
    let set = table["set"]
        .as_table_mut()
        .expect("shell environment set table");
    let no_proxy = local_no_proxy_value();
    set["NO_PROXY"] = value(no_proxy.as_str());
    set["no_proxy"] = value(no_proxy.as_str());
}

async fn install_proxy_bypass_if_standard_codex_path(
    codex_config_path: &Path,
) -> Result<Option<ProxyBypassInstall>> {
    let Some(home) = home_for_standard_codex_path(codex_config_path) else {
        return Ok(None);
    };
    let codex_dir = home.join(".codex");
    tokio::fs::create_dir_all(&codex_dir).await?;
    let no_proxy_value = local_no_proxy_value();
    let env_script = codex_dir.join(ENV_SCRIPT_NAME);
    tokio::fs::write(&env_script, proxy_bypass_script()).await?;
    for profile in [home.join(".zshrc"), home.join(".zprofile")] {
        upsert_profile_source_block(&profile).await?;
    }
    install_launchctl_no_proxy(&no_proxy_value).await;
    Ok(Some(ProxyBypassInstall { no_proxy_value }))
}

fn home_for_standard_codex_path(path: &Path) -> Option<PathBuf> {
    let file = path.file_name()?.to_str()?;
    if file != "config.toml" {
        return None;
    }
    let codex_dir = path.parent()?;
    if codex_dir.file_name()?.to_str()? != ".codex" {
        return None;
    }
    codex_dir.parent().map(Path::to_path_buf)
}

fn local_no_proxy_value() -> String {
    LOCAL_NO_PROXY.join(",")
}

fn proxy_bypass_script() -> String {
    let mut out = String::from(
        "# Generated by Other Model. Keeps local Codex gateway requests out of HTTP proxies.\n",
    );
    for var in ["NO_PROXY", "no_proxy"] {
        for entry in LOCAL_NO_PROXY {
            out.push_str(&format!(
                "case \",${{{var}:-}},\" in *\",{entry},\"*) ;; *) export {var}=\"${{{var}:+${var},}}{entry}\" ;; esac\n"
            ));
        }
    }
    out
}

async fn upsert_profile_source_block(path: &Path) -> Result<()> {
    let original = tokio::fs::read_to_string(path).await.unwrap_or_default();
    let block = format!(
        "{PROFILE_BLOCK_START}\n[ -f \"$HOME/.codex/{ENV_SCRIPT_NAME}\" ] && . \"$HOME/.codex/{ENV_SCRIPT_NAME}\"\n{PROFILE_BLOCK_END}"
    );
    let next = if let Some(start) = original.find(PROFILE_BLOCK_START) {
        if let Some(end_offset) = original[start..].find(PROFILE_BLOCK_END) {
            let end = start + end_offset + PROFILE_BLOCK_END.len();
            format!("{}{}{}", &original[..start], block, &original[end..])
        } else {
            format!("{}\n{}\n", original.trim_end(), block)
        }
    } else if original.trim().is_empty() {
        format!("{block}\n")
    } else {
        format!("{}\n\n{}\n", original.trim_end(), block)
    };
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, next).await?;
    Ok(())
}

async fn install_launchctl_no_proxy(no_proxy_value: &str) {
    #[cfg(all(target_os = "macos", not(test)))]
    {
        use std::process::Command;
        for key in ["NO_PROXY", "no_proxy"] {
            let existing = Command::new("launchctl")
                .args(["getenv", key])
                .output()
                .ok()
                .and_then(|output| {
                    if output.status.success() {
                        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
                    } else {
                        None
                    }
                })
                .filter(|s| !s.is_empty());
            let merged = merge_no_proxy(existing.as_deref(), no_proxy_value);
            let _ = Command::new("launchctl")
                .args(["setenv", key, merged.as_str()])
                .status();
        }
    }
    let _ = no_proxy_value;
}

fn merge_no_proxy(existing: Option<&str>, required: &str) -> String {
    let mut items: Vec<String> = existing
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    for item in required.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if !items.iter().any(|existing| existing == item) {
            items.push(item.to_string());
        }
    }
    items.join(",")
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

pub async fn configure_codex_proxy_bypass_only() -> Result<()> {
    let path = codex_config_path()?;
    if let Ok(original) = tokio::fs::read_to_string(&path).await {
        let mut doc = original
            .parse::<DocumentMut>()
            .with_context(|| format!("parse {}", path.display()))?;
        ensure_shell_environment_policy(&mut doc);
        tokio::fs::write(&path, doc.to_string())
            .await
            .with_context(|| format!("write {}", path.display()))?;
    }
    let _ = install_proxy_bypass_if_standard_codex_path(&path).await?;
    Ok(())
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
                auto_compact_token_limit: None,
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
        assert!(content.contains("[shell_environment_policy.set]"));
        assert!(content.contains("NO_PROXY = \"localhost,127.0.0.1,::1\""));
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
                auto_compact_token_limit: None,
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

    #[tokio::test]
    async fn standard_codex_path_installs_proxy_bypass_files() {
        let dir = tempdir().unwrap();
        let home = dir.path().join("home");
        let path = home.join(".codex").join("config.toml");
        configure_codex_at_path(
            &path,
            ConfigureCodexRequest {
                model: "gpt-5.5".to_string(),
                provider_name: None,
                auto_compact_token_limit: None,
            },
            "http://127.0.0.1:14555/v1".to_string(),
            "local-token".to_string(),
        )
        .await
        .unwrap();

        let env_script = tokio::fs::read_to_string(home.join(".codex/other-model-env.sh"))
            .await
            .unwrap();
        assert!(env_script.contains("NO_PROXY"));
        assert!(env_script.contains("127.0.0.1"));
        let zshrc = tokio::fs::read_to_string(home.join(".zshrc"))
            .await
            .unwrap();
        let zprofile = tokio::fs::read_to_string(home.join(".zprofile"))
            .await
            .unwrap();
        assert!(zshrc.contains(PROFILE_BLOCK_START));
        assert!(zprofile.contains(PROFILE_BLOCK_START));
    }

    #[test]
    fn merges_no_proxy_values_without_duplicates() {
        assert_eq!(
            merge_no_proxy(Some("example.com,localhost"), "localhost,127.0.0.1,::1"),
            "example.com,localhost,127.0.0.1,::1"
        );
    }
}
