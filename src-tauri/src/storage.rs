use crate::models::{AppConfig, ModelsCache, RequestLogEntry, RuntimeState};
use anyhow::{Context, Result};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    dir: PathBuf,
    config: RwLock<AppConfig>,
    runtime_state: RwLock<RuntimeState>,
    models_cache: RwLock<ModelsCache>,
}

impl Storage {
    pub async fn load() -> Result<Self> {
        let dir = app_data_dir()?;
        Self::load_from_dir_inner(dir).await
    }

    #[cfg(test)]
    pub async fn load_from_dir(dir: PathBuf) -> Result<Self> {
        Self::load_from_dir_inner(dir).await
    }

    async fn load_from_dir_inner(dir: PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create app data dir {}", dir.display()))?;
        let config_path = dir.join("config.toml");
        let state_path = dir.join("state.json");
        let models_path = dir.join("models_cache.json");

        let (config, should_save_config) = if config_path.exists() {
            let raw = tokio::fs::read_to_string(&config_path)
                .await
                .with_context(|| format!("read {}", config_path.display()))?;
            let cfg = toml::from_str::<AppConfig>(&raw)
                .with_context(|| format!("parse {}", config_path.display()))?;
            let should_save = !raw.contains("auto_round_robin")
                || !raw.contains("auto_failover")
                || !raw.contains("selected_provider_id")
                || !raw.contains("stream_idle_timeout_secs")
                || !raw.contains("max_request_body_mb");
            (cfg, should_save)
        } else {
            let cfg = AppConfig::default();
            let raw = toml::to_string_pretty(&cfg)?;
            tokio::fs::write(&config_path, raw)
                .await
                .with_context(|| format!("write {}", config_path.display()))?;
            (cfg, false)
        };
        if should_save_config {
            let raw = toml::to_string_pretty(&config)?;
            tokio::fs::write(&config_path, raw)
                .await
                .with_context(|| format!("migrate {}", config_path.display()))?;
        }

        let runtime_state = if state_path.exists() {
            let raw = tokio::fs::read_to_string(&state_path)
                .await
                .unwrap_or_default();
            serde_json::from_str::<RuntimeState>(&raw).unwrap_or_default()
        } else {
            RuntimeState::default()
        };

        let models_cache = if models_path.exists() {
            let raw = tokio::fs::read_to_string(&models_path)
                .await
                .unwrap_or_default();
            serde_json::from_str::<ModelsCache>(&raw).unwrap_or_default()
        } else {
            ModelsCache::default()
        };

        Ok(Self {
            inner: Arc::new(StorageInner {
                dir,
                config: RwLock::new(config),
                runtime_state: RwLock::new(runtime_state),
                models_cache: RwLock::new(models_cache),
            }),
        })
    }

    pub fn dir(&self) -> PathBuf {
        self.inner.dir.clone()
    }

    fn config_path(&self) -> PathBuf {
        self.inner.dir.join("config.toml")
    }

    fn state_path(&self) -> PathBuf {
        self.inner.dir.join("state.json")
    }

    fn models_path(&self) -> PathBuf {
        self.inner.dir.join("models_cache.json")
    }

    fn log_path(&self) -> PathBuf {
        self.inner.dir.join("request-log.jsonl")
    }

    pub async fn config(&self) -> AppConfig {
        self.inner.config.read().await.clone()
    }

    pub async fn set_config(&self, config: AppConfig) -> Result<()> {
        {
            let mut guard = self.inner.config.write().await;
            *guard = config.clone();
        }
        self.save_config(&config).await
    }

    pub async fn update_config<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut AppConfig) -> R,
    {
        let (result, snapshot) = {
            let mut guard = self.inner.config.write().await;
            let result = f(&mut guard);
            (result, guard.clone())
        };
        self.save_config(&snapshot).await?;
        Ok(result)
    }

    async fn save_config(&self, config: &AppConfig) -> Result<()> {
        let raw = toml::to_string_pretty(config)?;
        tokio::fs::write(self.config_path(), raw).await?;
        Ok(())
    }

    pub async fn runtime_state(&self) -> RuntimeState {
        self.inner.runtime_state.read().await.clone()
    }

    pub async fn update_runtime_state<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut RuntimeState) -> R,
    {
        let (result, snapshot) = {
            let mut guard = self.inner.runtime_state.write().await;
            let result = f(&mut guard);
            (result, guard.clone())
        };
        let raw = serde_json::to_string_pretty(&snapshot)?;
        tokio::fs::write(self.state_path(), raw).await?;
        Ok(result)
    }

    pub async fn models_cache(&self) -> ModelsCache {
        self.inner.models_cache.read().await.clone()
    }

    pub async fn set_models_cache(&self, cache: ModelsCache) -> Result<()> {
        {
            let mut guard = self.inner.models_cache.write().await;
            *guard = cache.clone();
        }
        let raw = serde_json::to_string_pretty(&cache)?;
        tokio::fs::write(self.models_path(), raw).await?;
        Ok(())
    }

    pub async fn append_log(&self, entry: &RequestLogEntry) -> Result<()> {
        let path = self.log_path();
        let mut line = serde_json::to_string(entry)?;
        line.push('\n');
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        file.write_all(line.as_bytes()).await?;
        Ok(())
    }

    pub async fn read_logs(&self, limit: usize) -> Result<Vec<RequestLogEntry>> {
        let path = self.log_path();
        if !path.exists() {
            return Ok(Vec::new());
        }
        let raw = tokio::fs::read_to_string(path).await?;
        let mut entries: Vec<_> = raw
            .lines()
            .rev()
            .take(limit)
            .filter_map(|line| serde_json::from_str::<RequestLogEntry>(line).ok())
            .collect();
        entries.reverse();
        Ok(entries)
    }
}

pub fn app_data_dir() -> Result<PathBuf> {
    if let Ok(dir) = std::env::var("OTHER_MODEL_DATA_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            return Ok(PathBuf::from(trimmed));
        }
    }
    let base = dirs::data_dir()
        .or_else(dirs::data_local_dir)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let next = base.join("Other Model");
    if next.exists() {
        return Ok(next);
    }
    let legacy = base.join("APIDev Gateway");
    if legacy.exists() {
        return Ok(legacy);
    }
    Ok(next)
}
