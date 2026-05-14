use crate::models::{AppConfig, ModelsCache, RequestLogEntry, RuntimeState};
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    dir: PathBuf,
    backend: StorageBackend,
    config: RwLock<AppConfig>,
    runtime_state: RwLock<RuntimeState>,
    models_cache: RwLock<ModelsCache>,
}

#[derive(Clone)]
enum StorageBackend {
    File,
    Sqlite { db_path: PathBuf },
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

        let (mut config, mut should_save_config) = if config_path.exists() {
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
        if config.normalize_balance_auth() {
            should_save_config = true;
        }
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
                backend: StorageBackend::File,
                config: RwLock::new(config),
                runtime_state: RwLock::new(runtime_state),
                models_cache: RwLock::new(models_cache),
            }),
        })
    }

    pub async fn load_sqlite(db_path: PathBuf) -> Result<Self> {
        let dir = db_path
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        tokio::fs::create_dir_all(&dir)
            .await
            .with_context(|| format!("create sqlite data dir {}", dir.display()))?;
        let (config, runtime_state, models_cache) = {
            let conn = sqlite_connection(&db_path)?;
            migrate_sqlite(&conn)?;
            let mut config = match sqlite_get_app_value::<AppConfig>(&conn, "config")? {
                Some(cfg) => cfg,
                None => {
                    let cfg = AppConfig::default();
                    sqlite_set_app_value(&conn, "config", &cfg)?;
                    sqlite_sync_providers(&conn, &cfg)?;
                    cfg
                }
            };
            if config.normalize_balance_auth() {
                sqlite_set_app_value(&conn, "config", &config)?;
            }
            let runtime_state =
                sqlite_get_app_value::<RuntimeState>(&conn, "runtime_state")?.unwrap_or_default();
            let models_cache =
                sqlite_get_app_value::<ModelsCache>(&conn, "models_cache")?.unwrap_or_default();
            sqlite_sync_providers(&conn, &config)?;
            sqlite_sync_runtime_state(&conn, &runtime_state)?;
            sqlite_sync_models_cache(&conn, &models_cache)?;
            (config, runtime_state, models_cache)
        };

        Ok(Self {
            inner: Arc::new(StorageInner {
                dir,
                backend: StorageBackend::Sqlite { db_path },
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
        let mut config = config;
        config.normalize_balance_auth();
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
            guard.normalize_balance_auth();
            (result, guard.clone())
        };
        self.save_config(&snapshot).await?;
        Ok(result)
    }

    async fn save_config(&self, config: &AppConfig) -> Result<()> {
        match &self.inner.backend {
            StorageBackend::File => {
                let raw = toml::to_string_pretty(config)?;
                tokio::fs::write(self.config_path(), raw).await?;
            }
            StorageBackend::Sqlite { db_path } => {
                let conn = sqlite_connection(db_path)?;
                sqlite_set_app_value(&conn, "config", config)?;
                sqlite_sync_providers(&conn, config)?;
            }
        }
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
        match &self.inner.backend {
            StorageBackend::File => {
                let raw = serde_json::to_string_pretty(&snapshot)?;
                tokio::fs::write(self.state_path(), raw).await?;
            }
            StorageBackend::Sqlite { db_path } => {
                let conn = sqlite_connection(db_path)?;
                sqlite_set_app_value(&conn, "runtime_state", &snapshot)?;
                sqlite_sync_runtime_state(&conn, &snapshot)?;
            }
        }
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
        match &self.inner.backend {
            StorageBackend::File => {
                let raw = serde_json::to_string_pretty(&cache)?;
                tokio::fs::write(self.models_path(), raw).await?;
            }
            StorageBackend::Sqlite { db_path } => {
                let conn = sqlite_connection(db_path)?;
                sqlite_set_app_value(&conn, "models_cache", &cache)?;
                sqlite_sync_models_cache(&conn, &cache)?;
            }
        }
        Ok(())
    }

    pub async fn append_log(&self, entry: &RequestLogEntry) -> Result<()> {
        match &self.inner.backend {
            StorageBackend::File => {
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
            }
            StorageBackend::Sqlite { db_path } => {
                let conn = sqlite_connection(db_path)?;
                conn.execute(
                    "INSERT OR REPLACE INTO request_logs (id, timestamp, value) VALUES (?1, ?2, ?3)",
                    params![
                        &entry.id,
                        entry.timestamp.to_rfc3339(),
                        serde_json::to_string(entry)?
                    ],
                )?;
            }
        }
        Ok(())
    }

    pub async fn read_logs(&self, limit: usize) -> Result<Vec<RequestLogEntry>> {
        match &self.inner.backend {
            StorageBackend::File => {
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
            StorageBackend::Sqlite { db_path } => {
                let conn = sqlite_connection(db_path)?;
                let mut stmt = conn
                    .prepare("SELECT value FROM request_logs ORDER BY timestamp DESC LIMIT ?1")?;
                let rows = stmt.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
                let mut entries = Vec::new();
                for raw in rows.flatten() {
                    if let Ok(entry) = serde_json::from_str::<RequestLogEntry>(&raw) {
                        entries.push(entry);
                    }
                }
                entries.reverse();
                Ok(entries)
            }
        }
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

fn sqlite_connection(path: &PathBuf) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("open sqlite {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

fn migrate_sqlite(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS providers (
            id TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS provider_state (
            provider_id TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS models_cache (
            provider_id TEXT PRIMARY KEY,
            fetched_at TEXT,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS request_logs (
            id TEXT PRIMARY KEY,
            timestamp TEXT NOT NULL,
            value TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_request_logs_timestamp ON request_logs(timestamp);
        CREATE TABLE IF NOT EXISTS response_routes (
            response_id TEXT PRIMARY KEY,
            provider_id TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        "#,
    )?;
    Ok(())
}

fn sqlite_get_app_value<T>(conn: &Connection, key: &str) -> Result<Option<T>>
where
    T: serde::de::DeserializeOwned,
{
    let mut stmt = conn.prepare("SELECT value FROM app_config WHERE key = ?1")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        let raw: String = row.get(0)?;
        Ok(Some(serde_json::from_str(&raw)?))
    } else {
        Ok(None)
    }
}

pub(crate) fn sqlite_get_raw_app_value(db_path: &PathBuf, key: &str) -> Result<Option<String>> {
    let conn = sqlite_connection(db_path)?;
    let mut stmt = conn.prepare("SELECT value FROM app_config WHERE key = ?1")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get(0)?))
    } else {
        Ok(None)
    }
}

pub(crate) fn sqlite_set_raw_app_value(db_path: &PathBuf, key: &str, value: &str) -> Result<()> {
    let conn = sqlite_connection(db_path)?;
    conn.execute(
        "INSERT INTO app_config (key, value, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
        params![key, value],
    )?;
    Ok(())
}

fn sqlite_set_app_value<T>(conn: &Connection, key: &str, value: &T) -> Result<()>
where
    T: serde::Serialize,
{
    conn.execute(
        "INSERT INTO app_config (key, value, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP",
        params![key, serde_json::to_string_pretty(value)?],
    )?;
    Ok(())
}

fn sqlite_sync_providers(conn: &Connection, config: &AppConfig) -> Result<()> {
    conn.execute("DELETE FROM providers", [])?;
    for provider in &config.providers {
        conn.execute(
            "INSERT OR REPLACE INTO providers (id, value, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)",
            params![&provider.id, serde_json::to_string_pretty(provider)?],
        )?;
    }
    Ok(())
}

fn sqlite_sync_runtime_state(conn: &Connection, state: &RuntimeState) -> Result<()> {
    conn.execute("DELETE FROM provider_state", [])?;
    for item in state.providers.values() {
        conn.execute(
            "INSERT OR REPLACE INTO provider_state (provider_id, value, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)",
            params![&item.provider_id, serde_json::to_string_pretty(item)?],
        )?;
    }
    conn.execute("DELETE FROM response_routes", [])?;
    for (response_id, provider_id) in &state.response_routes {
        conn.execute(
            "INSERT OR REPLACE INTO response_routes (response_id, provider_id, updated_at) VALUES (?1, ?2, CURRENT_TIMESTAMP)",
            params![response_id, provider_id],
        )?;
    }
    Ok(())
}

fn sqlite_sync_models_cache(conn: &Connection, cache: &ModelsCache) -> Result<()> {
    conn.execute("DELETE FROM models_cache", [])?;
    for item in cache.providers.values() {
        conn.execute(
            "INSERT OR REPLACE INTO models_cache (provider_id, fetched_at, value, updated_at) VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP)",
            params![
                item.provider_id,
                item.fetched_at.as_ref().map(|v| v.to_rfc3339()),
                serde_json::to_string_pretty(item)?
            ],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod sqlite_tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_storage_initializes_and_persists() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("other-model.sqlite");
        let storage = Storage::load_sqlite(db.clone()).await.unwrap();
        assert!(db.exists());
        let mut cfg = storage.config().await;
        cfg.gateway.port = 14556;
        storage.set_config(cfg.clone()).await.unwrap();
        storage
            .append_log(&RequestLogEntry {
                method: "GET".to_string(),
                path: "/v1/models".to_string(),
                ..Default::default()
            })
            .await
            .unwrap();

        let reopened = Storage::load_sqlite(db).await.unwrap();
        assert_eq!(reopened.config().await.gateway.port, 14556);
        assert_eq!(reopened.read_logs(10).await.unwrap().len(), 1);
    }
}
