mod codex_config;
mod commands;
mod gateway;
mod models;
mod quota;
mod storage;

use commands::AppState;
use gateway::GatewayManager;
use storage::Storage;
use tauri::Manager;

pub async fn run_gateway_only() -> anyhow::Result<()> {
    let storage = Storage::load().await?;
    let gateway = GatewayManager::new(storage);
    let status = gateway.start().await?;
    eprintln!("Other Model headless gateway listening at {}", status.bind_url);
    tokio::signal::ctrl_c().await?;
    let _ = gateway.stop().await;
    Ok(())
}

pub async fn configure_codex_no_proxy_only() -> anyhow::Result<()> {
    codex_config::configure_codex_proxy_bypass_only().await
}

pub async fn configure_codex_from_local_config(model: String) -> anyhow::Result<()> {
    let storage = Storage::load().await?;
    let cfg = storage.config().await;
    let gateway_base_url = format!("http://{}:{}/v1", cfg.gateway.host, cfg.gateway.port);
    let result = codex_config::configure_codex(
        models::ConfigureCodexRequest {
            model,
            provider_name: None,
        },
        gateway_base_url,
        cfg.local_auth_token,
    )
    .await?;
    eprintln!("{}", result.message);
    Ok(())
}

pub fn run() {
    tauri::async_runtime::block_on(async move {
        let storage = Storage::load().await.expect("load storage");
        let gateway = GatewayManager::new(storage.clone());
        tauri::Builder::default()
            .plugin(tauri_plugin_opener::init())
            .plugin(tauri_plugin_dialog::init())
            .manage(AppState { storage, gateway })
            .invoke_handler(commands::invoke_handler())
            .setup(|app| {
                let state = app.state::<AppState>().inner().clone();
                tauri::async_runtime::spawn(async move {
                    if let Err(err) = state.gateway.start().await {
                        eprintln!("failed to auto-start gateway: {err}");
                    }
                });
                Ok(())
            })
            .run(tauri::generate_context!())
            .expect("error while running tauri application");
    });
}
