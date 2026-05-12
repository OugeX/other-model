# Other Model

![Other Model logo](docs/logo.png)

Other Model is a local OpenAI-compatible desktop gateway for Codex and GPT-series upstream providers. It lets Codex call `http://127.0.0.1:14555/v1` first, then routes requests to a managed pool of upstream providers with round-robin, failover, model discovery, health checks, quota hints, logs, and one-click Codex configuration.

## Features

- Cross-platform Tauri v2 desktop app with React + TypeScript UI.
- Local OpenAI-compatible gateway at `http://127.0.0.1:14555/v1`.
- Provider management: add, edit, delete, enable/disable, import, export, and health state.
- Export providers to a user-selected directory as JSON.
- OpenAI-compatible proxy endpoints:
  - `GET /v1/models`
  - `GET /v1/models/{model}`
  - `POST /v1/responses`
  - `POST /v1/chat/completions`
  - best-effort transparent forwarding for other `/v1/*` paths.
- Routing modes:
  - automatic provider round-robin;
  - single selected provider when round-robin is disabled;
  - optional automatic failover for 401/402/403/429/5xx/model errors.
- SSE streaming support: failover before first output; no replay after output begins.
- Model discovery, GPT model filtering, and provider-level model tests.
- Optional quota endpoint adapter plus request logs.
- One-click Codex config update with automatic backup.
- Lightweight local plaintext storage.

## Downloads

Release packages are published on the GitHub Releases page:

- macOS Apple Silicon (`aarch64` / ARM64)
- macOS Intel (`x86_64`)
- Windows x64

> If a platform asset is missing on a draft release, wait for the GitHub Actions release build matrix to finish.

## Development

```bash
npm install
npm run build
npm run tauri -- build
```

Rust tests:

```bash
cd src-tauri
cargo test
```

Run the gateway without the desktop UI:

```bash
npm run tauri -- build
./src-tauri/target/release/other-model --gateway-only
```

## Local storage

Runtime config is stored in the OS data directory under `Other Model`:

- `config.toml`
- `models_cache.json`
- `request-log.jsonl`
- `state.json`

API keys are stored in plaintext for this MVP. Provider import/export JSON files also contain plaintext API keys, so keep them private.

## Codex configuration

The app writes `~/.codex/config.toml` with an `other_model_gateway` provider when no active provider exists, or replaces the currently active provider in-place when one is already configured:

```toml
model_provider = "other_model_gateway"
model = "gpt-5.5"

[model_providers.other_model_gateway]
name = "Other Model"
base_url = "http://127.0.0.1:14555/v1"
wire_api = "responses"
supports_websockets = false
experimental_bearer_token = "..."
```

Before modifying Codex config, the app creates `config.toml.other-model-bak-<timestamp>` next to the original file.

## Provider import/export

- Export: choose a target folder and Other Model writes `other-model-providers-YYYYMMDD-HHMMSS.json`.
- Import: choose a JSON file. Providers with matching IDs are updated; new IDs are added; invalid or duplicate entries are skipped.

## Release workflow

The repository includes a GitHub Actions workflow that builds release artifacts for macOS ARM64, macOS Intel, and Windows x64 when a version tag such as `v0.1.0` is pushed.
