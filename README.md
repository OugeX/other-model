# Other Model

![Other Model logo](docs/logo.png)

> Chinese documentation: [README.zh-CN.md](README.zh-CN.md)

Other Model is a local OpenAI-compatible gateway for Codex and GPT-series upstream providers. It ships as a Tauri desktop app and as a single-machine self-hosted Web service. Codex calls the local `/v1` gateway first, then Other Model routes requests to a managed pool of upstream providers with round-robin, failover, model discovery, health checks, quota hints, logs, and Codex configuration helpers.

## Features

- Cross-platform Tauri v2 desktop app with React + TypeScript UI.
- Self-hosted Web version: one Rust service serves the admin UI, REST API, and `/v1` gateway.
- Desktop local gateway: `http://127.0.0.1:14555/v1`.
- Web local gateway: `http://127.0.0.1:14556/v1`.
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
- Large Codex request support: local body limit defaults to 512 MB instead of Axum's small default, with structured 413 logs when exceeded.
- Long-running Codex stream support with configurable idle timeout.
- Model discovery is intentionally limited to `gpt-5.4` and `gpt-5.5`, with provider-level tests for those models only.
- Optional quota endpoint adapter plus request logs.
- Desktop: one-click Codex config update with automatic backup.
- Web: copyable `config.toml` snippet and downloadable `configure-codex.sh`; it never writes local Codex files from the browser.
- One-click gateway self-check for health, models, Responses, large bodies, streaming, and failover readiness.
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

Run the self-hosted Web version:

```bash
npm run build
cd src-tauri
cargo run --bin other-model-web -- serve --host 127.0.0.1 --port 14556
```

Or from the repository root:

```bash
npm run web:serve
```

Web environment variables:

- `OTHER_MODEL_DB=/path/to/other-model.sqlite` overrides the default SQLite path.
- `OTHER_MODEL_ADMIN_PASSWORD=...` sets or resets the single admin password.
- `OTHER_MODEL_WEB_DIST=/path/to/dist` overrides the React static asset directory.

## Local storage

Runtime config is stored in the OS data directory under `Other Model`:

- `config.toml`
- `models_cache.json`
- `request-log.jsonl`
- `state.json`

API keys are stored in plaintext for this MVP. Provider import/export JSON files also contain plaintext API keys, so keep them private.

The Web version uses a single SQLite database:

- default path: `./data/other-model.sqlite`
- override path: `OTHER_MODEL_DB=/path/to/other-model.sqlite`
- tables include `app_config`, `providers`, `provider_state`, `models_cache`, `request_logs`, and `response_routes`.

The first Web launch generates an admin password and prints it once in the terminal. If you lose it, restart with `OTHER_MODEL_ADMIN_PASSWORD=your-new-password`.

Gateway defaults:

- local base URL: `http://127.0.0.1:14555/v1`
- max request body: `512 MB`
- non-stream request timeout: `300s`
- stream idle timeout: `300s`

If a request exceeds the configured local body limit, Other Model returns a structured `413 Payload Too Large` JSON response and writes a request log row with `local_rejected=true`.

## Codex configuration

### Desktop app

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

[shell_environment_policy.set]
NO_PROXY = "localhost,127.0.0.1,::1"
no_proxy = "localhost,127.0.0.1,::1"
```

Before modifying Codex config, the app creates `config.toml.other-model-bak-<timestamp>` next to the original file. On macOS it also writes `~/.codex/other-model-env.sh` and sources it from `~/.zshrc` / `~/.zprofile`, then sets `launchctl` `NO_PROXY` for newly launched GUI apps. This prevents local gateway requests from being routed through a system HTTP proxy and returning `502 Bad Gateway`.

If Codex CLI still reports `unexpected status 502 Bad Gateway` and the Other Model request log has no matching request, restart Terminal/Codex or run the CLI with:

```bash
NO_PROXY=localhost,127.0.0.1,::1 no_proxy=localhost,127.0.0.1,::1 codex exec "只回复 pong"
```

### Web version

The Web version does not modify `~/.codex/config.toml`. Open the Codex page, choose `gpt-5.4` or `gpt-5.5`, then copy the generated snippet or download `configure-codex.sh`.

Example snippet:

```toml
model = "gpt-5.5"
model_provider = "other_model_web"

[model_providers.other_model_web]
name = "Other Model Web"
base_url = "http://127.0.0.1:14556/v1"
wire_api = "responses"
supports_websockets = false
experimental_bearer_token = "..."
```

## macOS unsigned builds

Public CI builds are currently unsigned. If macOS shows `"Other Model" is damaged and can't be opened` after copying from WeChat, a browser, or another Mac, remove the quarantine flag:

```bash
xattr -dr com.apple.quarantine "/Applications/Other Model.app"
```

If the app was copied through WeChat or AirDrop and still shows the same dialog on Apple Silicon / M-series Macs, run the included helper after dragging the app into `/Applications`:

```bash
scripts/fix-macos-damaged-app.command
```

Or run the full manual fix:

```bash
xattr -cr "/Applications/Other Model.app"
codesign --force --deep --sign - "/Applications/Other Model.app"
xattr -cr "/Applications/Other Model.app"
open "/Applications/Other Model.app"
```

For local redistribution you can also ad-hoc sign the app before packaging:

```bash
codesign --force --deep --sign - "/Applications/Other Model.app"
```

## Provider import/export

- Export: choose a target folder and Other Model writes `other-model-providers-YYYYMMDD-HHMMSS.json`.
- Import: choose a JSON file. Providers with matching IDs are updated; new IDs are added; invalid or duplicate entries are skipped.

## Release workflow

The repository includes a GitHub Actions workflow that builds release artifacts for macOS ARM64, macOS Intel, and Windows x64 when a version tag such as `v0.1.1` is pushed.

## Real Codex CLI smoke test

After installing the app and writing Codex config from the UI:

```bash
NO_PROXY=localhost,127.0.0.1,::1 no_proxy=localhost,127.0.0.1,::1 scripts/codex-smoke.sh
```

The script prefers `/Applications/Codex.app/Contents/Resources/codex` and falls back to `codex` on `PATH`.
