import { invoke } from '@tauri-apps/api/core';
import type {
  AppConfig,
  CodexConfigResult,
  GatewayStatus,
  ModelInfo,
  ModelsCache,
  ProviderConfig,
  ProviderExportResult,
  ProviderImportResult,
  ProviderView,
  QuotaResult,
  RequestLogEntry,
  TestResult,
} from './types';

const fallbackProviderViews: ProviderView[] = [
  {
    config: {
      id: 'ciyuanshen',
      name: '词原神',
      base_url: 'https://ciyuanshen.top/v1',
      api_key: '',
      enabled: false,
      timeout_secs: 120,
      headers: {},
      query: {},
      quota: null,
    },
    state: { provider_id: 'ciyuanshen', health: 'unknown', consecutive_failures: 0 },
    model_count: 0,
    gpt_model_count: 0,
  },
  {
    config: {
      id: 'uocode',
      name: 'Uocode',
      base_url: 'https://uocode.com/v1',
      api_key: '',
      enabled: false,
      timeout_secs: 120,
      headers: {},
      query: {},
      quota: null,
    },
    state: { provider_id: 'uocode', health: 'unknown', consecutive_failures: 0 },
    model_count: 0,
    gpt_model_count: 0,
  },
];

const isTauri = '__TAURI_INTERNALS__' in window;

async function call<T>(command: string, args?: Record<string, unknown>, fallback?: T): Promise<T> {
  if (!isTauri) {
    await new Promise((resolve) => setTimeout(resolve, 120));
    if (fallback !== undefined) return fallback;
    throw new Error(`Command ${command} is only available inside the Tauri app.`);
  }
  return invoke<T>(command, args);
}

export const api = {
  status: () => call<GatewayStatus>('get_gateway_status', undefined, {
    running: false,
    bind_url: 'http://127.0.0.1:14555/v1',
    provider_count: fallbackProviderViews.length,
    enabled_provider_count: 0,
  }),
  start: () => call<GatewayStatus>('start_gateway'),
  stop: () => call<GatewayStatus>('stop_gateway'),
  getConfig: () => call<AppConfig>('get_app_config'),
  saveConfig: (config: AppConfig) => call<AppConfig>('save_app_config', { config }),
  listProviders: () => call<ProviderView[]>('list_providers', undefined, fallbackProviderViews),
  createProvider: (provider: ProviderConfig) => call<ProviderConfig>('create_provider', { provider }),
  updateProvider: (provider: ProviderConfig) => call<ProviderConfig>('update_provider', { provider }),
  deleteProvider: (providerId: string) => call<boolean>('delete_provider', { providerId }),
  exportProviders: (directory?: string) => call<ProviderExportResult>('export_providers', { directory }),
  importProviders: (raw: string) => call<ProviderImportResult>('import_providers', { raw }),
  discoverModels: () => call<ModelsCache>('discover_models'),
  getModelsCache: () => call<ModelsCache>('get_models_cache', undefined, { providers: {} }),
  listGptModels: () => call<ModelInfo[]>('list_gpt_models', undefined, []),
  testModel: (providerId: string, model?: string) => call<TestResult>('test_model', { providerId, model }),
  testProvider: (providerId: string) => call<TestResult>('test_provider', { providerId }),
  getQuota: (providerId: string) => call<QuotaResult>('get_quota', { providerId }),
  getLogs: (limit = 200) => call<RequestLogEntry[]>('get_logs', { limit }, []),
  configureCodex: (model: string, providerName = 'other_model_gateway') =>
    call<CodexConfigResult>('configure_codex', { request: { model, provider_name: providerName } }),
  restoreCodex: () => call<CodexConfigResult>('restore_codex_backup'),
  codexConfigPath: () => call<string>('get_codex_config_path', undefined, '~/.codex/config.toml'),
  appDataDir: () => call<string>('app_data_dir', undefined, 'Tauri app data dir'),
};
