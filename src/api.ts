import { invoke } from '@tauri-apps/api/core';
import type {
  AppConfig,
  CodexSnippet,
  CodexConfigResult,
  GatewayStatus,
  GatewaySelfCheckResult,
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
      timeout_secs: 300,
      headers: {},
      query: {},
      quota: null,
      balance_auth: { mode: 'disabled', username: null, password: null },
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
      timeout_secs: 300,
      headers: {},
      query: {},
      quota: null,
      balance_auth: { mode: 'disabled', username: null, password: null },
    },
    state: { provider_id: 'uocode', health: 'unknown', consecutive_failures: 0 },
    model_count: 0,
    gpt_model_count: 0,
  },
];

const isTauri = '__TAURI_INTERNALS__' in window;
const ADMIN_TOKEN_KEY = 'other-model-admin-token';

function adminToken() {
  return window.localStorage.getItem(ADMIN_TOKEN_KEY) ?? '';
}

function setAdminToken(token: string) {
  if (token) window.localStorage.setItem(ADMIN_TOKEN_KEY, token);
  else window.localStorage.removeItem(ADMIN_TOKEN_KEY);
}

async function call<T>(command: string, args?: Record<string, unknown>, fallback?: T): Promise<T> {
  if (!isTauri) {
    await new Promise((resolve) => setTimeout(resolve, 120));
    if (fallback !== undefined) return fallback;
    throw new Error(`Command ${command} is only available inside the Tauri app.`);
  }
  return invoke<T>(command, args);
}

async function webResponse(path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  if (adminToken()) headers.set('Authorization', `Bearer ${adminToken()}`);
  if (init.body && !headers.has('Content-Type')) headers.set('Content-Type', 'application/json');
  const response = await fetch(path, { ...init, headers });
  if (response.status === 401) {
    setAdminToken('');
    const error = new Error('请先登录 Other Model Web 管理后台。');
    (error as Error & { status?: number }).status = 401;
    throw error;
  }
  if (!response.ok) {
    let message = `${response.status} ${response.statusText}`;
    try {
      const value = await response.json();
      message = value?.error?.message ?? message;
    } catch {
      const text = await response.text().catch(() => '');
      if (text) message = text;
    }
    throw new Error(message);
  }
  return response;
}

async function webJson<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await webResponse(path, init);
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

async function dispatch<T>(command: string, args?: Record<string, unknown>, fallback?: T): Promise<T> {
  if (isTauri) return call<T>(command, args, fallback);
  switch (command) {
    case 'get_gateway_status':
      return webJson<T>('/api/status');
    case 'start_gateway':
      return webJson<T>('/api/gateway/start', { method: 'POST' });
    case 'stop_gateway':
      return webJson<T>('/api/gateway/stop', { method: 'POST' });
    case 'get_app_config':
      return webJson<T>('/api/config');
    case 'save_app_config':
      return webJson<T>('/api/config', { method: 'POST', body: JSON.stringify(args?.config) });
    case 'list_providers':
      return webJson<T>('/api/providers');
    case 'create_provider':
      return webJson<T>('/api/providers', { method: 'POST', body: JSON.stringify(args?.provider) });
    case 'update_provider': {
      const provider = args?.provider as ProviderConfig;
      return webJson<T>(`/api/providers/${encodeURIComponent(provider.id)}`, { method: 'PUT', body: JSON.stringify(provider) });
    }
    case 'delete_provider':
      return webJson<T>(`/api/providers/${encodeURIComponent(String(args?.providerId))}`, { method: 'DELETE' });
    case 'import_providers':
      return webJson<T>('/api/providers/import', { method: 'POST', body: JSON.stringify({ raw: args?.raw }) });
    case 'discover_models':
      return webJson<T>('/api/models/discover', { method: 'POST' });
    case 'get_models_cache':
      return webJson<T>('/api/models/cache');
    case 'list_gpt_models': {
      const cache = await webJson<ModelsCache>('/api/models/cache');
      const map = new Map<string, ModelInfo>();
      Object.values(cache.providers).forEach((provider) => {
        provider.models.forEach((model) => map.set(model.id, model));
      });
      return Array.from(map.values()) as T;
    }
    case 'test_model':
      return webJson<T>('/api/models/test', { method: 'POST', body: JSON.stringify({ provider_id: args?.providerId, model: args?.model }) });
    case 'test_provider':
      return webJson<T>('/api/models/test', { method: 'POST', body: JSON.stringify({ provider_id: args?.providerId }) });
    case 'get_quota':
      return webJson<T>(`/api/quota/${encodeURIComponent(String(args?.providerId))}`);
    case 'get_logs':
      return webJson<T>(`/api/logs?limit=${encodeURIComponent(String(args?.limit ?? 200))}`);
    case 'run_gateway_self_check':
      return webJson<T>('/api/self-check', { method: 'POST', body: JSON.stringify({ model: args?.model }) });
    case 'app_data_dir': {
      const value = await webJson<{ db_path: string; data_dir: string }>('/api/storage/path');
      return (`SQLite: ${value.db_path}`) as T;
    }
    default:
      if (fallback !== undefined) return fallback;
      throw new Error(`Web API mapping for ${command} is not implemented.`);
  }
}

async function webExportProviders(): Promise<ProviderExportResult> {
  const response = await webResponse('/api/providers/export');
  const raw = await response.text();
  const data = JSON.parse(raw) as { providers?: ProviderConfig[] };
  const disposition = response.headers.get('content-disposition') ?? '';
  const match = disposition.match(/filename="?([^"]+)"?/i);
  const filename = match?.[1] ?? `other-model-providers-${Date.now()}.json`;
  const blob = new Blob([raw], { type: 'application/json;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  const count = data.providers?.length ?? 0;
  return {
    ok: true,
    path: filename,
    count,
    message: `已导出 ${count} 个供应商为 ${filename}。导出文件包含明文 API Key，请妥善保管。`,
  };
}

export const api = {
  isTauriMode: () => isTauri,
  hasAdminToken: () => isTauri || Boolean(adminToken()),
  login: async (password: string) => {
    const result = await webJson<{ ok: boolean; token: string }>('/api/auth/login', { method: 'POST', body: JSON.stringify({ password }) });
    setAdminToken(result.token);
    return result;
  },
  logout: async () => {
    if (!isTauri) {
      await webJson<{ ok: boolean }>('/api/auth/logout', { method: 'POST' }).catch(() => undefined);
      setAdminToken('');
    }
  },
  status: () => dispatch<GatewayStatus>('get_gateway_status', undefined, {
    running: false,
    bind_url: isTauri ? 'http://127.0.0.1:14555/v1' : 'http://127.0.0.1:14556/v1',
    provider_count: fallbackProviderViews.length,
    enabled_provider_count: 0,
  }),
  start: () => dispatch<GatewayStatus>('start_gateway'),
  stop: () => dispatch<GatewayStatus>('stop_gateway'),
  getConfig: () => dispatch<AppConfig>('get_app_config'),
  saveConfig: (config: AppConfig) => dispatch<AppConfig>('save_app_config', { config }),
  listProviders: () => dispatch<ProviderView[]>('list_providers', undefined, fallbackProviderViews),
  createProvider: (provider: ProviderConfig) => dispatch<ProviderConfig>('create_provider', { provider }),
  updateProvider: (provider: ProviderConfig) => dispatch<ProviderConfig>('update_provider', { provider }),
  deleteProvider: (providerId: string) => dispatch<boolean>('delete_provider', { providerId }),
  exportProviders: (directory?: string) => (isTauri ? call<ProviderExportResult>('export_providers', { directory }) : webExportProviders()),
  importProviders: (raw: string) => dispatch<ProviderImportResult>('import_providers', { raw }),
  discoverModels: () => dispatch<ModelsCache>('discover_models'),
  getModelsCache: () => dispatch<ModelsCache>('get_models_cache', undefined, { providers: {} }),
  listGptModels: () => dispatch<ModelInfo[]>('list_gpt_models', undefined, []),
  testModel: (providerId: string, model?: string) => dispatch<TestResult>('test_model', { providerId, model }),
  testProvider: (providerId: string) => dispatch<TestResult>('test_provider', { providerId }),
  getQuota: (providerId: string) => dispatch<QuotaResult>('get_quota', { providerId }),
  getLogs: (limit = 200) => dispatch<RequestLogEntry[]>('get_logs', { limit }, []),
  selfCheck: (model?: string) => dispatch<GatewaySelfCheckResult>('run_gateway_self_check', { model }),
  configureCodex: (model: string, providerName = 'other_model_gateway') =>
    call<CodexConfigResult>('configure_codex', { request: { model, provider_name: providerName } }),
  restoreCodex: () => call<CodexConfigResult>('restore_codex_backup'),
  codexConfigPath: () => call<string>('get_codex_config_path', undefined, '~/.codex/config.toml'),
  appDataDir: () => dispatch<string>('app_data_dir', undefined, 'Tauri app data dir'),
  codexSnippet: (model: string) => webJson<CodexSnippet>(`/api/codex/snippet?model=${encodeURIComponent(model)}`),
};
