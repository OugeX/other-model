export type ProviderHealth = 'healthy' | 'degraded' | 'cooling_down' | 'auth_failed' | 'disabled' | 'unknown';

export interface GatewayConfig {
  host: string;
  port: number;
  require_local_token: boolean;
  request_timeout_secs: number;
}

export interface RoutingConfig {
  auto_round_robin: boolean;
  auto_failover: boolean;
  max_attempts_per_request: number;
  cooldown_secs: number;
  selected_provider_id?: string | null;
}

export interface QuotaConfig {
  url: string;
  method: string;
  headers: Record<string, string>;
  balance_json_path?: string | null;
}

export interface ProviderConfig {
  id: string;
  name: string;
  base_url: string;
  api_key: string;
  enabled: boolean;
  timeout_secs: number;
  headers: Record<string, string>;
  query: Record<string, string>;
  quota?: QuotaConfig | null;
}

export interface AppConfig {
  gateway: GatewayConfig;
  providers: ProviderConfig[];
  local_auth_token: string;
  routing: RoutingConfig;
}

export interface ProviderState {
  provider_id: string;
  health: ProviderHealth;
  last_checked_at?: string | null;
  cooldown_until?: string | null;
  consecutive_failures: number;
  last_error?: string | null;
  last_latency_ms?: number | null;
  last_status?: number | null;
  remaining_hint?: string | null;
}

export interface ProviderView {
  config: ProviderConfig;
  state: ProviderState;
  model_count: number;
  gpt_model_count: number;
}

export interface ProviderExportResult {
  ok: boolean;
  path: string;
  count: number;
  message: string;
}

export interface ProviderImportResult {
  ok: boolean;
  imported: number;
  updated: number;
  skipped: number;
  total: number;
  message: string;
}

export interface GatewayStatus {
  running: boolean;
  bind_url: string;
  provider_count: number;
  enabled_provider_count: number;
}

export interface ModelInfo {
  id: string;
  object?: string | null;
  created?: number | null;
  owned_by?: string | null;
  raw?: unknown;
}

export interface ProviderModels {
  provider_id: string;
  fetched_at?: string | null;
  models: ModelInfo[];
  error?: string | null;
}

export interface ModelsCache {
  refreshed_at?: string | null;
  providers: Record<string, ProviderModels>;
}

export interface RequestLogEntry {
  id: string;
  timestamp: string;
  method: string;
  path: string;
  model?: string | null;
  provider_id?: string | null;
  provider_name?: string | null;
  status?: number | null;
  latency_ms: number;
  attempts: number;
  streamed: boolean;
  error?: string | null;
  usage?: unknown;
}

export interface TestResult {
  ok: boolean;
  provider_id: string;
  model?: string | null;
  status?: number | null;
  latency_ms: number;
  error?: string | null;
  response_preview?: string | null;
}

export interface QuotaResult {
  provider_id: string;
  ok: boolean;
  balance?: string | null;
  health_hint?: string | null;
  status?: number | null;
  error?: string | null;
  raw?: unknown;
}

export interface CodexConfigResult {
  ok: boolean;
  config_path: string;
  backup_path?: string | null;
  message: string;
}

export type Tab = 'dashboard' | 'providers' | 'models' | 'quota' | 'logs' | 'codex' | 'settings';
