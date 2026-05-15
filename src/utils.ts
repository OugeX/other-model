import type { ProviderConfig, ProviderHealth, ProviderState } from './types';

export const newProvider = (): ProviderConfig => ({
  id: crypto.randomUUID(),
  name: 'New Provider',
  base_url: 'https://api.openai.com/v1',
  api_key: '',
  enabled: true,
  timeout_secs: 300,
  headers: {},
  query: {},
  quota: null,
  balance_auth: {
    mode: 'disabled',
    username: null,
    password: null,
  },
});

export function mask(value: string, visible = 6) {
  if (!value) return '未填写';
  if (value.length <= visible * 2) return '••••••';
  return `${value.slice(0, visible)}…${value.slice(-4)}`;
}

export function healthLabel(health: ProviderHealth) {
  const map: Record<ProviderHealth, string> = {
    healthy: '健康',
    degraded: '降级',
    cooling_down: '冷却中',
    auth_failed: '认证异常',
    disabled: '禁用',
    unknown: '未知',
  };
  return map[health] ?? health;
}

export function errorKindLabel(kind?: string | null) {
  const map: Record<string, string> = {
    auth_suspect: '认证异常（待复核）',
    auth_failed: '认证异常',
    permission: '分组权限限制',
    quota: '余额/额度不足',
    rate_limit: '上游限流',
    upstream_5xx: '上游异常',
    network: '网络异常',
    unknown: '未知异常',
  };
  return kind ? (map[kind] ?? kind) : '';
}

export function providerStateSummary(state: ProviderState) {
  const base = healthLabel(state.health);
  const reason = errorKindLabel(state.error_kind);
  return reason && reason !== base ? `${base} / ${reason}` : base;
}

export function healthClass(health: ProviderHealth) {
  if (health === 'healthy') return 'ok';
  if (health === 'degraded' || health === 'cooling_down') return 'warn';
  if (health === 'auth_failed' || health === 'disabled') return 'bad';
  return 'muted';
}

export function flattenModels(cache: { providers: Record<string, { models: { id: string }[] }> }) {
  const seen = new Set<string>();
  return Object.values(cache.providers)
    .flatMap((p) => p.models)
    .filter((m) => isGptModel(m.id))
    .filter((m) => {
      if (seen.has(m.id)) return false;
      seen.add(m.id);
      return true;
    })
    .sort((a, b) => a.id.localeCompare(b.id));
}

export function isGptModel(id: string) {
  const normalized = id.trim().toLowerCase().replace(/^openai\//, '').replace(/^models\//, '');
  return normalized === 'gpt-5.4' || normalized === 'gpt-5.5';
}


export function modelsForProvider(cache: { providers: Record<string, { models: { id: string }[] }> }, providerId: string) {
  return [...(cache.providers[providerId]?.models ?? [])].filter((m) => isGptModel(m.id)).sort((a, b) => {
    const ag = isGptModel(a.id) ? 0 : 1;
    const bg = isGptModel(b.id) ? 0 : 1;
    return ag - bg || a.id.localeCompare(b.id);
  });
}

export function gptModelsFromCache(cache: { providers: Record<string, { models: { id: string }[] }> }) {
  return flattenModels(cache);
}
