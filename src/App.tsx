import { useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { open } from '@tauri-apps/plugin-dialog';
import { api } from './api';
import type {
  AppConfig,
  BalanceAuthMode,
  CodexSnippet,
  CodexConfigResult,
  GatewayStatus,
  ModelsCache,
  ProviderConfig,
  ProviderView,
  QuotaResult,
  RequestLogEntry,
  Tab,
  TestResult,
} from './types';
import { flattenModels, gptModelsFromCache, healthClass, isGptModel, mask, modelsForProvider, newProvider, providerStateSummary } from './utils';
import './styles.css';

const PAGE_SIZE = 10;
const IS_TAURI = api.isTauriMode();

type PopupTone = 'info' | 'success' | 'error' | 'warning';
type TotalPopupState = {
  title: string;
  message: string;
  tone?: PopupTone;
  details?: string;
  manual?: boolean;
};

type Notify = (popup: TotalPopupState) => void;
type BusyRunner = (fn: () => Promise<void>, ok?: string, popup?: Partial<TotalPopupState>) => Promise<void>;

const tabs: { key: Tab; label: string; hint: string }[] = [
  { key: 'dashboard', label: '仪表盘', hint: '运行状态' },
  { key: 'providers', label: '供应商', hint: '增删改查' },
  { key: 'models', label: '模型', hint: '仅 GPT-5.4/5.5' },
  { key: 'quota', label: '额度', hint: '余额/健康' },
  { key: 'logs', label: '日志', hint: '请求流水' },
  { key: 'codex', label: 'Codex', hint: IS_TAURI ? '一键配置' : '复制配置' },
  { key: 'settings', label: '设置', hint: '网关参数' },
];

const DEFAULT_BALANCE_AUTH = {
  mode: 'disabled' as BalanceAuthMode,
  username: null as string | null,
  password: null as string | null,
};

const balanceQueryMode = (provider: ProviderConfig): BalanceAuthMode =>
  provider.balance_auth?.mode ?? (provider.quota ? 'quota_api' : 'disabled');

const isLoginBalanceMode = (mode: BalanceAuthMode) => mode === 'newapi_login' || mode === 'sub2api_login';

const cloneProvider = (provider: ProviderConfig): ProviderConfig => ({
  ...provider,
  headers: { ...provider.headers },
  query: { ...provider.query },
  quota: provider.quota
    ? {
        ...provider.quota,
        headers: { ...provider.quota.headers },
      }
    : null,
  balance_auth: {
    ...DEFAULT_BALANCE_AUTH,
    ...(provider.balance_auth ?? {}),
    mode: balanceQueryMode(provider),
  },
  capabilities: {
    responses_api: provider.capabilities?.responses_api ?? true,
    responses_compact: provider.capabilities?.responses_compact ?? false,
    token_count: provider.capabilities?.token_count ?? true,
  },
});

export default function App() {
  const [tab, setTab] = useState<Tab>('dashboard');
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [providers, setProviders] = useState<ProviderView[]>([]);
  const [modelsCache, setModelsCache] = useState<ModelsCache>({ providers: {} });
  const [logs, setLogs] = useState<RequestLogEntry[]>([]);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [popup, setPopup] = useState<TotalPopupState | null>(null);
  const [busy, setBusy] = useState(false);
  const [authenticated, setAuthenticated] = useState(api.hasAdminToken());

  const notify: Notify = (next) => setPopup({ tone: 'info', ...next });

  const refresh = async () => {
    const [s, p, m, l] = await Promise.all([api.status(), api.listProviders(), api.getModelsCache(), api.getLogs(200)]);
    setStatus(s);
    setProviders(p);
    setModelsCache(m);
    setLogs(l);
    try {
      setConfig(await api.getConfig());
    } catch {
      // Browser preview fallback has no config.
    }
  };

  useEffect(() => {
    if (!authenticated) return;
    refresh().catch((err) => {
      if (!IS_TAURI && (err as Error & { status?: number }).status === 401) setAuthenticated(false);
      else notify({ title: '刷新失败', message: formatError(err), tone: 'error', manual: true });
    });
    const timer = window.setInterval(() => refresh().catch((err) => {
      if (!IS_TAURI && (err as Error & { status?: number }).status === 401) setAuthenticated(false);
    }), 5000);
    return () => window.clearInterval(timer);
  }, [authenticated]);

  const gptModels = useMemo(() => gptModelsFromCache(modelsCache), [modelsCache]);

  const runBusy: BusyRunner = async (fn, ok, popupOptions) => {
    setBusy(true);
    try {
      await fn();
      if (ok) notify({ title: popupOptions?.title ?? '操作成功', message: ok, tone: 'success', ...popupOptions });
      await refresh();
    } catch (err) {
      notify({ title: '操作失败', message: formatError(err), tone: 'error', manual: true });
    } finally {
      setBusy(false);
    }
  };

  const logout = async () => {
    setBusy(true);
    try {
      await api.logout();
      setAuthenticated(false);
      notify({ title: '已退出登录', message: '已退出 Other Model Web 管理后台。', tone: 'success' });
    } catch (err) {
      setAuthenticated(false);
      notify({ title: '已退出登录', message: `本地会话已清除。${formatError(err)}`, tone: 'warning', manual: true });
    } finally {
      setBusy(false);
    }
  };

  if (!IS_TAURI && !authenticated) {
    return (
      <>
        <LoginScreen
          busy={busy}
          onLogin={async (password) => {
            setBusy(true);
            try {
              await api.login(password);
              setAuthenticated(true);
              notify({ title: '登录成功', message: '已进入 Other Model Web 管理后台。', tone: 'success' });
            } catch (err) {
              notify({ title: '登录失败', message: formatError(err), tone: 'error', manual: true });
            } finally {
              setBusy(false);
            }
          }}
        />
        {popup && <TotalPopup popup={popup} onClose={() => setPopup(null)} />}
      </>
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-icon"><img src="/logo.svg" alt="Other Model" /></div>
          <div>
            <h1>Other Model</h1>
            <p>Local multi-provider model gateway</p>
          </div>
        </div>
        <nav>
          {tabs.map((item) => (
            <button key={item.key} className={tab === item.key ? 'active' : ''} onClick={() => setTab(item.key)}>
              <span>{item.label}</span>
              <small>{item.hint}</small>
            </button>
          ))}
        </nav>
        <div className="side-status">
          <span className={`dot ${status?.running ? 'ok' : 'bad'}`} />
          <div>
            <strong>{status?.running ? '网关运行中' : '网关未运行'}</strong>
            <code>{status?.bind_url ?? 'http://127.0.0.1:14555/v1'}</code>
          </div>
        </div>
      </aside>
      <main>
        <header className="topbar">
          <div>
            <h2>{tabs.find((t) => t.key === tab)?.label}</h2>
            <p>{tabs.find((t) => t.key === tab)?.hint}</p>
          </div>
          <div className="top-actions">
            {IS_TAURI ? (
              <>
                <button disabled={busy} onClick={() => runBusy(() => api.start().then(setStatus), '网关已启动')}>启动</button>
                <button disabled={busy} onClick={() => runBusy(() => api.stop().then(setStatus), '网关已停止')}>停止</button>
              </>
            ) : (
              <span className="pill ok">Web 服务运行中</span>
            )}
            <button disabled={busy} onClick={() => runBusy(refresh, '已刷新')}>刷新</button>
            {!IS_TAURI && <button disabled={busy} onClick={logout}>退出</button>}
          </div>
        </header>
        {tab === 'dashboard' && <Dashboard status={status} providers={providers} models={gptModels.length} logs={logs} />}
        {tab === 'providers' && <Providers providers={providers} cache={modelsCache} busy={busy} runBusy={runBusy} notify={notify} />}
        {tab === 'models' && <Models cache={modelsCache} busy={busy} runBusy={runBusy} />}
        {tab === 'quota' && <Quota providers={providers} busy={busy} runBusy={runBusy} notify={notify} />}
        {tab === 'logs' && <Logs logs={logs} />}
        {tab === 'codex' && <Codex models={gptModels.map((m) => m.id)} status={status} busy={busy} runBusy={runBusy} notify={notify} />}
        {tab === 'settings' && <Settings config={config} setConfig={setConfig} providers={providers} defaultModel={gptModels[0]?.id} busy={busy} runBusy={runBusy} notify={notify} />}
      </main>
      {popup && <TotalPopup popup={popup} onClose={() => setPopup(null)} />}
    </div>
  );
}

function TotalPopup({ popup, onClose }: { popup: TotalPopupState; onClose: () => void }) {
  useEffect(() => {
    if (popup.manual) return;
    const timer = window.setTimeout(onClose, 2400);
    return () => window.clearTimeout(timer);
  }, [popup, onClose]);

  return createPortal(
    <div className="total-popup-backdrop" role="presentation" onClick={onClose}>
      <div className={`total-popup ${popup.tone ?? 'info'}`} role="status" aria-live="polite" onClick={(event) => event.stopPropagation()}>
        <div className="total-popup-icon">{popup.tone === 'success' ? '✓' : popup.tone === 'error' ? '!' : popup.tone === 'warning' ? '⚠' : 'i'}</div>
        <div className="total-popup-content">
          <h3>{popup.title}</h3>
          <p>{popup.message}</p>
          {popup.details && <pre>{popup.details}</pre>}
          {popup.manual && <button onClick={onClose}>知道了</button>}
        </div>
      </div>
    </div>,
    document.body,
  );
}

function LoginScreen({ busy, onLogin }: { busy: boolean; onLogin: (password: string) => Promise<void> }) {
  const [password, setPassword] = useState('');
  return (
    <div className="login-shell">
      <div className="login-card">
        <div className="brand login-brand">
          <div className="brand-icon"><img src="/logo.svg" alt="Other Model" /></div>
          <div>
            <h1>Other Model Web</h1>
            <p>单机自托管模型网关</p>
          </div>
        </div>
        <p>请输入管理员密码。首次启动时密码会打印在 <code>other-model-web</code> 终端输出中；也可以通过 <code>OTHER_MODEL_ADMIN_PASSWORD</code> 重置。</p>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            onLogin(password);
          }}
        >
          <label>管理员密码
            <input autoFocus type="password" value={password} onChange={(event) => setPassword(event.target.value)} placeholder="输入管理员密码" />
          </label>
          <button disabled={busy || !password.trim()} type="submit">登录</button>
        </form>
      </div>
    </div>
  );
}

function Pagination({ page, total, onPageChange }: { page: number; total: number; onPageChange: (page: number) => void }) {
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  if (total <= PAGE_SIZE) {
    return <div className="pagination"><span>共 {total} 条</span></div>;
  }
  return (
    <div className="pagination">
      <span>共 {total} 条，第 {page}/{totalPages} 页</span>
      <div className="pagination-actions">
        <button disabled={page <= 1} onClick={() => onPageChange(page - 1)}>上一页</button>
        {Array.from({ length: totalPages }, (_, index) => index + 1).map((item) => (
          <button key={item} className={item === page ? 'active' : ''} onClick={() => onPageChange(item)}>{item}</button>
        ))}
        <button disabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>下一页</button>
      </div>
    </div>
  );
}

function usePaginated<T>(items: T[], deps: unknown[] = []) {
  const [page, setPage] = useState(1);
  const totalPages = Math.max(1, Math.ceil(items.length / PAGE_SIZE));
  useEffect(() => setPage(1), deps);
  useEffect(() => {
    if (page > totalPages) setPage(totalPages);
  }, [page, totalPages]);
  const pageItems = useMemo(() => items.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE), [items, page]);
  return { page, setPage, pageItems };
}

function Dashboard({ status, providers, models, logs }: { status: GatewayStatus | null; providers: ProviderView[]; models: number; logs: RequestLogEntry[] }) {
  const lastError = [...logs].reverse().find((l) => l.error);
  const healthy = providers.filter((p) => p.state.health === 'healthy').length;
  return (
    <section className="grid dashboard-grid">
      <Metric title="网关地址" value={status?.bind_url ?? '-'} tone={status?.running ? 'ok' : 'bad'} />
      <Metric title="启用供应商" value={`${status?.enabled_provider_count ?? 0}/${status?.provider_count ?? 0}`} tone="ok" />
      <Metric title="健康供应商" value={`${healthy}/${providers.length}`} tone={healthy ? 'ok' : 'warn'} />
      <Metric title="可用模型" value={String(models)} tone="ok" />
      <div className="card wide">
        <h3>运行说明</h3>
        <p>Codex 配置为本地网关后，请求会进入本机 `/v1`，再按当前路由设置转发到上游供应商。流式请求在输出前可故障转移，输出后不重放。</p>
        {lastError && <p className="danger-text">最近错误：{lastError.error}</p>}
      </div>
      <div className="card wide">
        <h3>供应商状态</h3>
        <div className="provider-pills">
          {providers.map((p) => (
            <span key={p.config.id} className={`pill ${healthClass(p.state.health)}`}>{p.config.name}: {providerStateSummary(p.state)}</span>
          ))}
        </div>
      </div>
    </section>
  );
}

function Metric({ title, value, tone }: { title: string; value: string; tone: string }) {
  return (
    <div className={`card metric ${tone}`}>
      <span>{title}</span>
      <strong>{value}</strong>
    </div>
  );
}

function Providers({ providers, cache, busy, runBusy, notify }: { providers: ProviderView[]; cache: ModelsCache; busy: boolean; runBusy: BusyRunner; notify: Notify }) {
  const [editing, setEditing] = useState<ProviderConfig | null>(null);
  const [modelTester, setModelTester] = useState<ProviderView | null>(null);
  const [rowResults, setRowResults] = useState<Record<string, TestResult>>({});
  const importInputId = 'provider-import-json';
  const { page, setPage, pageItems } = usePaginated(providers, [providers.length]);

  const importProviders = async (file: File | undefined) => {
    if (!file) return;
    const raw = await file.text();
    await runBusy(async () => {
      const result = await api.importProviders(raw);
      if (!result.ok) throw new Error(result.message);
      notify({
        title: '供应商导入完成',
        message: result.message,
        tone: 'success',
        manual: true,
        details: `总数 ${result.total} / 新增 ${result.imported} / 更新 ${result.updated} / 跳过 ${result.skipped}`,
      });
    });
  };

  const exportProviders = async () => {
    let selected: string | undefined;
    if (IS_TAURI) {
      const result = await open({ directory: true, multiple: false, title: '选择供应商导出目录' });
      if (!result || Array.isArray(result)) {
        notify({ title: '已取消导出', message: '未选择导出目录。', tone: 'info' });
        return;
      }
      selected = result;
    }
    await runBusy(async () => {
      const result = await api.exportProviders(selected);
      if (!result.ok) throw new Error(result.message);
      notify({
        title: '供应商导出完成',
        message: result.message,
        tone: 'success',
        manual: true,
        details: `文件：${result.path}\n数量：${result.count}\n注意：导出文件包含明文 API Key，请妥善保管。`,
      });
    });
  };

  return (
    <section className="stack">
      <div className="section-actions">
        <button onClick={() => setEditing(newProvider())}>新增供应商</button>
        <button disabled={busy} onClick={() => runBusy(() => api.discoverModels().then(() => undefined), '模型发现完成')}>一键查询模型</button>
        <button disabled={busy || providers.length === 0} onClick={exportProviders}>一键导出供应商</button>
        <label className={`button-like ${busy ? 'disabled' : ''}`} htmlFor={importInputId}>一键导入供应商</label>
        <input
          id={importInputId}
          className="hidden-file"
          type="file"
          accept="application/json,.json"
          disabled={busy}
          onChange={(e) => {
            importProviders(e.currentTarget.files?.[0]).catch((err) => notify({ title: '导入失败', message: formatError(err), tone: 'error', manual: true }));
            e.currentTarget.value = '';
          }}
        />
      </div>
      <p className="inline-help">导入/导出文件包含明文 API Key。导入时同 ID 供应商会被更新，新 ID 会新增。</p>
      <div className="card table-card">
        <table>
          <thead>
            <tr><th>名称</th><th>Base URL</th><th>Key</th><th>状态</th><th>模型</th><th>最后错误 / 检测结果</th><th>操作</th></tr>
          </thead>
          <tbody>
            {pageItems.map((p) => {
              const result = rowResults[p.config.id];
              return (
                <tr key={p.config.id}>
                  <td><strong>{p.config.name}</strong><br/><small>{p.config.enabled ? '启用' : '禁用'}</small></td>
                  <td><code>{p.config.base_url}</code></td>
                  <td>{mask(p.config.api_key)}</td>
                  <td><span className={`pill ${healthClass(p.state.health)}`}>{providerStateSummary(p.state)}</span></td>
                  <td>{p.gpt_model_count} 个（GPT-5.4/5.5）</td>
                  <td className="truncate">
                    {result ? `${result.model ?? '模型'}：${result.ok ? '可用' : '失败'} ${result.status ?? ''} ${result.error ?? ''}` : (p.state.last_error ?? '-')}
                  </td>
                  <td className="row-actions">
                    <button onClick={() => setEditing(p.config)}>编辑</button>
                    <button disabled={busy} onClick={() => runBusy(() => api.testProvider(p.config.id).then((r) => { setRowResults((prev) => ({ ...prev, [p.config.id]: r })); }), '供应商检测完成')}>检测</button>
                    <button disabled={busy} onClick={() => setModelTester(p)}>检测模型</button>
                    <button className="danger" disabled={busy} onClick={() => runBusy(() => api.deleteProvider(p.config.id).then(() => undefined), '已删除')}>删除</button>
                  </td>
                </tr>
              );
            })}
            {!providers.length && <tr><td colSpan={7}>暂无供应商，请新增或导入供应商。</td></tr>}
          </tbody>
        </table>
        <Pagination page={page} total={providers.length} onPageChange={setPage} />
      </div>
      {editing && <ProviderEditor provider={editing} onClose={() => setEditing(null)} runBusy={runBusy} />}
      {modelTester && (
        <ProviderModelTester
          provider={modelTester}
          models={modelsForProvider(cache, modelTester.config.id)}
          busy={busy}
          onClose={() => setModelTester(null)}
          onResult={(result) => setRowResults((prev) => ({ ...prev, [modelTester.config.id]: result }))}
          runBusy={runBusy}
          notify={notify}
        />
      )}
    </section>
  );
}

function ProviderEditor({ provider, onClose, runBusy }: { provider: ProviderConfig; onClose: () => void; runBusy: BusyRunner }) {
  const [draft, setDraft] = useState<ProviderConfig>(() => cloneProvider(provider));
  const isNew = !provider.id || provider.name === 'New Provider';
  const balanceMode = balanceQueryMode(draft);
  const balanceAuth = { ...DEFAULT_BALANCE_AUTH, ...(draft.balance_auth ?? {}), mode: balanceMode };

  const setBalanceMode = (mode: BalanceAuthMode) => {
    setDraft({
      ...draft,
      balance_auth: {
        ...balanceAuth,
        mode,
      },
    });
  };

  const save = async () => {
    await runBusy(async () => {
      if (isNew) await api.createProvider(draft);
      else await api.updateProvider(draft);
      onClose();
    }, '供应商已保存');
  };
  return (
    <div className="modal-backdrop">
      <div className="modal">
        <h3>{isNew ? '新增供应商' : '编辑供应商'}</h3>
        <label>名称<input value={draft.name} onChange={(e) => setDraft({ ...draft, name: e.target.value })} /></label>
        <label>Base URL<input value={draft.base_url} onChange={(e) => setDraft({ ...draft, base_url: e.target.value })} /></label>
        <label>API Key<input value={draft.api_key} type="password" onChange={(e) => setDraft({ ...draft, api_key: e.target.value })} /></label>
        <div className="form-row">
          <label><input type="checkbox" checked={draft.enabled} onChange={(e) => setDraft({ ...draft, enabled: e.target.checked })} /> 启用</label>
          <label>超时秒<input type="number" value={draft.timeout_secs} onChange={(e) => setDraft({ ...draft, timeout_secs: Number(e.target.value) || 300 })} /></label>
        </div>
        <div className="card">
          <h4>能力声明</h4>
          <label><input type="checkbox" checked={draft.capabilities.responses_api} onChange={(e) => setDraft({ ...draft, capabilities: { ...draft.capabilities, responses_api: e.target.checked } })} /> 支持 Responses API</label>
          <label><input type="checkbox" checked={draft.capabilities.responses_compact} onChange={(e) => setDraft({ ...draft, capabilities: { ...draft.capabilities, responses_compact: e.target.checked } })} /> 遗留：支持上游原生 /v1/responses/compact</label>
          <label><input type="checkbox" checked={draft.capabilities.token_count} onChange={(e) => setDraft({ ...draft, capabilities: { ...draft.capabilities, token_count: e.target.checked } })} /> 支持 token 估算/计数</label>
        </div>
        <div className="card">
          <h4>余额查询方式</h4>
          <label>查询模式
            <select value={balanceMode} onChange={(e) => setBalanceMode(e.target.value as BalanceAuthMode)}>
              <option value="disabled">关闭</option>
              <option value="quota_api">余额接口</option>
              <option value="newapi_login">newapi 登录查余额</option>
              <option value="sub2api_login">sub2api 登录查余额</option>
            </select>
          </label>
          <p className="inline-help">仅支持账号密码直登；不支持 2FA / 验证码。凭据会按当前本地存储策略保存。</p>
          {balanceMode === 'sub2api_login' && (
            <p className="inline-help">sub2api 官方登录接口通常要求填写完整登录邮箱，不能只填站内昵称或数字账号。</p>
          )}
          {balanceMode === 'quota_api' && (
            <>
              <label>Quota URL<input value={draft.quota?.url ?? ''} onChange={(e) => setDraft({ ...draft, quota: { ...(draft.quota ?? { method: 'GET', headers: {} }), url: e.target.value } })} /></label>
              <label>余额 JSON 路径<input placeholder="$.data.balance" value={draft.quota?.balance_json_path ?? ''} onChange={(e) => setDraft({ ...draft, quota: { ...(draft.quota ?? { method: 'GET', headers: {}, url: '' }), balance_json_path: e.target.value } })} /></label>
            </>
          )}
          {isLoginBalanceMode(balanceMode) && (
            <>
              <label>{balanceMode === 'newapi_login' ? '用户名' : '邮箱/账号'}
                <input
                  value={balanceAuth.username ?? ''}
                  onChange={(e) => setDraft({
                    ...draft,
                    balance_auth: {
                      ...balanceAuth,
                      username: e.target.value,
                    },
                  })}
                />
              </label>
              <label>登录密码
                <input
                  type="password"
                  value={balanceAuth.password ?? ''}
                  onChange={(e) => setDraft({
                    ...draft,
                    balance_auth: {
                      ...balanceAuth,
                      password: e.target.value,
                    },
                  })}
                />
              </label>
            </>
          )}
        </div>
        <div className="modal-actions"><button onClick={save}>保存</button><button onClick={onClose}>取消</button></div>
      </div>
    </div>
  );
}

function ProviderModelTester({
  provider,
  models,
  busy,
  onClose,
  onResult,
  runBusy,
  notify,
}: {
  provider: ProviderView;
  models: { id: string }[];
  busy: boolean;
  onClose: () => void;
  onResult: (result: TestResult) => void;
  runBusy: BusyRunner;
  notify: Notify;
}) {
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState(models.find((m) => isGptModel(m.id))?.id ?? models[0]?.id ?? '');
  const filtered = models.filter((m) => m.id.toLowerCase().includes(query.toLowerCase()));

  const testSelected = async () => {
    if (!selected) return;
    await runBusy(async () => {
      const next = await api.testModel(provider.config.id, selected);
      onResult(next);
      notify({
        title: next.ok ? '模型检测成功' : '模型检测失败',
        message: `${next.model ?? selected}：${next.ok ? '可用' : '失败'}，状态 ${next.status ?? '-'}，耗时 ${next.latency_ms}ms`,
        tone: next.ok ? 'success' : 'error',
        manual: true,
        details: next.error ?? next.response_preview ?? undefined,
      });
    });
  };

  return (
    <div className="modal-backdrop">
      <div className="modal model-modal">
        <h3>检测模型：{provider.config.name}</h3>
        <p>从该供应商已发现的 GPT-5.4 / GPT-5.5 中选择一个模型执行 `/responses` ping 检测。</p>
        {!models.length && <p className="empty-state error">该供应商暂无已发现的 GPT-5.4 / GPT-5.5，请先点击供应商页“一键查询模型”。</p>}
        <label>搜索模型<input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="输入模型名称筛选" /></label>
        <div className="model-list">
          {filtered.map((model) => (
            <button
              key={model.id}
              className={`model-choice ${selected === model.id ? 'selected' : ''}`}
              onClick={() => setSelected(model.id)}
            >
              <code>{model.id}</code>
              <span className="pill ok">支持</span>
            </button>
          ))}
          {models.length > 0 && filtered.length === 0 && <p className="muted-text">没有匹配的模型。</p>}
        </div>
        <div className="modal-actions">
          <button disabled={busy || !selected} onClick={testSelected}>检测所选模型</button>
          <button onClick={onClose}>关闭</button>
        </div>
      </div>
    </div>
  );
}

function Models({ cache, busy, runBusy }: { cache: ModelsCache; busy: boolean; runBusy: BusyRunner }) {
  const models = flattenModels(cache);
  const providerNames = (providerIds: string[]) => providerIds.join(', ');
  const { page, setPage, pageItems } = usePaginated(models, [models.length, cache.refreshed_at]);
  return (
    <section className="stack">
      <div className="section-actions">
        <button disabled={busy} onClick={() => runBusy(() => api.discoverModels().then(() => undefined), '模型发现完成')}>一键查询模型</button>
        <span>上次刷新：{cache.refreshed_at ?? '未刷新'}</span>
      </div>
      <div className="card table-card">
        <table>
      <thead><tr><th>模型</th><th>类型</th><th>发现供应商</th></tr></thead>
          <tbody>
            {pageItems.map((m) => {
              const foundIn = Object.values(cache.providers).filter((p) => p.models.some((pm) => pm.id === m.id)).map((p) => p.provider_id);
              return (
                <tr key={m.id}>
                  <td><code>{m.id}</code></td>
                  <td><span className="pill ok">GPT-5.4/5.5</span></td>
                  <td>{providerNames(foundIn) || '-'}</td>
                </tr>
              );
            })}
            {!models.length && <tr><td colSpan={3}>暂无 GPT-5.4 / GPT-5.5，请到供应商页点击“一键查询模型”。</td></tr>}
          </tbody>
        </table>
        <Pagination page={page} total={models.length} onPageChange={setPage} />
      </div>
    </section>
  );
}

function Quota({ providers, busy, runBusy, notify }: { providers: ProviderView[]; busy: boolean; runBusy: BusyRunner; notify: Notify }) {
  const [results, setResults] = useState<Record<string, QuotaResult>>({});
  const [queryingAll, setQueryingAll] = useState(false);

  const storeResult = (providerId: string, result: QuotaResult) => {
    setResults((current) => ({ ...current, [providerId]: result }));
  };

  const queryProviderBalance = async (provider: ProviderView) => {
    const result = await api.getQuota(provider.config.id);
    storeResult(provider.config.id, result);
    return result;
  };

  const queryAllBalances = async () => {
    if (!providers.length) {
      notify({ title: '暂无可查询供应商', message: '请先在供应商页面添加供应商。', tone: 'warning', manual: true });
      return;
    }

    setQueryingAll(true);
    await runBusy(async () => {
      const settled = await Promise.all(
        providers.map(async (provider) => {
          try {
            const result = await queryProviderBalance(provider);
            return { provider, result };
          } catch (err) {
            const result: QuotaResult = {
              provider_id: provider.config.id,
              ok: false,
              error: formatError(err),
            };
            storeResult(provider.config.id, result);
            return { provider, result };
          }
        }),
      );

      const successCount = settled.filter((item) => item.result.ok).length;
      const failureCount = settled.length - successCount;
      const details = settled
        .map(({ provider, result }) => {
          if (result.ok) {
            const summary = [result.balance, result.health_hint].filter(Boolean).join(' · ') || '查询完成';
            return `✓ ${provider.config.name}: ${summary}`;
          }
          return `✗ ${provider.config.name}: ${result.error ?? (result.status ? `HTTP ${result.status}` : '查询失败')}`;
        })
        .join('\n');

      notify({
        title: failureCount ? '批量查询完成（部分失败）' : '批量查询完成',
        message: failureCount ? `成功 ${successCount} 个，失败 ${failureCount} 个。` : `已完成 ${successCount} 个供应商余额查询。`,
        tone: failureCount ? 'warning' : 'success',
        manual: failureCount > 0,
        details,
      });
    });
    setQueryingAll(false);
  };

  return (
    <section className="stack">
      <div className="section-actions quota-toolbar">
        <button disabled={busy || !providers.length} onClick={queryAllBalances}>
          {queryingAll ? '正在查询全部余额...' : '一键查询全部余额'}
        </button>
        <p className="inline-help">会批量查询当前列表中的全部供应商，并把结果更新到下方卡片。</p>
      </div>
      <div className="grid">
        {providers.map((p) => {
          const result = results[p.config.id];
          const mode = balanceQueryMode(p.config);
          const modeLabel = mode === 'quota_api'
            ? '余额接口'
            : mode === 'newapi_login'
              ? 'newapi 登录'
              : mode === 'sub2api_login'
                ? 'sub2api 登录'
                : '关闭';
          return (
            <div className="card" key={p.config.id}>
              <h3>{p.config.name}</h3>
              <p><span className={`pill ${healthClass(p.state.health)}`}>{providerStateSummary(p.state)}</span></p>
              <p>查询方式：{modeLabel}</p>
              <p>余额：{result?.balance ?? p.state.remaining_hint ?? result?.health_hint ?? '未查询/未配置适配器'}</p>
              {result?.health_hint && <p className="inline-help">{result.health_hint}</p>}
              {result?.error && <p className="danger-text">{result.error}</p>}
              <button disabled={busy} onClick={() => runBusy(async () => { await queryProviderBalance(p); }, '余额查询完成')}>查询余额</button>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function Logs({ logs }: { logs: RequestLogEntry[] }) {
  const ordered = useMemo(() => [...logs].reverse(), [logs]);
  const { page, setPage, pageItems } = usePaginated(ordered, [logs.length]);
  return (
    <section className="card table-card">
      <table>
        <thead><tr><th>时间</th><th>方法</th><th>路径</th><th>模型</th><th>供应商</th><th>状态</th><th>大小</th><th>估算 Token</th><th>耗时</th><th>错误 / Compact</th></tr></thead>
        <tbody>
          {pageItems.map((l) => (
            <tr key={l.id}>
              <td>{new Date(l.timestamp).toLocaleString()}</td><td>{l.method}</td><td><code>{l.path}</code></td><td>{l.model ?? '-'}</td><td>{l.provider_name ?? (l.local_rejected ? (l.error_kind === 'context_too_large' ? '本地压缩提示' : '本地拒绝') : '-')}</td><td>{l.status ?? '-'}</td><td>{formatBytes(l.body_size_bytes)}</td><td>{l.estimated_input_tokens ?? '-'}</td><td>{l.latency_ms}ms</td><td className="truncate">{l.error_kind ? `${l.error_kind}: ` : ''}{l.error ?? l.failover_reason ?? '-'}{l.compact_attempted ? ` · compact:${l.compacted ? 'ok' : 'attempted'}${l.compact_provider_name ? ` @ ${l.compact_provider_name}` : ''}${l.compact_error ? ` · ${l.compact_error}` : ''}` : ''}</td>
            </tr>
          ))}
          {!logs.length && <tr><td colSpan={10}>暂无请求日志。</td></tr>}
        </tbody>
      </table>
      <Pagination page={page} total={logs.length} onPageChange={setPage} />
    </section>
  );
}

function Codex({ models, status, busy, runBusy, notify }: { models: string[]; status: GatewayStatus | null; busy: boolean; runBusy: BusyRunner; notify: Notify }) {
  const [model, setModel] = useState(models[0] ?? '');
  const [configPath, setConfigPath] = useState('');
  const [snippet, setSnippet] = useState<CodexSnippet | null>(null);
  useEffect(() => {
    if (IS_TAURI) api.codexConfigPath().then(setConfigPath).catch(() => setConfigPath('~/.codex/config.toml'));
  }, []);
  useEffect(() => {
    if (!IS_TAURI) {
      if (!model || !['gpt-5.4', 'gpt-5.5'].includes(model)) setModel('gpt-5.5');
      return;
    }
    if (!models.length) {
      setModel('');
      return;
    }
    if (!model || !models.includes(model)) setModel(models[0]);
  }, [models, model]);
  useEffect(() => {
    if (IS_TAURI || !model) return;
    api.codexSnippet(model).then(setSnippet).catch(() => setSnippet(null));
  }, [model]);
  const canConfigure = models.length > 0 && Boolean(model);
  const showCodexResult = (result: CodexConfigResult) => {
    notify({
      title: result.ok ? 'Codex 配置完成' : 'Codex 操作失败',
      message: result.message,
      tone: result.ok ? 'success' : 'error',
      manual: true,
      details: `配置文件：${result.config_path}${result.backup_path ? `\n备份文件：${result.backup_path}` : ''}\n本地代理绕过：NO_PROXY=localhost,127.0.0.1,::1\n如 Codex CLI 或终端已打开，请重启终端/Codex 后再测试。`,
    });
  };
  if (!IS_TAURI) {
    const effectiveSnippet = snippet;
    const downloadScript = () => {
      if (!effectiveSnippet) return;
      const blob = new Blob([effectiveSnippet.configure_script], { type: 'application/x-sh;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = effectiveSnippet.download_name || 'configure-codex.sh';
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    };
    const copySnippet = async () => {
      if (!effectiveSnippet) return;
      await navigator.clipboard.writeText(effectiveSnippet.config_toml);
      notify({ title: '已复制配置片段', message: '请粘贴到 ~/.codex/config.toml 中，或下载脚本后手动执行。', tone: 'success' });
    };
    return (
      <section className="stack">
        <div className="card">
          <h3>Codex 手动配置片段</h3>
          <p>Web 版本不会修改本机文件，只提供配置片段和脚本下载。网关地址：<code>{effectiveSnippet?.base_url ?? status?.bind_url ?? 'http://127.0.0.1:14556/v1'}</code></p>
          <p className="inline-help">请将下面片段合并到 <code>~/.codex/config.toml</code>。如果已有 <code>model</code>、<code>model_provider</code> 或同名 provider，请替换旧值，不要重复插入。</p>
          <label>默认模型
            <select value={model} onChange={(e) => setModel(e.target.value)}>
              {['gpt-5.5', 'gpt-5.4'].map((m) => <option key={m} value={m}>{m}</option>)}
            </select>
          </label>
          <label>本地网关 Bearer Token
            <input readOnly value={effectiveSnippet?.bearer_token ?? ''} />
          </label>
          <pre className="snippet-box">{effectiveSnippet?.config_toml ?? '正在生成配置片段...'}</pre>
          <div className="section-actions">
            <button disabled={!effectiveSnippet} onClick={copySnippet}>复制 config.toml 片段</button>
            <button disabled={!effectiveSnippet} onClick={downloadScript}>下载 configure-codex.sh</button>
          </div>
        </div>
      </section>
    );
  }
  return (
    <section className="stack">
      <div className="card">
        <h3>一键配置 Codex</h3>
        <p>将 Codex 配置到本地网关：<code>{status?.bind_url ?? 'http://127.0.0.1:14555/v1'}</code></p>
        <p>配置文件路径：<code>{configPath || '~/.codex/config.toml'}</code></p>
        <p className="inline-help">写入时会同时配置 <code>NO_PROXY=localhost,127.0.0.1,::1</code>，避免系统代理把本地网关请求拦截成 502。</p>
        {!models.length && <p className="empty-state error">暂无已发现的 GPT-5.4 / GPT-5.5，请先到供应商页点击“一键查询模型”。</p>}
        <label>默认模型
          <select value={model} disabled={!models.length} onChange={(e) => setModel(e.target.value)}>
            {models.map((m) => <option key={m} value={m}>{m}</option>)}
          </select>
        </label>
        <div className="section-actions">
          <button disabled={busy || !canConfigure} onClick={() => runBusy(async () => showCodexResult(await api.configureCodex(model)))}>写入 Codex 配置</button>
          <button disabled={busy} onClick={() => runBusy(async () => showCodexResult(await api.restoreCodex()))}>恢复最近备份</button>
        </div>
      </div>
    </section>
  );
}

function Settings({
  config,
  setConfig,
  providers,
  defaultModel,
  busy,
  runBusy,
  notify,
}: {
  config: AppConfig | null;
  setConfig: (cfg: AppConfig) => void;
  providers: ProviderView[];
  defaultModel?: string;
  busy: boolean;
  runBusy: BusyRunner;
  notify: Notify;
}) {
  const [dir, setDir] = useState('');
  useEffect(() => { api.appDataDir().then(setDir).catch(() => undefined); }, []);
  if (!config) return <div className="card">配置仅在 Tauri App 内可用。</div>;
  const update = (patch: Partial<AppConfig>) => setConfig({ ...config, ...patch });
  const selectedProviderId = config.routing.selected_provider_id ?? '';
  return (
    <section className="stack">
      <div className="card settings-card">
        <h3>网关设置</h3>
        <p>配置目录：<code>{dir}</code></p>
        <div className="form-row">
          <label>Host<input value={config.gateway.host} onChange={(e) => update({ gateway: { ...config.gateway, host: e.target.value } })} /></label>
          <label>Port<input type="number" value={config.gateway.port} onChange={(e) => update({ gateway: { ...config.gateway, port: Number(e.target.value) || 14555 } })} /></label>
          <label>非流式超时秒<input type="number" value={config.gateway.request_timeout_secs} onChange={(e) => update({ gateway: { ...config.gateway, request_timeout_secs: Number(e.target.value) || 300 } })} /></label>
          <label>流式闲置超时秒<input type="number" value={config.gateway.stream_idle_timeout_secs} onChange={(e) => update({ gateway: { ...config.gateway, stream_idle_timeout_secs: Number(e.target.value) || 300 } })} /></label>
          <label>最大请求体 MB<input type="number" value={config.gateway.max_request_body_mb} onChange={(e) => update({ gateway: { ...config.gateway, max_request_body_mb: Number(e.target.value) || 512 } })} /></label>
          <label>Codex 上下文软限 MB<input type="number" value={config.gateway.codex_context_body_limit_mb ?? 32} onChange={(e) => update({ gateway: { ...config.gateway, codex_context_body_limit_mb: Number(e.target.value) || 0 } })} /></label>
          <label>Codex 上下文软限 token<input type="number" value={config.gateway.codex_context_soft_token_limit ?? 264192} onChange={(e) => update({ gateway: { ...config.gateway, codex_context_soft_token_limit: Number(e.target.value) || 264192 } })} /></label>
          <label>Codex 自动压缩 token<input type="number" value={config.gateway.codex_auto_compact_token_limit ?? 240000} onChange={(e) => update({ gateway: { ...config.gateway, codex_auto_compact_token_limit: Number(e.target.value) || 240000 } })} /></label>
          <label>compact 最大次数<input type="number" value={config.gateway.codex_compact_max_attempts ?? 1} onChange={(e) => update({ gateway: { ...config.gateway, codex_compact_max_attempts: Number(e.target.value) || 1 } })} /></label>
          <label>最大重试<input type="number" value={config.routing.max_attempts_per_request} onChange={(e) => update({ routing: { ...config.routing, max_attempts_per_request: Number(e.target.value) || 1 } })} /></label>
          <label>基础冷却秒<input type="number" value={config.routing.cooldown_secs} onChange={(e) => update({ routing: { ...config.routing, cooldown_secs: Number(e.target.value) || 60 } })} /></label>
          <label>认证失败阈值<input type="number" value={config.routing.auth_failure_threshold ?? 2} onChange={(e) => update({ routing: { ...config.routing, auth_failure_threshold: Number(e.target.value) || 2 } })} /></label>
          <label>探活间隔秒<input type="number" value={config.routing.probe_interval_secs ?? 300} onChange={(e) => update({ routing: { ...config.routing, probe_interval_secs: Number(e.target.value) || 300 } })} /></label>
          <label>最大冷却秒<input type="number" value={config.routing.max_cooldown_secs ?? 600} onChange={(e) => update({ routing: { ...config.routing, max_cooldown_secs: Number(e.target.value) || 600 } })} /></label>
        </div>
        <label><input type="checkbox" checked={config.routing.auto_round_robin ?? true} onChange={(e) => update({ routing: { ...config.routing, auto_round_robin: e.target.checked } })} /> 自动轮询供应商（开启后每个新请求按供应商池轮询）</label>
        <label>单选供应商（关闭自动轮询后生效）
          <select
            value={selectedProviderId}
            disabled={config.routing.auto_round_robin ?? true}
            onChange={(e) => update({ routing: { ...config.routing, selected_provider_id: e.target.value || null } })}
          >
            <option value="">不指定，使用第一个可用供应商</option>
            {providers.map((p) => <option key={p.config.id} value={p.config.id}>{p.config.name}{p.config.enabled ? '' : '（禁用）'}</option>)}
          </select>
        </label>
        {(config.routing.auto_round_robin ?? true) && <p className="inline-help">自动轮询开启时会忽略单选供应商。</p>}
        <p className="inline-help">MB 软限属于 body 级保护；token 软限更接近真实上下文限制。两者都会在超限时返回 <code>context_length_exceeded</code>，促使客户端压缩上下文。</p>
        <label><input type="checkbox" checked={config.gateway.codex_compact_retry_enabled ?? true} onChange={(e) => update({ gateway: { ...config.gateway, codex_compact_retry_enabled: e.target.checked } })} /> 启用自动 compact 重试（非流式自动重试；流式仅发送前预压缩）</label>
        <label><input type="checkbox" checked={config.routing.auto_failover ?? true} onChange={(e) => update({ routing: { ...config.routing, auto_failover: e.target.checked } })} /> 自动切换供应商（开启后认证异常、权限限制、余额/限流、5xx、模型错误会尝试下一个）</label>
        <label><input type="checkbox" checked={config.gateway.require_local_token} onChange={(e) => update({ gateway: { ...config.gateway, require_local_token: e.target.checked } })} /> 要求本地 Authorization token</label>
        <label>本地 Token<input value={config.local_auth_token} onChange={(e) => update({ local_auth_token: e.target.value })} /></label>
        <div className="section-actions">
          <button disabled={busy} onClick={() => runBusy(() => api.saveConfig(config).then(setConfig), '设置已保存，重启网关后端口/请求体限制变更生效')}>保存设置</button>
          <button
            disabled={busy}
            onClick={() => runBusy(async () => {
              const result = await api.selfCheck(defaultModel);
              notify({
                title: result.ok ? '网关自检通过' : '网关自检发现问题',
                message: result.message,
                tone: result.ok ? 'success' : 'warning',
                manual: true,
                details: result.checks.map((item) => `${item.ok ? '✓' : '✗'} ${item.name} ${item.status ? `HTTP ${item.status}` : ''} ${item.latency_ms}ms\n${item.details ?? ''}`).join('\n\n'),
              });
            })}
          >一键网关自检</button>
        </div>
      </div>
    </section>
  );
}

function formatError(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}

function formatBytes(value?: number | null) {
  if (!value) return '-';
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}
