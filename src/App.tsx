import { useEffect, useMemo, useState } from 'react';
import { open } from '@tauri-apps/plugin-dialog';
import { api } from './api';
import type {
  AppConfig,
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
import { flattenModels, gptModelsFromCache, healthClass, healthLabel, isGptModel, mask, modelsForProvider, newProvider } from './utils';
import './styles.css';

const PAGE_SIZE = 10;

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
  { key: 'models', label: '模型', hint: '发现/检测' },
  { key: 'quota', label: '额度', hint: '余额/健康' },
  { key: 'logs', label: '日志', hint: '请求流水' },
  { key: 'codex', label: 'Codex', hint: '一键配置' },
  { key: 'settings', label: '设置', hint: '网关参数' },
];

export default function App() {
  const [tab, setTab] = useState<Tab>('dashboard');
  const [status, setStatus] = useState<GatewayStatus | null>(null);
  const [providers, setProviders] = useState<ProviderView[]>([]);
  const [modelsCache, setModelsCache] = useState<ModelsCache>({ providers: {} });
  const [logs, setLogs] = useState<RequestLogEntry[]>([]);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const [popup, setPopup] = useState<TotalPopupState | null>(null);
  const [busy, setBusy] = useState(false);

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
    refresh().catch((err) => notify({ title: '刷新失败', message: formatError(err), tone: 'error', manual: true }));
    const timer = window.setInterval(() => refresh().catch(() => undefined), 5000);
    return () => window.clearInterval(timer);
  }, []);

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
            <button disabled={busy} onClick={() => runBusy(() => api.start().then(setStatus), '网关已启动')}>启动</button>
            <button disabled={busy} onClick={() => runBusy(() => api.stop().then(setStatus), '网关已停止')}>停止</button>
            <button disabled={busy} onClick={() => runBusy(refresh, '已刷新')}>刷新</button>
          </div>
        </header>
        {tab === 'dashboard' && <Dashboard status={status} providers={providers} models={gptModels.length} logs={logs} />}
        {tab === 'providers' && <Providers providers={providers} cache={modelsCache} busy={busy} runBusy={runBusy} notify={notify} />}
        {tab === 'models' && <Models cache={modelsCache} busy={busy} runBusy={runBusy} />}
        {tab === 'quota' && <Quota providers={providers} busy={busy} runBusy={runBusy} />}
        {tab === 'logs' && <Logs logs={logs} />}
        {tab === 'codex' && <Codex models={gptModels.map((m) => m.id)} status={status} busy={busy} runBusy={runBusy} notify={notify} />}
        {tab === 'settings' && <Settings config={config} setConfig={setConfig} providers={providers} busy={busy} runBusy={runBusy} />}
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

  return (
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
      <Metric title="GPT 模型" value={String(models)} tone="ok" />
      <div className="card wide">
        <h3>运行说明</h3>
        <p>Codex 配置为本地网关后，请求会进入本机 `/v1`，再按当前路由设置转发到上游供应商。流式请求在输出前可故障转移，输出后不重放。</p>
        {lastError && <p className="danger-text">最近错误：{lastError.error}</p>}
      </div>
      <div className="card wide">
        <h3>供应商状态</h3>
        <div className="provider-pills">
          {providers.map((p) => (
            <span key={p.config.id} className={`pill ${healthClass(p.state.health)}`}>{p.config.name}: {healthLabel(p.state.health)}</span>
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
    const selected = await open({ directory: true, multiple: false, title: '选择供应商导出目录' });
    if (!selected || Array.isArray(selected)) {
      notify({ title: '已取消导出', message: '未选择导出目录。', tone: 'info' });
      return;
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
                  <td><span className={`pill ${healthClass(p.state.health)}`}>{healthLabel(p.state.health)}</span></td>
                  <td>{p.gpt_model_count} GPT / {p.model_count} 全部</td>
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
  const [draft, setDraft] = useState<ProviderConfig>({ ...provider });
  const isNew = !provider.id || provider.name === 'New Provider';
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
          <label>超时秒<input type="number" value={draft.timeout_secs} onChange={(e) => setDraft({ ...draft, timeout_secs: Number(e.target.value) || 120 })} /></label>
        </div>
        <details>
          <summary>额度接口（可选）</summary>
          <label>Quota URL<input value={draft.quota?.url ?? ''} onChange={(e) => setDraft({ ...draft, quota: { ...(draft.quota ?? { method: 'GET', headers: {} }), url: e.target.value } })} /></label>
          <label>余额 JSON 路径<input placeholder="$.data.balance" value={draft.quota?.balance_json_path ?? ''} onChange={(e) => setDraft({ ...draft, quota: { ...(draft.quota ?? { method: 'GET', headers: {}, url: '' }), balance_json_path: e.target.value } })} /></label>
        </details>
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
        <p>从该供应商已发现的模型中选择一个模型执行 `/responses` ping 检测。GPT 模型已优先排序。</p>
        {!models.length && <p className="empty-state error">该供应商暂无已发现模型，请先点击供应商页“一键查询模型”。</p>}
        <label>搜索模型<input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="输入模型名称筛选" /></label>
        <div className="model-list">
          {filtered.map((model) => (
            <button
              key={model.id}
              className={`model-choice ${selected === model.id ? 'selected' : ''}`}
              onClick={() => setSelected(model.id)}
            >
              <code>{model.id}</code>
              {isGptModel(model.id) ? <span className="pill ok">GPT</span> : <span className="pill muted">其他</span>}
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
                  <td>{isGptModel(m.id) ? <span className="pill ok">GPT</span> : <span className="pill muted">其他</span>}</td>
                  <td>{providerNames(foundIn) || '-'}</td>
                </tr>
              );
            })}
            {!models.length && <tr><td colSpan={3}>暂无模型，请到供应商页点击“一键查询模型”。</td></tr>}
          </tbody>
        </table>
        <Pagination page={page} total={models.length} onPageChange={setPage} />
      </div>
    </section>
  );
}

function Quota({ providers, busy, runBusy }: { providers: ProviderView[]; busy: boolean; runBusy: BusyRunner }) {
  const [results, setResults] = useState<Record<string, QuotaResult>>({});
  return (
    <section className="grid">
      {providers.map((p) => {
        const result = results[p.config.id];
        return (
          <div className="card" key={p.config.id}>
            <h3>{p.config.name}</h3>
            <p><span className={`pill ${healthClass(p.state.health)}`}>{healthLabel(p.state.health)}</span></p>
            <p>额度：{result?.balance ?? p.state.remaining_hint ?? result?.health_hint ?? '未查询/未配置适配器'}</p>
            {result?.error && <p className="danger-text">{result.error}</p>}
            <button disabled={busy} onClick={() => runBusy(async () => setResults({ ...results, [p.config.id]: await api.getQuota(p.config.id) }), '额度查询完成')}>查询额度</button>
          </div>
        );
      })}
    </section>
  );
}

function Logs({ logs }: { logs: RequestLogEntry[] }) {
  const ordered = useMemo(() => [...logs].reverse(), [logs]);
  const { page, setPage, pageItems } = usePaginated(ordered, [logs.length]);
  return (
    <section className="card table-card">
      <table>
        <thead><tr><th>时间</th><th>方法</th><th>路径</th><th>模型</th><th>供应商</th><th>状态</th><th>耗时</th><th>错误</th></tr></thead>
        <tbody>
          {pageItems.map((l) => (
            <tr key={l.id}>
              <td>{new Date(l.timestamp).toLocaleString()}</td><td>{l.method}</td><td><code>{l.path}</code></td><td>{l.model ?? '-'}</td><td>{l.provider_name ?? '-'}</td><td>{l.status ?? '-'}</td><td>{l.latency_ms}ms</td><td className="truncate">{l.error ?? '-'}</td>
            </tr>
          ))}
          {!logs.length && <tr><td colSpan={8}>暂无请求日志。</td></tr>}
        </tbody>
      </table>
      <Pagination page={page} total={logs.length} onPageChange={setPage} />
    </section>
  );
}

function Codex({ models, status, busy, runBusy, notify }: { models: string[]; status: GatewayStatus | null; busy: boolean; runBusy: BusyRunner; notify: Notify }) {
  const [model, setModel] = useState(models[0] ?? '');
  const [configPath, setConfigPath] = useState('');
  useEffect(() => { api.codexConfigPath().then(setConfigPath).catch(() => setConfigPath('~/.codex/config.toml')); }, []);
  useEffect(() => {
    if (!models.length) {
      setModel('');
      return;
    }
    if (!model || !models.includes(model)) setModel(models[0]);
  }, [models, model]);
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
  return (
    <section className="stack">
      <div className="card">
        <h3>一键配置 Codex</h3>
        <p>将 Codex 配置到本地网关：<code>{status?.bind_url ?? 'http://127.0.0.1:14555/v1'}</code></p>
        <p>配置文件路径：<code>{configPath || '~/.codex/config.toml'}</code></p>
        <p className="inline-help">写入时会同时配置 <code>NO_PROXY=localhost,127.0.0.1,::1</code>，避免系统代理把本地网关请求拦截成 502。</p>
        {!models.length && <p className="empty-state error">暂无已发现的 GPT 模型，请先到供应商页点击“一键查询模型”。</p>}
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

function Settings({ config, setConfig, providers, busy, runBusy }: { config: AppConfig | null; setConfig: (cfg: AppConfig) => void; providers: ProviderView[]; busy: boolean; runBusy: BusyRunner }) {
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
          <label>最大重试<input type="number" value={config.routing.max_attempts_per_request} onChange={(e) => update({ routing: { ...config.routing, max_attempts_per_request: Number(e.target.value) || 1 } })} /></label>
          <label>冷却秒<input type="number" value={config.routing.cooldown_secs} onChange={(e) => update({ routing: { ...config.routing, cooldown_secs: Number(e.target.value) || 60 } })} /></label>
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
        <label><input type="checkbox" checked={config.routing.auto_failover ?? true} onChange={(e) => update({ routing: { ...config.routing, auto_failover: e.target.checked } })} /> 自动切换供应商（开启后上游 401/402/429/5xx/模型错误会尝试下一个）</label>
        <label><input type="checkbox" checked={config.gateway.require_local_token} onChange={(e) => update({ gateway: { ...config.gateway, require_local_token: e.target.checked } })} /> 要求本地 Authorization token</label>
        <label>本地 Token<input value={config.local_auth_token} onChange={(e) => update({ local_auth_token: e.target.value })} /></label>
        <button disabled={busy} onClick={() => runBusy(() => api.saveConfig(config).then(setConfig), '设置已保存，重启网关后端口变更生效')}>保存设置</button>
      </div>
    </section>
  );
}

function formatError(err: unknown) {
  return err instanceof Error ? err.message : String(err);
}
