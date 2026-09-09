// The worker roster and one worker's detail sheet.
//
// loadFlows lives here because the flow table it fills is on this page; it
// also fills the deployment form's flow picker, which is why boot calls it
// once before any view is open.
import { act, api, canWrite, toast } from '../api.js';
import { publish } from '../bridge.js';
import { line } from '../charts.js';
import { esc, when } from '../fmt.js';
import { registerViews, show } from '../router.js';
import { wkComponentsFor, wkConnOf, wkEnv, wkK8s } from './worker-wizard.js';

async function loadWorkers() {
  const ws = (await api('/workers')) || [];
  document.getElementById('workers').innerHTML = ws.length ? ws.map(w => `
    <tr onclick="openWorkerDetail('${w.id}')" style="cursor:pointer" title="View parameters, config, YAML and packages">
      <td><strong>${esc(w.name)}</strong><div class="muted mono">${esc(w.id.slice(0, 8))}</div></td>
      <td>${w.online ? '<span class="pill s-COMPLETED">online</span>' : '<span class="pill s-FAILED">stale</span>'}</td>
      <td class="mono">${(w.queues || []).map(esc).join(', ')}</td>
      <td>${w.active_runs}</td><td>${w.concurrency}</td><td>${when(w.last_heartbeat)}</td>
    </tr>`).join('') : '<tr><td colspan="6" class="empty">No workers have checked in.</td></tr>';
}

let currentWorkerDetailId = null;
async function openWorkerDetail(id) {
  currentWorkerDetailId = id;
  const dlg = document.getElementById('worker-detail');
  const body = document.getElementById('wdt-body');
  body.innerHTML = '<div class="empty">Loading…</div>';
  dlg.showModal();
  const [ws, flows, git, specs] = await Promise.all([
    api('/workers'), api('/flows').catch(() => []), api('/settings/git').catch(() => ({})),
    api('/worker-specs').catch(() => []),
  ]);
  const w = (ws || []).find(x => x.id === id);
  if (!w) { body.innerHTML = '<div class="empty">Worker not found — it may have deregistered.</div>'; return; }
  document.getElementById('wdt-title').textContent = w.name;
  const tags = [...new Set((flows || []).flatMap(f => f.tags || []))];
  const comps = wkComponentsFor(tags);
  const queues = (w.queues || []).join(',') || 'default';
  const spec = (specs || []).find(s => s.name === w.name);
  const env = wkEnv(w.name, w.concurrency, queues, wkConnOf(spec));
  const km = wkK8s(w.name, 'primex/primeflow:latest', env);
  const path = ((git && git.base_path) ? git.base_path + '/' : '') + w.name;
  body.innerHTML = `
    <dl class="kv">
      <dt>Name</dt><dd>${esc(w.name)}</dd>
      <dt>ID</dt><dd class="mono">${esc(w.id)}</dd>
      <dt>Status</dt><dd>${w.online ? '<span class="pill s-COMPLETED">online</span>' : '<span class="pill s-FAILED">stale</span>'}</dd>
      <dt>Pools</dt><dd class="mono">${esc(queues)}</dd>
      <dt>Concurrency</dt><dd>${w.concurrency}</dd>
      <dt>Active runs</dt><dd>${w.active_runs}</dd>
      <dt>Started</dt><dd>${when(w.started_at)}</dd>
      <dt>Last heartbeat</dt><dd>${when(w.last_heartbeat)}</dd>
    </dl>
    <div class="panel" style="margin:0;border-radius:0;border-left:0;border-right:0">
      <h2>Configuration (env)</h2>
      <pre class="logs" style="margin:0 16px">${esc(env.map(([k, v]) => k + '=' + v).join('\n'))}</pre>
    </div>
    <div class="panel" style="margin:0;border-radius:0;border:0;border-top:1px solid var(--line)">
      <h2>Deployment YAML <span class="muted">— rendered from this worker's live config</span></h2>
      <pre class="logs" id="wdt-yaml" style="margin:0 16px">${esc(km.secret + '\n---\n' + km.dep)}</pre>
      <div class="row" style="padding:10px 16px"><button class="act" onclick="navigator.clipboard.writeText(document.getElementById('wdt-yaml').textContent).then(()=>toast('Copied'))">Copy YAML</button></div>
    </div>
    <div class="panel" style="margin:0;border-radius:0;border:0;border-top:1px solid var(--line)">
      <h2>Package list <span class="muted">— components this worker's host needs</span></h2>
      <ul style="margin:0;padding:12px 32px;color:var(--ink);line-height:1.6">${comps.map(c => `<li>${esc(c)}</li>`).join('')}</ul>
    </div>
    ${gitDeliveryPanel(w, git, spec)}`;
}

// gitDeliveryPanel renders the GitOps status for a worker: live Sync now /
// Auto-sync controls when a pf_worker_specs row backs it, a "create spec" nudge
// when a repo is configured but no spec exists, and nothing at all otherwise.
function gitDeliveryPanel(w, git, spec) {
  if (!spec && !(git && git.repo_url)) return '';
  const head = `<div class="panel" style="margin:0;border-radius:0;border:0;border-top:1px solid var(--line)"><h2>GitOps delivery</h2>`;
  if (!spec) {
    return head + `<p class="muted" style="padding:0 16px 12px">No worker spec backs this worker yet.
      <a href="#" class="rlink" onclick="show('newworker');return false">Add one from the wizard</a> to enable server-side sync.</p></div>`;
  }
  const st = spec.sync_state || 'pending';
  const pill = { synced: 's-COMPLETED', pending: 's-SCHEDULED', drift: 's-RUNNING', error: 's-FAILED' }[st] || 's-SCHEDULED';
  return head + `
    <p class="muted" style="padding:0 16px">Target <span class="mono">${esc(git.repo_url || '')}</span> @ <span class="mono">${esc(git.branch || 'main')}</span></p>
    <dl class="kv" style="padding:6px 16px">
      <dt>State</dt><dd><span class="pill ${pill}">${esc(st)}</span></dd>
      <dt>Last synced</dt><dd>${spec.last_synced_at ? when(spec.last_synced_at) : '—'}</dd>
      <dt>Commit</dt><dd class="mono">${spec.last_synced_sha ? esc(spec.last_synced_sha.slice(0, 10)) : '—'}</dd>
      ${spec.last_error ? `<dt>Error</dt><dd class="mono" style="color:var(--bad)">${esc(spec.last_error)}</dd>` : ''}
    </dl>
    <div class="row" style="padding:10px 16px;gap:12px">
      <button class="act primary" onclick="syncWorkerSpecNow('${spec.id}')">Sync now</button>
      <label class="wk-chk"><input type="checkbox" ${spec.auto_sync ? 'checked' : ''} onchange="toggleWorkerSpecAutoSync('${spec.id}', this.checked)">Auto-sync</label>
    </div></div>`;
}

async function syncWorkerSpecNow(id) {
  if (!canWrite()) return toast('Your account is read-only', true);
  toast('Syncing…');
  try {
    const res = await api('/worker-specs/' + id + '/sync', { method: 'POST' });
    if (res && res.last_error) toast('Sync failed: ' + res.last_error, true);
    else toast('Synced — commit ' + ((res.last_synced_sha || '').slice(0, 8) || 'ok'));
  } catch (e) {
    toast('Sync failed: ' + e.message, true);
  }
  if (currentWorkerDetailId) openWorkerDetail(currentWorkerDetailId);
}

async function toggleWorkerSpecAutoSync(id, on) {
  if (!canWrite()) return toast('Your account is read-only', true);
  try {
    await api('/worker-specs/' + id, { method: 'POST', body: { auto_sync: on } });
    toast('Auto-sync ' + (on ? 'on' : 'off'));
  } catch (e) {
    toast('Update failed: ' + e.message, true);
  }
}

async function loadFlows() {
  const fs = (await api('/flows')) || [];
  document.getElementById('flows').innerHTML = fs.length ? fs.map(f => `
    <tr><td class="mono">${esc(f.name)}</td><td>${esc(f.version)}</td><td class="muted">${esc(f.description || '')}</td></tr>`).join('')
    : '<tr><td colspan="3" class="empty">No worker has registered a flow yet.</td></tr>';
  const sel = document.getElementById('dep-flow');
  const keep = sel.value;
  sel.innerHTML = fs.map(f => `<option>${esc(f.name)}</option>`).join('');
  if (keep) sel.value = keep;
}

registerViews({ workers: { refresh: async () => { await loadWorkers(); await loadFlows(); } } });

export {
  loadFlows,
};

publish({
  loadWorkers, openWorkerDetail, syncWorkerSpecNow,
  toggleWorkerSpecAutoSync,
});
