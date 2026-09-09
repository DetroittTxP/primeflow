// The Add-worker wizard: a form that writes no server state until Save, and
// generates the env file, unit file or manifest that gets a worker running.
import { ME, act, api, canWrite, isAdmin, toast } from '../api.js';
import { publish } from '../bridge.js';
import { esc, state } from '../fmt.js';
import { registerViews, show } from '../router.js';

// PrimeFlow never launches workers itself. This wizard captures the config,
// makes the operator confirm the host-side package/component requirements,
// then emits a copy-paste bootstrap package for the new worker host.
const WK_COMPONENTS = {
  base: [
    'primex-worker binary on the host (or Go 1.25+ to build ./examples/primex-worker)',
    'ca-certificates — TLS trust store for the flow\'s outbound calls',
  ],
  // What the host must reach depends on how the worker talks to the
  // orchestrator. Beside the database it needs Postgres and the bus; at a
  // remote site it needs outbound HTTPS to the server and nothing else.
  conn: {
    database: ['Outbound network to PostgreSQL :5432 and the notification bus (NATS :4222 / Redis :6379)'],
    api: [
      'Outbound HTTPS to the PrimeFlow API URL below — no database or bus access is needed on the host',
      'A pool-scoped api-worker key for exactly the pools ticked above (Issue key… below, or Settings → External API)',
    ],
  },
  vcd: [
    'VMware Cloud Director API reachable + a service account with vApp/VM rights',
    'The vCD client library / govc the flow imports, installed on the host',
  ],
  metering: [
    'Billing API endpoint reachable + an API token for usage submission',
  ],
  provisioning: [
    'Guest OS templates published in the target vDC catalog',
  ],
  fleet: [
    'Deployment "provision-vm-standard" exists (the sub-flow target this flow calls)',
  ],
};

function wkComponentsFor(tags, mode) {
  const out = [...WK_COMPONENTS.base, ...(WK_COMPONENTS.conn[mode] || WK_COMPONENTS.conn.database)];
  for (const t of (tags || [])) if (Array.isArray(WK_COMPONENTS[t])) out.push(...WK_COMPONENTS[t]);
  return out;
}
function wkReqComponents() { return wkComponentsFor(window.__wkTags, wkConn().mode); }

async function renderNewWorker() {
  const body = document.getElementById('nw-body');
  if (!canWrite()) { body.innerHTML = '<div class="panel"><p class="muted" style="padding:16px">Your account is read-only.</p></div>'; return; }
  body.innerHTML = '<div class="panel"><p class="muted" style="padding:16px">Loading…</p></div>';
  const [qs, flows, git] = await Promise.all([
    api('/queues').catch(() => []), api('/flows').catch(() => []), api('/settings/git').catch(() => ({})),
  ]);
  window.__nwPools = (qs || []).filter(q => (q.pool_type || 'pull') === 'pull').map(q => q.name);
  window.__wkTags = [...new Set((flows || []).flatMap(f => f.tags || []))];
  window.__git = git || {};
  const n = document.querySelectorAll('#workers tr').length + 1;
  const nm = 'primex-worker-' + n;
  const gRepo = esc(git.repo_url || '');
  const gBranch = esc(git.branch || 'main');
  const gBase = (git.base_path || 'workers').replace(/\/+$/, '');
  const gAuto = git.auto_sync ? ' checked' : '';
  body.innerHTML = `
    <div class="panel">
      <h2>Worker</h2>
      <form class="inline" id="wk-form" onsubmit="return false" oninput="wkGen()">
        <label>Worker name<input name="name" value="${nm}" required></label>
        <label>Concurrency<input name="concurrency" type="number" min="1" value="4"></label>
        <label>Image<input name="image" value="primex/primeflow:latest"></label>
        <label>Connection
          <select name="conn" onchange="nwConnToggle()">
            <option value="database">Database (beside Postgres)</option>
            <option value="api">API (remote site)</option>
          </select>
        </label>
        <label class="full">Work pools this worker serves
          <span class="wk-pools" id="wk-pools" onchange="wkGen()">${nwPoolChecks()}</span>
          <button type="button" class="act" style="margin-top:8px;align-self:flex-start" onclick="openCreatePool()">+ Create pool…</button>
        </label>
        <div class="full nw-api" style="display:none;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px">
          <label>PrimeFlow API URL<input name="api_url" value="${esc(location.origin)}" placeholder="https://primeflow.example.com"></label>
          <label>Worker token<input name="token" placeholder="pmx_…  (api-worker key scoped to the pools above)" autocomplete="off" spellcheck="false"></label>
          <p class="muted" style="margin:0;grid-column:1/-1">
            The worker holds this key and no database credential. It may lease only from the pools the key names;
            wake-ups ride <code>/api/v1/worker/stream</code> on the same connection and the poll backstop defaults to 15s.
            ${isAdmin()
              ? '<button type="button" class="act" style="margin-left:8px" onclick="wkIssueKey()">Issue key…</button> mints an api-worker key scoped to the ticked pools and fills it in.'
              : 'Ask an admin for an api-worker key scoped to these pools (Settings → External API).'}
          </p>
        </div>
      </form>
    </div>
    <div class="panel">
      <h2>Delivery <span class="muted">— how the worker reaches its host / cluster</span></h2>
      <form class="inline" id="wk-delivery" onsubmit="return false" oninput="wkGen()">
        <label>Method
          <select name="method" onchange="nwDeliveryToggle(); wkGen()">
            <option value="git">Git commit + PR/MR (GitHub / GitLab)</option>
            <option value="argocd">Argo CD Application (GitOps)</option>
            <option value="flux">Flux Kustomization (GitOps)</option>
            <option value="script">Script — Docker / systemd / kubectl</option>
          </select>
        </label>
        <label id="nw-runtime" style="display:none">Runtime target
          <select name="target" onchange="wkGen()">
            <option value="docker">Docker</option>
            <option value="systemd">systemd (bare host)</option>
            <option value="k8s">Kubernetes</option>
          </select>
        </label>
        <label class="wk-chk full"><input type="checkbox" name="autosync"${gAuto} onchange="wkGen()">Auto-sync — the GitOps controller continuously reconciles this worker (off = one-shot / manual sync)</label>
        <div class="full nw-git" style="grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px">
          <label>Git repo URL<input name="repo" value="${gRepo}" placeholder="https://github.com/acme/gitops.git"></label>
          <label>Path in repo<input name="path" value="${esc(gBase)}/${nm}"></label>
          <label>Base branch<input name="branch" value="${gBranch}"></label>
          <label>Feature branch<input name="head" value="add-worker-${nm}"></label>
        </div>
        <div class="full nw-argo" style="display:none;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px">
          <label>Argo CD project<input name="project" value="default"></label>
          <label>Dest namespace<input name="namespace" value="primeflow"></label>
          <label>Dest cluster<input name="server" value="https://kubernetes.default.svc"></label>
          <label>Target revision<input name="revision" value="HEAD"></label>
        </div>
      </form>
    </div>
    <div class="panel">
      <h2>Host requirements <span class="muted">— confirm each is satisfied on the target</span></h2>
      <div id="wk-reqs"></div>
    </div>
    <div class="panel">
      <h2>Bootstrap package</h2>
      <p class="muted" style="padding:0 16px 8px">
        PrimeFlow does not deploy workers itself. It emits the artifacts below; you apply them with the
        chosen method. Credentials are placeholders — fill from your server config or a sealed secret.
      </p>
      <pre class="logs" id="wk-out" style="margin:0 16px"></pre>
      <div class="row" style="padding:12px 16px;gap:8px">
        <button class="act primary" id="wk-gen" disabled onclick="wkGen()">Generate</button>
        <button class="act" onclick="navigator.clipboard.writeText(document.getElementById('wk-out').textContent).then(()=>toast('Copied'))">Copy</button>
        <span class="grow"></span>
        <button class="act" id="wk-save" disabled onclick="saveWorkerSpec(false)" title="Persist this as a worker spec (git/argocd/flux delivery only)">Save spec</button>
        <button class="act primary" id="wk-savesync" disabled onclick="saveWorkerSpec(true)" title="Save the spec and commit it to the GitOps repo now">Save &amp; sync</button>
      </div>
      <p class="muted" id="wk-save-note" style="padding:0 16px 12px"></p>
    </div>`;
  nwDeliveryToggle();
  wkReqs();
}

function nwPoolChecks() {
  const pools = window.__nwPools || [];
  if (!pools.length) return '<span class="muted">No pull pools yet — create one below, or the worker makes the pools it subscribes to on first check-in.</span>';
  return pools.map(p => `<label class="wk-chk"><input type="checkbox" name="pool" value="${esc(p)}"${p === 'default' ? ' checked' : ''}>${esc(p)}</label>`).join('');
}

async function nwReloadPools(pick) {
  const qs = await api('/queues').catch(() => []);
  window.__nwPools = (qs || []).filter(q => (q.pool_type || 'pull') === 'pull').map(q => q.name);
  const box = document.getElementById('wk-pools');
  if (!box) return;
  const chosen = new Set([...box.querySelectorAll('input:checked')].map(i => i.value));
  if (pick) chosen.add(pick);
  box.innerHTML = nwPoolChecks();
  box.querySelectorAll('input[name=pool]').forEach(i => { if (chosen.has(i.value)) i.checked = true; });
  wkGen();
}

// wkConn reads how the new worker reaches the orchestrator. Database mode is
// the classic shape beside Postgres; API mode is a worker at a remote site that
// holds a pool-scoped key and nothing else (README → Workers at a remote site).
function wkConn() {
  const f = document.getElementById('wk-form');
  if (!f || !f.elements.conn) return { mode: 'database', apiURL: '', token: '' };
  return {
    mode: f.elements.conn.value === 'api' ? 'api' : 'database',
    apiURL: (f.elements.api_url.value || location.origin).trim().replace(/\/+$/, ''),
    token: (f.elements.token.value || '').trim(),
  };
}

function nwConnToggle() {
  const f = document.getElementById('wk-form');
  if (!f) return;
  document.querySelector('.nw-api').style.display = f.elements.conn.value === 'api' ? 'grid' : 'none';
  wkReqs(); // the network requirement differs per mode, so it is re-confirmed
}

// wkIssueKey mints the credential an API-mode worker needs: an api-worker key
// scoped to exactly the pools ticked above. Pools are mandatory here — an
// api-worker key with no pools may lease from every lane, which is never what
// a site should hold.
async function wkIssueKey() {
  if (!isAdmin()) return toast('Only an admin can issue API keys', true);
  const f = document.getElementById('wk-form');
  const name = (f.elements.name.value || 'primex-worker').trim();
  const pools = [...f.querySelectorAll('input[name=pool]:checked')].map(b => b.value);
  if (!pools.length) return toast('Tick at least one pool first — the key is scoped to them', true);
  try {
    const res = await api('/api-keys', { method: 'POST', body: JSON.stringify({
      name: name + ' worker', role: 'api-worker', pools,
      description: 'Issued from Workers → Add worker for ' + name,
    }) });
    f.elements.token.value = res.secret;
    toast('Key issued for ' + pools.join(', ') + ' — it is shown only here, copy the package now');
    wkGen();
  } catch (e) { toast(e.message, true); }
}

function nwDeliveryToggle() {
  const d = document.getElementById('wk-delivery');
  if (!d) return;
  const m = d.elements.method.value;
  document.getElementById('nw-runtime').style.display = m === 'script' ? '' : 'none';
  document.querySelector('.nw-git').style.display = m === 'script' ? 'none' : 'grid';
  document.querySelector('.nw-argo').style.display = m === 'argocd' ? 'grid' : 'none';
  const boxes = [...document.querySelectorAll('.wk-req')];
  wkSaveGate(boxes.length > 0 && boxes.every(b => b.checked));
}

function wkReqs() {
  const el = document.getElementById('wk-reqs');
  if (!el) return;
  el.innerHTML = `<div class="wk-reqlist">${wkReqComponents().map(c =>
    `<label class="wk-chk"><input type="checkbox" class="wk-req" onchange="wkReqGate()">${esc(c)}</label>`).join('')}
    <label class="wk-chk"><input type="checkbox" class="wk-req" onchange="wkReqGate()">All flow-specific SDKs / CLIs the registered flows import are installed and on PATH</label>
  </div>`;
  wkReqGate();
}

function wkReqGate() {
  const boxes = [...document.querySelectorAll('.wk-req')];
  const ok = boxes.length > 0 && boxes.every(b => b.checked);
  const gen = document.getElementById('wk-gen');
  if (gen) gen.disabled = !ok;
  wkSaveGate(ok);
  const out = document.getElementById('wk-out');
  if (!out) return;
  if (ok) wkGen();
  else out.textContent = '# Confirm every host requirement above to generate the bootstrap package.';
}

// Save spec / Save & sync are available once the host requirements are ticked
// and the delivery method is one the server can render (git / argocd / flux).
function wkSaveGate(reqsOK) {
  const d = document.getElementById('wk-delivery');
  const save = document.getElementById('wk-save');
  const savesync = document.getElementById('wk-savesync');
  const note = document.getElementById('wk-save-note');
  if (!d || !save || !savesync) return;
  const method = d.elements.method.value;
  const renderable = method !== 'script';
  save.disabled = !(reqsOK && renderable);
  savesync.disabled = !(reqsOK && renderable && (window.__git && window.__git.repo_url));
  if (note) {
    if (!renderable) note.textContent = 'Script delivery is copy-paste only — pick Git / Argo CD / Flux to save a server-managed spec.';
    else if (!(window.__git && window.__git.repo_url)) note.textContent = 'Set a repo in Settings → Git connection to enable Save & sync.';
    else note.textContent = 'Save spec stores it; Save & sync also commits the manifests to ' + esc(window.__git.repo_url) + ' @ ' + esc(window.__git.branch || 'main') + '.';
  }
}

// saveWorkerSpec turns the wizard forms into a pf_worker_specs row and,
// optionally, triggers an immediate server-side git sync.
async function saveWorkerSpec(alsoSync) {
  if (!canWrite()) return toast('Your account is read-only', true);
  const f = document.getElementById('wk-form');
  const d = document.getElementById('wk-delivery');
  if (!f || !d) return;
  const method = d.elements.method.value;
  if (method === 'script') return toast('Script delivery has no server spec', true);
  const pools = [...f.querySelectorAll('input[name=pool]:checked')].map(b => b.value);
  if (!pools.length) return toast('Pick at least one work pool', true);

  const body = {
    name: (f.elements.name.value || '').trim(),
    image: (f.elements.image.value || 'primex/primeflow:latest').trim(),
    queues: pools,
    concurrency: parseInt(f.elements.concurrency.value, 10) || 4,
    replicas: 1,
    delivery: method,
    auto_sync: d.elements.autosync.checked,
    repo_path: (d.elements.path.value || '').trim(),
  };
  if (method === 'argocd') {
    body.namespace = (d.elements.namespace.value || 'primeflow').trim();
    body.argocd = {
      project: (d.elements.project.value || 'default').trim(),
      dest_server: (d.elements.server.value || 'https://kubernetes.default.svc').trim(),
      revision: (d.elements.revision.value || 'HEAD').trim(),
    };
  }

  let spec;
  try {
    // Upsert by name: if a spec already exists, update it in place.
    const existing = (await api('/worker-specs').catch(() => [])) || [];
    const match = existing.find(s => s.name === body.name);
    spec = await api('/worker-specs' + (match ? '/' + match.id : ''), { method: 'POST', body });
  } catch (e) {
    return toast('Save failed: ' + e.message, true);
  }
  toast('Worker spec saved');
  if (!alsoSync) { show('workers'); return; }

  try {
    const res = await api('/worker-specs/' + spec.id + '/sync', { method: 'POST' });
    if (res && res.last_error) toast('Sync failed: ' + res.last_error, true);
    else toast('Synced — commit ' + ((res.last_synced_sha || '').slice(0, 8) || 'ok'));
  } catch (e) {
    toast('Sync failed: ' + e.message, true);
  }
  show('workers');
}

// wkConnEnv is the pair of variables a worker needs to reach the orchestrator,
// and the single place that decides them. Database mode is the classic shape
// beside Postgres (DSN + notification bus); API mode is a remote site holding a
// pool-scoped key and no database credential (README -> Workers at a remote
// site). primex-worker gates on exactly these names: PRIMEFLOW_DATABASE_URL, or
// PRIMEFLOW_API_URL together with PRIMEFLOW_WORKER_TOKEN. Values the console
// cannot know stay placeholders, matching what the server commits to git
// (internal/gitsync/render.go) -- a real credential is never invented here.
function wkConnEnv(conn) {
  const c = conn || {};
  if (c.mode === 'api') {
    return [
      ['PRIMEFLOW_API_URL', c.apiURL || location.origin],
      ['PRIMEFLOW_WORKER_TOKEN', c.token || 'REPLACE_ME'],
    ];
  }
  return [
    ['PRIMEFLOW_DATABASE_URL', c.dbURL || 'postgres://primeflow:REPLACE_ME@POSTGRES_HOST:5432/primeflow?sslmode=disable'],
    ['PRIMEFLOW_NATS_URL', c.busURL || 'nats://NATS_HOST:4222'],
  ];
}

// wkConnOf infers how an already-registered worker reaches the orchestrator.
// A heartbeat (core.WorkerInfo) carries no connection mode, so the env of the
// worker spec backing it is the only evidence there is; with no spec, assume
// the database shape and let wkConnEnv render placeholders.
function wkConnOf(spec) {
  const e = (spec && spec.env) || {};
  if (e.PRIMEFLOW_API_URL || e.PRIMEFLOW_WORKER_TOKEN) {
    return { mode: 'api', apiURL: e.PRIMEFLOW_API_URL || '', token: e.PRIMEFLOW_WORKER_TOKEN || '' };
  }
  return { mode: 'database', dbURL: e.PRIMEFLOW_DATABASE_URL || '', busURL: e.PRIMEFLOW_NATS_URL || '' };
}

function wkEnv(name, conc, queues, conn) {
  return [
    ...wkConnEnv(conn),
    ['PRIMEFLOW_QUEUES', queues],
    ['PRIMEFLOW_CONCURRENCY', String(conc)],
    ['PRIMEFLOW_WORKER_NAME', name],
  ];
}

function wkK8s(name, image, env) {
  const secret = 'apiVersion: v1\nkind: Secret\nmetadata:\n  name: ' + name + '-env\ntype: Opaque\nstringData:\n' +
    env.map(([k, v]) => '  ' + k + ': "' + v + '"').join('\n');
  const dep = 'apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: ' + name + '\n' +
    '  labels: { app.kubernetes.io/name: ' + name + ', app.kubernetes.io/part-of: primeflow }\n' +
    'spec:\n  replicas: 1\n  selector: { matchLabels: { app: ' + name + ' } }\n' +
    '  template:\n    metadata: { labels: { app: ' + name + ' } }\n    spec:\n      containers:\n' +
    '        - name: worker\n          image: ' + image + '\n' +
    '          command: ["/usr/local/bin/primex-worker"]\n' +
    '          envFrom:\n            - secretRef: { name: ' + name + '-env }';
  const kust = 'apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - secret.yaml\n  - deployment.yaml';
  return { secret, dep, kust };
}

function wkGen() {
  const gen = document.getElementById('wk-gen');
  if (!gen || gen.disabled) return;
  const f = document.getElementById('wk-form');
  const d = document.getElementById('wk-delivery');
  if (!f || !d) return;
  const name = (f.elements.name.value || 'primex-worker').trim();
  const conc = parseInt(f.elements.concurrency.value, 10) || 4;
  const image = (f.elements.image.value || 'primex/primeflow:latest').trim();
  const pools = [...f.querySelectorAll('input[name=pool]:checked')].map(b => b.value);
  const queues = pools.length ? pools.join(',') : 'default';
  const method = d.elements.method.value;
  const autosync = d.elements.autosync.checked;
  const conn = wkConn();
  const env = wkEnv(name, conc, queues, conn);
  const envLines = env.map(([k, v]) => k + '=' + v).join('\n');
  const { secret, dep, kust } = wkK8s(name, image, env);

  let out = '# worker "' + name + '"  pools=' + queues + '  concurrency=' + conc + '  connection=' + conn.mode + '\n' +
    '# host requirements confirmed by ' + esc(ME.email) + ':\n' +
    wkReqComponents().map(c => '#   [x] ' + c).join('\n') + '\n\n';
  if (conn.mode === 'api') {
    out += '# Reaches the orchestrator through ' + conn.apiURL + '/api/v1/worker/* — outbound HTTPS only,\n' +
      '# no database credential on the host. Wake-ups arrive on /api/v1/worker/stream; the poll\n' +
      '# backstop defaults to 15s (PRIMEFLOW_POLL). Every task checkpoint is one round trip.\n' +
      (conn.token ? '' :
        '# PRIMEFLOW_WORKER_TOKEN is a placeholder: issue an api-worker key scoped to pools=' + queues + '\n' +
        '# (Issue key… above, or Settings → External API) and paste it in.\n') + '\n';
  }

  if (method === 'script') {
    const target = d.elements.target.value;
    out += '# ---- worker.env ----\n' + envLines + '\n\n';
    if (target === 'docker') {
      out += '# ---- Docker ----\n' +
        'docker run -d --name ' + name + ' --env-file worker.env --restart unless-stopped \\\n' +
        '  ' + image + ' /usr/local/bin/primex-worker\n';
    } else if (target === 'systemd') {
      out += '# ---- /etc/systemd/system/' + name + '.service ----\n' +
        '[Unit]\nDescription=PrimeFlow worker ' + name + '\nAfter=network-online.target\n\n' +
        '[Service]\nEnvironmentFile=/etc/' + name + '.env\nExecStart=/usr/local/bin/primex-worker\n' +
        'Restart=always\nRestartSec=3\nUser=primeflow\n\n' +
        '[Install]\nWantedBy=multi-user.target\n\n' +
        '# then:\nsudo install -m600 worker.env /etc/' + name + '.env\n' +
        'sudo systemctl daemon-reload && sudo systemctl enable --now ' + name + '\n';
    } else {
      out += '# ---- secret.yaml ----\n' + secret + '\n\n# ---- deployment.yaml ----\n' + dep + '\n\n' +
        'kubectl apply -f secret.yaml -f deployment.yaml\n';
    }
  } else {
    const repo = (d.elements.repo.value || 'https://github.com/ORG/gitops.git').trim();
    const path = (d.elements.path.value || ('workers/' + name)).trim().replace(/^\/+|\/+$/g, '');
    const base = (d.elements.branch.value || 'main').trim();
    const head = (d.elements.head.value || ('add-worker-' + name)).trim();
    const host = /gitlab/i.test(repo) ? 'gitlab' : (/github/i.test(repo) ? 'github' : 'git');
    out += '# ==== files, committed under ' + path + '/ ====\n\n';
    out += '# ' + path + '/secret.yaml   (replace with a SealedSecret / SOPS for real GitOps)\n' + secret + '\n\n';
    out += '# ' + path + '/deployment.yaml\n' + dep + '\n\n';
    out += '# ' + path + '/kustomization.yaml\n' + kust + '\n\n';

    if (method === 'argocd') {
      const project = (d.elements.project.value || 'default').trim();
      const ns = (d.elements.namespace.value || 'primeflow').trim();
      const server = (d.elements.server.value || 'https://kubernetes.default.svc').trim();
      const rev = (d.elements.revision.value || 'HEAD').trim();
      const syncPolicy = autosync
        ? '  syncPolicy:\n    automated: { prune: true, selfHeal: true }\n    syncOptions: [CreateNamespace=true]\n'
        : '  syncPolicy:\n    syncOptions: [CreateNamespace=true]   # auto-sync OFF: sync manually\n';
      out += '# ---- Argo CD Application (commit to your app-of-apps, or: kubectl apply -n argocd -f -) ----\n' +
        'apiVersion: argoproj.io/v1alpha1\nkind: Application\nmetadata:\n  name: ' + name + '\n  namespace: argocd\n' +
        'spec:\n  project: ' + project + '\n  source:\n    repoURL: ' + repo + '\n    path: ' + path + '\n' +
        '    targetRevision: ' + rev + '\n  destination:\n    server: ' + server + '\n    namespace: ' + ns + '\n' +
        syncPolicy + '\n' +
        '# or imperatively:\n' +
        'argocd app create ' + name + ' --repo ' + repo + ' --path ' + path + ' --revision ' + rev +
        ' --dest-server ' + server + ' --dest-namespace ' + ns + ' --project ' + project +
        (autosync ? ' --sync-policy automated' : '') + ' --sync-option CreateNamespace=true\n\n';
    } else if (method === 'flux') {
      out += '# ---- Flux Kustomization (in your fleet repo) ----\n' +
        'apiVersion: kustomize.toolkit.fluxcd.io/v1\nkind: Kustomization\nmetadata:\n  name: ' + name + '\n' +
        '  namespace: flux-system\nspec:\n  interval: 5m\n  path: ./' + path + '\n  prune: true\n' +
        (autosync ? '' : '  suspend: true   # auto-sync OFF: run `flux resume` / `flux reconcile` to apply\n') +
        '  sourceRef: { kind: GitRepository, name: gitops }\n  targetNamespace: primeflow\n\n';
    }

    out += '# ---- commit & open a change ----\n' +
      'git clone ' + repo + ' gitops && cd gitops\n' +
      'git checkout -b ' + head + '\n' +
      'mkdir -p ' + path + '   # write the files above into it\n' +
      'git add ' + path + '\n' +
      'git commit -m "add PrimeFlow worker ' + name + ' (pools: ' + queues + ')"\n' +
      'git push -u origin ' + head + '\n';
    if (host === 'github') out += 'gh pr create --base ' + base + ' --head ' + head + ' --fill\n';
    else if (host === 'gitlab') out += 'glab mr create --source-branch ' + head + ' --target-branch ' + base + ' --fill\n';
    else out += '# open a merge / pull request: ' + head + ' -> ' + base + '\n';

    out += '\n# ---- sync ' + (autosync ? '(auto-sync is ON — the controller reconciles on merge) ----\n' : '(auto-sync is OFF — trigger it) ----\n');
    if (method === 'argocd') out += 'argocd app sync ' + name + (autosync ? '   # optional: force an immediate reconcile\n' : '\n');
    else if (method === 'flux') out += 'flux reconcile kustomization ' + name + ' --with-source\n';
    else out += 'argocd app sync ' + name + '   # or: flux reconcile kustomization ' + name + '   # or your CD tool\'s sync trigger\n';
  }

  out += '\n# The worker registers on start and appears on the Workers page as "' + name + '".';
  document.getElementById('wk-out').textContent = out;
}

// The wizard is unsaved form state, not a view of the server: a background tick
// has nothing to catch it up on, and rebuilding the pool checkboxes under the
// pointer would re-tick "default" after the user had cleared it. The one thing
// that can change while it is open -- a pool created from its own dialog --
// calls nwReloadPools() right then. So arriving builds it, and the tick that
// follows does nothing.
registerViews({ newworker: { refresh: () => {}, enter: renderNewWorker } });

export {
  nwReloadPools, wkComponentsFor, wkConnOf, wkEnv, wkK8s,
};

publish({
  nwConnToggle, nwDeliveryToggle, wkIssueKey, wkReqGate, wkGen,
  saveWorkerSpec,
});
