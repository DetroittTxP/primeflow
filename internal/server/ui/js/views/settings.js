// Settings: the external API switch and its keys, the Git connection, and
// user administration. Each pane loads on its own; the view registration at
// the foot says which one a background tick reloads.
import { act, api, isAdmin, toast } from '../api.js';
import { publish } from '../bridge.js';
import { esc, when } from '../fmt.js';
import { menuCell } from '../menu.js';
import { registerRoutes, registerViews, show, syncURL } from '../router.js';

let settingsTab = 'external', extTab = 'keys', apiRoles = null;
const SETTINGS_TABS = ['external', 'git', 'users'];
const EXT_TABS = ['keys', 'explorer'];
const keyCache = {};

function loadSettings() {
  if (!isAdmin()) { show('dashboard', true); return; }
  showSettings(settingsTab);
}
function showSettings(s) {
  settingsTab = s;
  syncURL('settings');
  document.querySelectorAll('#settings-subnav button').forEach(b => b.classList.toggle('active', b.dataset.s === s));
  document.getElementById('settings-external').hidden = s !== 'external';
  document.getElementById('settings-git').hidden = s !== 'git';
  document.getElementById('settings-users').hidden = s !== 'users';
  if (s === 'external') { showExtTab(extTab); loadExternalSettings(); loadRoles().then(() => { loadKeys(); renderExplorer(); }); }
  else if (s === 'git') loadGitConnection();
  else loadUsers();
}

async function loadGitConnection() {
  try {
    const g = await api('/settings/git');
    const el = document.getElementById('git-form').elements;
    el.repo_url.value = g.repo_url || '';
    el.branch.value = g.branch || 'main';
    el.base_path.value = g.base_path || '';
    el.provider.value = g.provider || '';
    el.author_name.value = g.author_name || '';
    el.author_email.value = g.author_email || '';
    el.auto_sync.checked = !!g.auto_sync;
    el.token.value = '';
    el.token.placeholder = g.has_token ? 'a token is stored — blank keeps it' : 'personal access token';
    document.getElementById('git-status').textContent = g.updated_at ? 'saved ' + when(g.updated_at) : '';
    window.__git = g;
  } catch (e) { toast(e.message, true); }
}

async function saveGitConnection(ev) {
  ev.preventDefault();
  const el = ev.target.elements;
  const body = {
    repo_url: el.repo_url.value.trim(),
    branch: el.branch.value.trim() || 'main',
    base_path: el.base_path.value.trim(),
    author_name: el.author_name.value.trim(),
    author_email: el.author_email.value.trim(),
    auto_sync: el.auto_sync.checked,
  };
  if (el.token.value.trim() !== '') body.token = el.token.value.trim();
  try {
    await api('/settings/git', { method: 'PUT', body: JSON.stringify(body) });
    toast('Git connection saved');
    loadGitConnection();
  } catch (e) { toast(e.message, true); }
}
function showExtTab(t) {
  extTab = t;
  syncURL('settings');
  document.querySelectorAll('#settings-external .subnav button[data-t]').forEach(b => b.classList.toggle('active', b.dataset.t === t));
  document.getElementById('ext-keys').hidden = t !== 'keys';
  document.getElementById('ext-explorer').hidden = t !== 'explorer';
}

async function loadExternalSettings() {
  try {
    const cfg = await api('/settings/external-api');
    document.getElementById('ext-enabled').setAttribute('aria-checked', String(!!cfg.enabled));
  } catch (e) { toast(e.message, true); }
  try {
    const lr = await api('/settings/log-retention');
    document.getElementById('lr-enabled').checked = !!lr.enabled;
    document.getElementById('lr-hours').value = lr.max_age_hours || '';
  } catch (e) {}
}
async function saveRetention(ev) {
  ev.preventDefault();
  const hours = parseInt(document.getElementById('lr-hours').value, 10);
  try {
    await api('/settings/log-retention', { method: 'PUT', body: JSON.stringify({
      enabled: document.getElementById('lr-enabled').checked,
      max_age_hours: hours > 0 ? hours : undefined,
    }) });
    toast('Log retention saved');
  } catch (e) { toast(e.message, true); }
}
async function toggleExternal() {
  const tog = document.getElementById('ext-enabled');
  const next = tog.getAttribute('aria-checked') !== 'true';
  try {
    await api('/settings/external-api', { method: 'PUT', body: JSON.stringify({ enabled: next }) });
    tog.setAttribute('aria-checked', String(next));
    toast('External API ' + (next ? 'enabled' : 'disabled'));
  } catch (e) { toast(e.message, true); }
}

async function loadRoles() {
  if (apiRoles) return apiRoles;
  apiRoles = await api('/api-roles');
  const sel = document.getElementById('k-role');
  if (sel.options.length <= 1) apiRoles.roles.forEach(r => sel.add(new Option(r.label, r.id)));
  return apiRoles;
}
function renderExplorer() {
  if (!apiRoles) return;
  const roles = apiRoles.roles;
  document.getElementById('explorer-head').innerHTML =
    '<th>Method</th><th>Path</th><th>Scope</th>' + roles.map(r => `<th>${esc(r.label)}</th>`).join('');
  document.getElementById('explorer').innerHTML = apiRoles.routes.map(rt => `
    <tr>
      <td class="mono">${esc(rt.method)}</td>
      <td class="mono">/api/external/v1${esc(rt.path)}</td>
      <td class="mono muted">${esc(rt.scope || '—')}</td>
      ${roles.map(r => `<td>${!rt.scope || (r.scopes || []).includes(rt.scope) ? '✓' : '·'}</td>`).join('')}
    </tr>`).join('');
}

async function loadKeys() {
  const p = new URLSearchParams();
  const q = document.getElementById('k-search').value.trim();
  if (q) p.set('search', q);
  const role = document.getElementById('k-role').value; if (role) p.set('role', role);
  const st = document.getElementById('k-status').value; if (st) p.set('status', st);
  const ks = (await api('/api-keys?' + p)) || [];
  for (const k in keyCache) delete keyCache[k];
  ks.forEach(k => { keyCache[k.id] = k; });
  document.getElementById('keys').innerHTML = ks.length ? ks.map(k => {
    const chips = [
      k.redact_pii ? '<span class="chip on">PII redacted</span>' : '',
      (k.ip_allowlist && k.ip_allowlist.length) ? '<span class="chip on">IP allowlist</span>' : '',
      k.require_mtls ? '<span class="chip on">mTLS</span>' : '',
    ].join('');
    const roleLabel = ((apiRoles && apiRoles.roles.find(r => r.id === k.role)) || {}).label || k.role;
    return `<tr>
      <td><strong>${esc(k.name)}</strong><div class="muted">${esc(k.description || '')}</div></td>
      <td class="mono">pmx_${esc(k.prefix)}…</td>
      <td>${esc(k.owner_email || '—')}</td>
      <td>${esc(roleLabel)}<div class="muted">${k.rate_limit_per_min ? k.rate_limit_per_min + '/min' : 'default'}</div></td>
      <td>${chips || '<span class="muted">—</span>'}</td>
      <td>${when(k.last_used_at)}</td>
      <td>${k.expires_at ? esc(new Date(k.expires_at).toLocaleDateString()) : '—'}</td>
      <td>${k.active ? '<span class="pill s-COMPLETED">active</span>' : '<span class="pill s-PAUSED">inactive</span>'}</td>
      ${menuCell([
        ['Edit', `openKeyForm('${k.id}')`],
        ['History', `keyHistory('${k.id}')`],
        ['Rotate', `rotateKey('${k.id}')`],
        ['Delete', `deleteKey('${k.id}')`, true],
      ])}
    </tr>`;
  }).join('') : '<tr><td colspan="9" class="empty">No API keys yet.</td></tr>';
}

function openKeyForm(id) {
  const k = id ? keyCache[id] : null;
  const roles = (apiRoles && apiRoles.roles) || [];
  document.getElementById('kd-title').textContent = k ? 'Edit API key' : 'New API key';
  document.getElementById('kd-body').innerHTML = `
    <form class="inline" onsubmit="saveKey(event, ${k ? `'${k.id}'` : 'null'})">
      <label>Name<input name="name" required value="${k ? esc(k.name) : ''}"></label>
      <label>Owner email<input name="owner_email" type="email" value="${k ? esc(k.owner_email || '') : ''}"></label>
      <label>Expires<input name="expires_at" type="date" value="${k && k.expires_at ? k.expires_at.slice(0, 10) : ''}"></label>
      <label class="full">Description<textarea name="description">${k ? esc(k.description || '') : ''}</textarea></label>
      <label>Role<select name="role" onchange="showScopes()">
        ${roles.map(r => `<option value="${r.id}" ${k && k.role === r.id ? 'selected' : ''}>${esc(r.label)}</option>`).join('')}
      </select></label>
      <label>Rate limit / min<input name="rate_limit_per_min" type="number" min="0" placeholder="role default"
        value="${k && k.rate_limit_per_min ? k.rate_limit_per_min : ''}"></label>
      <label class="full">IP allowlist — comma-separated IPs or CIDRs, blank = any
        <input name="ip_allowlist" value="${k && k.ip_allowlist ? esc(k.ip_allowlist.join(', ')) : ''}"></label>
      <label class="row"><input type="checkbox" name="active" ${!k || k.active ? 'checked' : ''}> Active</label>
      <label class="row"><input type="checkbox" name="require_mtls" ${k && k.require_mtls ? 'checked' : ''}> Require mutual TLS</label>
      <label class="row"><input type="checkbox" name="redact_pii" ${k && k.redact_pii ? 'checked' : ''}> Redact PII in responses</label>
      <div class="full" id="kd-scopes"></div>
      <div class="full row"><button class="act primary" type="submit">${k ? 'Save changes' : 'Create key'}</button></div>
    </form>`;
  showScopes();
  document.getElementById('key-dialog').showModal();
}
function showScopes() {
  const sel = document.querySelector('#kd-body select[name=role]');
  if (!sel) return;
  const r = ((apiRoles && apiRoles.roles) || []).find(x => x.id === sel.value);
  document.getElementById('kd-scopes').innerHTML = r
    ? `<div class="muted" style="margin-bottom:4px">${esc(r.description)}</div>` +
      r.scopes.map(s => `<span class="chip">${esc(s)}</span>`).join('')
    : '';
}
async function saveKey(ev, id) {
  ev.preventDefault();
  const fd = new FormData(ev.target);
  const body = {
    name: fd.get('name'),
    owner_email: fd.get('owner_email') || '',
    description: fd.get('description') || '',
    role: fd.get('role'),
    expires_at: fd.get('expires_at') || '',
    active: fd.get('active') === 'on',
    require_mtls: fd.get('require_mtls') === 'on',
    redact_pii: fd.get('redact_pii') === 'on',
    ip_allowlist: (fd.get('ip_allowlist') || '').split(',').map(s => s.trim()).filter(Boolean),
    rate_limit_per_min: fd.get('rate_limit_per_min') ? parseInt(fd.get('rate_limit_per_min'), 10) : 0,
  };
  try {
    if (id) {
      await api('/api-keys/' + id, { method: 'PATCH', body: JSON.stringify(body) });
      toast('Key updated');
      document.getElementById('key-dialog').close();
    } else {
      const res = await api('/api-keys', { method: 'POST', body: JSON.stringify(body) });
      showSecret(res.secret, 'Key created — copy it now, it is shown only once.');
    }
    loadKeys();
  } catch (e) { toast(e.message, true); }
}
function showSecret(secret, note) {
  document.getElementById('kd-title').textContent = 'API key secret';
  document.getElementById('kd-body').innerHTML = `
    <p class="muted" style="padding:12px 14px 0;margin:0">${esc(note)}</p>
    <div class="secretbox"><code id="sv">${esc(secret)}</code>
      <button class="act" onclick="navigator.clipboard.writeText(document.getElementById('sv').textContent).then(()=>toast('Copied'))">Copy</button>
    </div>`;
  document.getElementById('key-dialog').showModal();
}
async function rotateKey(id) {
  if (!confirm('Rotate this key? The current secret stops working immediately.')) return;
  try {
    const res = await api('/api-keys/' + id + '/rotate', { method: 'POST' });
    showSecret(res.secret, 'Key rotated — the previous secret no longer works.');
    loadKeys();
  } catch (e) { toast(e.message, true); }
}
async function deleteKey(id) {
  const k = keyCache[id] || {};
  if (!confirm('Delete API key "' + (k.name || id) + '"? This cannot be undone.')) return;
  try { await api('/api-keys/' + id, { method: 'DELETE' }); toast('Key deleted'); loadKeys(); }
  catch (e) { toast(e.message, true); }
}
async function keyHistory(id) {
  const k = keyCache[id] || {};
  const evs = (await api('/api-keys/' + id + '/history?limit=200')) || [];
  document.getElementById('kd-title').textContent = 'History — ' + (k.name || id);
  document.getElementById('kd-body').innerHTML = `<div class="scroll"><table>
    <thead><tr><th>When</th><th>Actor</th><th>Action</th><th>Detail</th></tr></thead>
    <tbody>${evs.length ? evs.map(e => `<tr>
      <td>${when(e.at)}</td><td>${esc(e.actor)}</td><td class="mono">${esc(e.action)}</td>
      <td class="mono muted">${esc(JSON.stringify(e.detail ?? {}).slice(0, 160))}</td></tr>`).join('')
      : '<tr><td colspan="4" class="empty">No history.</td></tr>'}</tbody></table></div>`;
  document.getElementById('key-dialog').showModal();
}

// --- settings: users ---------------------------------------------------------
async function loadUsers() {
  const us = (await api('/users')) || [];
  document.getElementById('users').innerHTML = us.map(u => {
    const sso = u.auth_provider === 'oidc';
    return `<tr>
      <td><strong>${esc(u.email)}</strong> ${sso ? '<span class="chip on">SSO</span>' : ''}</td>
      <td><select onchange="setUserRole('${u.id}', this.value)">
        ${['viewer', 'operator', 'admin'].map(r => `<option ${u.role === r ? 'selected' : ''}>${r}</option>`).join('')}
      </select></td>
      <td>${u.active ? '<span class="pill s-COMPLETED">active</span>' : '<span class="pill s-PAUSED">disabled</span>'}</td>
      <td>${when(u.last_login_at)}</td>
      ${menuCell([
        !sso && ['Reset password', `resetUserPw('${u.id}')`],
        !sso && ['Reset link', `resetLink('${u.id}')`],
        [u.active ? 'Deactivate' : 'Activate', `setUserActive('${u.id}', ${!u.active})`],
      ])}
    </tr>`;
  }).join('');
}
async function resetLink(id) {
  try {
    const res = await api('/users/' + id + '/reset-link', { method: 'POST' });
    showSecret(res.url && (location.origin + res.url) || '',
      'Password-reset link — hand it to the user. It expires ' + new Date(res.expires_at).toLocaleString() + ' and works once.');
  } catch (e) { toast(e.message, true); }
}
async function createUser(ev) {
  ev.preventDefault();
  const fd = new FormData(ev.target);
  try {
    await api('/users', { method: 'POST', body: JSON.stringify({
      email: fd.get('email'), password: fd.get('password'), role: fd.get('role'),
    }) });
    toast('User created'); ev.target.reset(); loadUsers();
  } catch (e) { toast(e.message, true); }
}
const setUserRole = (id, role) => api('/users/' + id, { method: 'PATCH', body: JSON.stringify({ role }) })
  .then(() => { toast('Role updated'); loadUsers(); }).catch(e => { toast(e.message, true); loadUsers(); });
const setUserActive = (id, active) => api('/users/' + id, { method: 'PATCH', body: JSON.stringify({ active }) })
  .then(() => { toast(active ? 'Activated' : 'Deactivated'); loadUsers(); }).catch(e => toast(e.message, true));
function resetUserPw(id) {
  const pw = prompt('New password (at least 8 characters):');
  if (!pw) return;
  api('/users/' + id, { method: 'PATCH', body: JSON.stringify({ password: pw }) })
    .then(() => toast('Password reset')).catch(e => toast(e.message, true));
}

// Settings spells out its open pane rather than just the view around it: the
// tab is the second segment, and External API's own switch is the third.
// Arriving loads the whole pane; a background tick reloads only the table the
// open pane is showing.
registerViews({
  settings: {
    refresh: () => settingsTab === 'external' ? loadKeys() : settingsTab === 'git' ? loadGitConnection() : loadUsers(),
    enter: loadSettings,
    path: () => '/settings/' + settingsTab + (settingsTab === 'external' ? '/' + extTab : ''),
  },
});

// A pane this build does not have leaves the open one alone rather than
// snapping to a default: an old link should not silently move the operator.
registerRoutes([['settings', (tab, ext) => {
  if (SETTINGS_TABS.includes(tab)) {
    settingsTab = tab;
    if (tab === 'external' && EXT_TABS.includes(ext)) extTab = ext;
  }
  return 'settings';
}]]);

publish({
  showSettings, showExtTab, toggleExternal, saveRetention,
  saveGitConnection, loadKeys, openKeyForm, showScopes, saveKey, rotateKey,
  deleteKey, keyHistory, loadUsers, createUser, setUserRole, setUserActive,
  resetUserPw, resetLink,
});
