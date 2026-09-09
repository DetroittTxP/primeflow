// Work pools: the table of pools, the dialog that creates one, and the
// pending queue underneath -- the order the next worker takes work in.
import { act, api, canWrite, toast } from '../api.js';
import { esc, prio, when } from '../fmt.js';
import { menuCell } from '../menu.js';
import { currentView, registerViews, show } from '../router.js';
import { registerActions } from '../actions.js';
import { desiredWorkers } from './dashboard.js';
import { runLink } from './run.js';
import { nwReloadPools } from './worker-wizard.js';

// The pool rows a menu item refers to. An action is named by a string and
// carries strings, so "Autoscale…" passes the pool's name and looks the rest of
// it up here rather than smuggling a JSON blob through an attribute.
let poolsByName = {};

async function loadQueues() {
  const [qs, ws] = await Promise.all([api('/queues').then(x => x || []), api('/workers').catch(() => [])]);
  poolsByName = Object.fromEntries(qs.map(q => [q.name, q]));
  const online = q => ws.filter(w => w.online && (w.queues || []).includes(q)).length;
  document.getElementById('queues').innerHTML = qs.map(q => {
    const push = q.pool_type === 'push';
    return `<tr>
      <td><strong>${esc(q.name)}</strong> ${push ? '<span class="chip on">push</span>' : ''}
        <div class="muted">${esc(q.description || '')}${push && q.push_endpoint ? ' · ' + esc(q.push_endpoint) : ''}</div></td>
      <td>${q.paused ? '<span class="pill s-PAUSED">paused</span>' : '<span class="pill s-COMPLETED">active</span>'}</td>
      <td>${q.concurrency_limit ?? '∞'}</td>
      <td>${q.ready}</td><td>${q.scheduled}</td><td>${q.running}</td>
      <td>${push ? '—' : online(q.name)}</td>
      <td>${push ? '<span class="muted">dispatched</span>' : `<b>${desiredWorkers(q)}</b> <span class="muted">${q.min_workers || 0}–${q.max_workers ?? '∞'}</span>`}</td>
      <td class="muted">${esc(q.owner || '—')}</td>
      ${menuCell([
        [q.paused ? 'Resume' : 'Pause', { act: q.paused ? 'resumePool' : 'pausePool', name: q.name }],
        ['Limit…', { act: 'setLimit', name: q.name }],
        push ? ['Push endpoint…', { act: 'setPushEndpoint', name: q.name }]
             : ['Autoscale…', { act: 'setAutoscale', name: q.name }],
        !push && ['Make push…', { act: 'makePush', name: q.name }],
      ], 'writer-only')}
    </tr>`;
  }).join('');
  const pick = document.getElementById('q-pick');
  const keep = pick.value;
  pick.innerHTML = qs.map(q => `<option>${esc(q.name)}</option>`).join('');
  if (keep && qs.some(q => q.name === keep)) pick.value = keep;
  else if (qs.length) pick.value = qs.reduce((a, b) => (b.scheduled > a.scheduled ? b : a), qs[0]).name;
}

function setLimit(name, cur) {
  const v = prompt('Concurrency limit for "' + name + '" (blank = unlimited)', cur ?? '');
  if (v === null) return;
  const limit = v.trim() === '' ? null : parseInt(v, 10);
  if (limit !== null && (isNaN(limit) || limit < 0)) return toast('Limit must be a non-negative number', true);
  act('/queues', { method: 'POST', body: JSON.stringify({ name, concurrency_limit: limit }) });
}

function setAutoscale(q) {
  if (!canWrite()) return toast('Your account is read-only', true);
  const min = prompt('Minimum workers for pool "' + q.name + '"', q.min_workers ?? 0);
  if (min === null) return;
  const max = prompt('Maximum workers (blank = unbounded)', q.max_workers ?? '');
  if (max === null) return;
  const target = prompt('Target ready runs per worker', q.target_ready_per_worker ?? 5);
  if (target === null) return;
  const owner = prompt('Owner / team (blank = none)', q.owner ?? '');
  if (owner === null) return;
  const body = {
    name: q.name, min_workers: parseInt(min, 10) || 0,
    target_ready_per_worker: parseInt(target, 10) || 5, owner: owner.trim(),
  };
  if (max.trim() === '') body.clear_max_workers = true;
  else body.max_workers = parseInt(max, 10);
  act('/queues', { method: 'POST', body: JSON.stringify(body) });
}

function setPushEndpoint(q) {
  if (!canWrite()) return toast('Your account is read-only', true);
  const url = prompt('Push endpoint URL for pool "' + q.name + '"\n(the server POSTs each ready run here; blank cancels)', q.push_endpoint || '');
  if (url === null || url.trim() === '') return;
  const secret = prompt('Shared secret for HMAC signing (blank keeps the stored one)', '');
  if (secret === null) return;
  const body = { name: q.name, pool_type: 'push', push_endpoint: url.trim() };
  if (secret.trim() !== '') body.push_secret = secret.trim();
  act('/queues', { method: 'POST', body: JSON.stringify(body) });
}

// --- create work pool -------------------------------------------------
function openCreatePool() {
  if (!canWrite()) return toast('Your account is read-only', true);
  const f = document.getElementById('pool-form');
  f.reset();
  poolTypeToggle();
  document.getElementById('pool-dialog').showModal();
}

function poolTypeToggle() {
  const el = document.getElementById('pool-form').elements;
  const push = el.pool_type.value === 'push';
  document.getElementById('pool-pull').style.display = push ? 'none' : 'grid';
  document.getElementById('pool-push').style.display = push ? 'grid' : 'none';
  el.push_endpoint.required = push;
}

async function createPool(ev) {
  ev.preventDefault();
  const el = ev.target.elements;
  const push = el.pool_type.value === 'push';
  const body = {
    name: el.name.value.trim(),
    description: el.description.value.trim(),
    pool_type: el.pool_type.value,
    owner: el.owner.value.trim(),
    paused: el.paused.checked,
  };
  if (el.concurrency_limit.value.trim() !== '') {
    const n = parseInt(el.concurrency_limit.value, 10);
    if (isNaN(n) || n < 0) return toast('Concurrency limit must be a non-negative number', true);
    body.concurrency_limit = n;
  }
  if (push) {
    body.push_endpoint = el.push_endpoint.value.trim();
    if (!body.push_endpoint) return toast('A push pool needs an endpoint URL', true);
    if (el.push_secret.value.trim() !== '') body.push_secret = el.push_secret.value.trim();
  } else {
    body.min_workers = parseInt(el.min_workers.value, 10) || 0;
    body.target_ready_per_worker = parseInt(el.target_ready_per_worker.value, 10) || 5;
    if (el.max_workers.value.trim() !== '') {
      const mx = parseInt(el.max_workers.value, 10);
      if (isNaN(mx) || mx < body.min_workers) return toast('Max workers must be ≥ min workers', true);
      body.max_workers = mx;
    }
  }
  try {
    await api('/queues', { method: 'POST', body: JSON.stringify(body) });
    toast('Pool "' + body.name + '" created');
    document.getElementById('pool-dialog').close();
    if (currentView() === 'newworker') { nwReloadPools(body.name); }
    else { show('queues'); loadQueues(); }
  } catch (e) { toast(e.message, true); }
}

async function loadPending() {
  const q = document.getElementById('q-pick').value;
  if (!q) { document.getElementById('pending').innerHTML = ''; return; }
  const runs = (await api('/queues/' + encodeURIComponent(q) + '/pending')) || [];
  document.getElementById('pending').innerHTML = runs.length ? runs.map((r, i) => `
    <tr>
      <td class="mono">${i + 1}</td>
      <td>${runLink(r.id, esc(r.name), 'mono')}</td>
      <td>${esc(r.flow_name)}</td>
      <td>${prio(r)}</td>
      <td>${when(r.scheduled_at)}</td>
      ${menuCell([
        ['<span title="Run this next">▲ Front</span>', { act: 'runToFront', id: r.id }],
        ['Urgent', { act: 'bump', id: r.id, priority: 100 }],
        ['Normal', { act: 'bump', id: r.id, priority: 50 }],
        ['▼ Back', { act: 'runToBack', id: r.id }],
        r.queue_position != null && ['Unpin', { act: 'runUnpin', id: r.id }],
        ['Move…', { act: 'moveQueue', id: r.id }],
      ], 'writer-only')}
    </tr>`).join('') : '<tr><td colspan="6" class="empty">Queue is empty.</td></tr>';
}

const bump = (id, p) => act('/runs/' + id + '/priority', { method: 'POST', body: JSON.stringify({ priority: p }) });
function moveQueue(id) {
  const q = prompt('Move run to which work pool?');
  if (q) act('/runs/' + id + '/queue', { method: 'POST', body: JSON.stringify({ queue: q }) });
}

registerViews({ queues: { refresh: async () => { await loadQueues(); await loadPending(); } } });

registerActions({
  loadQueues,
  loadPending,
  openCreatePool,
  poolTypeToggle,
  createPool: (el, ev) => createPool(ev),
  // The prompts want the pool's current numbers, which the row does not carry:
  // the name is the key, poolsByName is the record.
  setLimit: el => setLimit(el.dataset.name, poolsByName[el.dataset.name].concurrency_limit ?? null),
  setAutoscale: el => setAutoscale(poolsByName[el.dataset.name]),
  setPushEndpoint: el => setPushEndpoint(poolsByName[el.dataset.name]),
  makePush: el => setPushEndpoint({ name: el.dataset.name, pool_type: 'pull' }),
  pausePool: el => act(`/queues/${encodeURIComponent(el.dataset.name)}/pause`, { method: 'POST' }),
  resumePool: el => act(`/queues/${encodeURIComponent(el.dataset.name)}/resume`, { method: 'POST' }),
  bump: el => bump(el.dataset.id, +el.dataset.priority),
  moveQueue: el => moveQueue(el.dataset.id),
  runToFront: el => act(`/runs/${el.dataset.id}/front`, { method: 'POST' }),
  runToBack: el => act(`/runs/${el.dataset.id}/back`, { method: 'POST' }),
  runUnpin: el => act(`/runs/${el.dataset.id}/unpin`, { method: 'POST' }),
});
