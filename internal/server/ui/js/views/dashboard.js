// The landing view: counters, the activity charts, and the two short lists
// that answer "is anything stuck right now".
import { api } from '../api.js';
import { bars, line, spark } from '../charts.js';
import { STCOL, esc, state, when } from '../fmt.js';
import { registerActions } from '../actions.js';
import { registerViews } from '../router.js';
import { runHref } from './run.js';

// The window an operator last chose outlives the tab, so the remembered one is
// marked before anything is drawn.
let dashWindow = localStorage.getItem('pf-window') || '24h';
document.querySelectorAll('#win-switch button').forEach(b =>
  b.classList.toggle('active', b.dataset.w === dashWindow));
const winLabel = w => ({ '8h': 'last 8 hours', '24h': 'last 24 hours', '168h': 'last 7 days' }[w] || w);
const trend = arr => {
  const h = arr.slice(0, Math.floor(arr.length / 2)).reduce((a, b) => a + b, 0);
  const t = arr.slice(Math.floor(arr.length / 2)).reduce((a, b) => a + b, 0);
  if (!h) return t ? { d: 100, up: true } : { d: 0, up: true };
  return { d: Math.round(((t - h) / h) * 100), up: t >= h };
};

async function loadDashboard() {
  let s;
  try { s = await api('/stats?window=' + dashWindow); }
  catch (e) { return; }
  const fr = s.flow_runs || { by_state: {}, buckets: [] };
  const tr = s.task_runs || { buckets: [] };
  const ev = s.events || { buckets: [] };
  const taskVals = (tr.buckets || []).map(b => b.completed + b.failed);
  const evVals = (ev.buckets || []).map(b => b.n);
  const tD = trend(taskVals), eD = trend(evVals);
  const deltaEl = d => d.d ? `<span class="delta ${d.up ? 'up' : 'down'}">${d.up ? '↑' : '↓'} ${Math.abs(d.d)}%</span>` : '<span class="muted" style="font-size:12px">flat</span>';

  document.getElementById('dash-cards').innerHTML = `
    <div class="card"><div class="k">Flow runs</div>
      <div class="n">${fr.total || 0}</div>
      <div class="spark">${line((fr.buckets || []).map(b => b.completed + b.failed + b.other), 'var(--accent)', 34)}</div></div>
    <div class="card"><div class="k">Task runs</div>
      <div class="n">${tr.total || 0} ${deltaEl(tD)}</div>
      <div class="spark">${spark(taskVals, 'var(--ok)')}</div></div>
    <div class="card"><div class="k">Events</div>
      <div class="n">${ev.total || 0} ${deltaEl(eD)}</div>
      <div class="spark">${spark(evVals, 'var(--accent-2)')}</div></div>
    <div class="card mini"><div class="k">Completed</div><div class="n" style="color:var(--ok)">${fr.by_state.COMPLETED || 0}</div></div>
    <div class="card mini"><div class="k">Failed / crashed</div><div class="n" style="color:var(--err)">${(fr.by_state.FAILED || 0) + (fr.by_state.CRASHED || 0)}</div></div>
    <div class="card mini"><div class="k">Running</div><div class="n" style="color:var(--run)">${(fr.by_state.RUNNING || 0) + (fr.by_state.PENDING || 0)}</div></div>`;

  document.getElementById('dash-fr-window').textContent = winLabel(dashWindow);
  document.getElementById('dash-fr-chart').innerHTML = bars(fr.buckets || [], [
    { key: 'completed', color: 'var(--ok)' },
    { key: 'failed', color: 'var(--err)' },
    { key: 'other', color: 'var(--idle)' },
  ], 130);

  // recent flows, grouped
  try {
    const { runs } = await api('/runs?limit=120');
    const byFlow = {};
    runs.forEach(r => (byFlow[r.flow_name] = byFlow[r.flow_name] || []).push(r));
    const cards = Object.entries(byFlow).sort((a, b) =>
      new Date(b[1][0].scheduled_at) - new Date(a[1][0].scheduled_at)).slice(0, 9).map(([flow, rs]) => `
      <div class="fcard">
        <h3>${esc(flow)}</h3>
        <div class="muted" style="font-size:12px">${rs.length} recent · last ${when(rs[0].scheduled_at)}</div>
        <div class="fdots">${rs.slice(0, 14).map(r => `<span class="fdot" style="background:${STCOL(r.state)}" title="${esc(r.name)} — ${r.state}"></span>`).join('')}</div>
        ${rs.slice(0, 2).map(r => `<div class="fmini"><a href="${runHref(r.id)}" data-click="openRun" data-id="${esc(r.id)}">${esc(r.name)}</a>${state(r.state)}</div>`).join('')}
      </div>`).join('');
    document.getElementById('dash-flows').innerHTML = cards || '<div class="empty">No runs yet.</div>';
  } catch (e) { /* non-fatal */ }

  // work pools
  try {
    const [qs, ws] = await Promise.all([api('/queues').then(x => x || []), api('/workers').catch(() => [])]);
    document.getElementById('dash-pools').innerHTML = qs.map(q => {
      const online = ws.filter(w => w.online && (w.queues || []).includes(q.name)).length;
      return `<div class="fcard">
        <h3>${esc(q.name)} ${q.paused ? '<span class="pill s-PAUSED">paused</span>' : ''}</h3>
        <div class="muted" style="font-size:12px">${esc(q.owner || 'no owner')}</div>
        <div class="poolrow">
          <div><div class="pv">${q.ready}</div><div class="pk">ready</div></div>
          <div><div class="pv">${q.running}</div><div class="pk">running</div></div>
          <div><div class="pv">${online}</div><div class="pk">workers</div></div>
          <div><div class="pv">${desiredWorkers(q)}</div><div class="pk">desired</div></div>
        </div></div>`;
    }).join('') || '<div class="empty">No pools.</div>';
  } catch (e) { /* non-fatal */ }
}

const desiredWorkers = q => {
  const t = q.target_ready_per_worker || 5;
  let n = Math.ceil((q.ready || 0) / t);
  if (n < (q.min_workers || 0)) n = q.min_workers || 0;
  if (q.max_workers != null && n > q.max_workers) n = q.max_workers;
  return n < 0 ? 0 : n;
};

registerViews({ dashboard: { refresh: loadDashboard } });

registerActions({
  setDashWindow: el => {
    dashWindow = el.dataset.w;
    localStorage.setItem('pf-window', dashWindow);
    document.querySelectorAll('#win-switch button').forEach(b => b.classList.toggle('active', b === el));
    loadDashboard();
  },
});

export {
  desiredWorkers,
};
