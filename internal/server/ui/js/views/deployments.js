// Deployments: the table, and one deployment's own page under it.
//
// The two are one module because they are one route -- /deployments is the
// table and /deployments/<id> is the page -- and splitting them only bought
// an import cycle between the list and the row it opens.
import { act, api, canWrite, toast } from '../api.js';
import { poolBlocker, poolContext, poolPill, runBlocker } from '../blockers.js';
import { publish } from '../bridge.js';
import { bars, svg } from '../charts.js';
import { STCOL, dur, esc, goDur, prio, runAt, runMS, state, when } from '../fmt.js';
import { menuCell } from '../menu.js';
import { registerRoutes, registerViews, show, syncURL } from '../router.js';
import { paramFields, readParams } from './flows.js';
import { runLink } from './run.js';

async function loadDeployments() {
  const [ds, ctx] = await Promise.all([api('/deployments').then(x => x || []), poolContext()]);
  document.getElementById('deployments').innerHTML = ds.length ? ds.map(d => `
    <tr onclick="openDeployment(event, '${esc(d.id)}')" style="cursor:pointer">
      <td><strong>${esc(d.name)}</strong><div class="muted">${esc(d.description || '')}</div></td>
      <td>${esc(d.flow_name)}</td>
      <td class="mono">${d.schedule ? esc(d.schedule_kind + ' ' + d.schedule) + ' <span class="muted">' + esc(d.timezone || 'UTC') + '</span>' : '<span class="muted">manual</span>'}</td>
      <td>${esc(d.work_queue)}${poolPill(poolBlocker(ctx.pool[d.work_queue], ctx.online(d.work_queue)))}</td>
      <td>${d.priority}</td>
      <td>${d.paused ? '<span class="pill s-PAUSED">paused</span>' : '<span class="pill s-COMPLETED">active</span>'}</td>
      ${menuCell([
        ['Open', `openDeployment(null, '${d.id}')`],
        ['Run now', `act('/deployments/${d.id}/run',{method:'POST',body:'{}'})`],
        ['Run…', `openDeploymentRun('${d.id}')`],
        [d.paused ? 'Resume' : 'Pause', `act('/deployments/${d.id}/${d.paused ? 'resume' : 'pause'}',{method:'POST'})`],
        ['Delete', `if(confirm('Delete deployment ' + ${JSON.stringify(d.name)} + '?'))act('/deployments/${d.id}',{method:'DELETE'})`, true],
      ], 'writer-only')}
    </tr>`).join('') : '<tr><td colspan="7" class="empty">No deployments yet.</td></tr>';
}

// Run a deployment with parameters, as opposed to the row's "Run now", which
// posts an empty body and takes the deployment's stored defaults.
//
// The form comes from the flow function's own parameter struct -- the schema
// sdk.ParamsSchema reflected and the worker published -- and the run goes
// through POST /deployments/{id}/run, so it keeps the deployment's queue,
// priority, retries, timeout and tags. That is the difference from the Flows
// dialog, whose quick run is ad-hoc and carries none of them.
async function openDeploymentRun(id) {
  const dlg = document.getElementById('dep-run-dialog');
  document.getElementById('rnd-title').textContent = 'Run deployment';
  document.getElementById('rnd-body').innerHTML = '<div class="empty">Loading…</div>';
  dlg.showModal();
  const d = await api('/deployments/' + encodeURIComponent(id));
  document.getElementById('rnd-title').textContent = 'Run ' + d.name;
  // A flow whose worker has never started is not in the catalogue. That is not
  // a reason to refuse the dialog -- fall back to the JSON textarea.
  const f = await api('/flows/' + encodeURIComponent(d.flow_name)).catch(() => null);
  const schema = f && f.params_schema && f.params_schema.fields || [];
  document.getElementById('rnd-body').innerHTML = `
    <dl class="kv">
      <dt>Flow</dt><dd class="mono">${esc(d.flow_name)}</dd>
      <dt>Pool</dt><dd class="mono">${esc(d.work_queue)}</dd>
      <dt>Priority</dt><dd>${d.priority}</dd>
      ${schema.length ? '' : '<dt>Schema</dt><dd class="muted">This flow published none — enter parameters as JSON.</dd>'}
    </dl>
    <div class="panel" style="margin:0;border-radius:0;border-left:0;border-right:0">
      <h2>Parameters</h2>
      ${canWrite() ? `<form class="inline" onsubmit="runDeployment(event, '${esc(d.id)}')">
        ${paramFields(schema, d.parameters || {})}
        <label>priority <span class="muted">override</span><input name="__priority" type="number" min="0" max="100" placeholder="${d.priority}"></label>
        <label>pool <span class="muted">override</span><input name="__queue" type="text" placeholder="${esc(d.work_queue)}"></label>
        <label>delay <span class="muted">e.g. 5m</span><input name="__delay" type="text" placeholder="none"></label>
        <div class="full row"><button class="act primary" type="submit">Run deployment</button></div>
      </form>` : '<p class="muted" style="padding:14px">Your account is read-only.</p>'}
    </div>`;
}

async function runDeployment(ev, id) {
  ev.preventDefault();
  if (!canWrite()) { toast('Your account is read-only', true); return; }
  const params = readParams(ev.target);
  if (!params) return;
  const fd = new FormData(ev.target);
  const body = { parameters: params };
  const priority = parseInt(fd.get('__priority') || '', 10);
  if (!isNaN(priority)) body.priority = priority;
  const queue = (fd.get('__queue') || '').trim();
  if (queue) body.work_queue = queue;
  const delay = (fd.get('__delay') || '').trim();
  if (delay) body.delay = delay;
  try {
    const run = await api('/deployments/' + encodeURIComponent(id) + '/run',
      { method: 'POST', body: JSON.stringify(body) });
    toast('Started ' + run.name);
    document.getElementById('dep-run-dialog').close();
    show('runs');
  } catch (e) { toast(e.message, true); }
}

// --- deployment detail ----------------------------------------------------
// Clicking a row on the Deployments list opens the deployment's own page: what
// it does on the left -- the runs it has produced, the ones queued next, the
// parameters it carries, the settings its runs inherit -- and what it is on the
// right. The address is /deployments/<id>/<tab>, so a tab can be linked to and
// the back button walks out of it the way it walked in.
const DEP_TABS = ['runs', 'upcoming', 'parameters', 'configuration', 'description'];
// Everything but SCHEDULED: the work already handed over, as opposed to the
// work the scheduler has only queued. The two tabs ask for them separately so
// a per-minute cron cannot push its own history off the end of one page.
const DEP_PAST = 'PENDING,RUNNING,COMPLETED,FAILED,CRASHED,CANCELLING,CANCELLED,PAUSED';
let depID = null, depTab = 'runs';

// A deployments row carries a "⋯" menu of its own, so a click that landed on a
// control belongs to that control rather than to the row around it.
function openDeployment(ev, id) {
  if (ev && ev.target.closest('button, .menu, a')) return;
  if (id !== depID) depTab = 'runs';
  depID = id;
  show('deployment');
}

function showDepTab(t) {
  depTab = t;
  syncURL('deployment');
  document.querySelectorAll('#dep-subnav button').forEach(b => b.classList.toggle('active', b.dataset.t === t));
  DEP_TABS.forEach(x => {
    const el = document.getElementById('dep-t-' + x);
    if (el) el.hidden = x !== t;
  });
}

const depStatus = d => d.paused
  ? '<span class="pill s-PAUSED">paused</span>'
  : '<span class="pill s-COMPLETED">active</span>';
const depPrio = d => `<span class="prio"><b>${d.priority}</b><span class="bar"><i style="width:${d.priority}%"></i></span></span>`;
const depTags = d => (d.tags || []).length
  ? d.tags.map(t => `<span class="chip">${esc(t)}</span>`).join(' ')
  : '<span class="muted">none</span>';

function removeDeployment(id, name) {
  if (!canWrite()) return toast('Your account is read-only', true);
  if (!confirm('Delete deployment ' + name + '?')) return;
  // Not act(): its refresh would reload the page of a deployment that is gone.
  api('/deployments/' + encodeURIComponent(id), { method: 'DELETE' })
    .then(() => { toast('Deleted ' + name); depID = null; show('deployments'); })
    .catch(e => toast(e.message, true));
}

// A bar per recent run, oldest on the left: height is its duration against the
// slowest in view, colour is its state. It answers "is this deployment getting
// slower, and when did it start failing?" without opening a single run.
function runBars(runs, h = 76) {
  const rs = runs.slice(0, 40).reverse();
  if (!rs.length) return '<div class="empty" style="padding:14px 0">Nothing has run yet.</div>';
  const max = Math.max(1, ...rs.map(r => runMS(r) || 0));
  const W = 1000, bw = W / rs.length;
  const bars = rs.map((r, i) => {
    const ms = runMS(r);
    // A run still going, or one that never started, has no length to draw:
    // give it a stub so the gap in the row is not mistaken for no run at all.
    const bh = Math.max(5, ((ms || 0) / max) * (h - 5));
    return `<rect x="${(i * bw + bw * 0.15).toFixed(1)}" y="${(h - bh).toFixed(1)}" ` +
      `width="${(bw * 0.7).toFixed(1)}" height="${bh.toFixed(1)}" rx="2" ` +
      `fill="${STCOL(r.state)}" opacity="${ms == null ? '0.45' : '0.9'}" onclick="openRun('${r.id}')">` +
      `<title>${esc(r.name)} — ${esc(r.state)}${ms == null ? '' : ' — ' + dur(ms)} · ` +
      `${esc(new Date(runAt(r)).toLocaleString())}</title></rect>`;
  }).join('');
  return svg(bars, W, h) +
    `<div class="muted" style="font-size:10.5px;display:flex;justify-content:space-between">
       <span>${esc(new Date(runAt(rs[0])).toLocaleString())}</span>
       <span>${esc(new Date(runAt(rs[rs.length - 1])).toLocaleString())}</span></div>`;
}

// The stored parameters read against the flow's published schema, so a value
// the flow no longer takes -- a renamed field -- shows up as an extra instead
// of being silently dropped at run time.
function depParams(d, flow) {
  const vals = d.parameters || {};
  const fields = flow && flow.params_schema && flow.params_schema.fields || [];
  const keys = Object.keys(vals);
  const fmt = v => typeof v === 'object' ? JSON.stringify(v) : String(v);
  if (!fields.length) {
    return keys.length ? `<pre class="logs">${esc(JSON.stringify(vals, null, 2))}</pre>`
      : '<div class="empty">No parameters set, and the flow published no schema to fill in.</div>';
  }
  const extra = keys.filter(k => !fields.some(f => f.name === k));
  return `<div class="scroll"><table>
    <thead><tr><th>Parameter</th><th>Type</th><th>Value</th></tr></thead>
    <tbody>
      ${fields.map(f => `<tr>
        <td class="mono">${esc(f.name)}${f.required ? ' <span class="muted" title="required">*</span>' : ''}</td>
        <td class="muted">${esc(f.type)}</td>
        <td class="mono">${vals[f.name] !== undefined ? esc(fmt(vals[f.name]))
          : `<span class="muted">flow default${f.example != null ? ' · e.g. ' + esc(fmt(f.example)) : ''}</span>`}</td>
      </tr>`).join('')}
      ${extra.map(k => `<tr>
        <td class="mono">${esc(k)}</td>
        <td><span class="pill s-CANCELLED plain">not in schema</span></td>
        <td class="mono">${esc(fmt(vals[k]))}</td>
      </tr>`).join('')}
    </tbody></table></div>`;
}

async function loadDeployment() {
  if (!depID) return show('deployments', true);
  const main = document.getElementById('dep-main'), side = document.getElementById('dep-side');
  // Only blank the page when it is a different deployment: the 15s refresh and
  // every live event land here too, and they should not flash the whole view.
  if (main.dataset.id !== depID) {
    main.innerHTML = '<div class="panel"><div class="empty">Loading…</div></div>';
    side.innerHTML = '';
    document.getElementById('dep-title').textContent = 'Deployment';
    document.getElementById('dep-state').innerHTML = '';
    document.getElementById('dep-acts').innerHTML = '';
  }
  let d;
  try {
    d = await api('/deployments/' + encodeURIComponent(depID));
  } catch (e) {
    // Deleted from under us, or a link to an id that never existed.
    main.dataset.id = '';
    main.innerHTML = `<div class="panel"><div class="empty">${esc(e.message)}</div></div>`;
    return;
  }
  main.dataset.id = d.id;
  const [hist, up, ctx, flow] = await Promise.all([
    api(`/runs?deployment_id=${encodeURIComponent(d.id)}&state=${DEP_PAST}&limit=60`),
    api(`/runs?deployment_id=${encodeURIComponent(d.id)}&state=SCHEDULED&limit=40`),
    poolContext(),
    // A flow whose worker has never started is not in the catalogue; the page
    // still has everything else to show.
    api('/flows/' + encodeURIComponent(d.flow_name)).catch(() => null),
  ]);
  const runs = hist.runs || [];
  const upcoming = (up.runs || []).slice().sort((a, b) => new Date(a.scheduled_at) - new Date(b.scheduled_at));
  const fin = runs.filter(r => ['COMPLETED', 'FAILED', 'CRASHED', 'CANCELLED'].includes(r.state));
  const ok = fin.filter(r => r.state === 'COMPLETED').length;
  const timed = runs.map(runMS).filter(x => x != null);
  const avg = timed.length ? timed.reduce((a, b) => a + b, 0) / timed.length : null;
  const blocker = poolBlocker(ctx.pool[d.work_queue], ctx.online(d.work_queue));
  const sched = d.schedule ? d.schedule_kind + ' ' + d.schedule : '';

  document.getElementById('dep-title').textContent = d.name;
  document.getElementById('dep-state').innerHTML = depStatus(d) + poolPill(blocker);
  document.getElementById('dep-acts').innerHTML = canWrite() ? `
    <button class="act primary" onclick="act('/deployments/${d.id}/run',{method:'POST',body:'{}'})">Run now</button>
    <button class="act" onclick="openDeploymentRun('${d.id}')">Run…</button>
    <button class="act" onclick="act('/deployments/${d.id}/${d.paused ? 'resume' : 'pause'}',{method:'POST'})">${d.paused ? 'Resume' : 'Pause'}</button>
    <button class="act" onclick="removeDeployment('${d.id}', ${esc(JSON.stringify(d.name))})">Delete</button>` : '';

  main.innerHTML = `
    <div class="cards">
      <div class="card mini"><div class="k">Runs</div><div class="n">${hist.total}</div>
        <div class="muted" style="font-size:12px">${upcoming.length ? upcoming.length + ' queued ahead' : 'none queued'}</div></div>
      <div class="card mini"><div class="k">Success rate</div>
        <div class="n">${fin.length ? Math.round(ok / fin.length * 100) + '%' : '—'}</div>
        <div class="muted" style="font-size:12px">${fin.length ? 'last ' + fin.length + ' finished' : 'nothing finished yet'}</div></div>
      <div class="card mini"><div class="k">Avg duration</div><div class="n">${avg == null ? '—' : dur(avg)}</div>
        <div class="muted" style="font-size:12px">${timed.length} timed run${timed.length === 1 ? '' : 's'}</div></div>
      <div class="card mini"><div class="k">Next run</div>
        <div class="n" style="font-size:19px">${upcoming[0] ? when(upcoming[0].scheduled_at)
          : d.paused ? 'paused' : sched ? 'not queued yet' : 'on demand'}</div>
        <div class="muted" style="font-size:12px">${sched ? esc(sched) + ' · ' + esc(d.timezone || 'UTC') : 'no schedule'}</div></div>
    </div>

    <div class="panel">
      <h2>Run history<span class="grow"></span>
        <span class="muted" style="font-weight:400">${runs.length ? 'last ' + Math.min(runs.length, 40) + ' runs — bar height is duration' : ''}</span></h2>
      <div class="bars">${runBars(runs)}</div>
    </div>

    <div class="subnav flat" id="dep-subnav">${DEP_TABS.map(t => {
      const n = t === 'runs' ? hist.total : t === 'upcoming' ? upcoming.length : 0;
      return `<button data-t="${t}" onclick="showDepTab('${t}')">${t[0].toUpperCase() + t.slice(1)}` +
        `${n ? ' <span class="muted">' + n + '</span>' : ''}</button>`;
    }).join('')}</div>

    <div id="dep-t-runs" hidden><div class="panel">
      <h2>Flow runs<span class="grow"></span><span class="muted" style="font-weight:400">newest first</span></h2>
      <div class="scroll"><table>
        <thead><tr><th>Run</th><th>State</th><th>Pool</th><th>Started</th><th>Duration</th><th>Attempt</th></tr></thead>
        <tbody>${runs.length ? runs.map(r => `<tr onclick="openRun('${r.id}')" style="cursor:pointer">
          <td class="mono">${esc(r.name)}</td>
          <td>${state(r.state)}${r.state_message ? ` <span class="muted" title="${esc(r.state_message)}">ⓘ</span>` : ''}</td>
          <td>${esc(r.work_queue)}</td>
          <td>${when(r.started_at)}</td>
          <td>${dur(runMS(r))}</td>
          <td>${r.run_count}${r.retries ? ' / ' + (r.retries + 1) : ''}</td>
        </tr>`).join('') : '<tr><td colspan="6" class="empty">This deployment has not run yet.</td></tr>'}</tbody>
      </table></div>
    </div></div>

    <div id="dep-t-upcoming" hidden><div class="panel">
      <h2>Upcoming runs<span class="grow"></span>
        <span class="muted" style="font-weight:400">the scheduler materialises about an hour ahead</span></h2>
      <div class="scroll"><table>
        <thead><tr><th>Run</th><th>Pool</th><th>Priority</th><th>Scheduled</th><th></th></tr></thead>
        <tbody>${upcoming.length ? upcoming.map(r => `<tr>
          <td>${runLink(r.id, esc(r.name), 'mono')}</td>
          <td>${esc(r.work_queue)}${poolPill(runBlocker(r, ctx))}</td>
          <td>${prio(r)}</td>
          <td>${when(r.scheduled_at)} <span class="muted">${esc(new Date(r.scheduled_at).toLocaleString())}</span></td>
          <td class="writer-only" style="text-align:right">
            <button class="act" onclick="act('/runs/${r.id}/cancel',{method:'POST'})">Cancel</button></td>
        </tr>`).join('') : `<tr><td colspan="5" class="empty">${
          !sched ? 'No schedule — this deployment runs only when something triggers it.'
            : d.paused ? 'Paused: nothing is queued while a deployment is paused.'
            : 'Nothing queued yet.'}</td></tr>`}</tbody>
      </table></div>
    </div></div>

    <div id="dep-t-parameters" hidden><div class="panel">
      <h2>Parameters<span class="grow"></span>
        <span class="muted" style="font-weight:400">what every run starts with unless it overrides them</span></h2>
      ${depParams(d, flow)}
    </div></div>

    <div id="dep-t-configuration" hidden><div class="panel">
      <h2>Configuration<span class="grow"></span>
        <span class="muted" style="font-weight:400">every run this deployment creates inherits these</span></h2>
      <dl class="kv">
        <dt>Flow</dt><dd class="mono">${esc(d.flow_name)}${flow ? ''
          : ' <span class="pill s-FAILED" title="No worker has registered this flow, so its runs have nowhere to go.">unregistered</span>'}</dd>
        <dt>Work pool</dt><dd>${esc(d.work_queue)}${poolPill(blocker)}</dd>
        <dt>Priority</dt><dd>${depPrio(d)}</dd>
        <dt>Schedule</dt><dd>${sched ? `<span class="mono">${esc(sched)}</span> <span class="muted">${esc(d.timezone || 'UTC')}</span>`
          : '<span class="muted">manual — nothing schedules it</span>'}</dd>
        <dt>Catch up</dt><dd>${d.catchup ? 'on — windows missed during downtime are backfilled'
          : '<span class="muted">off — a missed window is skipped, not backfilled</span>'}</dd>
        <dt>Retries</dt><dd>${d.retries || 0}${d.retries ? ' · ' + goDur(d.retry_delay) + ' apart' : ''}</dd>
        <dt>Timeout</dt><dd>${d.timeout ? goDur(d.timeout) : '<span class="muted">none</span>'}</dd>
        <dt>Tags</dt><dd>${depTags(d)}</dd>
      </dl>
    </div></div>

    <div id="dep-t-description" hidden><div class="panel">
      <h2>Description</h2>
      ${d.description ? `<p style="padding:13px 15px;margin:0;white-space:pre-wrap">${esc(d.description)}</p>`
        : '<div class="empty">No description. Add one on the Deployments form.</div>'}
    </div></div>`;

  side.innerHTML = `
    <div class="panel">
      <h2>Deployment</h2>
      <dl class="kv">
        <dt>Flow</dt><dd><a href="#" class="rlink" onclick="openFlow('${esc(d.flow_name)}');return false">${esc(d.flow_name)}</a></dd>
        <dt>Status</dt><dd>${depStatus(d)}</dd>
        <dt>Work pool</dt><dd>${esc(d.work_queue)}${poolPill(blocker)}</dd>
        <dt>Priority</dt><dd>${depPrio(d)}</dd>
        <dt>Schedule</dt><dd>${sched ? `<span class="mono">${esc(sched)}</span><div class="muted">${esc(d.timezone || 'UTC')}</div>`
          : '<span class="muted">manual</span>'}</dd>
        <dt>Next run</dt><dd>${upcoming[0] ? when(upcoming[0].scheduled_at) : '<span class="muted">—</span>'}</dd>
        <dt>Last run</dt><dd>${runs[0] ? state(runs[0].state) + ' ' + when(runAt(runs[0]))
          : '<span class="muted">never</span>'}</dd>
        <dt>Tags</dt><dd>${depTags(d)}</dd>
        <dt>Created</dt><dd>${when(d.created_at)}</dd>
        <dt>Updated</dt><dd>${when(d.updated_at)}</dd>
        <dt>ID</dt><dd class="mono" style="font-size:11px;word-break:break-all">${esc(d.id)}</dd>
      </dl>
    </div>`;

  showDepTab(DEP_TABS.includes(depTab) ? depTab : 'runs');
}

async function saveDeployment(ev) {
  ev.preventDefault();
  const fd = new FormData(ev.target), body = {};
  for (const [k, v] of fd.entries()) if (String(v).trim() !== '') body[k] = v;
  body.priority = parseInt(body.priority || '50', 10);
  body.retries = parseInt(body.retries || '0', 10);
  if (body.parameters) {
    try { body.parameters = JSON.parse(body.parameters); }
    catch { return toast('Parameters must be valid JSON', true); }
  }
  try {
    await api('/deployments', { method: 'POST', body: JSON.stringify(body) });
    toast('Deployment saved');
    ev.target.reset();
    loadDeployments();
  } catch (e) { toast(e.message, true); }
}

registerViews({
  deployments: { refresh: loadDeployments },
  deployment: {
    refresh: loadDeployment,
    path: () => '/deployments/' + encodeURIComponent(depID) + '/' + depTab,
  },
});

// One deployment's own page hangs off the list it was opened from --
// /deployments/<id> -- with the open tab as a third segment. With no id the
// segment is just the list, so the claim declines and /deployments stays the
// table it always was.
registerRoutes([['deployments', (id, tab) => {
  if (!id) return null;
  depID = decodeURIComponent(id);
  depTab = DEP_TABS.includes(tab) ? tab : 'runs';
  return 'deployment';
}]]);

publish({
  loadDeployments, openDeployment, openDeploymentRun, runDeployment,
  removeDeployment, showDepTab, saveDeployment,
});
