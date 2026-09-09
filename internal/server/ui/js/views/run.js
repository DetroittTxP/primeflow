// One flow run: the tabs, the swimlane, and everything under them.
//
// It also owns which run is open (runID/runTab) and the /runs/<id> route
// that lands there, so a link to a run is this module's business too.
import { act, api } from '../api.js';
import { poolContext, poolPill, runBlocker } from '../blockers.js';
import { bars, line, svg } from '../charts.js';
import { STCOL, dataAttrs, dur, esc, goDur, prio, runMS, state, when } from '../fmt.js';
import { registerRoutes, registerViews, show, syncURL } from '../router.js';
import { registerActions } from '../actions.js';
import { evClass } from './events.js';

// A run gets a page rather than a dialog: an operator arrives here from a link
// in an alert, keeps it open while the run is still moving, and leaves with
// the browser's own back button. Everything the run produced hangs off it --
// how it executed, the checkpoints it wrote, its logs, its artifacts, its
// sub-flows, and the parameters it was handed.
let runID = '', runTab = 'timeline', runData = null;
const RUN_TABS = ['timeline', 'checkpoints', 'logs', 'artifacts', 'subflows', 'data'];
const RUN_TAB_LABEL = {
  timeline: 'Timeline', checkpoints: 'Checkpoints', logs: 'Logs',
  artifacts: 'Artifacts', subflows: 'Sub-flows', data: 'Parameters',
};

// A real href, so a run can be middle-clicked into a new tab or copied out of
// the page, with a plain click handled in-app. The query string rides along:
// it carries the API token.
const runHref = id => '/runs/' + encodeURIComponent(id) + location.search;
const runLink = (id, html, cls) =>
  `<a href="${runHref(id)}" class="rlink${cls ? ' ' + cls : ''}" data-click="openRun" data-id="${esc(id)}">${html}</a>`;

// openRun is the single entry point every list links through.
function openRun(id, tab) {
  document.querySelectorAll('dialog[open]').forEach(d => d.close());
  if (runID !== id) runData = null;
  runID = id;
  runTab = RUN_TABS.includes(tab) ? tab : 'timeline';
  show('run');
}
// The whole table row opens its run -- aiming at the name is a needless bit of
// precision -- but a click that landed on a control of its own (Cancel, the
// "⋯" menu, a nested link) belongs to that control.
function rowOpen(ev, id) {
  if (ev.target.closest('button, a, input, select, label, summary, .menu')) return;
  openRun(id);
}
function showRunTab(t) {
  runTab = t;
  syncURL('run');
  renderRun();
}

// Everything the page draws, fetched together. Sub-flows and events degrade to
// empty rather than failing the page: an older server may not serve them, and
// a run's own facts are still worth showing.
async function loadRun() {
  const id = runID;
  if (!id) { show('runs', true); return; }
  const main = document.getElementById('run-main');
  if (!runData || runData.run.id !== id) {
    document.getElementById('run-title').textContent = 'Run';
    document.getElementById('run-state').innerHTML = '';
    document.getElementById('run-acts').innerHTML = '';
    document.getElementById('run-side').innerHTML = '';
    main.innerHTML = '<div class="panel"><div class="empty">Loading…</div></div>';
  }
  let d;
  try {
    const [run, tasks, logs, arts, kids, evs, ctx] = await Promise.all([
      api('/runs/' + id),
      api('/runs/' + id + '/tasks').then(x => x || []),
      api('/runs/' + id + '/logs?limit=1000').then(x => x || []),
      api('/runs/' + id + '/artifacts').then(x => x || []),
      api('/runs/' + id + '/children').then(x => x || []).catch(() => []),
      api('/runs/' + id + '/events?limit=200').then(x => x || []).catch(() => []),
      poolContext(),
    ]);
    d = { run, tasks, logs, arts, kids, evs, ctx };
  } catch (e) {
    main.innerHTML = `<div class="panel"><div class="empty">${esc(e.message)}</div></div>`;
    return;
  }
  if (runID !== id) return; // navigated on while the fetch was in flight
  runData = d;
  renderRun();
}

function renderRun() {
  const d = runData;
  if (!d) return;
  const { run, tasks, logs, arts, kids, ctx } = d;
  if (!RUN_TABS.includes(runTab) || (runTab === 'subflows' && !kids.length)) runTab = 'timeline';

  document.getElementById('run-title').textContent = run.name;
  document.getElementById('run-state').innerHTML = state(run.state) + poolPill(runBlocker(run, ctx));
  document.getElementById('run-acts').innerHTML = runActions(run);
  document.getElementById('run-side').innerHTML = runFacts(run, ctx);

  const count = { checkpoints: tasks.length, logs: logs.length, artifacts: arts.length, subflows: kids.length };
  const nav = RUN_TABS.filter(t => t !== 'subflows' || kids.length).map(t =>
    `<button data-t="${t}"${t === runTab ? ' class="active"' : ''} data-click="showRunTab" data-tab="${t}">${RUN_TAB_LABEL[t]}` +
    (count[t] ? ` <span class="muted">${count[t]}</span>` : '') + '</button>').join('');
  const body = {
    timeline: runTimeline, checkpoints: runCheckpointsTab, logs: runLogsTab,
    artifacts: runArtifactsTab, subflows: runSubflowsTab, data: runDataTab,
  }[runTab];

  const main = document.getElementById('run-main');
  const keep = logScrollState();
  main.innerHTML = runTiles(d) + `<div class="subnav flat">${nav}</div>` + body(d);
  restoreLogScroll(keep);
}

function runActions(run) {
  const live = ['SCHEDULED', 'RUNNING', 'PENDING'].includes(run.state);
  const settled = ['FAILED', 'CRASHED', 'CANCELLED', 'COMPLETED'].includes(run.state);
  return (live ? `<button class="act writer-only" data-click="cancelRun" data-id="${esc(run.id)}">Cancel</button>` : '')
    + (settled ? `<button class="act primary writer-only" data-click="retryRun" data-id="${esc(run.id)}">Resume</button>` : '')
    + '<button class="act" data-click="loadRun">Refresh</button>';
}

// The five numbers an operator checks first. Queue wait is split out from
// duration on purpose: a run that took an hour because nothing polled its pool
// is a pool problem, not a slow flow.
function runTiles({ run, tasks, logs }) {
  const done = tasks.filter(t => t.state === 'COMPLETED').length;
  const bad = tasks.filter(t => t.state === 'FAILED' || t.state === 'CRASHED').length;
  const errs = logs.filter(l => l.level === 'ERROR').length;
  const started = run.started_at ? +new Date(run.started_at) : null;
  const ended = run.ended_at ? +new Date(run.ended_at) : null;
  const elapsed = started ? (ended || Date.now()) - started : null;
  const wait = (started || Date.now()) - +new Date(run.scheduled_at);
  const tile = (k, n, s) => `<div class="tile"><div class="k">${k}</div><div class="n">${n}</div><div class="s">${s}</div></div>`;
  return `<div class="tiles">
    ${tile(ended ? 'Duration' : 'Running for', dur(elapsed), ended ? 'wall clock' : started ? 'still going' : 'not started yet')}
    ${tile('Queue wait', dur(wait > 0 ? wait : 0), started ? 'scheduled → started' : 'waiting now')}
    ${tile('Checkpoints', tasks.length ? done + ' / ' + tasks.length : '—', bad ? bad + ' failed' : 'completed')}
    ${tile('Attempt', run.run_count + (run.retries ? ' / ' + (run.retries + 1) : ''), run.retries ? 'retries budgeted' : 'no retry budget')}
    ${tile('Log lines', logs.length, errs ? errs + (errs === 1 ? ' error' : ' errors') : 'no errors')}
  </div>`;
}

// The facts column: what the run is, what it was given, and who is holding it.
function runFacts(run, ctx) {
  const rows = [
    ['State', state(run.state)],
    ['Run id', `<span class="mono">${esc(run.id)}</span>`],
    ['Flow', `<a href="/flows" class="rlink" data-click="openFlow" data-name="${esc(run.flow_name)}">${esc(run.flow_name)}</a>`],
    run.deployment_id && ['Deployment', `<span class="mono" title="${esc(run.deployment_id)}">${esc(run.deployment_id.slice(0, 8))}</span>`],
    ['Pool', esc(run.work_queue) + poolPill(runBlocker(run, ctx))],
    ['Priority', prio(run)],
    ['Attempt', run.run_count + (run.retries ? ' of ' + (run.retries + 1) : '')],
    run.retry_delay && ['Retry delay', goDur(run.retry_delay)],
    run.timeout && ['Timeout', goDur(run.timeout)],
    ['Scheduled', when(run.scheduled_at)],
    ['Started', when(run.started_at)],
    ['Ended', when(run.ended_at)],
    ['Duration', dur(runMS(run))],
    run.worker_id && ['Worker', `<span class="mono" title="${esc(run.worker_id)}">${esc(run.worker_id.slice(0, 12))}</span>`],
    run.lease_expires_at && ['Lease', when(run.lease_expires_at)],
    run.parent_run_id && ['Parent', runLink(run.parent_run_id, esc(run.parent_run_id.slice(0, 8)), 'mono')],
    run.tags && run.tags.length && ['Tags', run.tags.map(t => `<span class="chip">${esc(t)}</span>`).join('')],
    run.cancel_requested && ['Cancel', '<span class="pill s-CANCELLING">requested</span>'],
  ].filter(Boolean);
  return `<div class="panel"><h2>Facts</h2>
    <dl class="kv">${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join('')}</dl></div>`;
}

// --- timeline tab ---------------------------------------------------------
function runTimeline(d) {
  const bars = taskBars(d.tasks);
  return `<div class="panel">
      <h2>Execution<span class="grow"></span><span class="muted">flow, checkpoints and sub-flows on one axis</span></h2>
      <div class="scroll" style="padding:10px 14px">${swimlane(d.run, d.tasks, d.kids)}</div>
    </div>
    <div class="panel">
      <h2>State history<span class="grow"></span><span class="muted">${d.evs.length || 'no'} transition${d.evs.length === 1 ? '' : 's'}</span></h2>
      ${stateRibbon(d.run, d.evs)}
    </div>
    ${bars ? `<div class="panel"><h2>Where the time went<span class="grow"></span><span class="muted">longest checkpoints first</span></h2>${bars}</div>` : ''}`;
}

// swimlane: a Temporal-style execution view -- the flow run's own lane, one
// lane per checkpoint, one per sub-flow -- laid out on a shared time axis. The
// flow lane carries a hollow bar for the stretch the run sat in the queue, so
// a run that was merely waiting is not read as a slow flow. Pure SVG.
function swimlane(run, tasks, kids) {
  const num = t => t ? +new Date(t) : null;
  const lanes = [{
    label: 'Flow run', state: run.state,
    start: num(run.started_at), end: num(run.ended_at),
    wait: [num(run.scheduled_at), num(run.started_at)],
  }];
  tasks.forEach(t => lanes.push({
    label: t.task_key, state: t.state, start: num(t.started_at), end: num(t.ended_at),
    sub: t.run_count > 1 ? '×' + t.run_count : '', action: { act: 'showRunTab', tab: 'checkpoints' },
  }));
  (kids || []).forEach(k => lanes.push({
    label: '⑃ ' + k.name, state: k.state,
    start: num(k.started_at) || num(k.scheduled_at), end: num(k.ended_at),
    action: { act: 'openRun', id: k.id },
  }));

  const live = !run.ended_at && ['RUNNING', 'PENDING', 'CANCELLING', 'SCHEDULED'].includes(run.state);
  const times = lanes.flatMap(l => [l.start, l.end, ...(l.wait || [])]).filter(Boolean);
  if (!times.length) return '<span class="muted">Nothing has executed yet.</span>';
  const now = Date.now();
  const t0 = Math.min(...times);
  const t1 = Math.max(...times, t0 + 1000, live ? now : 0);
  const span = Math.max(1, t1 - t0);
  const W = 900, LABEL = 220, TRACK = W - LABEL - 12, ROW = 24, TOP = 22, H = TOP + lanes.length * ROW + 6;
  const X = t => LABEL + ((t - t0) / span) * TRACK;

  let axis = `<line x1="${LABEL}" y1="${TOP - 6}" x2="${W}" y2="${TOP - 6}" class="gridline"/>`;
  for (let i = 0; i <= 4; i++) {
    const tt = t0 + (span * i / 4), x = X(tt);
    axis += `<line x1="${x}" y1="${TOP - 6}" x2="${x}" y2="${H}" class="gridline"/>`;
    axis += `<text class="axis" x="${x + 2}" y="${TOP - 10}">${new Date(tt).toLocaleTimeString()}</text>`;
  }
  if (live) {
    axis += `<line x1="${X(now)}" y1="${TOP - 6}" x2="${X(now)}" y2="${H}" stroke="var(--accent)" stroke-width="1" stroke-dasharray="3 3"/>`;
  }

  const rows = lanes.map((l, i) => {
    const y = TOP + i * ROW;
    const label = l.label.length > 34 ? l.label.slice(0, 33) + '…' : l.label;
    let body = '';
    // Queued time: outlined rather than filled, so it never reads as work.
    if (l.wait && l.wait[0] != null) {
      const wx = X(l.wait[0]), ww = Math.max(1, X(l.wait[1] != null ? l.wait[1] : (live ? now : t1)) - wx);
      body += `<rect x="${wx.toFixed(1)}" y="${(y + 4).toFixed(1)}" width="${ww.toFixed(1)}" height="${ROW - 10}" rx="3"
        fill="var(--idle)" opacity=".16" stroke="var(--idle)" stroke-dasharray="2 2">
        <title>queued — ${dur((l.wait[1] != null ? l.wait[1] : now) - l.wait[0])}</title></rect>`;
    }
    if (l.start == null) {
      body += `<text class="axis" x="${LABEL}" y="${y + ROW / 2 + 3}">not started</text>`;
    } else {
      const e = l.end != null ? l.end : (live ? now : t1);
      const x = X(l.start), w = Math.max(3, X(e) - X(l.start));
      body += `<rect x="${x.toFixed(1)}" y="${(y + 4).toFixed(1)}" width="${w.toFixed(1)}" height="${ROW - 10}" rx="3" fill="${STCOL(l.state)}" opacity="0.9">
        <title>${esc(l.label)} — ${l.state}${l.end != null ? ' — ' + dur(l.end - l.start) : ' — running, ' + dur(now - l.start)}</title></rect>`;
      if (l.sub) body += `<text class="axis" x="${(x + w + 4).toFixed(1)}" y="${y + ROW / 2 + 3}">${l.sub}</text>`;
    }
    const cur = l.action ? ` style="cursor:pointer" ${dataAttrs(l.action)}` : '';
    return `<g${cur}>
      <text class="axis" x="0" y="${y + ROW / 2 + 3}" fill="var(--muted)">${esc(label)}</text>${body}</g>`;
  }).join('');
  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;min-width:${(W / 1.4).toFixed(0)}px" xmlns="http://www.w3.org/2000/svg">${axis}${rows}</svg>`;
}

// The path the run actually took. The run row only carries where it ended up,
// so without its events a retried run looks like it went straight to Running.
function stateRibbon(run, evs) {
  if (!evs || !evs.length) return '<div class="empty">No transitions recorded for this run.</div>';
  const seq = evs.slice().reverse(); // the API returns newest first
  const t0 = +new Date(seq[0].occurred);
  const t1 = Math.max(run.ended_at ? +new Date(run.ended_at) : Date.now(), t0 + 1000);
  const span = Math.max(1, t1 - t0);
  const W = 1000, H = 26;
  const segs = seq.map((e, i) => {
    const s = +new Date(e.occurred);
    const end = i + 1 < seq.length ? +new Date(seq[i + 1].occurred) : t1;
    const st = e.event.split('.').pop();
    const x = ((s - t0) / span) * W, w = Math.max(2, ((end - s) / span) * W);
    return `<rect x="${x.toFixed(1)}" y="4" width="${w.toFixed(1)}" height="${H - 8}" rx="3" fill="${STCOL(st)}" opacity=".9">
      <title>${esc(st)} — ${new Date(s).toLocaleString()} — held ${dur(end - s)}</title></rect>`;
  }).join('');
  const rows = evs.map(e => {
    const st = e.event.split('.').pop();
    const p = e.payload || {};
    const bits = [p.worker_id ? 'worker ' + String(p.worker_id).slice(0, 8) : '', p.attempt ? 'attempt ' + p.attempt : '']
      .filter(Boolean).join(' · ');
    return `<div class="tl-row">
      <div class="tl-t" title="${esc(new Date(e.occurred).toLocaleString())}">${when(e.occurred).replace(/<[^>]+>/g, '')}</div>
      <div class="tl-rail"><span class="tl-dot ${evClass(e.event)}"></span></div>
      <div class="tl-body">${state(st)} <span class="muted">${esc(bits)}</span>
        ${p.state_message ? `<div class="muted mono" style="margin-top:2px">${esc(p.state_message)}</div>` : ''}</div>
    </div>`;
  }).join('');
  return `<div class="ribbon">${svg(segs, W, H)}
      <div class="muted" style="font-size:10.5px;display:flex;justify-content:space-between">
        <span>${new Date(t0).toLocaleString()}</span><span>${new Date(t1).toLocaleString()}</span></div>
    </div><div class="tl">${rows}</div>`;
}

// Longest checkpoints first -- the answer to "where did the half hour go",
// which a Gantt on a shared axis cannot give when one task dwarfs the rest.
function taskBars(tasks) {
  const rows = tasks.filter(t => t.started_at && t.ended_at)
    .map(t => ({ k: t.task_key, state: t.state, ms: Math.max(0, new Date(t.ended_at) - new Date(t.started_at)) }))
    .sort((a, b) => b.ms - a.ms).slice(0, 18);
  if (!rows.length) return '';
  const max = Math.max(1, ...rows.map(r => r.ms));
  const W = 900, LABEL = 220, TRACK = W - LABEL - 74, ROW = 22, H = rows.length * ROW + 6;
  const body = rows.map((r, i) => {
    const y = i * ROW, w = Math.max(2, (r.ms / max) * TRACK);
    return `<g data-click="showRunTab" data-tab="checkpoints">
      <text class="axis" x="0" y="${y + ROW / 2 + 4}" fill="var(--muted)">${esc(r.k.length > 34 ? r.k.slice(0, 33) + '…' : r.k)}</text>
      <rect x="${LABEL}" y="${y + 4}" width="${w.toFixed(1)}" height="${ROW - 9}" rx="3" fill="${STCOL(r.state)}" opacity=".9">
        <title>${esc(r.k)} — ${dur(r.ms)}</title></rect>
      <text class="axis" x="${(LABEL + w + 6).toFixed(1)}" y="${y + ROW / 2 + 4}">${dur(r.ms)}</text>
    </g>`;
  }).join('');
  return `<div class="bars"><svg viewBox="0 0 ${W} ${H}" style="width:100%;min-width:${(W / 1.5).toFixed(0)}px" xmlns="http://www.w3.org/2000/svg">${body}</svg></div>`;
}

// --- checkpoints tab ------------------------------------------------------
function runCheckpointsTab({ tasks }) {
  if (!tasks.length) {
    return '<div class="panel"><div class="empty">No checkpoints recorded. A flow only writes one per <span class="mono">Task</span> it runs.</div></div>';
  }
  const rows = tasks.map(t => {
    const ms = t.started_at && t.ended_at ? new Date(t.ended_at) - new Date(t.started_at) : null;
    const res = t.result === undefined ? null : t.result;
    const flat = JSON.stringify(res);
    return `<tr>
      <td class="mono">${esc(t.task_key)}${t.task_name && t.task_name !== t.task_key ? `<div class="muted">${esc(t.task_name)}</div>` : ''}</td>
      <td>${state(t.state)}${t.state_message ? `<div class="muted mono" style="font-size:11px">${esc(t.state_message)}</div>` : ''}</td>
      <td>${t.run_count}${t.retries ? ' / ' + (t.retries + 1) : ''}</td>
      <td>${when(t.started_at)}</td>
      <td class="mono">${dur(ms)}</td>
      <td>${t.cache_key
        ? `<span class="chip on" title="${esc(t.cache_key)}">cached${t.cache_expires_at ? ' · ' + when(t.cache_expires_at).replace(/<[^>]+>/g, '') : ''}</span>`
        : '<span class="muted">—</span>'}</td>
      <td>${flat && flat !== 'null'
        ? `<details><summary class="mono muted">${esc(flat.length > 54 ? flat.slice(0, 53) + '…' : flat)}</summary>
             <pre class="json" style="padding:8px 0 0;max-height:240px">${esc(JSON.stringify(res, null, 2))}</pre></details>`
        : '<span class="muted">—</span>'}</td>
    </tr>`;
  }).join('');
  return `<div class="panel">
    <h2>Checkpoints<span class="grow"></span><span class="muted">a resume replays from these instead of re-running them</span></h2>
    <div class="scroll"><table>
      <thead><tr><th>Key</th><th>State</th><th>Attempts</th><th>Started</th><th>Duration</th><th>Cache</th><th>Result</th></tr></thead>
      <tbody>${rows}</tbody>
    </table></div></div>`;
}

// --- logs tab -------------------------------------------------------------
let logLevel = '', logQuery = '';
const visibleLogs = logs => logs.filter(l =>
  (!logLevel || l.level === logLevel)
  && (!logQuery || String(l.message || '').toLowerCase().includes(logQuery.toLowerCase())));

function runLogsTab({ logs }) {
  const levels = ['', 'ERROR', 'WARN', 'INFO', 'DEBUG'];
  const seg = `<span class="seg" id="run-loglevels">${levels.map(l =>
    `<button data-l="${l}"${l === logLevel ? ' class="active"' : ''} data-click="setLogLevel" data-level="${l}">${l || 'All'}</button>`).join('')}</span>`;
  return `<div class="panel">
    <h2>Logs<span class="grow"></span><span class="muted" id="run-logcount">${visibleLogs(logs).length} of ${logs.length}</span></h2>
    <div class="logbar">${seg}
      <input placeholder="filter message" value="${esc(logQuery)}" data-input="filterLogs" size="20">
      <span class="grow"></span><span class="muted">oldest first</span>
    </div>
    <pre class="logs" id="run-logs">${logLines(logs)}</pre></div>`;
}
function logLines(logs) {
  const v = visibleLogs(logs);
  if (!v.length) return '<span class="muted">No log line matches.</span>';
  return v.map(l => `<span class="lvl-${esc(l.level)}">${new Date(l.timestamp).toLocaleTimeString()} ` +
    `${esc(String(l.level || '').padEnd(5))} ${esc(l.message)}</span>`).join('\n');
}
// Filtering redraws the log pane alone: re-rendering the page would take the
// focus out of the box being typed into.
function filterLogs(v) {
  logQuery = v;
  if (!runData) return;
  const pre = document.getElementById('run-logs');
  if (pre) pre.innerHTML = logLines(runData.logs);
  const c = document.getElementById('run-logcount');
  if (c) c.textContent = visibleLogs(runData.logs).length + ' of ' + runData.logs.length;
}
function setLogLevel(l) {
  logLevel = l;
  document.querySelectorAll('#run-loglevels button').forEach(b => b.classList.toggle('active', b.dataset.l === l));
  filterLogs(logQuery);
}
// Keep the log pane where the operator left it across a live refresh -- and
// keep following the tail when that is where they were.
function logScrollState() {
  const el = document.getElementById('run-logs');
  return el ? { top: el.scrollTop, tail: el.scrollHeight - el.scrollTop - el.clientHeight < 24 } : null;
}
function restoreLogScroll(s) {
  const el = document.getElementById('run-logs');
  if (el) el.scrollTop = !s || s.tail ? el.scrollHeight : s.top;
}

// --- artifacts tab --------------------------------------------------------
function runArtifactsTab({ arts }) {
  if (!arts.length) {
    return '<div class="panel"><div class="empty">This run wrote no artifacts.</div></div>';
  }
  return `<div class="panel"><h2>Artifacts<span class="grow"></span><span class="muted">${arts.length}</span></h2>
    ${arts.map(artCard).join('')}</div>`;
}
function artCard(a) {
  return `<div class="art">
    <h3>${esc(a.key)} <span class="chip">${esc(a.kind)}</span><span class="grow"></span>
      <span class="muted" style="font-weight:400;font-size:11.5px">${when(a.created_at)}</span></h3>
    ${a.description && a.kind !== 'link' ? `<div class="muted">${esc(a.description)}</div>` : ''}
    ${artBody(a)}</div>`;
}
// Artifacts render the way the flow author meant them: a markdown summary as
// prose, a table as a table, a link as a link. Anything else, and anything
// whose payload is not the shape its kind promises, falls back to its JSON.
function artBody(a) {
  const d = a.data;
  if (a.kind === 'link' && typeof d === 'string') return `<div>${extLink(d, a.description || d)}</div>`;
  if (a.kind === 'markdown' && typeof d === 'string') return `<div class="md">${md(d)}</div>`;
  if (a.kind === 'table' && Array.isArray(d) && d.length) return artTable(d);
  return `<pre>${esc(JSON.stringify(d, null, 2))}</pre>`;
}
const extLink = (u, t) => /^https?:\/\//i.test(String(u || ''))
  ? `<a href="${esc(u)}" class="xlink" target="_blank" rel="noopener noreferrer">${esc(t)}</a>` : esc(t);
function artTable(rows) {
  const cols = [...new Set(rows.flatMap(r => r && typeof r === 'object' && !Array.isArray(r) ? Object.keys(r) : []))];
  if (!cols.length) return `<pre>${esc(JSON.stringify(rows, null, 2))}</pre>`;
  const cell = v => v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);
  return `<div class="scroll" style="margin-top:8px"><table>
    <thead><tr>${cols.map(c => `<th>${esc(c)}</th>`).join('')}</tr></thead>
    <tbody>${rows.map(r => `<tr>${cols.map(c => `<td>${esc(cell(r && r[c]))}</td>`).join('')}</tr>`).join('')}</tbody>
  </table></div>`;
}
// A deliberately small markdown subset: headings, bullets, bold, italics,
// code, http links. The source is escaped before a single tag is put back, so
// the only markup that can reach the page is markup written here.
function md(src) {
  const inline = s => s
    .replace(/`([^`]+)`/g, '<code>$1</code>')
    .replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|[^*])\*([^*\s][^*]*)\*/g, '$1<em>$2</em>')
    .replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, '<a href="$2" class="xlink" target="_blank" rel="noopener noreferrer">$1</a>');
  let out = '', list = false;
  for (const raw of esc(src).split('\n')) {
    const l = raw.trim();
    const bullet = /^[-*]\s+(.*)$/.exec(l);
    if (bullet) {
      if (!list) { out += '<ul>'; list = true; }
      out += `<li>${inline(bullet[1])}</li>`;
      continue;
    }
    if (list) { out += '</ul>'; list = false; }
    const h = /^(#{1,4})\s+(.*)$/.exec(l);
    if (h) { out += `<h${h[1].length + 2}>${inline(h[2])}</h${h[1].length + 2}>`; continue; }
    if (l) out += `<p>${inline(l)}</p>`;
  }
  return (list ? out + '</ul>' : out) || '<span class="muted">empty</span>';
}

// --- sub-flows tab --------------------------------------------------------
function runSubflowsTab({ kids }) {
  return `<div class="panel"><h2>Sub-flows<span class="grow"></span><span class="muted">${kids.length}</span></h2>
    <div class="scroll"><table>
      <thead><tr><th>Run</th><th>Flow</th><th>State</th><th>Pool</th><th>Started</th><th>Duration</th></tr></thead>
      <tbody>${kids.map(k => `<tr class="pick" data-click="rowOpen" data-id="${esc(k.id)}">
        <td>${runLink(k.id, esc(k.name), 'mono')}</td>
        <td>${esc(k.flow_name)}</td>
        <td>${state(k.state)}</td>
        <td>${esc(k.work_queue)}</td>
        <td>${when(k.started_at)}</td>
        <td class="mono">${dur(runMS(k))}</td>
      </tr>`).join('')}</tbody>
    </table></div></div>`;
}

// --- parameters tab -------------------------------------------------------
function runDataTab({ run }) {
  const block = (title, v, empty) => `<div class="panel"><h2>${title}</h2>${
    v === undefined || v === null
      ? `<div class="empty">${empty}</div>`
      : `<pre class="json">${esc(JSON.stringify(v, null, 2))}</pre>`}</div>`;
  return block('Parameters', run.parameters, 'This run was started with no parameters.')
    + block('Result', run.result, run.state === 'COMPLETED'
        ? 'The flow returned nothing.'
        : 'A run only records a result when it completes — write anything you need to keep as an artifact.')
    + (run.state_message
        ? `<div class="panel"><h2>State message</h2><pre class="json">${esc(run.state_message)}</pre></div>` : '');
}

registerViews({
  run: {
    refresh: loadRun,
    path: () => '/runs/' + encodeURIComponent(runID) + '/' + runTab,
  },
});

// A run's own page hangs off the list it was opened from, the same way a
// deployment's does: /runs/<id>, with the open tab as a third segment.
registerRoutes([['runs', (id, tab) => {
  if (!id) return null;
  runID = decodeURIComponent(id);
  runTab = RUN_TABS.includes(tab) ? tab : 'timeline';
  return 'run';
}]]);

export {
  runHref, runLink,
};

registerActions({
  loadRun,
  openRun: el => openRun(el.dataset.id),
  rowOpen: (el, ev) => rowOpen(ev, el.dataset.id),
  showRunTab: el => showRunTab(el.dataset.tab),
  setLogLevel: el => setLogLevel(el.dataset.level),
  filterLogs: el => filterLogs(el.value),
  cancelRun: el => act(`/runs/${el.dataset.id}/cancel`, { method: 'POST' }),
  retryRun: el => act(`/runs/${el.dataset.id}/retry`, { method: 'POST' }),
});
