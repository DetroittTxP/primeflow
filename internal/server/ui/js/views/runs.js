// The flow-run table, with the state filter above it and the strip of
// counters that doubles as that filter.
import { act, api } from '../api.js';
import { poolContext, poolPill, runBlocker } from '../blockers.js';
import { publish } from '../bridge.js';
import { line, svg } from '../charts.js';
import { STCOL, esc, prio, state, when } from '../fmt.js';
import { registerViews } from '../router.js';
import { runLink } from './run.js';

let runsFilterState = '';
document.querySelectorAll('#f-state button').forEach(b => b.onclick = () => {
  runsFilterState = b.dataset.s;
  document.querySelectorAll('#f-state button').forEach(x => x.classList.toggle('active', x === b));
  loadRuns();
});

async function loadRuns() {
  const q = document.getElementById('f-search').value.trim();
  const queue = document.getElementById('f-queue').value;
  const params = new URLSearchParams({ limit: '100' });
  if (runsFilterState) params.set('state', runsFilterState);
  if (q) params.set('search', q);
  if (queue) params.set('queue', queue);
  const [{ runs }, ctx] = await Promise.all([api('/runs?' + params), poolContext()]);

  // pool filter options (once)
  const qsel = document.getElementById('f-queue');
  if (qsel.options.length <= 1) ctx.queues.forEach(x => qsel.add(new Option(x.name, x.name)));

  document.getElementById('runs-strip').innerHTML = runsStrip(runs);
  document.getElementById('runs').innerHTML = runs.length ? runs.map(r => `
    <tr class="pick" onclick="rowOpen(event,'${r.id}')">
      <td>${runLink(r.id, esc(r.name), 'mono')}</td>
      <td>${esc(r.flow_name)}</td>
      <td>${state(r.state)}</td>
      <td>${esc(r.work_queue)}${poolPill(runBlocker(r, ctx))}</td>
      <td>${prio(r)}</td>
      <td>${when(r.scheduled_at)}</td>
      <td>${r.run_count}${r.retries ? ' / ' + (r.retries + 1) : ''}</td>
      <td class="row">
        ${['SCHEDULED', 'RUNNING', 'PENDING'].includes(r.state)
          ? `<button class="act writer-only" onclick="act('/runs/${r.id}/cancel',{method:'POST'})">Cancel</button>` : ''}
        ${['FAILED', 'CRASHED', 'CANCELLED', 'COMPLETED'].includes(r.state)
          ? `<button class="act writer-only" onclick="act('/runs/${r.id}/retry',{method:'POST'})">Resume</button>` : ''}
      </td>
    </tr>`).join('') : '<tr><td colspan="8" class="empty">No runs match.</td></tr>';
}

// runsStrip: a 60px band placing each run by scheduled_at, coloured by state.
function runsStrip(runs) {
  if (!runs.length) return '';
  const ts = runs.map(r => +new Date(r.scheduled_at));
  const t0 = Math.min(...ts), t1 = Math.max(...ts, t0 + 1000), span = Math.max(1, t1 - t0);
  const W = 1000, H = 46;
  const ticks = runs.map(r => {
    const x = ((+new Date(r.scheduled_at) - t0) / span) * (W - 8) + 4;
    return `<rect x="${x.toFixed(1)}" y="8" width="3" height="30" rx="1.5" fill="${STCOL(r.state)}"
      style="cursor:pointer" onclick="openRun('${r.id}')"><title>${esc(r.name)} — ${r.state} — ${new Date(r.scheduled_at).toLocaleString()}</title></rect>`;
  }).join('');
  return svg(`<line x1="0" y1="38" x2="${W}" y2="38" class="gridline"/>` + ticks, W, H) +
    `<div class="muted" style="font-size:10.5px;display:flex;justify-content:space-between">
       <span>${new Date(t0).toLocaleString()}</span><span>${new Date(t1).toLocaleString()}</span></div>`;
}

registerViews({ runs: { refresh: loadRuns } });

publish({
  loadRuns,
});
