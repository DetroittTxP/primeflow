// Turning API values into the strings a table cell shows: escaping, relative
// times, durations, and the small pills that carry a run's state and priority.
// Pure functions over their arguments -- no DOM, no fetch, nothing to mock.
const esc = s => String(s ?? '').replace(/[&<>"']/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

function when(ts) {
  if (!ts) return '—';
  const d = new Date(ts), diff = (Date.now() - d) / 1000;
  const rel = Math.abs(diff) < 60 ? Math.round(Math.abs(diff)) + 's'
    : Math.abs(diff) < 3600 ? Math.round(Math.abs(diff) / 60) + 'm'
    : Math.abs(diff) < 86400 ? Math.round(Math.abs(diff) / 3600) + 'h'
    : Math.round(Math.abs(diff) / 86400) + 'd';
  return `<span title="${esc(d.toLocaleString())}">${diff >= 0 ? rel + ' ago' : 'in ' + rel}</span>`;
}
// A span in units a human reads at a glance. Used by the deployment page's
// run table and its duration bars, where "1847s" is not an answer.
function dur(ms) {
  if (ms == null || !isFinite(ms) || ms < 0) return '—';
  const s = ms / 1000;
  if (s < 90) return s.toFixed(s < 10 ? 2 : 0) + 's';
  if (s < 5400) return Math.round(s / 60) + 'm';
  return (s / 3600).toFixed(1) + 'h';
}
// When a run actually happened. Not scheduled_at: a delayed or a cancelled
// run carries a scheduled_at that never arrived, and dating it by that puts
// finished work in the future.
const runAt = r => r.started_at || r.created_at;
// A run's wall-clock time, null until it has both ends.
const runMS = r => r && r.started_at && r.ended_at ? new Date(r.ended_at) - new Date(r.started_at) : null;
// Go durations cross the wire as nanoseconds.
const goDur = v => v ? dur(v / 1e6) : '—';
const state = s => `<span class="pill s-${esc(s)}">${esc(s)}</span>`;
const prio = r => `<span class="prio">${r.queue_position != null ? '<span class="pin" title="pinned to front">▲</span>' : ''}` +
  `<b>${r.priority}</b><span class="bar"><i style="width:${r.priority}%"></i></span></span>`;
const STCOL = s => ({
  COMPLETED: 'var(--ok)', RUNNING: 'var(--run)', PENDING: 'var(--run)',
  FAILED: 'var(--err)', CRASHED: 'var(--err)', CANCELLED: 'var(--warn)', CANCELLING: 'var(--warn)',
}[s] || 'var(--idle)');

const safeJSON = (s, fb) => { try { return JSON.parse(s); } catch { return fb; } };

// An action and its arguments as data attributes: { act: 'bump', id: 'r1' }
// becomes data-click="bump" data-id="r1". A nullish value is dropped rather
// than written out as "undefined", so a caller can pass a field that is only
// sometimes there. Keys stay single words -- the DOM lowercases them, and
// data-poolType would come back as poolType only by accident.
const dataAttrs = ({ act, ...args }, on = 'click') =>
  `data-${on}="${esc(act)}"` +
  Object.entries(args).map(([k, v]) => v == null ? '' : ` data-${k}="${esc(v)}"`).join('');

export { esc, when, dur, runAt, runMS, goDur, state, prio, STCOL, safeJSON, dataAttrs };
