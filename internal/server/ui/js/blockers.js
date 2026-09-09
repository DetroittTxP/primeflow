import { api } from './api.js';
import { esc } from './fmt.js';

// --- "why is this waiting?" -----------------------------------------------

// poolBlocker names the reason a run handed to this pool would sit in
// SCHEDULED instead of starting -- the question "Run now did nothing" always
// turns out to be. Ordered the way dispatch hits them: a paused pool is never
// read, a pull pool nobody polls has no one to hand work to, and a saturated
// pool holds the rest back until a slot frees. null means nothing is in the way.
function poolBlocker(q, online) {
  if (!q) return null;
  if (q.paused) return { label: 'pool paused', cls: 's-PAUSED',
    why: 'Dispatch is stopped for this pool. Resume it under Pools.' };
  if (q.pool_type === 'push') {
    return q.push_endpoint ? null : { label: 'no endpoint', cls: 's-FAILED',
      why: 'Push pool with no endpoint configured, so nothing receives its runs.' };
  }
  if (!online) return { label: 'no worker', cls: 's-FAILED',
    why: 'No online worker is polling this pool. Runs stay Scheduled until one connects.' };
  if (q.concurrency_limit != null && q.running >= q.concurrency_limit)
    return { label: 'at limit', cls: 's-SCHEDULED',
      why: `The pool is running its limit of ${q.concurrency_limit}. Queued runs start as slots free.` };
  return null;
}

// A specific run is only held up by its pool once its scheduled time has
// passed; before that it is waiting on the clock, which is not a fault to flag.
// Anything past SCHEDULED has already been handed to a worker.
const runBlocker = (r, ctx) =>
  r.state === 'SCHEDULED' && new Date(r.scheduled_at) <= Date.now()
    ? poolBlocker(ctx.pool[r.work_queue], ctx.online(r.work_queue)) : null;

const poolPill = b => b ? ` <span class="pill ${b.cls}" title="${esc(b.why)}">${esc(b.label)}</span>` : '';

// poolContext is the pool counters and the worker roster, the two things that
// turn "Scheduled" into a reason. Neither is worth failing a table over, so a
// failure degrades to "no badge" rather than an empty page.
async function poolContext() {
  const [queues, ws] = await Promise.all([
    api('/queues').then(x => x || []).catch(() => []),
    api('/workers').then(x => x || []).catch(() => []),
  ]);
  return {
    queues,
    pool: Object.fromEntries(queues.map(q => [q.name, q])),
    online: name => ws.filter(w => w.online && (w.queues || []).includes(name)).length,
  };
}

export { poolBlocker, runBlocker, poolPill, poolContext };
