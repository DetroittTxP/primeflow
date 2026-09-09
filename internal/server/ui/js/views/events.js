// The event feed.
import { api } from '../api.js';
import { esc, when } from '../fmt.js';
import { registerViews } from '../router.js';
import { registerActions } from '../actions.js';

function evClass(name) {
  if (/\.(FAILED|CRASHED)$/.test(name)) return 'ev-err';
  if (name.startsWith('automation') || name.startsWith('external-api') || name.startsWith('work-queue')) return 'ev-auto';
  if (name.startsWith('flow-run') || name.startsWith('webhook')) return 'ev-run';
  return '';
}
async function loadEvents() {
  const evs = (await api('/events?limit=150')) || [];
  document.getElementById('events').innerHTML = evs.length ? evs.map(e => {
    const payload = JSON.stringify(e.payload ?? {}, null, 1);
    const hasPayload = payload && payload !== '{}' && payload !== 'null';
    return `<div class="tl-row">
      <div class="tl-t" title="${esc(new Date(e.occurred).toLocaleString())}">${when(e.occurred).replace(/<[^>]+>/g, '')}</div>
      <div class="tl-rail"><span class="tl-dot ${evClass(e.event)}"></span></div>
      <div class="tl-body">
        <details ${hasPayload ? '' : 'open'}>
          <summary><span class="mono">${esc(e.event)}</span>
            <span class="muted mono"> · ${esc(e.resource_type)}/${esc(String(e.resource_id).slice(0, 8))}</span></summary>
          ${hasPayload ? `<pre>${esc(payload)}</pre>` : ''}
        </details>
      </div>
    </div>`;
  }).join('') : '<div class="empty">No events yet.</div>';
}

registerViews({ events: { refresh: loadEvents } });

export {
  evClass,
};

registerActions({ loadEvents });
