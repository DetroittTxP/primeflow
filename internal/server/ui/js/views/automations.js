// Automations: the event-triggered rules table and the form that adds one.
import { act, api, toast } from '../api.js';
import { esc, when } from '../fmt.js';
import { registerViews } from '../router.js';
import { registerActions } from '../actions.js';

async function loadAutomations() {
  const as = (await api('/automations')) || [];
  document.getElementById('automations').innerHTML = as.length ? as.map(a => `
    <tr>
      <td><strong>${esc(a.name)}</strong> ${a.enabled ? '' : '<span class="pill s-PAUSED">off</span>'}
          <div class="muted">${esc(a.description || '')}</div></td>
      <td class="mono">${esc(a.match_event || 'any')}${a.match_flow ? ' · flow=' + esc(a.match_flow) : ''}${a.match_work_queue ? ' · pool=' + esc(a.match_work_queue) : ''}</td>
      <td>${a.threshold}${a.window ? ' / ' + Math.round(a.window / 1e9) + 's' : ''}</td>
      <td class="mono">${esc(a.action)}</td>
      <td>${when(a.last_fired_at)}</td>
      <td class="writer-only"><button class="act" data-click="deleteAutomation" data-id="${esc(a.id)}" data-name="${esc(a.name)}">Delete</button></td>
    </tr>`).join('') : '<tr><td colspan="6" class="empty">No automations yet.</td></tr>';
}

async function saveAutomation(ev) {
  ev.preventDefault();
  const fd = new FormData(ev.target), body = {};
  for (const [k, v] of fd.entries()) if (String(v).trim() !== '') body[k] = v;
  body.threshold = parseInt(body.threshold || '1', 10);
  if (body.action_config) {
    try { body.action_config = JSON.parse(body.action_config); }
    catch { return toast('Action config must be valid JSON', true); }
  }
  try {
    await api('/automations', { method: 'POST', body: JSON.stringify(body) });
    toast('Automation saved');
    ev.target.reset();
    loadAutomations();
  } catch (e) { toast(e.message, true); }
}

registerViews({ automations: { refresh: loadAutomations } });

registerActions({
  loadAutomations,
  saveAutomation: (el, ev) => saveAutomation(ev),
  deleteAutomation: el => {
    if (confirm('Delete ' + el.dataset.name + '?')) act('/automations/' + el.dataset.id, { method: 'DELETE' });
  },
});
