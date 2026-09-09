// The flow catalogue, and the parameter form built from a flow's own schema.
// paramFields/readParams are shared with the deployment views, which build
// the same form from the same schema.
import { act, api, canWrite, toast } from '../api.js';
import { line } from '../charts.js';
import { esc, safeJSON, state, when } from '../fmt.js';
import { registerViews, show } from '../router.js';
import { registerActions } from '../actions.js';
import { runLink } from './run.js';

async function loadFlowsPage() {
  const fs = (await api('/flows')) || [];
  const rows = await Promise.all(fs.map(async f => {
    let last = '—', count = 0;
    try {
      const { runs, total } = await api('/runs?flow=' + encodeURIComponent(f.name) + '&limit=1');
      count = total; if (runs[0]) last = state(runs[0].state) + ' ' + when(runs[0].scheduled_at);
    } catch (e) {}
    return `<tr data-click="openFlow" data-name="${esc(f.name)}" style="cursor:pointer">
      <td><strong>${esc(f.name)}</strong></td>
      <td class="mono">${esc(f.version)}</td>
      <td class="muted">${esc(f.description || '')}</td>
      <td class="mono muted">${(f.tags || []).map(esc).join(', ')}</td>
      <td>${last}</td><td>${count}</td>
    </tr>`;
  }));
  document.getElementById('flows-page').innerHTML = rows.length ? rows.join('')
    : '<tr><td colspan="6" class="empty">No worker has registered a flow yet.</td></tr>';
}

// A flow's parameter form is rendered from the schema its Go struct published,
// in two places: the Flows dialog, which starts an ad-hoc run, and the
// Deployments dialog, which runs through a deployment. One renderer, so a field
// type handled in one is handled in the other.
//
// `values` pre-fills the inputs -- the Deployments dialog passes the
// deployment's stored parameters, so the form opens on the defaults an operator
// is about to edit rather than empty.
function paramField(fld, values) {
  const asText = x => typeof x === 'object' ? JSON.stringify(x) : String(x);
  const v = values ? values[fld.name] : undefined;
  const has = v !== undefined && v !== null;
  const ex = fld.example != null ? esc(asText(fld.example)) : '';
  if (fld.type === 'boolean') {
    // data-kind matters here: without it readParams() cannot tell a boolean
    // from a string and the flow receives "true" instead of true.
    const cur = has ? String(!!v) : '';
    return `<select name="${esc(fld.name)}" data-kind="boolean">`
      + `<option value=""${cur === '' ? ' selected' : ''}>—</option>`
      + `<option${cur === 'true' ? ' selected' : ''}>true</option>`
      + `<option${cur === 'false' ? ' selected' : ''}>false</option></select>`;
  }
  const t = fld.type === 'integer' || fld.type === 'number' ? 'number' : 'text';
  return `<input name="${esc(fld.name)}" type="${t}" ${fld.required ? 'required' : ''}`
    + ` placeholder="${ex}" value="${has ? esc(asText(v)) : ''}" data-kind="${esc(fld.type)}">`;
}

// The fields of a parameter form. A flow with no published schema -- an older
// worker, or one registered without sdk.ParamsSchema -- falls back to a raw
// JSON textarea rather than to nothing.
function paramFields(schema, values) {
  if (!schema.length) {
    const json = values && Object.keys(values).length ? JSON.stringify(values, null, 2) : '';
    return `<label class="full">Parameters (JSON)<textarea name="__json" placeholder="{}">${esc(json)}</textarea></label>`;
  }
  return schema.map(fld => `<label>${esc(fld.name)} `
    + `<span class="muted">${esc(fld.type)}${fld.required ? ' *' : ''}</span>`
    + `${paramField(fld, values)}</label>`).join('');
}

// Read a parameter form back. Returns null when the JSON textarea does not
// parse, having already said so. Only [data-kind] inputs are collected, so a
// form may carry other controls -- the run dialog's priority, pool and delay
// overrides -- without them landing in the flow's parameters.
function readParams(form) {
  const fd = new FormData(form);
  if (fd.has('__json')) {
    try { return JSON.parse(fd.get('__json') || '{}'); }
    catch { toast('Parameters must be valid JSON', true); return null; }
  }
  const params = {};
  for (const el of form.querySelectorAll('[name][data-kind]')) {
    const v = el.value.trim();
    if (v === '') continue;
    const kind = el.dataset.kind;
    params[el.name] = kind === 'integer' ? parseInt(v, 10)
      : kind === 'number' ? parseFloat(v)
      : kind === 'boolean' ? v === 'true'
      : kind === 'array' || kind === 'object' ? safeJSON(v, v)
      : v;
  }
  return params;
}

async function openFlow(name) {
  const dlg = document.getElementById('flow-dialog');
  document.getElementById('fd-body').innerHTML = '<div class="empty">Loading…</div>';
  document.getElementById('fd-title').textContent = name;
  dlg.showModal();
  const f = await api('/flows/' + encodeURIComponent(name));
  const schema = f.params_schema && f.params_schema.fields || [];
  document.getElementById('fd-body').innerHTML = `
    <dl class="kv">
      <dt>Flow</dt><dd>${esc(f.name)}</dd>
      <dt>Description</dt><dd>${esc(f.description || '—')}</dd>
      <dt>Versions</dt><dd class="mono">${f.versions.map(v => esc(v.version)).join(', ')}</dd>
      <dt>Total runs</dt><dd>${f.total_runs}</dd>
    </dl>
    <div class="panel" style="margin:0;border-radius:0;border-left:0;border-right:0">
      <h2>Quick run</h2>
      ${canWrite() ? `<form class="inline" data-submit="quickRun" data-name="${esc(f.name)}">
        ${paramFields(schema, null)}
        <div class="full row"><button class="act primary" type="submit">Run flow</button></div>
      </form>` : '<p class="muted" style="padding:14px">Your account is read-only.</p>'}
    </div>
    <div class="panel" style="margin:0;border-radius:0;border:0;border-top:1px solid var(--line)">
      <h2>Recent runs (${f.recent_runs.length})</h2>
      <div class="scroll"><table>
        <thead><tr><th>Run</th><th>State</th><th>Pool</th><th>Scheduled</th></tr></thead>
        <tbody>${f.recent_runs.map(r => `<tr>
          <td>${runLink(r.id, esc(r.name), 'mono')}</td>
          <td>${state(r.state)}</td><td>${esc(r.work_queue)}</td><td>${when(r.scheduled_at)}</td></tr>`).join('')
          || '<tr><td colspan="4" class="empty">No runs yet.</td></tr>'}</tbody>
      </table></div>
    </div>`;
}

async function quickRun(ev, flow) {
  ev.preventDefault();
  const params = readParams(ev.target);
  if (!params) return;
  try {
    const run = await api('/runs', { method: 'POST', body: JSON.stringify({ flow_name: flow, parameters: params }) });
    toast('Started ' + run.name);
    document.getElementById('flow-dialog').close();
    show('runs');
  } catch (e) { toast(e.message, true); }
}

registerViews({ flows: { refresh: loadFlowsPage } });

export {
  paramFields, readParams,
};

registerActions({
  loadFlowsPage,
  openFlow: el => openFlow(el.dataset.name),
  quickRun: (el, ev) => quickRun(ev, el.dataset.name),
});
