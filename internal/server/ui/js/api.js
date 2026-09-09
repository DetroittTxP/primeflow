// Transport and the session: every call the console makes to /api/v1, and the
// four things that decide what the operator is allowed to do with the answer.
//
// This module knows nothing about views or navigation, and it should stay that
// way -- act() refreshes whatever is on screen after a write, but it reaches
// the router through onMutate() rather than importing it, so the dependency
// runs one way. app.js wires the two together at boot.
const TOKEN = new URLSearchParams(location.search).get('token') || '';

let ME = null;

const cookie = name =>
  (document.cookie.split('; ').find(c => c.startsWith(name + '=')) || '').split('=')[1] || '';

function toLogin() { location.href = '/login.html?next=' + encodeURIComponent(location.pathname); }
const canWrite = () => !!ME && (ME.machine || ME.role === 'operator' || ME.role === 'admin');
const isAdmin = () => !!ME && (ME.machine || ME.role === 'admin');

function api(path, opts = {}) {
  const headers = Object.assign({ 'Content-Type': 'application/json' }, opts.headers || {});
  if (TOKEN) headers['Authorization'] = 'Bearer ' + TOKEN;
  const method = (opts.method || 'GET').toUpperCase();
  if (method !== 'GET' && method !== 'HEAD') {
    const csrf = cookie('pf_csrf');
    if (csrf) headers['X-CSRF-Token'] = csrf;
  }
  return fetch('/api/v1' + path, Object.assign({ credentials: 'same-origin' }, opts, { headers })).then(async r => {
    if (r.status === 401) { toLogin(); throw new Error('not authenticated'); }
    if (r.status === 204) return null;
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(body.error || r.statusText);
    return body;
  });
}

function toast(msg, isErr) {
  const el = document.createElement('div');
  el.className = 'toast' + (isErr ? ' err' : '');
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 4200);
}
const act = (p, o) => {
  if (!canWrite()) { toast('Your account is read-only', true); return Promise.resolve(); }
  return api(p, o).then(r => { afterMutate(); return r; }).catch(e => toast(e.message, true));
};

// setME exists because an imported binding is read-only for the importer: the
// login handshake in app.js cannot assign ME itself, and canWrite/isAdmin here
// have to see the new value. The export is live, so they do.
function setME(me) { ME = me; }

let afterMutate = () => {};
function onMutate(fn) { afterMutate = fn; }

export { TOKEN, ME, setME, cookie, toLogin, canWrite, isAdmin, api, toast, act, onMutate };
