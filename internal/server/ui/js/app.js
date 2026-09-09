// PrimeFlow operator console -- entry point.
//
// The views live one to a file under views/, and each registers itself with the
// router and publishes its own handlers to the markup. Importing them here is
// what runs that registration, so the list below is the console's table of
// contents rather than a list of things this file calls.
//
// What is left here is boot: who is logged in, what that lets them see, and the
// two standing subscriptions -- the 15s tick and the event stream -- that keep
// whatever is on screen current.
import { registerActions } from './actions.js';
import { ME, TOKEN, api, canWrite, isAdmin, setME, toLogin, toast, onMutate } from './api.js';
import { esc } from './fmt.js';
import './menu.js';
import { refreshCurrent, showPath } from './router.js';
import { connect } from './stream.js';
import { loadFlows } from './views/workers.js';

import './views/dashboard.js';
import './views/runs.js';
import './views/pools.js';
import './views/flows.js';
import './views/deployments.js';
import './views/automations.js';
import './views/workers.js';
import './views/worker-wizard.js';
import './views/events.js';
import './views/run.js';
import './views/settings.js';

// act() writes, then asks whatever view is on screen to catch up. The stream
// asks the same thing when the server says something moved -- wired in boot,
// next to the tick that does it on a timer.
onMutate(refreshCurrent);

// --- boot ---------------------------------------------------------------
async function bootstrap() {
  try {
    const r = await fetch('/api/v1/auth/me', {
      credentials: 'same-origin',
      headers: TOKEN ? { Authorization: 'Bearer ' + TOKEN } : {},
    });
    if (!r.ok) return toLogin();
    setME(await r.json());
  } catch { return toLogin(); }

  document.body.classList.toggle('can-write', canWrite());
  document.body.classList.toggle('is-admin', isAdmin());
  document.getElementById('who').innerHTML =
    `<b>${esc(ME.email)}</b><span class="role">${esc(ME.machine ? 'machine' : ME.role)}</span>` +
    `<button class="act" data-click="logout">Log out</button>`;
  if (!isAdmin()) {
    const b = document.querySelector('#nav button[data-v="settings"]');
    if (b) b.remove();
  }

  showPath(true);
  loadFlows();
  connect(refreshCurrent);
  setInterval(refreshCurrent, 15000);
}
async function logout() {
  try { await api('/auth/logout', { method: 'POST' }); } catch (e) {}
  toLogin();
}

// The shell's own handlers. Everything else is registered by the view that owns
// it -- see actions.js for how the markup reaches any of them.
registerActions({
  logout,
  closeDialog: el => document.getElementById(el.dataset.dialog).close(),
  copy: el => navigator.clipboard.writeText(document.getElementById(el.dataset.target).textContent)
    .then(() => toast('Copied'), () => toast('Could not copy', true)),
});

bootstrap();
