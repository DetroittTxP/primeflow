// The login page stands on its own: it runs before there is a session, so it
// shares nothing with the console's modules and talks to the two auth
// endpoints directly.
const f = document.getElementById('f');
const err = document.getElementById('err');
const go = document.getElementById('go');
const next = new URLSearchParams(location.search).get('next') || '/';

if (new URLSearchParams(location.search).get('sso_error')) {
  err.textContent = 'Single sign-on failed (' + new URLSearchParams(location.search).get('sso_error') + '). Try again or use a password.';
}
fetch('/api/v1/auth/config', { credentials: 'same-origin' }).then(r => r.json()).then(cfg => {
  if (cfg && cfg.oidc) {
    document.getElementById('sso').textContent = 'Sign in with ' + (cfg.oidc_label || 'SSO');
    document.getElementById('sso').addEventListener('click', () => { location.href = '/api/v1/auth/oidc/login'; });
    document.getElementById('sso-wrap').hidden = false;
  }
}).catch(() => {});
f.addEventListener('submit', async (e) => {
  e.preventDefault();
  err.textContent = '';
  go.disabled = true;
  try {
    const r = await fetch('/api/v1/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify({
        email: document.getElementById('email').value.trim(),
        password: document.getElementById('password').value,
      }),
    });
    const body = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(body.error || r.statusText);
    location.href = next.startsWith('/') ? next : '/';
  } catch (ex) {
    err.textContent = ex.message;
    go.disabled = false;
  }
});
