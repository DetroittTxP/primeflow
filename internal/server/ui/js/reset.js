// The reset page stands on its own: it runs before there is a session, so it
// shares nothing with the console's modules and talks to the two auth
// endpoints directly.
const token = new URLSearchParams(location.search).get('token') || '';
const msg = document.getElementById('msg');
const go = document.getElementById('go');
if (!token) { msg.className = 'msg err'; msg.textContent = 'This link is missing its token.'; go.disabled = true; }
document.getElementById('f').addEventListener('submit', async (e) => {
  e.preventDefault();
  msg.textContent = '';
  if (document.getElementById('pw').value !== document.getElementById('pw2').value) {
    msg.className = 'msg err'; msg.textContent = 'The passwords do not match.'; return;
  }
  go.disabled = true;
  try {
    const r = await fetch('/api/v1/auth/reset', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'same-origin',
      body: JSON.stringify({ token, password: document.getElementById('pw').value }),
    });
    if (!r.ok) { const b = await r.json().catch(() => ({})); throw new Error(b.error || r.statusText); }
    msg.className = 'msg ok'; msg.textContent = 'Password set. Redirecting to sign in…';
    setTimeout(() => { location.href = '/login.html'; }, 1200);
  } catch (ex) { msg.className = 'msg err'; msg.textContent = ex.message; go.disabled = false; }
});
