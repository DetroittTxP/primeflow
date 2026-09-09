import { TOKEN } from './api.js';

// The caller passes what to do when the server says something changed, rather
// than this module reaching for the router: the stream's job is to debounce a
// burst of events into one nudge, not to know what a nudge refreshes.
// --- live stream --------------------------------------------------------
let pending = null;
function connect(onEvent) {
  const es = new EventSource('/api/v1/stream' + (TOKEN ? '?token=' + encodeURIComponent(TOKEN) : ''));
  es.onopen = () => {
    document.getElementById('livedot').classList.add('on');
    document.getElementById('livetext').textContent = 'live';
  };
  es.onerror = () => {
    document.getElementById('livedot').classList.remove('on');
    document.getElementById('livetext').textContent = 'reconnecting…';
  };
  es.onmessage = () => {
    if (pending) return;
    pending = setTimeout(() => { pending = null; onEvent(); }, 600);
  };
}

export { connect };
