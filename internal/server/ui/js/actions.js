// How the markup asks for something to happen.
//
// A control names an action in a data attribute -- data-click, data-submit,
// data-change, data-input -- and one listener per event type, on the document,
// looks that name up and calls it. Nothing is bound per element and nothing has
// to be rebound when a table is redrawn, which matters here: a view rewrites
// its whole table on every tick and on every event the server sends.
//
// This replaced onclick="..." and its siblings. An attribute handler is
// compiled in the global scope, so every function one named had to be hung on
// window for it to be reachable. Now no module writes to window at all, and the
// console can be served under a Content-Security-Policy with no unsafe-inline.
//
// A handler is called with (element, event). Its arguments ride along in the
// element's other data attributes, which is why the registrations below read
// el.dataset instead of taking parameters -- an action is named by a string, so
// a string is all the markup can pass.
const ACTIONS = {};

export function registerActions(map) {
  for (const name of Object.keys(map)) {
    // Two views claiming one name would leave the loser silently dead, and the
    // symptom -- a button that does the wrong thing -- is a long way from the
    // cause.
    if (name in ACTIONS) console.warn(`actions: "${name}" is registered twice`);
  }
  Object.assign(ACTIONS, map);
}

for (const type of ['click', 'submit', 'change', 'input']) {
  document.addEventListener(type, ev => {
    const el = ev.target instanceof Element && ev.target.closest(`[data-${type}]`);
    if (!el) return;
    // None of these forms posts itself: every one of them goes through the API
    // and re-renders. A form marked `data-submit` with no name is one that has
    // nothing to do on submit but must not navigate either.
    if (type === 'submit') ev.preventDefault();
    // A link keeps a real href so it can be opened in a new tab or copied. A
    // plain click on it is this handler's; a modified one is the browser's, so
    // cmd/ctrl-click still opens the run in a new tab and shift-click still
    // opens a window. (Middle-click never arrives here at all -- that is an
    // auxclick.)
    const plain = !ev.metaKey && !ev.ctrlKey && !ev.shiftKey && !ev.altKey;
    if (type === 'click' && el.tagName === 'A') {
      if (!plain) return;
      ev.preventDefault();
    }
    const name = el.dataset[type];
    if (!name) return;
    const fn = ACTIONS[name];
    if (!fn) return console.warn(`actions: nothing registered as "${name}"`);
    fn(el, ev);
  });
}
