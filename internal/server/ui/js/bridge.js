// The console drives its handlers from markup attributes -- onclick, onsubmit,
// onchange, oninput -- and the browser compiles an attribute handler in the
// global scope. Module scope is not global, so a name a handler reaches for is
// invisible to it unless something puts that name on window on purpose.
//
// publish() is that door, and it is meant to be the only one: nothing else in
// the console writes to window. Holding it in a file of its own keeps the
// coupling between markup and code somewhere you can open and read, instead of
// a habit scattered across the code.
//
// The list of names lives at the call site, so it sits next to the functions it
// names. It shrinks as handlers move to delegation -- data-action plus one
// listener -- and once it reaches zero this file goes away and the console can
// be served under a Content-Security-Policy with no unsafe-inline.
export function publish(names) {
  for (const k of Object.keys(names)) {
    // A name that collides with something the platform already put on window
    // would either shadow it or, for the read-only ones, be dropped in silence.
    // Neither is a thing to find out about from a button that does nothing.
    if (k in window) console.warn(`bridge: "${k}" already exists on window; pick another name`);
  }
  Object.assign(window, names);
}
