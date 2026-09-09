// Where the console is, and how the address bar says so.
//
// Every view owns a path, so the address bar names what is on screen: a view
// can be linked to, reloaded, and walked back through with the back button.
// The query string rides along untouched — it carries the API token.
//
// This file knows the names of the views and nothing about what any of them
// does. A view says what it can do -- refresh itself, take over on arrival,
// spell itself as a path -- and app.js does the saying. That is what lets a
// view import the router without the router importing the view back.

import { registerActions } from './actions.js';

// name -> { refresh, enter?, path? }
//   refresh  what a background tick calls, and what arriving falls back to
//   enter    arriving is not refreshing for every view: Settings loads a whole
//            pane, and the Add-worker wizard is built rather than reloaded
//   path     a view carrying state in the URL spells that path itself
const VIEW = {};
// [first path segment, (...rest) => view name | null]. A list and the detail
// page under it share a first segment -- /deployments is the table, and
// /deployments/<id> is one deployment -- so a claim reads the rest, keeps what
// belongs to its view, and answers with the view that makes it. Returning null
// declines, and the plain view name wins instead.
const ROUTES = [];

export function registerViews(views) { Object.assign(VIEW, views); }
export function registerRoutes(routes) { ROUTES.push(...routes); }

const VIEWS = [
  ['dashboard', 'Dashboard', '▦'], ['runs', 'Runs', '≣'], ['flows', 'Flows', 'ƒ'],
  ['queues', 'Work Pools', '⛁'], ['deployments', 'Deployments', '⇪'],
  ['automations', 'Automations', '⚙'], ['workers', 'Workers', '◈'],
  ['events', 'Events', '⌁'], ['settings', 'Settings', '⚑'],
];
const VIEW_PATHS = VIEWS.map(([v]) => v).concat('newworker');

const pathOf = v =>
  VIEW[v] && VIEW[v].path ? VIEW[v].path() : v === 'dashboard' ? '/' : '/' + v;

export function routeFromPath() {
  const [head, ...rest] = location.pathname.replace(/^\/+|\/+$/g, '').split('/');
  for (const [segment, claim] of ROUTES) {
    if (segment !== head) continue;
    const v = claim(...rest);
    if (v) return v;
  }
  return VIEW_PATHS.includes(head) ? head : 'dashboard';
}

// Open whatever the address bar asks for. A claim lands its view's own state --
// which tab, which run -- before show() runs, because show() reads that back
// out when it writes the path; otherwise a link to one tab would be rewritten
// to whichever tab happened to be open last.
export function showPath(replace) { show(routeFromPath(), replace); }

export function syncURL(v, replace) {
  const url = pathOf(v) + location.search;
  if (!replace && url === location.pathname + location.search) return;
  history[replace ? 'replaceState' : 'pushState'](null, '', url);
}
addEventListener('popstate', () => showPath(true));

let current = 'dashboard';
export const currentView = () => current;

// replace: rewrite the current history entry instead of adding one — for the
// initial view and for a redirect the operator never chose.
export function show(v, replace) {
  current = v;
  syncURL(v, replace);
  document.querySelectorAll('section').forEach(s => s.classList.toggle('active', s.id === 'v-' + v));
  // A detail page keeps the list it belongs to lit in the sidebar.
  const navFor = v === 'deployment' ? 'deployments' : v === 'run' ? 'runs' : v;
  document.querySelectorAll('#nav button').forEach(b => b.classList.toggle('active', b.dataset.v === navFor));
  document.getElementById('win-switch').hidden = v !== 'dashboard';
  const view = VIEW[v];
  if (view.enter) { view.enter(); return; }
  refreshCurrent(true);
}
// force: a deliberate navigation, which must load whatever the pointer is
// doing -- opening a page from a row's own "⋯" menu arrives here with that
// menu still open, and would otherwise land on an empty view.
export function refreshCurrent(force) {
  // A background refresh rebuilds the table's rows, which would yank an open
  // "⋯" menu out from under the pointer -- and live events can land at any
  // moment. Let the menu win: closing it is one click, and the next tick
  // catches the view up. Actions taken from the menu close it before their
  // refresh lands.
  if (!force && document.querySelector('.menu:popover-open')) return;
  VIEW[current].refresh();
}

document.getElementById('nav').innerHTML = VIEWS.map(([v, label, ic]) =>
  `<button data-v="${v}" data-click="show" data-view="${v}"><span class="ic">${ic}</span>${label}</button>`).join('');

// The sidebar, and every "← back to the list" button, name the view they open.
registerActions({ show: el => show(el.dataset.view) });
