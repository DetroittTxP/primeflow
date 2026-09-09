import { esc } from './fmt.js';

// Every table's row actions hang off a "⋯" button instead of sitting inline:
// a row of four or five buttons wrapped onto two lines and buried the row's
// own data, and the actions moved around as the buttons on offer changed.
//
// Items are [label, javascript] pairs, falsy entries dropped so a caller can
// write `cond && [...]`; a third element marks a destructive item. A row with
// nothing left to offer gets no button at all, only the empty cell that keeps
// the column count. The label is HTML, the javascript is an onclick body --
// esc() lets it carry the quotes that JSON.stringify puts in.
function menuCell(items, cls) {
  const on = items.filter(Boolean);
  if (!on.length) return `<td class="${cls || ''}"></td>`;
  return `<td class="${cls || ''}" style="text-align:right">
    <button class="act kebab" title="Actions" aria-haspopup="menu" onclick="rowMenu(this)">⋯</button>
    <div class="menu" role="menu" popover onclick="this.hidePopover()">
      ${on.map(([label, js, danger]) =>
        `<button role="menuitem"${danger ? ' class="danger"' : ''} onclick="${esc(js)}">${label}</button>`).join('')}
    </div>
  </td>`;
}

// The menu is a popover, so it lives in the top layer -- an absolutely
// positioned one would be clipped by the .scroll box around the table -- and
// the browser handles Esc and click-outside for us. It only has to be placed,
// which means closing it when the page scrolls out from under it.
function rowMenu(btn) {
  const m = btn.nextElementSibling, r = btn.getBoundingClientRect();
  m.style.top = '0px';
  m.style.left = '0px';
  m.showPopover();
  // Measured only once it is shown: hang the menu under the button, or above
  // it when the last rows of a long table would push it off screen.
  const below = r.bottom + 4;
  m.style.top = (below + m.offsetHeight > innerHeight - 8 && r.top - 4 - m.offsetHeight > 8
    ? r.top - 4 - m.offsetHeight : below) + 'px';
  m.style.left = Math.max(8, r.right - m.offsetWidth) + 'px';
  // Arm the listeners a frame late: scroll events are dispatched before the
  // next frame's callbacks, so a scroll already in flight when the menu opened
  // -- trackpad momentum, a scrollIntoView -- would otherwise shut it again at
  // once. Capture, because the scroll is the table's .scroll box, not the page.
  const close = () => { if (m.matches(':popover-open')) m.hidePopover(); };
  requestAnimationFrame(() => {
    addEventListener('scroll', close, { once: true, capture: true });
    addEventListener('resize', close, { once: true });
  });
}

export { menuCell, rowMenu };
