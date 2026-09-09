// --- tiny SVG chart kit (no library) --------------------------------------
function svg(inner, w, h) {
  return `<svg viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" ` +
    `style="width:100%;height:${h}px;display:block;overflow:visible">${inner}</svg>`;
}
// stacked bars from [{t, <key>:n, ...}] and series [{key,color}]
function bars(buckets, series, h = 120) {
  if (!buckets.length) return '<div class="empty">No activity in this window.</div>';
  const w = Math.max(buckets.length * 8, 300);
  const max = Math.max(1, ...buckets.map(b => series.reduce((a, s) => a + (b[s.key] || 0), 0)));
  const bw = w / buckets.length;
  let g = '';
  for (let i = 1; i < 4; i++) { const y = h - (h * i / 4); g += `<line class="gridline" x1="0" y1="${y}" x2="${w}" y2="${y}"/>`; }
  const rects = buckets.map((b, i) => {
    let y = h, x = i * bw + bw * 0.15, bar = '';
    for (const s of series) {
      const v = b[s.key] || 0; if (!v) continue;
      const bh = (v / max) * (h - 2);
      y -= bh;
      bar += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${(bw * 0.7).toFixed(1)}" height="${bh.toFixed(1)}" fill="${s.color}"><title>${new Date(b.t).toLocaleString()} — ${s.key}: ${v}</title></rect>`;
    }
    return bar;
  }).join('');
  return svg(g + rects, w, h) ;
}
// line + soft area from a number[]
function line(values, color = 'var(--accent)', h = 60) {
  if (values.length < 2) return '';
  const w = Math.max(values.length * 6, 240);
  const max = Math.max(1, ...values), stepx = w / (values.length - 1);
  const pts = values.map((v, i) => `${(i * stepx).toFixed(1)},${(h - (v / max) * (h - 4) - 2).toFixed(1)}`);
  return svg(
    `<polygon points="0,${h} ${pts.join(' ')} ${w},${h}" fill="${color}" opacity="0.12"/>` +
    `<polyline points="${pts.join(' ')}" fill="none" stroke="${color}" stroke-width="1.5"/>`,
    w, h);
}
const spark = (values, color) => line(values, color || 'var(--accent)', 34);

export { svg, bars, line, spark };
