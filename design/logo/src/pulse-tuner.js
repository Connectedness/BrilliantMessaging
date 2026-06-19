// Concept 3 "Pulse" — geometry + live tuner.
// The three spiral arms and node positions are fixed. The hub comes in three
// variants (diamond / reuleaux / hexagon), sized by a single "hub size" value
// (centre-to-outer-point radius). Sliders drive line thickness, node size and
// hub size; the static variant sections are rendered from the same builder.

const ARMS = [
  'M32 32 C38 22 40 17 32 10',
  'M32 32 C37.66 42.2 40.99 46.43 51.05 43',
  'M32 32 C20.34 31.8 15.01 32.57 12.95 43',
];
const NODES = [[32, 10], [51.05, 43], [12.95, 43]];

const f = (n) => Math.round(n * 100) / 100;
const vtx = (v, deg) => {
  const a = (deg * Math.PI) / 180;
  return [32 + v * Math.cos(a), 32 + v * Math.sin(a)];
};

function hubMarkup(variant, v, attrs) {
  if (variant === 'reuleaux') {
    // curved-sided triangle; arcs have radius = side length, centred on the
    // opposite vertex. Vertices aim down the three arms (-90 / 30 / 150).
    const V = [vtx(v, -90), vtx(v, 30), vtx(v, 150)];
    const s = f(v * Math.sqrt(3));
    const a = (p) => `A${s} ${s} 0 0 1 ${f(p[0])} ${f(p[1])}`;
    return `<path d="M${f(V[0][0])} ${f(V[0][1])} ${a(V[1])} ${a(V[2])} ${a(V[0])} Z" ${attrs}/>`;
  }
  if (variant === 'hexagon') {
    // pointy-top hexagon; the three arms exit at alternating vertices.
    const V = [-90, -30, 30, 90, 150, 210].map((d) => vtx(v, d));
    return `<path d="M${V.map((p) => `${f(p[0])} ${f(p[1])}`).join(' L')} Z" ${attrs}/>`;
  }
  // diamond (default) — a square on its corner, slightly tilted.
  const side = f(v * Math.SQRT2);
  const xy = f(32 - (v * Math.SQRT2) / 2);
  return `<rect x="${xy}" y="${xy}" width="${side}" height="${side}" rx="1.5" transform="rotate(50 32 32)" ${attrs}/>`;
}

// Builds the mark's inner markup. By default the elements carry CSS classes
// (route/hub/node) so they theme off --graphite / --teal / --paper for the
// on-page previews. Pass `colors: { struct, hub }` to additionally bake in
// literal stroke/fill values — needed when the SVG is rasterised off-document
// (the download export), where the page stylesheet doesn't apply.
export function buildMark({ variant, strokeW, nodeR, hub, colors }) {
  const routeAttrs = colors ? `class="route" stroke="${colors.struct}"` : 'class="route"';
  const hubAttrs = colors ? `class="hub" fill="${colors.hub}"` : 'class="hub"';
  const nodeAttrs = colors ? `class="node" fill="${colors.struct}"` : 'class="node"';
  let out = '';
  for (const d of ARMS) {
    out += `<path d="${d}" ${routeAttrs} stroke-width="${strokeW}" stroke-linecap="round"/>`;
  }
  out += hubMarkup(variant, hub, hubAttrs);
  for (const [cx, cy] of NODES) {
    out += `<circle cx="${cx}" cy="${cy}" r="${nodeR}" ${nodeAttrs}/>`;
  }
  return out;
}

export const DEFAULTS = { strokeW: 5, nodeR: 5.5, hub: 9 };

// --- render the static variant marks (cards + icon previews) ---
for (const svg of document.querySelectorAll('.pulse-static')) {
  svg.innerHTML = buildMark({ variant: svg.dataset.variant || 'diamond', ...DEFAULTS });
}

// --- the live tuner ---
if (document.querySelector('#p-thickness')) {
  const state = { variant: 'diamond', strokeW: 5, nodeR: 5.5, hub: 9 };
  const previews = [...document.querySelectorAll('.ptune-svg')];

  const render = () => {
    const inner = buildMark(state);
    for (const svg of previews) {
      svg.setAttribute('viewBox', '0 0 64 64');
      svg.innerHTML = inner;
    }
    document.querySelector('#p-thickness-val').textContent = state.strokeW.toFixed(1);
    document.querySelector('#p-node-val').textContent = state.nodeR.toFixed(1);
    document.querySelector('#p-hub-val').textContent = state.hub.toFixed(1);
  };

  const bind = (selector, key) => {
    const el = document.querySelector(selector);
    el.value = state[key];
    el.addEventListener('input', (e) => {
      state[key] = parseFloat(e.target.value);
      render();
    });
  };
  bind('#p-thickness', 'strokeW');
  bind('#p-node', 'nodeR');
  bind('#p-hub', 'hub');

  for (const btn of document.querySelectorAll('.variant-btn')) {
    btn.addEventListener('click', () => {
      state.variant = btn.dataset.variant;
      document.querySelectorAll('.variant-btn').forEach((b) =>
        b.classList.toggle('active', b === btn)
      );
      render();
    });
  }

  render();
}
