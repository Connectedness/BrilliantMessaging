// Downloads page — renders the Concept 3 "Pulse" mark in its Reuleaux variant
// as ready-to-ship PNG/SVG assets. Three colour schemes (full colour, and two
// monochromes), each at the common icon sizes, with an optional transparent
// background. Everything is generated from the same geometry as the live page
// (see pulse-tuner.js), so the assets can never drift from the design.

import { buildMark, DEFAULTS } from './pulse-tuner.js';

const SIZES = [16, 32, 64, 128, 256, 512];

// The mark is a 3-fold pinwheel. Tilting it 15° is the orientation that packs
// it tightest, and at that angle its bounding box comes out essentially square.
// We frame on that bounding box so the margin is equal on all four sides.
//
// (The mark isn't mirror-symmetric, so the hub can't be both dead-centre and
// evenly margined at once — we keep the margins even, which leaves the hub a
// few px off-centre; the tilt keeps that offset small.)
const MARGIN_FRACTION = 8 / 128;
const CENTER = 32;
const ROTATION = 15; // degrees, clockwise about the hub
const NODE_RING = 22; // node-centre distance from the hub
const NODE_R = 5.5; // node radius
const NODE_ANGLES = [-90, 30, 150]; // node bearings before rotation

// Bounding box of the rotated mark — the node disks set its extents.
const nodeX = NODE_ANGLES.map((a) => NODE_RING * Math.cos(((a + ROTATION) * Math.PI) / 180));
const nodeY = NODE_ANGLES.map((a) => NODE_RING * Math.sin(((a + ROTATION) * Math.PI) / 180));
const BOX = {
  x: CENTER + Math.min(...nodeX) - NODE_R,
  y: CENTER + Math.min(...nodeY) - NODE_R,
  w: Math.max(...nodeX) - Math.min(...nodeX) + 2 * NODE_R,
  h: Math.max(...nodeY) - Math.min(...nodeY) + 2 * NODE_R,
};

const VARIANTS = [
  {
    id: 'color',
    name: 'Full colour',
    caption: 'Graphite structure · teal pulse',
    swatch: 'light',
    struct: '--graphite',
    hub: '--teal',
    bg: '--paper',
    previewClass: 'logo--accent',
  },
  {
    id: 'mono-dark',
    name: 'Monochrome · black on white',
    caption: 'Single colour · graphite on paper',
    swatch: 'light',
    struct: '--graphite',
    hub: '--graphite',
    bg: '--paper',
    previewClass: 'logo--mono-dark',
  },
  {
    id: 'mono-light',
    name: 'Monochrome · white on black',
    caption: 'Single colour · paper on graphite',
    swatch: 'dark',
    struct: '--paper',
    hub: '--paper',
    bg: '--graphite',
    previewClass: 'logo--mono-light',
  },
];

const colorVariant = VARIANTS.find((v) => v.id === 'color');

const cssVar = (name) =>
  getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// Build a standalone, self-contained SVG string for export — literal colours
// baked in, the mark scaled + centred into a `size`×`size` canvas.
function exportSvg(variant, size, transparent) {
  const margin = size * MARGIN_FRACTION;
  const scale = (size - 2 * margin) / Math.max(BOX.w, BOX.h);
  const tx = (size - BOX.w * scale) / 2 - scale * BOX.x; // centre the bounding box
  const ty = (size - BOX.h * scale) / 2 - scale * BOX.y;

  const colors = { struct: cssVar(variant.struct), hub: cssVar(variant.hub) };
  const inner = buildMark({ variant: 'reuleaux', ...DEFAULTS, colors });
  const bg = transparent
    ? ''
    : `<rect width="${size}" height="${size}" fill="${cssVar(variant.bg)}"/>`;

  return (
    `<svg xmlns="http://www.w3.org/2000/svg" width="${size}" height="${size}" ` +
    `viewBox="0 0 ${size} ${size}" fill="none">${bg}` +
    `<g transform="translate(${tx},${ty}) scale(${scale}) ` +
    `rotate(${ROTATION} ${CENTER} ${CENTER})">${inner}</g></svg>`
  );
}

function triggerDownload(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

const baseName = (variant, size, transparent) =>
  `brilliantmessaging-reuleaux-${variant.id}-${size}px${transparent ? '-transparent' : ''}`;

function downloadSvg(variant, size, transparent) {
  const blob = new Blob([exportSvg(variant, size, transparent)], {
    type: 'image/svg+xml',
  });
  triggerDownload(blob, `${baseName(variant, size, transparent)}.svg`);
}

// Rasterise the export SVG to a PNG blob at `size`×`size`.
function rasterize(variant, size, transparent) {
  return new Promise((resolve) => {
    const svg = exportSvg(variant, size, transparent);
    const url = URL.createObjectURL(new Blob([svg], { type: 'image/svg+xml' }));
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = size;
      canvas.height = size;
      canvas.getContext('2d').drawImage(img, 0, 0, size, size);
      URL.revokeObjectURL(url);
      canvas.toBlob(resolve);
    };
    img.src = url;
  });
}

async function downloadPng(variant, size, transparent) {
  triggerDownload(
    await rasterize(variant, size, transparent),
    `${baseName(variant, size, transparent)}.png`
  );
}

// --- web & app icons (always full-colour brand mark) ---
const FAVICON_SIZES = [16, 32, 48];

// Pack PNG images into a multi-resolution .ico (PNG-in-ICO, supported by all
// modern browsers). Each directory entry points at a full PNG payload.
function buildIco(images) {
  const HEADER = 6;
  const ENTRY = 16;
  const total =
    HEADER +
    images.length * ENTRY +
    images.reduce((n, im) => n + im.data.length, 0);
  const buf = new ArrayBuffer(total);
  const view = new DataView(buf);
  const bytes = new Uint8Array(buf);

  view.setUint16(0, 0, true); // reserved
  view.setUint16(2, 1, true); // type: icon
  view.setUint16(4, images.length, true);

  let offset = HEADER + images.length * ENTRY;
  images.forEach((im, i) => {
    const entry = HEADER + i * ENTRY;
    const dim = im.size >= 256 ? 0 : im.size; // 0 encodes 256
    view.setUint8(entry, dim); // width
    view.setUint8(entry + 1, dim); // height
    view.setUint8(entry + 2, 0); // palette size
    view.setUint8(entry + 3, 0); // reserved
    view.setUint16(entry + 4, 1, true); // colour planes
    view.setUint16(entry + 6, 32, true); // bits per pixel
    view.setUint32(entry + 8, im.data.length, true); // payload bytes
    view.setUint32(entry + 12, offset, true); // payload offset
    bytes.set(im.data, offset);
    offset += im.data.length;
  });
  return buf;
}

async function downloadFavicon(transparent) {
  const pngs = await Promise.all(
    FAVICON_SIZES.map(async (size) => ({
      size,
      data: new Uint8Array(
        await (await rasterize(colorVariant, size, transparent)).arrayBuffer()
      ),
    }))
  );
  triggerDownload(
    new Blob([buildIco(pngs)], { type: 'image/x-icon' }),
    `brilliantmessaging-reuleaux-favicon${transparent ? '-transparent' : ''}.ico`
  );
}

async function downloadAppleTouch() {
  // iOS composites Apple Touch icons onto an opaque tile, turning any
  // transparency black — so these are always exported on the paper background.
  triggerDownload(
    await rasterize(colorVariant, 180, false),
    'brilliantmessaging-reuleaux-apple-touch-icon-180px.png'
  );
}

// --- render the page ---
const transparentToggle = document.querySelector('#opt-transparent');
const isTransparent = () => transparentToggle.checked;

const cards = document.querySelector('#download-cards');

for (const variant of VARIANTS) {
  const card = document.createElement('section');
  card.className = 'dl-card panel';

  const preview = document.createElement('figure');
  preview.className = `swatch swatch--${variant.swatch} dl-preview`;
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svg.setAttribute('class', `${variant.previewClass}`);
  svg.setAttribute('width', '104');
  svg.setAttribute('height', '104');
  svg.setAttribute('viewBox', '0 0 64 64');
  svg.setAttribute('fill', 'none');
  svg.innerHTML = `<g transform="rotate(${ROTATION} ${CENTER} ${CENTER})">${buildMark(
    { variant: 'reuleaux', ...DEFAULTS }
  )}</g>`;
  const caption = document.createElement('figcaption');
  caption.textContent = variant.caption;
  preview.append(svg, caption);

  const body = document.createElement('div');
  body.className = 'dl-body';
  const heading = document.createElement('h2');
  heading.textContent = variant.name;
  body.appendChild(heading);

  const pngRow = document.createElement('div');
  pngRow.className = 'dl-row';
  pngRow.append(rowLabel('PNG'));
  for (const size of SIZES) {
    pngRow.appendChild(
      sizeButton(size, () => downloadPng(variant, size, isTransparent()))
    );
  }

  const svgRow = document.createElement('div');
  svgRow.className = 'dl-row';
  svgRow.append(rowLabel('SVG'));
  svgRow.appendChild(
    actionButton('Vector', () => downloadSvg(variant, 512, isTransparent()))
  );

  body.append(pngRow, svgRow);
  card.append(preview, body);
  cards.appendChild(card);
}

document
  .querySelector('#dl-favicon')
  .addEventListener('click', () => downloadFavicon(isTransparent()));
document
  .querySelector('#dl-apple')
  .addEventListener('click', () => downloadAppleTouch());

function rowLabel(text) {
  const span = document.createElement('span');
  span.className = 'dl-row-label';
  span.textContent = text;
  return span;
}

function sizeButton(size, onClick) {
  return actionButton(`${size}`, onClick);
}

function actionButton(label, onClick) {
  const btn = document.createElement('button');
  btn.className = 'dl-btn';
  btn.type = 'button';
  btn.textContent = label;
  btn.addEventListener('click', onClick);
  return btn;
}
