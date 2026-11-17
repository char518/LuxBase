#!/usr/bin/env node
// Derives ui/theme-dark-auto.css from ui/styles.css.
//
// The renderer stylesheet carries ~1,200 hard-coded light-coloured literals, so a token
// override alone cannot produce a dark theme. This script mirrors every rule that contains a
// colour literal, prefixes the selector with [data-theme="dark"], and maps each literal through
// the palette function below. The originals are never touched, which keeps light mode
// byte-identical. Hand-picked values live in ui/theme-dark.css, which is loaded after this file.
//
//   node scripts/build-dark-theme.js          rewrite the derived stylesheet
//   node scripts/build-dark-theme.js --check  fail when it no longer matches ui/styles.css
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const sourcePath = resolve(root, "ui/styles.css");
const targetPath = resolve(root, "ui/theme-dark-auto.css");
const css = readFileSync(sourcePath, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");

const clamp = (value, min, max) => Math.min(max, Math.max(min, value));
const hueOf = (hue) => (hue === null || Number.isNaN(hue) ? 220 : hue);

function parseColor(text) {
  const hex = text.match(/^#([0-9a-f]{3}|[0-9a-f]{6}|[0-9a-f]{8})$/i);
  if (hex) {
    let digits = hex[1];
    if (digits.length === 3 || digits.length === 4) {
      digits = [...digits].map((d) => d + d).join("");
    }
    return {
      kind: "hex",
      r: parseInt(digits.slice(0, 2), 16),
      g: parseInt(digits.slice(2, 4), 16),
      b: parseInt(digits.slice(4, 6), 16),
      a: digits.length === 8 ? parseInt(digits.slice(6, 8), 16) / 255 : 1,
    };
  }
  const rgb = text.match(/^rgba?\(\s*([\d.]+)\s*[, ]\s*([\d.]+)\s*[, ]\s*([\d.]+)\s*(?:[,/]?\s*([\d.]+%?)\s*)?\)$/i);
  if (rgb) {
    const alpha = rgb[4] === undefined ? 1 : Number(String(rgb[4]).replace("%", "")) / (String(rgb[4]).includes("%") ? 100 : 1);
    return { kind: "rgb", r: Number(rgb[1]), g: Number(rgb[2]), b: Number(rgb[3]), a: alpha };
  }
  return null;
}

function toHsl({ r, g, b }) {
  const rn = r / 255, gn = g / 255, bn = b / 255;
  const max = Math.max(rn, gn, bn), min = Math.min(rn, gn, bn);
  const l = (max + min) / 2;
  const d = max - min;
  if (d === 0) return { h: null, s: 0, l };
  const s = d / (1 - Math.abs(2 * l - 1));
  let h;
  if (max === rn) h = 60 * (((gn - bn) / d) % 6);
  else if (max === gn) h = 60 * ((bn - rn) / d + 2);
  else h = 60 * ((rn - gn) / d + 4);
  return { h: (h + 360) % 360, s, l };
}

const round2 = (value) => Math.round(value * 100) / 100;
const hsl = (h, s, l) => `hsl(${Math.round(hueOf(h))}, ${Math.round(clamp(s, 0, 1) * 100)}%, ${Math.round(clamp(l, 0, 1) * 100)}%)`;

// Neutral light surfaces become the dark surface ramp, inverted so that the whiter (more
// elevated) a panel is in light mode, the lighter it becomes in dark mode. Hue survives as a
// whisper of tint, which is what keeps blue/red/amber chips distinguishable.
function mapColor(literal, role) {
  const color = parseColor(literal);
  if (!color) return literal;
  const { h, s, l } = toHsl(color);

  if (role === "text") {
    if (l > 0.78) return literal;                       // already light ink, sits on dark chrome
    if (s > 0.3) return hsl(h, clamp(s * 0.95 + 0.05, 0, 0.85), Math.max(0.62, 0.98 - l * 0.42));
    return hsl(h, 0.1, clamp(0.96 - l * 0.34, 0.66, 0.95));
  }

  if (role === "line" || role === "shadow") {
    if (s > 0.35) return hsl(h, clamp(s * 0.85, 0, 0.85), clamp(l < 0.5 ? l + 0.2 : l, 0.5, 0.78));
    if (l > 0.78) {
      const base = role === "shadow" ? 0.13 : 0.17;
      return hsl(h, s > 0.3 ? 0.22 : 0.12, clamp(base + (l - 0.78) * 0.22, base, 0.27));
    }
    return literal;                                     // dark hairlines keep their role
  }

  if (role === "surface") {
    if (l > 0.8) {
      const tint = s > 0.3 ? 0.22 : clamp(s * 0.5 + 0.08, 0, 0.22);
      return hsl(h, tint, clamp(0.055 + (l - 0.78) * 0.42, 0.055, 0.15));
    }
    if (s < 0.16) return literal;                       // near-black chrome is already dark
    return hsl(h, clamp(s * 0.92, 0.2, 0.85), clamp(l < 0.45 ? l + 0.04 : l > 0.62 ? 0.5 : l, 0.1, 0.56));
  }

  if (s > 0.3) return hsl(h, clamp(s * 0.9, 0, 0.85), clamp(l < 0.45 ? l + 0.16 : l, 0.5, 0.78));
  return mapColor(literal, l > 0.6 ? "surface" : "text");
}

function roleOf(prop) {
  if (prop === "color" || prop === "caret-color") return "text";
  if (prop === "fill" || prop === "stroke") return "text";
  if (prop.startsWith("border") || prop.startsWith("outline") || prop === "column-rule") return "line";
  if (prop === "box-shadow" || prop === "text-shadow") return "shadow";
  if (prop.startsWith("background") || prop === "accent-color") return "surface";
  return "other";
}

const literalRe = /#[0-9a-fA-F]{3,8}\b|\brgba?\([^()]*\)/g;
const hasLiteral = (value) => /#[0-9a-fA-F]{3,8}\b|\brgba?\([^()]*\)/.test(value);

// An 8-digit hex carries its alpha inside the literal, and the HSL palette function has no alpha
// channel — a translucent navy drop shadow would come back as a solid blue halo. Rewriting those
// literals as rgba() first routes them through the alpha-aware branches below.
function normalizeAlpha(literal) {
  const color = parseColor(literal);
  if (!color || color.a >= 1 || !/^#/i.test(literal)) return literal;
  return `rgba(${color.r}, ${color.g}, ${color.b}, ${round2(color.a)})`;
}

function mapValue(value, prop) {
  const role = roleOf(prop);
  return value.replace(literalRe, (raw) => {
    const literal = normalizeAlpha(raw);
    const parsed = parseColor(literal);
    if (parsed && role !== "text" && /^rgba?\(/i.test(literal)) {
      const { r, g, b, a } = parsed;
      const alpha = (next) => `rgba(${r}, ${g}, ${b}, ${round2(clamp(next, 0, 1))})`;
      if (r > 240 && g > 240 && b > 240) {
        // An almost-opaque white veil is a surface (context menu, popover, toast), not a highlight:
        // keeping it white would lay a translucent white sheet over the dark chrome beneath it.
        if (role === "surface" && a >= 0.5) {
          const dark = mapColor(`rgb(${r}, ${g}, ${b})`, "surface");
          return dark.replace(/^hsl\(/, "hsla(").replace(/\)$/, `, ${round2(a)})`);
        }
        return alpha(a * 0.45);                                        // white veil → subtle highlight
      }
      if (r < 80 && g < 80 && b < 80) return `rgba(0, 0, 0, ${round2(clamp(a * 1.75, 0, 0.72))})`; // shadow
      return alpha(a * 1.25);                                           // coloured wash
    }
    if (parsed && role === "text" && /^rgba?\(/i.test(literal)) {
      const { r, g, b, a } = parsed;
      if (r < 80 && g < 80 && b < 80) return `rgba(232, 238, 246, ${round2(clamp(a, 0, 1))})`;
      return literal;
    }
    const mapped = mapColor(literal, role === "other" ? "surface" : role);
    return mapped === literal ? literal : mapped;
  });
}

function splitTop(text, separator) {
  const parts = [];
  let depth = 0, current = "", quote = null;
  for (const ch of text) {
    if (quote) {
      current += ch;
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") quote = ch;
    if (ch === "(" || ch === "[") depth += 1;
    if (ch === ")" || ch === "]") depth -= 1;
    if (ch === separator && depth === 0) {
      parts.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  parts.push(current);
  return parts;
}

function parseRules(source, inherited = []) {
  const rules = [];
  let index = 0, buffer = "";
  while (index < source.length) {
    const ch = source[index];
    if (ch === "{") {
      const prelude = buffer.trim();
      buffer = "";
      const close = findClose(source, index);
      const body = source.slice(index + 1, close);
      if (/^@(keyframes|font-face)/i.test(prelude)) {
        // Animations here only touch opacity and transform; a colour mirror would be dead weight.
      } else if (prelude.startsWith("@")) {
        // Conditional groups are descended into, so a rule keeps exactly the conditions it has in
        // ui/styles.css; a top-level rule must never inherit a neighbouring @media.
        rules.push(...parseRules(body, [...inherited, prelude]));
      } else {
        rules.push({ selector: prelude, body, at: inherited });
      }
      index = close + 1;
      continue;
    }
    if (ch === "}") {
      buffer = "";
      index += 1;
      continue;
    }
    buffer += ch;
    index += 1;
  }
  return rules;
}

function findClose(source, openIndex) {
  let depth = 0;
  for (let i = openIndex; i < source.length; i += 1) {
    if (source[i] === "{") depth += 1;
    else if (source[i] === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  throw new Error("unbalanced braces in ui/styles.css");
}

function scopeSelector(selector) {
  return splitTop(selector, ",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      if (/^:root\b/.test(part)) return `[data-theme="dark"]${part.slice(5)}`;
      if (/^html\b/.test(part)) return part.replace(/^html/, 'html[data-theme="dark"]');
      return `[data-theme="dark"] ${part}`;
    })
    .join(", ");
}

const lines = [
  "/* Derived by scripts/build-dark-theme.js from ui/styles.css — do not edit this file. */",
  "/* Every rule below is a dark-mode mirror of a light rule; ui/theme-dark.css refines them. */",
  "",
];

const emitted = [];
let openGroups = [];
function closeGroups(lines, depth) {
  while (openGroups.length > depth) {
    lines.push(`${"  ".repeat(openGroups.length - 1)}}`);
    openGroups.pop();
  }
}

for (const rule of parseRules(css)) {
  const declarations = splitTop(rule.body, ";")
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((entry) => {
      const colon = entry.indexOf(":");
      if (colon < 0) return null;
      const prop = entry.slice(0, colon).trim();
      const value = entry.slice(colon + 1).trim();
      // Custom properties belong to the curated palette in ui/theme-dark.css, which wins over
      // anything derived here, so mirroring them would only create noise.
      if (prop.startsWith("--") || !hasLiteral(value)) return null;
      const mapped = mapValue(value, prop);
      return mapped === value ? null : `${prop}: ${mapped};`;
    })
    .filter(Boolean);
  if (declarations.length === 0) continue;
  const scoped = scopeSelector(rule.selector);
  let shared = 0;
  while (shared < rule.at.length && openGroups[shared] === rule.at[shared]) shared += 1;
  closeGroups(lines, shared);
  if (openGroups.length < rule.at.length) {
    for (let depth = openGroups.length; depth < rule.at.length; depth += 1) {
      lines.push(`${"  ".repeat(depth)}${rule.at[depth]} {`);
      openGroups.push(rule.at[depth]);
    }
  }
  const pad = "  ".repeat(rule.at.length);
  lines.push(`${pad}${scoped} { ${declarations.join(" ")} }`);
  emitted.push([...rule.at, scoped].join("\u0001"));
}
closeGroups(lines, 0);

const output = `${lines.join("\n")}\n`;
const ruleCount = (output.match(/^\s*\[/gm) || []).length;

// Round-trip guard: re-parse what was just written and require the same rule in the same
// conditions. A grouping bug that hides a rule inside a foreign @media fails here instead of
// silently leaving light-mode panels unpainted, which no screenshot audit can promise to catch.
const reparsed = parseRules(output.replace(/\/\*[\s\S]*?\*\//g, "")).map((rule) => [...rule.at, rule.selector].join("\u0001"));
if (reparsed.length !== emitted.length || reparsed.some((key, position) => key !== emitted[position])) {
  const drift = emitted.findIndex((key, position) => key !== reparsed[position]);
  throw new Error(`derived stylesheet does not round-trip at rule ${drift}: ${emitted[drift]} !== ${reparsed[drift]}`);
}

if (process.argv.includes("--check")) {
  const existing = readFileSync(targetPath, "utf8");
  if (existing !== output) {
    console.error("ui/theme-dark-auto.css is stale: run `node scripts/build-dark-theme.js` after editing ui/styles.css");
    process.exit(1);
  }
  console.log(`dark theme mirror is up to date (${ruleCount} rules)`);
} else {
  writeFileSync(targetPath, output);
  console.log(`wrote ${targetPath} (${output.split("\n").length} lines, ${ruleCount} rules)`);
}
