/*
  LuxBase language runtime (zh <-> en).

  The UI is authored in Chinese; this layer translates rendered text nodes and the
  title/placeholder/aria-label attributes through a source-string dictionary (i18n-dict-*.js).
  Keys are trimmed source segments; the original surrounding whitespace of a text node is
  re-attached so concatenation fragments keep their grammar. Data surfaces (result grids,
  editors, DDL/console/audit output, object and connection names) are excluded so identifiers
  and stored values are never rewritten.

  While English is active a MutationObserver translates re-rendered panels; switching back to
  Chinese runs the reverse map over the whole document. Choice persists in localStorage.
*/
(() => {
  const STORE_KEY = "luxbase-lang";
  const ATTRS = ["title", "placeholder", "aria-label"];
  const EXCLUDE = "pre, code, textarea, input, .monaco-editor, #monaco-editor-host, .code-editor, "
    + ".result-grid, .audit-table, .console-view, .console-result, .ddl-view, .compare-sql, "
    + ".connection-copy, .obj-name, .tree-row strong, .tree-row .tree-name, .tree-label, "
    + ".diff-table-head strong, .redis-json-pre, .es-doc-source-editor, "
    + ".tree-row[data-open-draft], #redis-key-name, [data-no-i18n]";
  const CJK = /[一-鿿]/;

  const dict = window.__LUX_I18N_DICT__ || {};
  const zh2en = new Map(Object.entries(dict));
  const en2zh = new Map();
  for (const [zh, en] of zh2en) if (!en2zh.has(en)) en2zh.set(en, zh);

  const UNITS_EN = { 毫秒: "ms", 秒: "second", 分钟: "minute", 小时: "hour", 天: "day" };
  const UNITS_ZH = { ms: "毫秒", second: "秒", seconds: "秒", minute: "分钟", minutes: "分钟", hour: "小时", hours: "小时", day: "天", days: "天" };
  const plural = (n, unit) => (Number(n) === 1 ? unit : unit === "ms" ? "ms" : unit + "s");
  const PATTERNS = [
    [/^(\d+(?:\.\d+)?) (毫秒|秒|分钟|小时|天)前$/, (m, n, u) => `${n} ${plural(n, UNITS_EN[u])} ago`,
      /^(\d+(?:\.\d+)?) (ms|seconds?|minutes?|hours?|days?) ago$/, (m, n, u) => `${n} ${UNITS_ZH[u]}前`],
    [/^第 (\d+) \/ (\d+) 页$/, (m, a, b) => `Page ${a} of ${b}`,
      /^Page (\d+) of (\d+)$/, (m, a, b) => `第 ${a} / ${b} 页`],
    [/^(\d+) 行受影响$/, (m, n) => `${n} rows affected`,
      /^(\d+) rows affected$/, (m, n) => `${n} 行受影响`],
    [/^(.+) · 单击查看结构，双击打开数据$/, (m, n) => `${n} · click to view structure, double-click to open data`,
      /^(.+) · click to view structure, double-click to open data$/, (m, n) => `${n} · 单击查看结构，双击打开数据`],
    [/^(.+) · 单击打开索引浏览$/, (m, n) => `${n} · click to browse index`,
      /^(.+) · click to browse index$/, (m, n) => `${n} · 单击打开索引浏览`],
    [/^文档 (.+)$/, (m, n) => `Document ${n}`,
      /^Document (.+)$/, (m, n) => `文档 ${n}`],
    [/^(\d+) 个警告$/, (m, n) => `${n} warnings`,
      /^(\d+) warnings$/, (m, n) => `${n} 个警告`],
    [/^(\d+) 个错误$/, (m, n) => `${n} errors`,
      /^(\d+) errors$/, (m, n) => `${n} 个错误`],
    [/^(\d+) 个错误 · (\d+) 个警告$/, (m, a, b) => `${a} errors · ${b} warnings`,
      /^(\d+) errors · (\d+) warnings$/, (m, a, b) => `${a} 个错误 · ${b} 个警告`],
    [/^(\d+) 处未保存的单元格修改$/, (m, n) => `${n} unsaved cell edits`,
      /^(\d+) unsaved cell edits$/, (m, n) => `${n} 处未保存的单元格修改`],
    [/^查询 (\d+)$/, (m, n) => `Query ${n}`,
      /^Query (\d+)$/, (m, n) => `查询 ${n}`],
    [/^查询 (\d+) · 未选择连接$/, (m, n) => `Query ${n} · no connection selected`,
      /^Query (\d+) · no connection selected$/, (m, n) => `查询 ${n} · 未选择连接`],
    [/^查询 (\d+) · (.+)$/, (m, n, rest) => `Query ${n} · ${rest}`,
      /^Query (\d+) · (.+)$/, (m, n, rest) => `查询 ${n} · ${rest}`],
    [/^(.+) · 未选择连接$/, (m, n) => `${n} · no connection selected`,
      /^(.+) · no connection selected$/, (m, n) => `${n} · 未选择连接`],
  ];

  let lang = "zh";
  try { lang = localStorage.getItem(STORE_KEY) === "en" ? "en" : "zh"; } catch {}

  function lookup(source, map) {
    if (map.has(source)) return map.get(source);
    const trimmed = source.trim();
    if (!trimmed) return null;
    const value = map.get(trimmed);
    if (value == null) return null;
    const lead = source.slice(0, source.length - source.trimStart().length);
    const trail = source.slice(source.trimEnd().length);
    return lead + value + trail;
  }

  function translate(source, dir) {
    if (typeof source !== "string" || !source.trim()) return null;
    const hit = lookup(source, dir === "en" ? zh2en : en2zh);
    if (hit != null) return hit;
    for (const [zhRe, toEn, enRe, toZh] of PATTERNS) {
      const re = dir === "en" ? zhRe : enRe;
      const fn = dir === "en" ? toEn : toZh;
      if (re.test(source)) return source.replace(re, fn);
    }
    return null;
  }

  function translateNode(node, dir) {
    const parent = node.parentElement;
    if (!parent || parent.closest(EXCLUDE)) return;
    const next = translate(node.nodeValue, dir);
    if (next != null && next !== node.nodeValue) node.nodeValue = next;
  }

  function applyTo(root, dir) {
    if (!root) return;
    if (root.nodeType === Node.TEXT_NODE) { translateNode(root, dir); return; }
    if (root.nodeType !== Node.ELEMENT_NODE && root.nodeType !== Node.DOCUMENT_NODE) return;
    if (root.nodeType === Node.ELEMENT_NODE && root.closest(EXCLUDE)) return;
    const elements = root.nodeType === Node.ELEMENT_NODE
      ? [root, ...root.querySelectorAll("*")]
      : [...root.querySelectorAll("*")];
    for (const el of elements) {
      const blocked = el.closest(EXCLUDE);
      for (const attr of ATTRS) {
        if (!el.hasAttribute(attr)) continue;
        // Placeholders are UI chrome even on inputs/textareas, whose values stay untouched.
        if (blocked && !(attr === "placeholder" && (el.tagName === "INPUT" || el.tagName === "TEXTAREA"))) continue;
        const next = translate(el.getAttribute(attr), dir);
        if (next != null) el.setAttribute(attr, next);
      }
    }
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const parent = node.parentElement;
        if (!parent || parent.closest(EXCLUDE) || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        return NodeFilter.FILTER_ACCEPT;
      },
    });
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    for (const node of nodes) translateNode(node, dir);
  }

  let observer = null;
  let pending = new Set();
  let scheduled = false;
  function flush() {
    scheduled = false;
    const items = [...pending];
    pending.clear();
    observer.disconnect();
    for (const item of items) {
      if (item.nodeType === Node.TEXT_NODE) translateNode(item, "en");
      else if (item.nodeType === Node.ELEMENT_NODE && !item.closest(EXCLUDE)) applyTo(item, "en");
    }
    paintButton();
    connect();
  }
  function connect() {
    if (observer) observer.observe(document.body, {
      childList: true, subtree: true, characterData: true,
      attributes: true, attributeFilter: ATTRS,
    });
  }
  function startObserver() {
    if (observer) return;
    observer = new MutationObserver((records) => {
      for (const record of records) {
        if (record.type === "characterData") pending.add(record.target);
        else if (record.type === "childList") record.addedNodes.forEach((n) => pending.add(n));
        else if (record.type === "attributes") pending.add(record.target);
      }
      // Microtask, not rAF: background tabs pause animation frames, which would stall
      // translation of panels rendered while the window is not frontmost.
      if (!scheduled) { scheduled = true; queueMicrotask(flush); }
    });
    connect();
  }
  function stopObserver() {
    if (observer) { observer.disconnect(); observer = null; }
    pending.clear();
  }

  const button = () => document.getElementById("lang-toggle");
  const label = () => document.getElementById("lang-toggle-label");
  function paintButton() {
    const el = button();
    if (!el) return;
    const toEn = lang === "zh";
    const text = label();
    if (text) text.textContent = toEn ? "中" : "EN";
    el.title = toEn ? "切换为英文" : "Switch to Chinese";
    el.setAttribute("aria-label", el.title);
    el.dataset.lang = lang;
  }
  function setLang(next) {
    lang = next === "en" ? "en" : "zh";
    try { localStorage.setItem(STORE_KEY, lang); } catch {}
    document.documentElement.lang = lang === "en" ? "en" : "zh-CN";
    paintButton();
    stopObserver();
    applyTo(document.body, lang === "en" ? "en" : "zh");
    if (lang === "en") startObserver();
    document.dispatchEvent(new CustomEvent("luxbase-lang", { detail: { lang } }));
  }

  document.documentElement.lang = lang === "en" ? "en" : "zh-CN";
  paintButton();
  // The toolbar re-renders on pointer interaction, which can detach the button before a trusted
  // click lands (the event then targets an ancestor). Toggle on pointerdown for pointers and on
  // detail-0 clicks for keyboard activation; synthetic clicks also carry detail 0.
  document.addEventListener("pointerdown", (event) => {
    if (event.target.closest?.("#lang-toggle")) setLang(lang === "zh" ? "en" : "zh");
  });
  document.addEventListener("click", (event) => {
    if (event.detail === 0 && event.target.closest?.("#lang-toggle")) setLang(lang === "zh" ? "en" : "zh");
  });
  if (lang === "en") {
    applyTo(document.body, "en");
    startObserver();
  }
  window.__luxI18n = { get lang() { return lang; }, setLang, translate };
})();