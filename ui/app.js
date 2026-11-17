export function mountUi() {
  if (window.__luxbaseMounted) return;
  window.__luxbaseMounted = true;
  const $ = (selector, root = document) => root.querySelector(selector);
  document.body.classList.add("app-shell-mounted");
  const $$ = (selector, root = document) => Array.from(root.querySelectorAll(selector));
  const modalBackdrop = $("#modal-backdrop");
  const modalContent = $("#modal-content");
  const modalTitle = $("#modal-title");
  const modalEyebrow = $("#modal-eyebrow");
  const toastRegion = $("#toast-region");
  const contextMenu = $("#context-menu");
  const appMenu = $("#app-menu-popover");
  const invokeTauri = window.__TAURI_INTERNALS__?.invoke;
  const backendAvailable = typeof invokeTauri === "function";
  const nativeWindow = window.__luxbaseNative;
  let activeConnection = null;
  let activeConnectionId = null;
  let activeProtocol = "mysql";
  let activeFamily = "sql";
  let activeSqlOperationId = null;
  let activeRedisOperationId = null;
  let savedConnections = [];
  let queryDrafts = new Map();
  let queryHistory = [];
  let protocolDescriptors = new Map();
  let currentRedisValue = null;
  let currentRedisDb = 0;
  let currentRedisKeyB64 = null;
  let redisBinaryEncoding = null;
  let currentRedisConn = null;
  let redisValueView = null;
  let redisTabOpen = false;
  let esTabOpen = false;
  const redisSessions = new Map();
  const esSessions = new Map();
  let lastRedisConn = null;
  let lastEsConn = null;
  let esDocView = null;
  let esDocModalDoc = null;
  let contextRedisKey = null;
  let confirmAction = null;
  let securityState = null;
  let connectionLookup = new Map();
  let redisScanState = { cursor: 0, pattern: "*", count: 300, db: 0, loading: false };
  const redisDbCursors = new Map();
  const redisInfoCache = new Map();
  let esState = {
    connectionId: null,
    index: null,
    indices: [],
    tab: "docs",
    dsl: '{\n  "query": { "match_all": {} }\n}',
    size: 50,
    from: 0,
    total: 0,
    totalRelation: "eq",
    hits: [],
    tookMs: null,
    aggregations: null,
    mapping: null,
    aliases: [],
    currentDoc: null,
    restMethod: "GET",
    restPath: "/_cluster/health",
    restBody: "",
    restResult: null,
    loading: false,
    docCols: { id: 160, score: 64, source: null },
  };
  let gridState = { columns: [], rows: [], offset: 0, hasMore: false, source: "query" };
  let activeTable = null;
  let activeSchema = null;
  let activePrimaryKey = null;
  let migrationDirection = "export";
  let lastBoundParameters = [];
  let pendingParameterEditor = null;
  let lastFocusedBeforeModal = null;
  let lastDiagnosticReport = null;
  let lastExplorerWorkspace = "objects";
  let lastLegacySecretImport = null;

  function backendCall(command, args = {}) {
    if (!backendAvailable) {
      return Promise.reject(new Error("桌面后端未连接。当前窗口只能预览界面，不能对数据库执行操作。"));
    }
    return invokeTauri(command, args);
  }

  function errorMessage(error) {
    if (!error) return "未知错误";
    if (typeof error === "string") return error;
    if (error.message) return error.message;
    try {
      return JSON.stringify(error);
    } catch {
      return String(error);
    }
  }

  function serverCommitNeedsRefresh(error) {
    return errorMessage(error).includes("committed on the server");
  }

  function showOperationError(error, context = "操作失败") {
    const message = errorMessage(error);
    const classLabel = error?.class ? ({
      connection_failed: "连接失败",
      permission_denied: "权限不足",
      syntax_error: "语法错误",
      timeout: "超时",
      cancelled: "已取消",
      protocol_unsupported: "协议不支持",
      validation: "输入或确认错误",
      conflict: "数据冲突",
      internal: "内部错误",
    }[error.class] || error.class) : null;
    toast(classLabel ? `${context} · ${classLabel}` : context, message, "danger");
    return message;
  }

  function requireSavedConnection(family = null) {
    if (!activeConnectionId) {
      throw new Error("请先选择并打开一个已保存的数据库连接。");
    }
    if (family && activeFamily !== family) {
      throw new Error(family === "sql" ? "当前连接不是 SQL 协议。" : "当前连接不是键值协议。");
    }
    return connectionLookup.get(activeConnectionId);
  }

  function operationId() {
    return globalThis.crypto?.randomUUID?.() ?? `op-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  }

  function setBackendUnavailable() {
    if (backendAvailable) return;
    const statusConnection = $(".status-connection");
    if (statusConnection) statusConnection.innerHTML = '<span class="health-dot muted"></span>界面预览';
    const sync = $(".status-center");
    if (sync) sync.innerHTML = "仅预览 · 数据库操作已禁用";
    const secure = $(".secure-chip");
    if (secure) secure.innerHTML = '<svg><use href="#i-lock"/></svg>等待安全后端';
  }

  async function refreshBootstrap() {
    if (!backendAvailable) {
      setBackendUnavailable();
      return;
    }
    try {
      const bootstrap = await backendCall("app_bootstrap");
      savedConnections = bootstrap.connections || [];
      connectionLookup = new Map(savedConnections.map((connection) => [connection.id, connection]));
      protocolDescriptors = new Map((bootstrap.protocols || []).map((descriptor) => [descriptor.id, descriptor]));
      queryHistory = bootstrap.recentHistory || [];
      renderSavedConnections();
      renderSqlTabs();
      const currentTab = activeSqlTab();
      if (currentTab?.kind === "query") renderQueryContext(currentTab);
      if (currentTab) renderBreadcrumbForTab(currentTab);
      renderAuditEntries(bootstrap.recentAudit || []);
      updateSecurityState(bootstrap.security);
      if (currentWorkspace() === "objects") renderObjectsView();
    } catch (error) {
      showOperationError(error, "无法加载本地工作区");
    }
  }

  function updateSecurityState(security) {
    if (!security) return;
    securityState = security;
    const chip = $(".secure-chip");
    if (chip) {
      chip.innerHTML = `<svg><use href="#i-lock"/></svg>${security.localVaultAvailable ? "本地保险库就绪" : "本地保险库不可用"}`;
      chip.title = security.localVaultAvailable
        ? "密码、SSH 口令与加密主密钥都保存在本机 LuxBase 数据目录，不再访问系统钥匙串。"
        : "LuxBase 数据目录当前不可读写，密码与 SQL 历史暂时无法保存。";
    }
    const setting = $("#setting-vault");
    if (setting) {
      setting.textContent = security.localVaultAvailable ? "● 本地保险库可用" : "● 本地保险库不可用";
      setting.classList.toggle("success-text", Boolean(security.localVaultAvailable));
    }
  }

  function protocolShort(protocol) {
    return protocolMeta[protocol]?.short || String(protocol || "?").slice(0, 1).toUpperCase();
  }

  function protocolBadgeClass(protocol) {
    return `protocol-badge ${protocolMeta[protocol]?.badge || "unknown"}`;
  }

  function protocolGlyphHtml(protocol) {
    const badge = protocolMeta[protocol]?.badge || "unknown";
    return badge === "unknown"
      ? escapeHtml(protocolShort(protocol))
      : `<svg aria-hidden="true"><use href="#proto-${badge}"/></svg>`;
  }

  function protocolBadgeHtml(protocol, extraClass = "") {
    return `<span class="${protocolBadgeClass(protocol)}${extraClass}">${protocolGlyphHtml(protocol)}</span>`;
  }

  function protocolFamily(protocol) {
    return protocolDescriptors.get(protocol)?.family
      || (protocol === "redis" ? "kv" : protocol === "elasticsearch" ? "search" : "sql");
  }

  function activeSupports(capability) {
    return Boolean(protocolDescriptors.get(activeProtocol)?.supports?.includes(capability));
  }

  function renderSavedConnections() {
    const list = $("#connection-list");
    if (!list) return;
    const searchCount = $(".sidebar-search .search-count");
    if (searchCount) searchCount.textContent = String(savedConnections.length);
    if (!savedConnections.length) {
      list.innerHTML = '<div class="empty-connections"><strong>还没有数据库连接</strong><span>点击“新建连接”并先测试连接。保存后，配置会加密写入本地 SQLite。</span></div>';
      return;
    }
    const expanded = new Set($$(".connection-group", list)
      .filter((group) => group.dataset.loaded === "true" && !$(".tree-children", group)?.hidden)
      .map((group) => group.dataset.connectionId));
    list.innerHTML = savedConnections.map((connection) => `
      <section class="connection-group" data-protocol="${escapeHtml(connection.protocol)}" data-connection-id="${escapeHtml(connection.id)}">
        <button class="connection-row" data-connection-select="${escapeHtml(connection.id)}" data-toggle-group>
          <span class="tree-chevron"><svg><use href="#i-chevron"/></svg></span>
          ${protocolBadgeHtml(connection.protocol)}
          <span class="connection-copy"><strong>${escapeHtml(connection.name)}</strong><small>${escapeHtml(connection.hostSummary)}</small></span>
          <span class="health-dot muted"></span>
        </button>
        <div class="tree-children" hidden></div>
      </section>
    `).join("");
    $$(".connection-row", list).forEach((row) => {
      row.classList.toggle("active-connection", row.dataset.connectionSelect === activeConnectionId);
      row.addEventListener("contextmenu", (event) => {
        const group = row.closest(".connection-group");
        showConnectionContext(event, group?.dataset.protocol || "mysql", group?.dataset.connectionId);
      });
    });
    // Re-rendering the list must not collapse trees the user had open.
    for (const group of $$(".connection-group", list)) {
      if (!expanded.has(group.dataset.connectionId)) continue;
      const children = $(".tree-children", group);
      if (children) children.hidden = false;
      $(".connection-row .tree-chevron", group)?.classList.add("open");
      loadConnectionTree(group);
    }
    applyConnectionFilter();
  }

  function renderAuditEntries(entries) {
    const tbody = $("#audit-rows");
    if (!tbody) return;
    const dangerCount = entries.filter((entry) => String(entry.eventKind || "").includes("danger")).length;
    const failedCount = entries.filter((entry) => entry.outcome === "failure").length;
    const total = $("#audit-total");
    const danger = $("#audit-danger");
    const failed = $("#audit-failed");
    if (total) total.textContent = String(entries.length);
    if (danger) danger.textContent = String(dangerCount);
    if (failed) failed.textContent = String(failedCount);
    if (!entries.length) {
      tbody.innerHTML = '<tr><td colspan="7" class="empty-table">暂无审计记录</td></tr>';
      return;
    }
    tbody.innerHTML = entries.map((entry) => {
      const date = new Date(entry.occurredAt);
      const time = Number.isNaN(date.getTime()) ? entry.occurredAt : date.toLocaleString("zh-CN", { hour12: false });
      const protocol = entry.protocol || "unknown";
      const kindClass = entry.eventKind?.toLowerCase().includes("danger") ? "danger" : entry.eventKind?.toLowerCase().includes("ddl") ? "ddl" : "query";
      return `<tr><td>${escapeHtml(time)}</td><td title="${escapeHtml(entry.connectionLabel || "")}">${protocolBadgeHtml(protocol, " tiny")}${escapeHtml(entry.connectionLabel || "—")}</td><td><span class="audit-kind ${kindClass}">${escapeHtml(entry.eventKind)}</span></td><td><code title="${escapeHtml(entry.commandSummary)}">${escapeHtml(entry.commandSummary)}</code></td><td>${escapeHtml(entry.affectedRows ?? "—")}</td><td><span class="pill ${entry.outcome === "success" || entry.outcome === "confirmed" ? "success" : "warning"}">${escapeHtml(entry.outcome)}</span></td><td><code>#${escapeHtml(entry.id)}</code></td></tr>`;
    }).join("");
    applyGenericColWidths();
  }

  async function refreshAuditOnly() {
    if (!backendAvailable) return;
    try {
      const entries = await backendCall("recent_audit", { limit: 100 });
      renderAuditEntries(entries);
    } catch (error) {
      showOperationError(error, "审计日志刷新失败");
    }
  }

  const protocolMeta = {
    mysql: { label: "MySQL", badge: "mysql", short: "M", defaults: { host: "", port: "3306" } },
    postgres: { label: "PostgreSQL", badge: "postgres", short: "P", defaults: { host: "", port: "5432" } },
    sqlite: { label: "SQLite", badge: "sqlite", short: "S", defaults: { host: "", port: "" } },
    redis: { label: "Redis", badge: "redis", short: "R", defaults: { host: "", port: "6379" } },
    elasticsearch: { label: "Elasticsearch", badge: "es", short: "ES", defaults: { host: "", port: "9200" } }
  };



  const typeClass = { hash: "hash", string: "string", list: "list", set: "set", zset: "zset", stream: "stream", none: "unknown", unsupported: "unknown" };
  const typeLetter = { hash: "H", string: "S", list: "L", set: "SET", zset: "Z", stream: "ST", unknown: "?" };

  function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#039;" }[ch]));
  }

  function toast(title, message, tone = "success") {
    const item = document.createElement("div");
    item.className = `toast ${tone === "danger" ? "danger" : ""}`;
    item.innerHTML = `<span class="toast-icon">${tone === "danger" ? "!" : "✓"}</span><div><strong>${escapeHtml(title)}</strong><span>${escapeHtml(message)}</span></div>`;
    toastRegion.appendChild(item);
    setTimeout(() => item.remove(), 4200);
  }

  function showModal(title, eyebrow, html, options = {}) {
    if (modalBackdrop.hidden) {
      lastFocusedBeforeModal = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    }
    modalTitle.textContent = title;
    modalEyebrow.textContent = eyebrow;
    modalContent.innerHTML = html;
    $(".modal").classList.toggle("danger-modal", Boolean(options.danger));
    $(".modal").classList.toggle("wide-modal", Boolean(options.wide));
    modalBackdrop.hidden = false;
    setTimeout(() => {
      const focusTarget = $("input, select, textarea, button.primary-button", modalContent);
      if (focusTarget) focusTarget.focus({ preventScroll: true });
    }, 30);
  }

  let pendingChoice = null;

  // A promise-based confirmation for flows that must pause until the user decides.
  function confirmChoice({ title, eyebrow = "CONFIRM", body, confirmLabel = "继续", danger = false }) {
    pendingChoice?.(false);
    return new Promise((resolve) => {
      showModal(title, eyebrow, `
        <div class="modal-text">${body}</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="${danger ? "danger-button" : "primary-button"}" data-action="choice-confirm">${escapeHtml(confirmLabel)}</button></div>
      `, { danger });
      pendingChoice = resolve;
    });
  }

  function settleChoice(value) {
    const resolve = pendingChoice;
    pendingChoice = null;
    resolve?.(value);
  }

  function cancelPendingDraftClose() {
    const tabId = $("#draft-tab-id")?.value;
    const tab = tabId ? sqlTabs.find((item) => item.id === tabId) : null;
    if (tab) tab.closeAfterSave = false;
  }

  function closeModal() {
    settleChoice(false);
    modalBackdrop.hidden = true;
    designer = null;
    modalContent.innerHTML = "";
    if (lastFocusedBeforeModal?.isConnected) {
      lastFocusedBeforeModal.focus({ preventScroll: true });
    }
    lastFocusedBeforeModal = null;
  }

  const TOOL_VIEWS = ["transfer", "sync", "compare", "console", "audit", "settings"];

  function currentWorkspace() {
    return $(".view.active")?.id?.replace("view-", "") || "objects";
  }

  function syncToolbarState(name) {
    $$(".tb-button[data-tool-view]").forEach((button) => button.classList.toggle("active", button.dataset.toolView === name));
    $$('.tb-button[data-action="show-objects"]').forEach((button) => button.classList.toggle("active", name === "objects" && button.dataset.category === objectsState.category));
  }

  function showWorkspace(name) {
    const isKnown = ["objects", "sql", "redis", "es", ...TOOL_VIEWS].includes(name);
    if (!isKnown) return;
    const toolTab = $(`.tool-tab[data-workspace="${name}"]`);
    if (toolTab && TOOL_VIEWS.includes(name)) toolTab.hidden = false;
    $$(".workspace-tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.sqlTab
      ? name === "sql" && tab.dataset.sqlTab === activeSqlTabId
      : tab.dataset.workspace === name));
    $$(".view").forEach((view) => view.classList.remove("active"));
    const view = $(`#view-${name}`);
    if (view) view.classList.add("active");
    if (!TOOL_VIEWS.includes(name)) lastExplorerWorkspace = name;
    $(".workspace")?.classList.toggle("tool-mode", TOOL_VIEWS.includes(name));
    $(".main-layout")?.classList.toggle("tool-view", name === "compare" || name === "console" || name === "transfer" || name === "sync");
    syncToolbarState(name);
    if (name === "objects") {
      const connection = connectionLookup.get(objectsState.connectionId);
      if (connection && connection.id !== activeConnectionId) setActiveConnectionState(connection);
      activeSchema = objectsState.schema;
      activeTable = null;
      activePrimaryKey = null;
      const selected = selectedObjectRow();
      if (selected?.dataset.openTable) selectObjectAsActiveTable();
      renderObjectsView();
    } else setStatusCenter("");
    if (name === "transfer") renderTransferView();
    if (name === "sync") renderSyncView();
    if (name === "compare") renderCompareView();
    if (name === "es") renderEsView();
    if (name === "console") {
      renderConsoleView();
      requestAnimationFrame(() => $("#console-input")?.focus({ preventScroll: true }));
    }
  }

  // Runs a UI action by name. Actions without a visible button go through a transient one so
  // native menu items share the exact code path of the toolbar and context menus.
  function runAction(name) {
    const existing = $$(`[data-action="${name}"]`).find((button) => button.offsetParent && !button.disabled);
    if (existing) {
      existing.click();
      return true;
    }
    const button = document.createElement("button");
    button.dataset.action = name;
    button.hidden = true;
    document.querySelector(".app-shell")?.appendChild(button);
    button.click();
    button.remove();
    return true;
  }

  function closeToolView(name) {
    const tab = $(`.tool-tab[data-workspace="${name}"]`);
    if (tab) tab.hidden = true;
    if (name === "redis") redisTabOpen = false;
    if (name === "es") esTabOpen = false;
    if (currentWorkspace() === name) {
      if (lastExplorerWorkspace === "sql" && activeSqlTab()) activateSqlTab(activeSqlTabId);
      else showWorkspace("objects");
    }
  }

  // ⌘W closes the innermost thing: a dialog, then the current tab. It never closes the window.
  function closeCurrentTab() {
    if (!modalBackdrop.hidden) {
      if (pendingParameterEditor) closeParameterEditor();
      cancelPendingDraftClose();
      closeModal();
      return;
    }
    const view = currentWorkspace();
    if (view === "sql" && activeSqlTabId) closeSqlTab(activeSqlTabId);
    else if (TOOL_VIEWS.includes(view) || view === "redis" || view === "es") closeToolView(view);
  }

  function focusSearch() {
    if (layout.sidebarCollapsed) toggleSidebar(false);
    const input = $("#connection-search");
    input?.focus();
    input?.select();
  }

  function invokeAppCommand(action) {
    const clickAction = runAction;

    switch (action) {
      case "new-connection":
        newConnectionModal();
        return true;
      case "run": {
        if (document.activeElement?.id === "console-input") {
          consoleSubmit();
          return true;
        }
        if (currentWorkspace() !== "sql") return false;
        return clickAction("run");
      }
      case "new-query":
      case "save":
      case "stop":
      case "window-minimize":
      case "window-maximize":
      case "window-close":
        return clickAction(action);
      case "view-explorer":
      case "view-objects":
        showWorkspace("objects");
        return true;
      case "close-tab":
        closeCurrentTab();
        return true;
      case "toggle-sidebar":
        toggleSidebar();
        return true;
      case "toggle-inspector":
        toggleInspector();
        return true;
      case "view-console":
        openConsole(activeConnectionId);
        return true;
      case "view-transfer":
        showWorkspace("transfer");
        return true;
      case "view-sync":
        showWorkspace("sync");
        return true;
      case "view-compare":
        showWorkspace("compare");
        return true;
      case "view-audit":
        showWorkspace("audit");
        return true;
      case "view-settings":
        showWorkspace("settings");
        return true;
      case "focus-search":
        focusSearch();
        return true;
      case "theme-system":
      case "theme-light":
      case "theme-dark":
        applyThemePreference(action.slice("theme-".length));
        return true;
      case "shortcuts":
        infoModal("快捷键", "KEYBOARD", [
          ["运行（选中时只运行选中部分）", "⌘ ↵"],
          ["停止", "⌘ ."],
          ["保存查询", "⌘ S"],
          ["新建连接", "⌘ N"],
          ["新建查询", "⌘ T"],
          ["关闭标签页", "⌘ W"],
          ["对象 / 命令控制台 / 结构比较 / 审计", "⌘ 1 / 2 / 3 / 4"],
          ["数据传输 / 数据同步", "⌘ 5 / 6"],
          ["连接面板 / 信息面板", "⌘ B / ⌥⌘ I"],
          ["搜索连接、库或表", "⌘ K"],
          ["对象列表：打开 / 删除 / 按首字母跳转", "↵ / ⌫ / 直接输入"],
          ["设置", "⌘ ,"],
          ["关闭弹窗", "Esc"],
        ]);
        return true;
      default:
        return runAction(action);
    }
  }

  // Native AppKit menu items call this bridge so macOS key equivalents remain
  // reliable even while Monaco or a form field owns the responder chain.
  window.__luxbaseCommand = (action) => {
    if (typeof action !== "string") return false;
    return invokeAppCommand(action);
  };

  function handleAppShortcut(event) {
    if (event.repeat || window.__luxbaseNativeShortcutMenu) return;
    const isMac = /Mac|iPhone|iPad|iPod/i.test(navigator.platform);
    const primaryModifier = isMac
      ? event.metaKey && !event.ctrlKey
      : event.ctrlKey && !event.metaKey;
    if (!primaryModifier || event.altKey) return;

    const key = event.key.toLowerCase();
    let action = null;
    if (!event.shiftKey) {
      if (key === "enter") action = "run";
      else if (key === ".") action = "stop";
      else if (key === "k") action = "focus-search";
      else if (key === "n") action = "new-connection";
      else if (key === "t") action = "new-query";
      else if (key === "s") action = "save";
      else if (key === "m") action = "window-minimize";
      else if (key === "w") action = "close-tab";
      else if (key === ",") action = "view-settings";
      else if (key === "1") action = "view-objects";
      else if (key === "2") action = "view-console";
      else if (key === "3") action = "view-compare";
      else if (key === "4") action = "view-audit";
      else if (key === "5") action = "view-transfer";
      else if (key === "6") action = "view-sync";
    } else if (key === "/" || key === "?") {
      action = "shortcuts";
    }
    if (!action) return;

    event.preventDefault();
    invokeAppCommand(action);
  }

  function closeAppMenu() {
    if (!appMenu) return;
    appMenu.hidden = true;
    $$('[data-action="more-menu"]').forEach((button) => button.setAttribute("aria-expanded", "false"));
  }

  function menuItem(action, label, shortcut = "") {
    return `<button data-action="${escapeHtml(action)}">${escapeHtml(label)}${shortcut ? `<span class="menu-shortcut">${escapeHtml(shortcut)}</span>` : ""}</button>`;
  }

  function showAppMenu(name, anchor) {
    if (!appMenu) return;
    const menus = {
      more: [
        menuItem("connections-import", "导入连接…"),
        menuItem("connections-export", "导出连接…"),
        "<span class=\"menu-separator\"></span>",
        menuItem("users", "用户与权限"),
        menuItem("diagnostics", "连接诊断"),
        menuItem("security-center", "安全中心"),
        "<span class=\"menu-separator\"></span>",
        menuItem("view-settings", "设置", "⌘,"),
        menuItem("shortcuts", "快捷键", "⇧⌘/"),
        menuItem("app-info", "关于 LuxBase"),
      ],
    };
    if (!menus[name]) return;
    const rect = anchor.getBoundingClientRect();
    appMenu.innerHTML = menus[name].join("");
    appMenu.hidden = false;
    const width = appMenu.offsetWidth || 220;
    appMenu.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - width - 8))}px`;
    appMenu.style.top = `${rect.bottom + 4}px`;
    anchor.setAttribute("aria-expanded", "true");
  }

  function focusedTextControl() {
    const active = document.activeElement;
    if (active instanceof HTMLTextAreaElement) return active;
    if (!(active instanceof HTMLInputElement)) return null;
    const nonTextTypes = new Set([
      "button", "checkbox", "color", "file", "hidden", "image", "radio", "range", "reset", "submit",
    ]);
    return nonTextTypes.has(active.type) ? null : active;
  }

  function monacoHasFocus() {
    return Boolean(document.activeElement?.closest?.("#monaco-editor-host"));
  }

  function selectedTextInContext() {
    if (monacoHasFocus() && typeof window.__luxbaseGetSqlSelection === "function") {
      return window.__luxbaseGetSqlSelection() || "";
    }
    const control = focusedTextControl();
    if (control && control.selectionStart != null && control.selectionEnd != null) {
      return control.value.slice(control.selectionStart, control.selectionEnd);
    }
    return window.getSelection()?.toString() || "";
  }

  function notifyTextChanged(control, text, inputType) {
    try {
      control.dispatchEvent(new InputEvent("input", { bubbles: true, inputType, data: text }));
    } catch {
      control.dispatchEvent(new Event("input", { bubbles: true }));
    }
  }

  function deleteSelectedTextInContext() {
    if (monacoHasFocus() && typeof window.__luxbaseEditorDeleteSelection === "function") {
      window.__luxbaseEditorDeleteSelection();
      return true;
    }
    const control = focusedTextControl();
    if (control) {
      const start = control.selectionStart ?? control.value.length;
      const end = control.selectionEnd ?? start;
      if (start === end) return false;
      control.setRangeText("", start, end, "end");
      notifyTextChanged(control, "", "deleteContentBackward");
      return true;
    }
    const active = document.activeElement;
    if (active?.isContentEditable) {
      return document.execCommand("delete", false);
    }
    return false;
  }

  function insertTextInContext(text) {
    if (typeof text !== "string") return false;
    if (monacoHasFocus() && typeof window.__luxbaseEditorInsertText === "function") {
      window.__luxbaseEditorInsertText(text);
      return true;
    }
    const control = focusedTextControl();
    if (control) {
      const start = control.selectionStart ?? control.value.length;
      const end = control.selectionEnd ?? start;
      control.setRangeText(text, start, end, "end");
      notifyTextChanged(control, text, "insertFromPaste");
      return true;
    }
    const active = document.activeElement;
    if (!active?.isContentEditable) return false;
    const selection = window.getSelection();
    const range = selection?.rangeCount ? selection.getRangeAt(0) : document.createRange();
    if (!selection?.rangeCount) {
      range.selectNodeContents(active);
      range.collapse(false);
    }
    range.deleteContents();
    const node = document.createTextNode(text);
    range.insertNode(node);
    range.setStartAfter(node);
    range.collapse(true);
    selection?.removeAllRanges();
    selection?.addRange(range);
    notifyTextChanged(active, text, "insertFromPaste");
    return true;
  }

  function selectAllInContext() {
    if (monacoHasFocus() && typeof window.__luxbaseEditorAction === "function") {
      window.__luxbaseEditorAction("selectAll");
      return true;
    }
    const control = focusedTextControl();
    if (control) {
      control.focus();
      control.select();
      return true;
    }
    const active = document.activeElement;
    if (active?.isContentEditable) {
      const range = document.createRange();
      range.selectNodeContents(active);
      const selection = window.getSelection();
      selection?.removeAllRanges();
      selection?.addRange(range);
      return true;
    }
    const surface = active?.closest?.(".code-editor, .console-result, .result-grid, .audit-table");
    const range = document.createRange();
    range.selectNodeContents(surface || document.body);
    const selection = window.getSelection();
    selection?.removeAllRanges();
    selection?.addRange(range);
    return true;
  }

  // The standalone macOS host uses these hooks for native Edit-menu commands.
  window.__luxbaseNativeGetSelection = selectedTextInContext;
  window.__luxbaseNativeDeleteSelection = deleteSelectedTextInContext;
  window.__luxbaseNativeInsertText = insertTextInContext;
  window.__luxbaseNativeSelectAll = selectAllInContext;

  function runEditorAction(action) {
    if (document.activeElement?.closest?.(".grid-wrap") && !cellEditor && gridState.columns.length) {
      if (action === "copy") {
        copyGridSelection("tsv");
        return;
      }
      if (action === "selectAll") {
        gridState.selection = { mode: "all" };
        refreshSelection();
        return;
      }
    }
    if (action === "selectAll") {
      selectAllInContext();
      return;
    }
    const control = focusedTextControl();
    const editable = control || document.activeElement?.isContentEditable;
    if (editable && !monacoHasFocus()) {
      if (action === "copy" || action === "cut") {
        const text = selectedTextInContext();
        if (!text) return;
        if (navigator.clipboard?.writeText) {
          void navigator.clipboard.writeText(text)
            .then(() => { if (action === "cut") deleteSelectedTextInContext(); })
            .catch(() => {
              if (document.execCommand(action) && action === "cut") deleteSelectedTextInContext();
            });
        } else if (document.execCommand(action) && action === "cut") {
          deleteSelectedTextInContext();
        }
        return;
      }
      if (action === "paste") {
        if (navigator.clipboard?.readText) {
          void navigator.clipboard.readText()
            .then((text) => insertTextInContext(text))
            .catch(() => toast("无法读取剪贴板", "请使用 ⌘V，或检查系统的剪贴板权限。", "danger"));
        } else {
          document.execCommand("paste");
        }
        return;
      }
    }
    if (typeof window.__luxbaseEditorAction === "function") {
      window.__luxbaseEditorAction(action);
      return;
    }
    if ((action === "copy" || action === "cut") && selectedTextInContext()) {
      document.execCommand(action);
      return;
    }
    toast("SQL 编辑器不可用", "请选择 SQL 连接并等待编辑器加载后再试。", "danger");
  }

  let connectionFilter = "";

  function filterConnections(query) {
    connectionFilter = String(query || "").trim().toLowerCase();
    applyConnectionFilter();
  }

  // Matches connection names, database names and the names of already-loaded tables, and
  // expands a database whose tables match so the hit is visible.
  function applyConnectionFilter() {
    const needle = connectionFilter;
    let visible = 0;
    $$(".connection-group").forEach((group) => {
      const groupMatch = !needle || ($(".connection-copy", group)?.textContent || "").toLowerCase().includes(needle);
      let childMatch = false;
      $$(".database-node", group).forEach((node) => {
        const database = ($("[data-sql-database]", node)?.dataset.sqlDatabase || "").toLowerCase();
        const nodeMatch = groupMatch || (database && database.includes(needle));
        let rowMatch = false;
        $$("[data-open-table]", node).forEach((row) => {
          const hit = row.dataset.openTable.toLowerCase().includes(needle);
          row.hidden = Boolean(needle) && !nodeMatch && !hit;
          if (needle && hit) rowMatch = true;
        });
        node.hidden = Boolean(needle) && !nodeMatch && !rowMatch;
        if (needle && rowMatch && !nodeMatch) {
          const children = $(".tree-grandchildren", node);
          if (children) children.hidden = false;
          $(".tree-chevron", $("[data-sql-database]", node) || node)?.classList.add("open");
        }
        if (!node.hidden && (nodeMatch || rowMatch)) childMatch = true;
      });
      group.hidden = Boolean(needle) && !groupMatch && !childMatch;
      if (!group.hidden) visible += 1;
    });
    const count = $(".sidebar-search .search-count");
    if (count) count.textContent = String(visible);
  }

  function infoModal(title, eyebrow, rows, extra = "") {
    showModal(title, eyebrow, `
      <div class="key-info-grid">${rows.map(([label, value]) => `<div><span>${escapeHtml(label)}</span><code>${escapeHtml(value)}</code></div>`).join("")}</div>
      ${extra}
      <div class="modal-footer"><button class="secondary-button" data-close-modal>关闭</button></div>
    `);
  }

  function legacyImportSummary(report) {
    if (!report) return "";
    const parts = [
      `导入 ${report.imported ?? 0} 条`,
      `保险库里已有 ${report.keptExisting ?? 0} 条`,
      `钥匙串里没有 ${report.missing ?? 0} 条`,
    ];
    if (report.accessDenied) parts.push(`系统拒绝 ${report.accessDenied} 条`);
    if (report.keychainCleanupFailed) parts.push(`钥匙串残留 ${report.keychainCleanupFailed} 条`);
    if (report.configKeyImported) parts.unshift("已接回旧主密钥");
    return parts.join(" · ");
  }

  // Older LuxBase Studio builds kept the master key and every password in the OS keychain, which made
  // macOS ask for permission on each launch. The vault replaced that, so keychain access now
  // happens only through this button, once, and the imported items are deleted afterwards.
  function showSecurityCenter() {
    infoModal("安全中心", "SECURITY", [
      ["本地保险库", securityState?.localVaultAvailable ? "可用" : "不可用或未连接后端"],
      ["密码存储", "本机加密保险库（启动不访问系统钥匙串）"],
      ["连接配置", securityState?.encryptionAtRest ? "AES-256-GCM 加密" : "等待安全后端"],
      ["审计日志", securityState?.auditAppendOnly ? "只追加 + SHA-256 链" : "等待安全后端"],
      ["说明", securityState
        ? (securityState.localVaultAvailable
          ? "主密钥在数据目录的 vault.key（0600），密码与 SQL 历史加密写入本地 SQLite"
          : "LuxBase 数据目录不可读写，密码与 SQL 历史暂时无法保存；请恢复数据目录权限后重试。")
        : "界面预览不执行任何数据库或安全操作。"],
    ], `
      <div class="recovery-block">
        <strong>从系统钥匙串找回旧密码</strong>
        <p class="muted-text">如果你用过旧版 LuxBase Studio，旧密码可能还留在系统钥匙串里。点击后 macOS 会询问一次授权；导入成功的条目会从钥匙串删除，之后启动不会再弹窗。不需要找回可以完全不点。</p>
        ${lastLegacySecretImport ? `<p class="recovery-result">上次结果：${escapeHtml(legacyImportSummary(lastLegacySecretImport))}</p>` : ""}
        <button class="secondary-button" data-action="legacy-secret-import" ${backendAvailable ? "" : "disabled"}>导入钥匙串里的旧密码</button>
      </div>
    `);
  }

  function setResultTab(name) {
    const index = activeSqlTab()?.activeResult || 0;
    $$(".result-tab").forEach((button) => button.classList.toggle("active", button.dataset.resultTab === name
      && (name !== "grid" || button.dataset.resultIndex == null || Number(button.dataset.resultIndex) === index)));
    $$(".result-content").forEach((panel) => panel.classList.toggle("active", panel.dataset.resultContent === name));
  }

  function setRedisTab(name) {
    $$(".redis-tab").forEach((tab) => tab.classList.toggle("active", tab.dataset.redisTab === name));
    $$(".redis-value-panel").forEach((panel) => panel.classList.toggle("active", panel.dataset.redisContent === name));
    if (name === "info" && currentRedisValue && activeConnectionId) {
      getRedisInfo(activeConnectionId)
        .then((info) => renderRedisInfoGrid(currentRedisValue, info))
        .catch(() => renderRedisInfoGrid(currentRedisValue, null));
    }
  }

  function splitSqlStatements(sql) {
    const statements = [];
    let current = "";
    let quote = null;
    let dollarTag = null;
    let lineComment = false;
    let blockComment = false;
    let hasCode = false;
    for (let index = 0; index < sql.length; index += 1) {
      const char = sql[index];
      const next = sql[index + 1];
      if (dollarTag) {
        if (sql.startsWith(dollarTag, index)) {
          current += dollarTag;
          index += dollarTag.length - 1;
          dollarTag = null;
        } else {
          current += char;
        }
        continue;
      }
      if (lineComment) {
        current += char;
        if (char === "\n") lineComment = false;
        continue;
      }
      if (blockComment) {
        current += char;
        if (char === "*" && next === "/") {
          current += next;
          index += 1;
          blockComment = false;
        }
        continue;
      }
      if (quote) {
        current += char;
        if (char === quote) {
          if (next === quote) {
            current += next;
            index += 1;
          } else {
            quote = null;
          }
        } else if (char === "\\" && quote !== "`") {
          current += next ?? "";
          index += 1;
        }
        continue;
      }
      if ((char === "-" && next === "-") || char === "#") {
        current += char === "#" ? char : char + next;
        if (char === "-") index += 1;
        lineComment = true;
        continue;
      }
      if (char === "/" && next === "*") {
        current += char + next;
        index += 1;
        blockComment = true;
        continue;
      }
      if (char === "$") {
        const tag = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
        if (tag) {
          dollarTag = tag;
          hasCode = true;
          current += tag;
          index += tag.length - 1;
          continue;
        }
      }
      if (char === "'" || char === '"' || char === "`") {
        quote = char;
        hasCode = true;
        current += char;
        continue;
      }
      if (char === ";") {
        if (hasCode && current.trim()) statements.push(current.trim());
        current = "";
        hasCode = false;
        continue;
      }
      if (!/\s/.test(char)) hasCode = true;
      current += char;
    }
    if (hasCode && current.trim()) statements.push(current.trim());
    return statements;
  }

  function formatSqlText(sql) {
    const keywords = new Set([
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER",
      "ON", "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "ALL",
      "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "ALTER", "DROP",
      "TABLE", "VIEW", "INDEX", "AND", "OR", "AS", "DISTINCT", "CASE", "WHEN", "THEN",
      "ELSE", "END", "WITH", "RETURNING", "ASC", "DESC", "NULL", "NOT", "IN", "EXISTS",
      "IS", "LIKE", "BETWEEN", "BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
    ]);
    const breakBefore = new Set([
      "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION",
      "INSERT", "UPDATE", "DELETE", "VALUES", "RETURNING", "LEFT", "RIGHT", "FULL",
      "INNER", "JOIN",
    ]);
    const tokens = [];
    let index = 0;
    while (index < sql.length) {
      const char = sql[index];
      const next = sql[index + 1];
      if (/\s/.test(char)) {
        index += 1;
        continue;
      }
      if (char === "-" && next === "-") {
        let end = sql.indexOf("\n", index + 2);
        if (end < 0) end = sql.length;
        tokens.push({ kind: "comment", value: sql.slice(index, end).trimEnd() });
        index = end;
        continue;
      }
      if (char === "/" && next === "*") {
        const end = sql.indexOf("*/", index + 2);
        const finish = end < 0 ? sql.length : end + 2;
        tokens.push({ kind: "comment", value: sql.slice(index, finish) });
        index = finish;
        continue;
      }
      if (char === "'" || char === '"' || char === "`") {
        const quote = char;
        let finish = index + 1;
        while (finish < sql.length) {
          if (sql[finish] === quote) {
            if (sql[finish + 1] === quote) {
              finish += 2;
              continue;
            }
            finish += 1;
            break;
          }
          if (sql[finish] === "\\" && quote !== "`") finish += 2;
          else finish += 1;
        }
        tokens.push({ kind: "literal", value: sql.slice(index, finish) });
        index = finish;
        continue;
      }
      if (char === "$") {
        const tag = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
        if (tag) {
          const end = sql.indexOf(tag, index + tag.length);
          const finish = end < 0 ? sql.length : end + tag.length;
          tokens.push({ kind: "literal", value: sql.slice(index, finish) });
          index = finish;
          continue;
        }
      }
      if (/[A-Za-z_]/.test(char)) {
        let finish = index + 1;
        while (finish < sql.length && /[A-Za-z0-9_$]/.test(sql[finish])) finish += 1;
        const value = sql.slice(index, finish);
        tokens.push({ kind: "word", value: keywords.has(value.toUpperCase()) ? value.toUpperCase() : value });
        index = finish;
        continue;
      }
      if (/[0-9]/.test(char)) {
        let finish = index + 1;
        while (finish < sql.length && /[0-9._]/.test(sql[finish])) finish += 1;
        tokens.push({ kind: "number", value: sql.slice(index, finish) });
        index = finish;
        continue;
      }
      if (char === ";") {
        tokens.push({ kind: "separator", value: ";" });
        index += 1;
        continue;
      }
      tokens.push({ kind: "punct", value: char });
      index += 1;
    }
    const lines = [];
    let line = "";
    const flush = () => {
      if (line.trim()) lines.push(line.trimEnd());
      line = "";
    };
    for (let tokenIndex = 0; tokenIndex < tokens.length; tokenIndex += 1) {
      const token = tokens[tokenIndex];
      if (token.kind === "separator") {
        line += ";";
        flush();
        continue;
      }
      if (token.kind === "comment") {
        if (line.trim()) flush();
        lines.push(token.value);
        continue;
      }
      const upper = token.kind === "word" ? token.value.toUpperCase() : "";
      if (breakBefore.has(upper) && line.trim()) flush();
      if (upper === "AND" || upper === "OR") {
        if (line.trim()) flush();
        line = `  ${token.value} `;
        continue;
      }
      if (!line) line = token.value;
      else if ([",", ")", "]"].includes(token.value) || [".", "(", "["].includes(line.slice(-1))) line += token.value;
      else line += ` ${token.value}`;
    }
    flush();
    return lines.join("\n");
  }

  function parameterCount(sql) {
    let numberedMax = 0;
    let positional = 0;
    let index = 0;
    let quote = null;
    let dollarTag = null;
    let lineComment = false;
    let blockComment = false;
    while (index < sql.length) {
      const char = sql[index];
      const next = sql[index + 1];
      if (dollarTag) {
        if (sql.startsWith(dollarTag, index)) {
          index += dollarTag.length;
          dollarTag = null;
        } else {
          index += 1;
        }
        continue;
      }
      if (lineComment) {
        if (char === "\n") lineComment = false;
        index += 1;
        continue;
      }
      if (blockComment) {
        if (char === "*" && next === "/") {
          blockComment = false;
          index += 2;
        } else {
          index += 1;
        }
        continue;
      }
      if (quote) {
        if (char === quote) {
          if (next === quote) index += 2;
          else {
            quote = null;
            index += 1;
          }
        } else if (char === "\\" && quote !== "`") {
          index += 2;
        } else {
          index += 1;
        }
        continue;
      }
      if ((char === "-" && next === "-") || char === "#") {
        lineComment = true;
        index += char === "#" ? 1 : 2;
        continue;
      }
      if (char === "/" && next === "*") {
        blockComment = true;
        index += 2;
        continue;
      }
      if (char === "'" || char === '"' || char === "`") {
        quote = char;
        index += 1;
        continue;
      }
      if (char === "$") {
        const tag = sql.slice(index).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0];
        if (tag) {
          dollarTag = tag;
          index += tag.length;
          continue;
        }
        const match = sql.slice(index).match(/^\$(\d+)/);
        if (match) numberedMax = Math.max(numberedMax, Number(match[1]));
      }
      if (char === "?") positional += 1;
      index += 1;
    }
    return Math.max(numberedMax, positional);
  }

  function parseBoundParameters(count) {
    if (!count) return Promise.resolve([]);
    return new Promise((resolve, reject) => {
      pendingParameterEditor = { resolve, reject, count };
      showModal("绑定参数", "BOUND PARAMETERS", `
        <div class="form-note" style="margin-top:0"><svg><use href="#i-shield"/></svg>检测到 ${count} 个位置参数。所有值按 JSON 解析后由驱动绑定，不拼接进 SQL。</div>
        <div class="form-grid">
          <label class="full">参数 JSON 数组<textarea id="parameter-json" class="modal-textarea" spellcheck="false" placeholder='["2026-11-01", 1000]'>[]</textarea></label>
        </div>
        <div id="parameter-error" class="form-note danger-note" role="alert" hidden></div>
        <div class="modal-footer"><button class="secondary-button" data-action="cancel-parameters">取消</button><button class="primary-button" data-action="confirm-parameters">确认并执行</button></div>
      `);
      setTimeout(() => $("#parameter-json")?.focus(), 30);
    });
  }

  function closeParameterEditor(message = "已取消参数输入，查询未发送。") {
    const pending = pendingParameterEditor;
    pendingParameterEditor = null;
    closeModal();
    pending?.reject(new Error(message));
  }

  function confirmParameterEditor() {
    const pending = pendingParameterEditor;
    if (!pending) return;
    let values;
    try {
      values = JSON.parse($("#parameter-json")?.value || "[]");
      if (!Array.isArray(values) || values.length !== pending.count) {
        throw new Error(`需要 ${pending.count} 个绑定参数，实际收到 ${Array.isArray(values) ? values.length : 0} 个。`);
      }
    } catch (error) {
      const box = $("#parameter-error");
      if (box) {
        box.hidden = false;
        box.textContent = errorMessage(error);
      }
      return;
    }
    const params = values.map((value) => {
      if (value === null) return { kind: "null" };
      if (typeof value === "boolean") return { kind: "bool", value };
      if (typeof value === "number") return Number.isSafeInteger(value)
        ? { kind: value < 0 ? "i64" : "u64", value }
        : { kind: "f64", value };
      if (typeof value === "string") return { kind: "text", value };
      return { kind: "json", value };
    });
    pendingParameterEditor = null;
    closeModal();
    pending.resolve(params);
  }

  async function refreshPoolMetrics() {
    if (!activeConnectionId || activeFamily !== "sql") return;
    try {
      const metrics = await backendCall("sql_pool_metrics", { connectionId: activeConnectionId });
      const metricGrid = $(".metric-grid");
      if (metricGrid) {
        metricGrid.innerHTML = `<div><span>Max size</span><b>${metrics.maxSize}</b></div><div><span>Waiters</span><b>${metrics.waiters}</b></div><div><span>Timeouts</span><b>${metrics.timeouts}</b></div><div><span>Acquired</span><b>${metrics.acquiredTotal}</b></div>`;
      }
      const poolStatus = $$(".status-item").find((item) => item.textContent?.startsWith("Pool"));
      if (poolStatus) poolStatus.textContent = `Pool ${metrics.active} / ${metrics.total} / ${metrics.maxSize}`;
      const donut = $(".donut");
      if (donut) donut.innerHTML = `<div><b>${metrics.total}</b><span>total</span></div>`;
      const legendValues = $$(".pool-legend b");
      if (legendValues.length >= 3) {
        legendValues[0].textContent = String(metrics.active);
        legendValues[1].textContent = String(metrics.idle);
        legendValues[2].textContent = String(Math.max(0, metrics.maxSize - metrics.total));
      }
    } catch {
      // Metrics are advisory; a closed pool simply keeps the placeholders.
    }
  }

  function resetInspectorHealth(connection) {
    const kv = protocolFamily(connection?.protocol) === "kv";
    $(".inspector-health")?.classList.toggle("kv-mode", kv);
    const metricGrid = $(".metric-grid");
    if (metricGrid) metricGrid.innerHTML = '<div><span>Max size</span><b>—</b></div><div><span>Waiters</span><b>—</b></div><div><span>Timeouts</span><b>—</b></div><div><span>Acquired</span><b>—</b></div>';
    const donut = $(".donut");
    if (donut) donut.innerHTML = "<div><b>—</b><span>total</span></div>";
    $$(".pool-legend b").forEach((value) => { value.textContent = "—"; });
    const guards = $(".metric-section.guardrails");
    if (guards) {
      const rows = kv
        ? [["危险命令二次确认", "启用"], ["KEYS 已禁用（使用 SCAN）", "强制"], ["批量删除需确认", "启用"]]
        : [["SQL 参数绑定", "强制"], ["多语句事务回滚", "启用"], ["危险操作确认", "启用"]];
      guards.innerHTML = `<div class="section-title"><span>安全护栏</span><span class="shield-mini"><svg><use href="#i-shield"/></svg></span></div>${rows.map(([label, value]) => `<div class="guard-row"><svg><use href="#i-check"/></svg><span>${label}</span><b>${value}</b></div>`).join("")}`;
    }
  }

  async function stopExecution() {
    if (activeQueryExport) {
      try { await stopQueryExport(); } catch (error) { showOperationError(error, "停止导出失败"); }
      return;
    }
    if (activeFamily === "kv" && activeConnectionId && activeRedisOperationId) {
      if (activeConnection?.redisMode === "cluster") {
        toast(
          "Redis Cluster 暂不支持服务端取消",
          "当前版本只能取消本地等待，不能确认集群节点已终止命令，因此不会发送误导性的成功提示。",
          "danger",
        );
        return;
      }
      try {
        await backendCall("redis_cancel", {
          connectionId: activeConnectionId,
          operationId: activeRedisOperationId,
        });
        toast("Redis 取消请求已发送", "底层连接已收到终止请求。", "danger");
      } catch (error) {
        showOperationError(error, "Redis 取消失败");
      }
      return;
    }
    const runningTab = activeSqlTab();
    const cancelConnection = runningTab?.connectionId || activeConnectionId;
    const cancelOperation = runningTab?.operationId || activeSqlOperationId;
    if (!cancelConnection || !cancelOperation) {
      toast("没有运行中的操作", "停止请求未发送。", "danger");
      return;
    }
    try {
      await backendCall("sql_cancel", {
        connectionId: cancelConnection,
        operationId: cancelOperation,
      });
      toast("取消请求已发送", "驱动层正在终止查询；事务会先回滚再释放连接。", "danger");
      const time = $(".execution-time");
      if (time) time.innerHTML = `<span class="live-dot" style="background:#d88b12"></span>正在取消`;
    } catch (error) {
      showOperationError(error, "取消失败");
    }
  }

  function newConnectionModal(connection = null) {
    const editing = Boolean(connection);
    showModal(editing ? "编辑数据库连接" : "新建数据库连接", editing ? "EDIT SECURE CONNECTION" : "SECURE CONNECTION", `
      <input type="hidden" id="conn-id" value="${escapeHtml(connection?.id || "")}" />
      <input type="hidden" id="conn-protocol" value="${escapeHtml(connection?.protocol || "mysql")}" />
      <div class="protocol-picker" id="protocol-picker">
        ${Object.entries(protocolMeta).map(([key, meta]) => `<button class="protocol-card ${key === (connection?.protocol || "mysql") ? "active" : ""}" data-protocol-card="${key}">${protocolBadgeHtml(key)}<span>${meta.label}</span></button>`).join("")}
      </div>
      <div class="form-grid" id="connection-form">
        <label class="full">连接名称<input id="conn-name" value="${escapeHtml(connection?.name || "")}" placeholder="例如：生产只读库" /></label>
        <label class="full" data-network-field>主机 / SQLite 文件<input id="conn-host" value="${escapeHtml(connection?.filePath || connection?.host || "")}" placeholder="数据库主机或 SQLite 文件路径" /></label>
        <label data-network-field>端口<input id="conn-port" type="number" value="${connection?.port || protocolMeta.mysql.defaults.port}" /></label>
        <label data-sql-only>数据库<input id="conn-database" value="${escapeHtml(connection?.database || "")}" /></label>
        <label class="full" data-es-field hidden>认证方式<select id="es-auth-mode">${["none:无认证", "basic:用户名 / 密码", "apiKey:API Key"].map((item) => { const [value, label] = item.split(":"); return `<option value="${value}" ${(connection?.esAuthMode || "basic") === value ? "selected" : ""}>${label}</option>`; }).join("")}</select></label>
        <label data-auth-field id="conn-username-label">用户名<input id="conn-username" value="${escapeHtml(connection?.username || "")}" autocomplete="username" /></label>
        <label data-auth-field id="conn-password-label">密码<input id="conn-password" type="password" placeholder="${editing ? "留空则保留已保存密码" : "写入本地保险库"}" autocomplete="current-password" /></label>
        <label class="full" data-sql-only>SSH 隧道<select id="ssh-enabled"><option value="false" ${connection?.ssh ? "" : "selected"}>不使用</option><option value="true" ${connection?.ssh ? "selected" : ""}>使用 SSH</option></select></label>
        <div class="full nested-fields" id="ssh-fields" ${connection?.ssh ? "" : "hidden"}>
          <label>SSH 主机<input id="ssh-host" value="${escapeHtml(connection?.ssh?.host || "")}" placeholder="jump.example.internal" /></label>
          <label>SSH 端口<input id="ssh-port" type="number" value="${connection?.ssh?.port || 22}" /></label>
          <label>SSH 用户<input id="ssh-username" value="${escapeHtml(connection?.ssh?.username || "")}" /></label>
          <label>私钥路径<input id="ssh-key-path" value="${escapeHtml(connection?.ssh?.privateKeyPath || "")}" placeholder="~/.ssh/id_ed25519" /></label>
          <label>私钥口令<input id="ssh-passphrase" type="password" placeholder="${editing ? "留空则保留" : "写入本地保险库"}" autocomplete="new-password" /></label>
          <label>SSH 密码<input id="ssh-password" type="password" placeholder="${editing ? "留空则保留；无密钥时使用" : "无密钥时使用；写入本地保险库"}" autocomplete="new-password" /></label>
          <label>known_hosts 路径<input id="ssh-known-hosts" value="${escapeHtml(connection?.ssh?.knownHostsPath || "")}" placeholder="~/.ssh/known_hosts" /></label>
          <label class="check-line"><input id="ssh-strict" type="checkbox" ${connection?.ssh?.strictHostKeyChecking === false ? "" : "checked"} />严格校验主机密钥</label>
        </div>
        <label class="full" data-tls-field>TLS<select id="tls-mode">${["disabled:不使用", "preferred:优先", "required:必需", "verify_full:验证证书"].map((item) => { const [value, label] = item.split(":"); return `<option value="${value}" ${connection?.tlsMode === value ? "selected" : ""}>${label}</option>`; }).join("")}</select></label>
        <div class="full form-note" id="redis-tls-note" hidden>Redis TLS 使用系统信任根；自定义 CA 或客户端证书会在前端校验时明确拒绝。</div>
        <div class="full nested-fields" id="tls-fields" ${connection?.tlsMode && connection.tlsMode !== "disabled" ? "" : "hidden"}>
          <label>CA 证书路径<input id="tls-ca" value="${escapeHtml(connection?.caCertPath || "")}" /></label>
          <label>客户端证书路径（含私钥 PEM）<input id="tls-cert" value="${escapeHtml(connection?.clientCertPath || "")}" /></label>
        </div>
        <label class="full" data-redis-field hidden>Redis 模式<select id="redis-mode">${["standalone:单机", "sentinel:哨兵", "cluster:集群"].map((item) => { const [value, label] = item.split(":"); return `<option value="${value}" ${(connection?.redisMode || "standalone") === value ? "selected" : ""}>${label}</option>`; }).join("")}</select></label>
        <label data-redis-field hidden>DB 编号<input id="redis-db" type="number" value="${connection?.redisDb ?? 0}" min="0" /></label>
        <label data-redis-field hidden>Sentinel master<input id="sentinel-master" value="${escapeHtml(connection?.sentinelMasterName || "")}" placeholder="mymaster" /></label>
        <label class="full" data-redis-field hidden>哨兵节点<input id="sentinel-nodes" value="${escapeHtml((connection?.sentinels || []).map((node) => `${node.host}:${node.port}`).join(", "))}" placeholder="host1:26379, host2:26379" /></label>
        <label class="full" data-redis-field hidden>集群节点<input id="cluster-nodes" value="${escapeHtml((connection?.clusterNodes || []).join(", "))}" placeholder="host1:6379, host2:6379" /></label>
        <label class="full">超时（毫秒）<input id="query-timeout" type="number" value="${connection?.queryTimeoutMs || 60000}" min="1000" /></label>
        <label class="full">连接池
          <div class="form-grid" style="grid-template-columns:repeat(5,1fr);gap:6px">
            <input id="pool-min-idle" type="number" value="${connection?.pool?.minIdle ?? 2}" min="0" aria-label="min_idle" title="min_idle" />
            <input id="pool-max-size" type="number" value="${connection?.pool?.maxSize ?? 20}" min="1" aria-label="max_size" title="max_size" />
            <input id="pool-conn-timeout" type="number" value="${connection?.pool?.connectionTimeoutMs ?? 10000}" min="100" aria-label="connection_timeout_ms" title="connection_timeout_ms" />
            <input id="pool-idle-timeout" type="number" value="${connection?.pool?.idleTimeoutMs ?? 300000}" min="1000" aria-label="idle_timeout_ms" title="idle_timeout_ms" />
            <input id="pool-max-lifetime" type="number" value="${connection?.pool?.maxLifetimeMs ?? 1800000}" min="1000" aria-label="max_lifetime_ms" title="max_lifetime_ms" />
          </div>
        </label>
      </div>
      <div id="connection-form-error" class="form-note danger-note" role="alert" hidden></div>
      <div id="connection-test-result" class="form-note test-result" role="status" hidden></div>
      <div class="form-note"><svg><use href="#i-lock"/></svg>主密码、SSH 密码与私钥口令加密保存在本机 LuxBase 数据目录的本地保险库中；连接配置同样以 AES-256-GCM 加密后写入本地 SQLite，全程不访问系统钥匙串。</div>
      <div class="modal-footer"><button class="secondary-button" data-action="test-connection">测试连接</button><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="save-connection">${editing ? "保存修改" : "保存连接"}</button></div>
    `);
    setProtocolInForm(connection?.protocol || "mysql", { resetDefaults: !editing });
  }

  function setProtocolInForm(protocol, options = {}) {
    $$("[data-protocol-card]").forEach((card) => card.classList.toggle("active", card.dataset.protocolCard === protocol));
    const meta = protocolMeta[protocol];
    const hiddenProtocol = $("#conn-protocol");
    if (hiddenProtocol) hiddenProtocol.value = protocol;
    const host = $("#conn-host");
    const port = $("#conn-port");
    if (options.resetDefaults !== false) {
      if (host) host.value = meta.defaults.host;
      if (port) port.value = meta.defaults.port;
    }
    const family = protocolFamily(protocol);
    $$("[data-sql-only]").forEach((field) => { field.hidden = family !== "sql"; });
    $$("[data-auth-field]").forEach((field) => { field.hidden = protocol === "sqlite"; });
    $$("[data-redis-field]").forEach((field) => { field.hidden = family !== "kv"; });
    $$("[data-tls-field]").forEach((field) => { field.hidden = protocol === "sqlite"; });
    const tls = $("#tls-mode");
    if (tls) tls.disabled = protocol === "sqlite";
    if (protocol === "sqlite") {
      const label = host?.closest("label");
      if (label?.firstChild) label.firstChild.textContent = "SQLite 文件";
      if (host) host.placeholder = "/path/to/database.sqlite";
    } else {
      const label = host?.closest("label");
      if (label?.firstChild) label.firstChild.textContent = "主机 / SQLite 文件";
      if (host) host.placeholder = "db.example.internal";
    }
    if (family === "kv") {
      const mode = $("#redis-mode")?.value || "standalone";
      const sentinel = $("#sentinel-master")?.closest("label");
      const sentinelNodes = $("#sentinel-nodes")?.closest("label");
      const clusterNodes = $("#cluster-nodes")?.closest("label");
      if (sentinel) sentinel.hidden = mode !== "sentinel";
      if (sentinelNodes) sentinelNodes.hidden = mode !== "sentinel";
      if (clusterNodes) clusterNodes.hidden = mode !== "cluster";
    }
    const sshFields = $("#ssh-fields");
    if (sshFields) sshFields.hidden = $("#ssh-enabled")?.value !== "true";
    const tlsFields = $("#tls-fields");
    if (tlsFields) tlsFields.hidden = !$("#tls-mode")?.value || $("#tls-mode")?.value === "disabled";
    const redisTlsNote = $("#redis-tls-note");
    if (redisTlsNote) redisTlsNote.hidden = family !== "kv";
    $$("[data-es-field]").forEach((field) => { field.hidden = protocol !== "elasticsearch"; });
    if (protocol === "elasticsearch") updateEsAuthFields();
  }

  function updateEsAuthFields() {
    const mode = $("#es-auth-mode")?.value || "basic";
    const usernameLabel = $("#conn-username-label");
    const passwordLabel = $("#conn-password-label");
    const password = $("#conn-password");
    if (usernameLabel) usernameLabel.hidden = mode !== "basic";
    if (passwordLabel) {
      passwordLabel.hidden = mode === "none";
      if (passwordLabel.firstChild) passwordLabel.firstChild.textContent = mode === "apiKey" ? "API Key" : "密码";
    }
    if (password) password.placeholder = mode === "apiKey" ? "Base64 编码的 id:api_key" : "写入本地保险库";
  }

  function numberValue(id, fallback) {
    const value = Number($(id)?.value);
    return Number.isFinite(value) && value >= 0 ? value : fallback;
  }

  function parseNodes(text) {
    return String(text || "")
      .split(",")
      .map((node) => node.trim())
      .filter(Boolean)
      .map((node) => {
        const separator = node.lastIndexOf(":");
        if (separator <= 0) throw new Error(`节点格式无效：${node}。请使用 host:port。`);
        const port = Number(node.slice(separator + 1));
        if (!Number.isInteger(port) || port <= 0 || port > 65535) throw new Error(`节点端口无效：${node}`);
        return { host: node.slice(0, separator), port };
      });
  }

  function connectionPayload() {
    const protocol = $("#conn-protocol")?.value || "mysql";
    const mode = $("#redis-mode")?.value || "standalone";
    const sshEnabled = $("#ssh-enabled")?.value === "true";
    const tlsMode = protocol === "sqlite" ? "disabled" : ($("#tls-mode")?.value || "disabled");
    const sentinelNodes = mode === "sentinel" ? parseNodes($("#sentinel-nodes")?.value) : [];
    const clusterNodes = mode === "cluster" ? parseNodes($("#cluster-nodes")?.value).map((node) => `${node.host}:${node.port}`) : [];
    const id = $("#conn-id")?.value?.trim() || globalThis.crypto?.randomUUID?.() || `conn-${Date.now()}`;
    const config = {
      id,
      name: $("#conn-name")?.value?.trim() || "",
      protocol,
      host: $("#conn-host")?.value?.trim() || "",
      port: numberValue("#conn-port", 0),
      username: $("#conn-username")?.value?.trim() || "",
      database: $("#conn-database")?.value?.trim() || "",
      filePath: protocol === "sqlite" ? ($("#conn-host")?.value?.trim() || null) : null,
      tlsMode,
      caCertPath: $("#tls-ca")?.value?.trim() || null,
      clientCertPath: $("#tls-cert")?.value?.trim() || null,
      ssh: sshEnabled ? {
        host: $("#ssh-host")?.value?.trim() || "",
        port: numberValue("#ssh-port", 22),
        username: $("#ssh-username")?.value?.trim() || "",
        privateKeyPath: $("#ssh-key-path")?.value?.trim() || null,
        knownHostsPath: $("#ssh-known-hosts")?.value?.trim() || null,
        strictHostKeyChecking: Boolean($("#ssh-strict")?.checked),
      } : null,
      redisMode: protocol === "redis" ? mode : null,
      sentinels: sentinelNodes,
      sentinelMasterName: mode === "sentinel" ? ($("#sentinel-master")?.value?.trim() || null) : null,
      clusterNodes,
      redisDb: numberValue("#redis-db", 0),
      esAuthMode: protocol === "elasticsearch" ? ($("#es-auth-mode")?.value || "basic") : null,
      pool: {
        minIdle: numberValue("#pool-min-idle", 2),
        maxSize: numberValue("#pool-max-size", 20),
        connectionTimeoutMs: numberValue("#pool-conn-timeout", 10000),
        idleTimeoutMs: numberValue("#pool-idle-timeout", 300000),
        maxLifetimeMs: numberValue("#pool-max-lifetime", 1800000),
        healthCheckTimeoutMs: 3000,
      },
      queryTimeoutMs: numberValue("#query-timeout", 60000),
    };
    return {
      config,
      password: $("#conn-password")?.value || null,
      sshPassphrase: sshEnabled ? ($("#ssh-passphrase")?.value || null) : null,
      sshPassword: sshEnabled ? ($("#ssh-password")?.value || null) : null,
    };
  }

  function showFormError(message) {
    const box = $("#connection-form-error");
    if (box) {
      box.hidden = !message;
      box.textContent = message || "";
    }
    if (message) {
      showTestResult(false, message);
      toast("连接配置有误", message, "danger");
    }
  }

  function showTestResult(ok, message) {
    const box = $("#connection-test-result");
    if (!box) return;
    box.hidden = !message;
    box.classList.toggle("success", Boolean(ok));
    box.classList.toggle("danger", !ok);
    box.innerHTML = message
      ? `<svg><use href="#i-${ok ? "check" : "warning"}"/></svg><span>${escapeHtml(message)}</span>`
      : "";
  }

  function validateConnectionPayload(payload) {
    const config = payload.config;
    if (!config.name) return "请填写连接名称。";
    if (config.protocol === "sqlite") {
      if (!config.filePath) return "请填写 SQLite 数据库文件路径。";
    } else if (
      config.protocol !== "redis"
      || config.redisMode === "standalone"
      || config.redisMode == null
    ) {
      if (!config.host) return "请填写数据库主机。";
      if (!Number.isInteger(config.port) || config.port <= 0 || config.port > 65535) return "数据库端口必须在 1-65535 之间。";
    }
    if (config.ssh) {
      if (!config.ssh.host) return "请填写 SSH 主机。";
      if (!config.ssh.username) return "请填写 SSH 用户名。";
      if (!Number.isInteger(config.ssh.port) || config.ssh.port <= 0 || config.ssh.port > 65535) return "SSH 端口必须在 1-65535 之间。";
      const editingExisting = Boolean($("#conn-id")?.value);
      if (!config.ssh.privateKeyPath && !payload.sshPassword && !editingExisting) {
        return "SSH 认证需要私钥路径或 SSH 密码。";
      }
    }
    if (config.protocol === "redis") {
      if (config.tlsMode !== "disabled" && (config.caCertPath || config.clientCertPath)) {
        return "Redis TLS 当前使用系统信任根；自定义 CA 与客户端证书尚未接入，不能保存为可用配置。";
      }
      if (config.redisMode === "sentinel") {
        if (!config.sentinelMasterName) return "Sentinel 模式必须填写 master 名称。";
        if (!config.sentinels.length) return "Sentinel 模式至少需要一个哨兵节点。";
      }
      if (config.redisMode === "cluster" && !config.clusterNodes.length) return "Cluster 模式至少需要一个集群节点。";
    }
    if (config.protocol === "elasticsearch") {
      if (config.tlsMode !== "disabled" && config.clientCertPath) {
        return "Elasticsearch 当前不支持客户端证书 TLS；请使用服务端 TLS（可配自定义 CA）或关闭 TLS。";
      }
      if (config.esAuthMode === "basic" && !config.username) return "Basic 认证需要填写用户名。";
    }
    if (config.pool.maxSize < 1) return "连接池 max_size 至少为 1。";
    if (config.pool.minIdle > config.pool.maxSize) return "连接池 min_idle 不能大于 max_size。";
    if (config.pool.connectionTimeoutMs < 100) return "连接超时不能小于 100 毫秒。";
    if (config.pool.idleTimeoutMs < 1000 || config.pool.maxLifetimeMs < 1000) return "空闲超时和最大生命周期不能小于 1000 毫秒。";
    if (config.queryTimeoutMs < 1000) return "查询或命令超时不能小于 1000 毫秒。";
    return "";
  }

  function saveDraftModal() {
    try {
      const tab = activeSqlTab();
      if (!tab || tab.kind !== "query" || currentWorkspace() !== "sql") throw new Error("请在查询标签页中保存草稿。");
      if (!tab.connectionId) throw new Error("请先为该查询选择连接，草稿会归属到该连接下。");
      const sql = window.__luxbaseGetSql?.() || "";
      if (!sql.trim()) throw new Error("当前 SQL 为空，未创建草稿。");
      const suggested = /^查询 \d+$/.test(tab.title) ? "" : tab.title;
      showModal(tab.draftId ? "更新保存的查询" : "保存查询草稿", "LOCAL QUERY DRAFT", `
        <input type="hidden" id="draft-tab-id" value="${escapeHtml(tab.id)}" />
        <input type="hidden" id="draft-existing-id" value="${escapeHtml(tab.draftId || '')}" />
        <div class="form-grid">
          <label class="full">草稿名称<input id="draft-name" value="${escapeHtml(suggested || "未命名查询")}" maxlength="120" /></label>
          <label class="full">SQL 预览<textarea id="draft-sql" class="modal-textarea" readonly>${escapeHtml(sql)}</textarea></label>
        </div>
        <div class="form-note"><svg><use href="#i-lock"/></svg>保存会更新原查询；“另存为”会创建独立副本。内容仅保存在本机，并使用 AES-256-GCM 加密。</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button>${tab.draftId ? '<button class="secondary-button" data-action="save-draft-copy">另存为新查询</button>' : ''}<button class="primary-button" data-action="save-draft-confirm">${tab.draftId ? '更新查询' : '保存查询'}</button></div>
      `);
    } catch (error) {
      showOperationError(error, "无法保存草稿");
    }
  }

  async function saveDraft(copy = false) {
    try {
      const name = $("#draft-name")?.value?.trim() || "";
      const sql = $('#draft-sql')?.value || '';
      if (!name) throw new Error("请填写草稿名称。");
      if (!sql.trim()) throw new Error("草稿 SQL 不能为空。");
      const tab = sqlTabs.find((item) => item.id === $('#draft-tab-id')?.value);
      if (!tab?.connectionId) throw new Error('保存目标标签页已关闭。');
      const draftConnectionId = tab.connectionId;
      const draft = await backendCall("save_query_draft", {
        connectionId: draftConnectionId,
        name,
        sqlText: sql,
        draftId: copy ? null : ($('#draft-existing-id')?.value ? Number($('#draft-existing-id').value) : null),
      });
      closeModal();
      tab.title = draft.name;
      tab.draftId = String(draft.id);
      tab.dirty = tab.sql !== sql;
      queryDrafts.set(String(draft.id), draft);
      renderSqlTabs();
      renderQueryContext(tab);
      persistSqlTabs();
      if (tab.closeAfterSave) closeSqlTab(tab.id, { force: true });
      await refreshDraftUi(draftConnectionId);
      toast(copy ? "查询副本已保存" : "查询草稿已保存", `${name} · 仅保存在本机。`);
      refreshAuditOnly();
    } catch (error) {
      showOperationError(error, "草稿保存失败");
    }
  }

  function replaceConnectionDrafts(connectionId, drafts) {
    for (const [id, draft] of queryDrafts) if (draft.connectionId === connectionId) queryDrafts.delete(id);
    for (const draft of drafts || []) queryDrafts.set(String(draft.id), draft);
  }

  async function refreshDraftUi(connectionId) {
    const drafts = await backendCall("list_query_drafts", { connectionId });
    replaceConnectionDrafts(connectionId, drafts);
    const group = $(`.connection-group[data-connection-id="${CSS.escape(connectionId)}"]`);
    if (group) {
      group.dataset.loaded = "false";
      const children = $(".tree-children", group);
      if (children) children.innerHTML = "";
      await loadConnectionTree(group);
    }
    if (objectsState.connectionId === connectionId && objectsState.category === "query") await renderObjectsView();
    return drafts;
  }

  function selectedDraft() {
    const id = selectedObjectRow()?.dataset.draftRow || objectsState.selected;
    return id ? queryDrafts.get(String(id)) || null : null;
  }

  function renameDraftModal(draftId = null) {
    const draft = queryDrafts.get(String(draftId)) || selectedDraft();
    if (!draft) {
      toast("请先选择查询", "在查询对象列表中选择要重命名的查询。", "danger");
      return;
    }
    showModal("重命名已保存查询", "RENAME SAVED QUERY", `
      <div class="form-grid"><label class="full">查询名称<input id="draft-rename-input" value="${escapeHtml(draft.name)}" maxlength="120" /></label></div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="draft-rename-confirm" data-draft-id="${escapeHtml(String(draft.id))}">重命名</button></div>
    `);
  }

  async function renameDraft(draftId) {
    const draft = queryDrafts.get(String(draftId));
    const name = $("#draft-rename-input")?.value?.trim() || "";
    if (!draft || !name) throw new Error("查询名称不能为空。");
    const renamed = await backendCall("rename_query_draft", { connectionId: draft.connectionId, draftId: Number(draft.id), name });
    queryDrafts.set(String(renamed.id), renamed);
    for (const tab of sqlTabs.filter((item) => item.draftId === String(renamed.id))) tab.title = renamed.name;
    closeModal();
    renderSqlTabs();
    await refreshDraftUi(draft.connectionId);
    toast("查询已重命名", renamed.name);
  }

  async function deleteDraft(draftId = null) {
    const draft = queryDrafts.get(String(draftId)) || selectedDraft();
    if (!draft) {
      toast("请先选择查询", "在查询对象列表中选择要删除的查询。", "danger");
      return;
    }
    const confirmed = await confirmChoice({
      title: `删除查询 ${draft.name}`,
      eyebrow: "DELETE SAVED QUERY",
      body: "只会删除本机保存的查询记录；已经打开的标签页会保留，并变为未保存状态。",
      confirmLabel: "删除",
      danger: true,
    });
    if (!confirmed) return;
    await backendCall("delete_query_draft", { connectionId: draft.connectionId, draftId: Number(draft.id) });
    queryDrafts.delete(String(draft.id));
    for (const tab of sqlTabs.filter((item) => item.draftId === String(draft.id))) {
      tab.draftId = null;
      tab.dirty = Boolean(tab.sql.trim());
    }
    objectsState.selected = null;
    renderSqlTabs();
    persistSqlTabs();
    await refreshDraftUi(draft.connectionId);
    toast("已删除保存的查询", `${draft.name} · 打开的标签页未关闭。`, "danger");
  }

  async function queryHistoryModal() {
    try {
      queryHistory = await backendCall("recent_history", { limit: 200 });
      const connectionId = objectsState.connectionId || activeConnectionId;
      const rows = queryHistory.filter((item) => !connectionId || item.connectionId === connectionId);
      showModal("查询执行历史", "LOCAL QUERY HISTORY", `
        <div class="query-history-list">${rows.map((item) => `<button class="query-history-item" data-history-open="${escapeHtml(String(item.id))}">
          <span class="history-status ${item.status === "success" ? "success" : "failure"}">${item.status === "success" ? "成功" : "失败"}</span>
          <span class="history-sql">${escapeHtml(summarizeSql(item.sqlText, 220))}</span>
          <span class="history-meta">${escapeHtml(new Date(item.executedAt).toLocaleString("zh-CN", { hour12: false }))}${item.durationMs == null ? "" : ` · ${escapeHtml(String(item.durationMs))} ms`}${item.rowsAffected == null ? "" : ` · ${escapeHtml(String(item.rowsAffected))} 行`}</span>
        </button>`).join("") || '<div class="empty-table">当前连接还没有查询执行记录。</div>'}</div>
        <div class="form-note">历史只保存在本机并加密存储。单击一条记录会在新查询标签页中打开，不会自动执行。</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>关闭</button></div>
      `, { wide: true });
    } catch (error) {
      showOperationError(error, "查询历史读取失败");
    }
  }

  function openHistoryEntry(id) {
    const entry = queryHistory.find((item) => String(item.id) === String(id));
    if (!entry) return;
    const connection = connectionLookup.get(entry.connectionId);
    if (!connection) {
      toast("原连接已删除", "历史 SQL 仍保留，但无法恢复原连接。", "danger");
      return;
    }
    closeModal();
    newQueryTab({ connectionId: entry.connectionId, schema: defaultSchemaFor(entry.connectionId), sql: entry.sqlText, title: "历史查询" });
  }

  async function testConnection() {
    let payload;
    try {
      payload = connectionPayload();
      const validationError = validateConnectionPayload(payload);
      if (validationError) {
        showFormError(validationError);
        return;
      }
      showFormError("");
      showTestResult(true, "正在测试连接…");
      const button = $('.modal-footer [data-action="test-connection"]');
      if (button) button.disabled = true;
      let result;
      try {
        result = await backendCall("test_connection", { payload });
      } finally {
        if (button) button.disabled = false;
      }
      const message = result.ok
        ? `连接成功 · ${result.latencyMs} ms${result.serverVersion ? ` · ${result.serverVersion}` : ""}`
        : `${result.errorClass || "error"} · ${result.message || "连接失败"}`;
      showTestResult(result.ok, message);
      if (!result.ok) showFormError(message);
      else toast("连接测试成功", message);
    } catch (error) {
      showFormError(errorMessage(error));
    }
  }

  async function saveConnection() {
    try {
      const payload = connectionPayload();
      const validationError = validateConnectionPayload(payload);
      if (validationError) {
        showFormError(validationError);
        return;
      }
      showFormError("");
      const editingExisting = Boolean($("#conn-id")?.value);
      const saved = await backendCall("save_connection", { payload });
      closeModal();
      toast("连接已安全保存", `${saved.name} · ${editingExisting ? "修改已加密保存" : "密码已写入本地保险库"}`);
      await refreshBootstrap();
      await selectConnection(saved.id);
    } catch (error) {
      showFormError(errorMessage(error));
    }
  }

  async function editConnection() {
    try {
      requireSavedConnection();
      const config = await backendCall("get_connection_config", { connectionId: activeConnectionId });
      newConnectionModal(config);
    } catch (error) {
      showOperationError(error, "无法编辑连接");
    }
  }

  async function testSavedConnection() {
    if (!activeConnectionId) {
      toast("没有活动连接", "请先选择连接。", "danger");
      return;
    }
    try {
      const result = await backendCall("test_saved_connection", { connectionId: activeConnectionId });
      const message = result.ok
        ? `连接成功 · ${result.latencyMs} ms${result.serverVersion ? ` · ${result.serverVersion}` : ""}`
        : `${result.errorClass || "error"} · ${result.message || "连接失败"}`;
      toast(result.ok ? "连接测试成功" : "连接测试失败", message, result.ok ? "success" : "danger");
    } catch (error) {
      showOperationError(error, "连接测试失败");
    }
  }

  function deleteConnection() {
    if (!activeConnectionId) {
      toast("没有活动连接", "请先选择连接。", "danger");
      return;
    }
    const connection = activeConnection;
    confirmDangerous(
      `DELETE CONNECTION ${connection?.name || ""}`,
      `${connection?.name || "连接"}\n将从本机删除连接配置与本地保险库凭据，并断开当前连接。`,
      async () => {
        await backendCall("delete_connection", { connectionId: activeConnectionId });
        savedConnections = savedConnections.filter((item) => item.id !== activeConnectionId);
        connectionLookup.delete(activeConnectionId);
        activeConnectionId = null;
        activeConnection = null;
        renderSavedConnections();
        toast("连接已删除", `${connection?.name || "连接"} · 本地配置与凭据已清理。`, "danger");
        await refreshBootstrap();
      },
    );
  }

  function confirmDangerous(command, details, action = null) {
    const title = command.split(/\s+/)[0].toUpperCase();
    confirmAction = typeof action === "function" ? action : null;
    showModal("确认危险操作", "DANGER GUARDRAIL", `
      <div class="danger-warning">
        <svg><use href="#i-warning"/></svg>
        <div><strong>${escapeHtml(title)} 可能造成不可逆的数据变更</strong><p>该命令已触发生产级危险命令护栏。请确认目标连接、作用范围与影响，操作会写入只追加审计日志。</p><code class="danger-code">${escapeHtml(details || command)}</code></div>
      </div>
      <label class="confirm-check"><input type="checkbox" id="danger-confirm" />我已核对连接与影响范围，并确认在生产环境执行此操作。</label>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="danger-button" data-action="danger-confirm" data-command="${escapeHtml(command)}">确认执行</button></div>
    `, { danger: true });
  }

  function formatBytes(value) {
    if (value == null) return "未知";
    const units = ["B", "KB", "MB", "GB", "TB"];
    let amount = Number(value);
    let index = 0;
    while (amount >= 1024 && index < units.length - 1) {
      amount /= 1024;
      index += 1;
    }
    return `${amount.toFixed(index ? 1 : 0)} ${units[index]}`;
  }

  function buildDiagnosticReport(cards) {
    const protocols = Array.from(protocolDescriptors.values()).map((descriptor) => ({
      id: descriptor.id,
      family: descriptor.family,
      label: descriptor.label,
      supports: Array.isArray(descriptor.supports) ? [...descriptor.supports] : [],
      unsupported: Array.isArray(descriptor.unsupported) ? [...descriptor.unsupported] : [],
    }));
    return {
      schemaVersion: 1,
      generatedAt: new Date().toISOString(),
      application: {
        name: "LuxBase",
        version: "2.0.0",
        desktopBackendConnected: backendAvailable,
        renderer: navigator.userAgent || "unknown",
        language: navigator.language || "unknown",
      },
      activeDriver: {
        protocol: activeProtocol,
        family: activeFamily,
        tlsEnabled: Boolean(activeConnection?.tlsEnabled),
        sshEnabled: Boolean(activeConnection?.hasSsh),
        redisMode: activeFamily === "kv" ? activeConnection?.redisMode || "standalone" : null,
        redisDb: activeFamily === "kv" ? redisScanState.db : null,
      },
      diagnostics: cards.map(([label, value, detail]) => ({
        label: String(label),
        value: String(value),
        detail: String(detail),
      })),
      security: securityState ? {
        localVaultAvailable: Boolean(securityState.localVaultAvailable),
        encryptionAtRest: Boolean(securityState.encryptionAtRest),
        auditAppendOnly: Boolean(securityState.auditAppendOnly),
      } : null,
      savedConnections: savedConnections.map((connection) => ({
        protocol: connection.protocol,
        tlsEnabled: Boolean(connection.tlsEnabled),
        sshEnabled: Boolean(connection.hasSsh),
        redisMode: connection.protocol === "redis" ? connection.redisMode || "standalone" : null,
        redisDb: connection.protocol === "redis" ? connection.redisDb ?? 0 : null,
      })),
      protocols,
      privacy: "This report omits connection names, host addresses, usernames, passwords, SSH keys, query text and Redis key names.",
    };
  }

  function downloadDiagnosticReport(contents, fileName) {
    const blob = new Blob([contents], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = fileName;
    link.rel = "noopener";
    document.body.appendChild(link);
    link.click();
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1_000);
  }

  async function exportDiagnosticReport() {
    if (!lastDiagnosticReport) throw new Error("请先打开连接诊断，再导出报告。");
    const report = { ...lastDiagnosticReport, generatedAt: new Date().toISOString() };
    const contents = JSON.stringify(report, null, 2);
    const fileName = `luxbase-diagnostics-${report.generatedAt.replace(/[:.]/g, "-")}.json`;
    let desktopError = null;
    if (backendAvailable) {
      try {
        const selected = await invokeTauri("plugin:dialog|save", {
          options: {
            title: "导出脱敏诊断报告",
            defaultPath: fileName,
            filters: [{ name: "JSON", extensions: ["json"] }],
          },
        });
        if (selected == null) return;
        const path = typeof selected === "string" ? selected : selected.path || selected.filePath;
        if (!path) throw new Error("保存对话框没有返回可用路径。");
        const savedPath = await backendCall("save_diagnostics_report", { path, report });
        toast("诊断报告已导出", `${savedPath} · 报告不含凭据、主机地址、查询文本或键名。`);
        return;
      } catch (error) {
        desktopError = error;
        console.warn("Desktop diagnostics export failed; using browser fallback.", error);
      }
    }
    let copied = false;
    try {
      await navigator.clipboard.writeText(contents);
      copied = true;
    } catch (error) {
      console.warn("Could not copy diagnostics report to the clipboard.", error);
    }
    downloadDiagnosticReport(contents, fileName);
    toast(
      copied ? "诊断报告已复制" : "诊断报告已生成",
      desktopError
        ? `桌面保存不可用，已改用${copied ? "剪贴板与" : ""}浏览器下载。`
        : `已开始下载 ${fileName}。`,
    );
  }

  async function diagnosticModal() {
    if (!activeConnectionId) {
      toast("没有活动连接", "请先选择连接再读取诊断。", "danger");
      return;
    }
    try {
      let cards;
      if (activeFamily === "kv") {
        const op = operationId();
        activeRedisOperationId = op;
        let info;
        try {
          info = await backendCall("redis_info", {
            connectionId: activeConnectionId,
            operationId: op,
          });
        } finally {
          if (activeRedisOperationId === op) activeRedisOperationId = null;
        }
        cards = [
          ["当前连接", activeConnection?.name || "Redis", "键值协议 · 实时 INFO"],
          ["内存", formatBytes(info.memoryUsedBytes), `峰值 ${formatBytes(info.memoryPeakBytes)}`],
          ["连接数", info.connectedClients ?? "未知", "connected_clients"],
          ["命中率", info.hitRate == null ? "无样本" : `${(info.hitRate * 100).toFixed(2)}%`, "keyspace hits / (hits + misses)"],
          ["主从状态", info.replicationRole || "未知", `副本 ${info.connectedReplicas ?? 0}`],
          ["集群", info.clusterEnabled ? "已启用" : "未启用", "cluster_enabled"],
        ];
      } else {
        const metrics = await backendCall("sql_pool_metrics", { connectionId: activeConnectionId });
        cards = [
          ["当前连接", activeConnection?.name || "SQL", activeConnection?.protocol || ""],
          ["连接池", `${metrics.active} / ${metrics.total} / ${metrics.maxSize}`, "active / total / max_size"],
          ["空闲连接", metrics.idle, "idle"],
          ["Waiters", metrics.waiters, "等待获取连接的请求"],
          ["Timeouts", metrics.timeouts, "超时次数"],
          ["累计获取", metrics.acquiredTotal, "acquired_total"],
        ];
      }
      lastDiagnosticReport = buildDiagnosticReport(cards);
      showModal("连接与驱动诊断", "RUNTIME DIAGNOSTICS", `
        <div class="diagnostic-grid">
          ${cards.map(([label, value, detail]) => `<div class="diagnostic-card"><span>${escapeHtml(label)}</span><b>${escapeHtml(value)}</b><small>${escapeHtml(detail)}</small></div>`).join("")}
        </div>
        <div class="form-note"><svg><use href="#i-shield"/></svg>诊断数据直接从当前驱动读取；导出报告会省略主机地址、用户名、凭据、查询文本和键名。</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>关闭</button><button class="primary-button" data-action="diagnostics-export">导出脱敏报告</button></div>
      `);
    } catch (error) {
      showOperationError(error, "诊断读取失败");
    }
  }

  function migrationModal(direction = "export") {
    migrationDirection = direction;
    const isExport = direction === "export";
    const supportsSql = activeFamily === "sql";
    if (!activeConnectionId) {
      toast("没有活动连接", "请先选择连接。", "danger");
      return;
    }
    const formats = supportsSql ? (isExport ? ["csv", "jsonl", "sql"] : ["csv", "jsonl"]) : ["jsonl"];
    const sqlSchema = activeSchema || activeTable?.schema || null;
    const sourceLabel = supportsSql
      ? `${activeConnection?.name || "SQL"} / ${sqlSchema || "请先选择数据库"}`
      : `${activeConnection?.name || "Redis"} / DB ${redisScanState.db} / ${redisScanState.pattern}`;
    const defaultScope = isExport && supportsSql && !activeTable && sqlSchema ? "database" : "table";
    showModal(isExport ? "导出数据" : "导入数据", "STREAMING DATA TRANSFER", `
      <div class="migration-tabs"><button class="${isExport ? "active" : ""}" data-migration-tab="export">导出</button><button class="${!isExport ? "active" : ""}" data-migration-tab="import">导入</button></div>
      <div class="form-grid" style="margin-top:12px">
        <label class="full">${isExport ? "来源" : "目标"}<input value="${escapeHtml(sourceLabel)}" disabled /></label>
        ${isExport && supportsSql ? `<label>导出范围<select id="transfer-scope"><option value="table" ${defaultScope === "table" ? "selected" : ""} ${activeTable ? "" : "disabled"}>当前表${activeTable ? ` · ${escapeHtml(activeTable.table)}` : "（请先打开表）"}</option><option value="database" ${defaultScope === "database" ? "selected" : ""} ${sqlSchema ? "" : "disabled"}>整个数据库 / Schema</option></select></label>` : ""}
        <label>格式<select id="transfer-format">${formats.map((format) => `<option value="${format}">${format === "jsonl" ? "JSON Lines（流式）" : format === "csv" ? "CSV（流式）" : "SQL 插入语句"}</option>`).join("")}</select></label>
        <label>每批行数<input id="transfer-batch" type="number" value="2000" min="1" max="10000" /></label>
        ${supportsSql ? "" : '<label class="full">键模式<input id="transfer-pattern" value="*" placeholder="user:*" /></label>'}
        <label class="full">${isExport ? "导出路径" : "导入文件路径"}<input id="transfer-path" placeholder="/Users/me/export.${supportsSql ? "csv" : "jsonl"}" /></label>
        ${supportsSql ? `<label class="full" style="display:flex;flex-direction:row;align-items:center;gap:7px"><input type="checkbox" checked disabled style="width:13px;height:13px" />${isExport ? "导出在一致只读快照中流式读取，并写入审计日志" : "SQL 导入使用单一事务，任一步失败全部回滚"}</label>` : '<label class="full" style="display:flex;flex-direction:row;align-items:center;gap:7px"><input type="checkbox" checked disabled style="width:13px;height:13px" />Redis 导入保留类型、TTL 与字段结构</label>'}
      </div>
      <div class="form-note"><svg><use href="#i-shield"/></svg>${isExport ? supportsSql ? "表级或库级导出都在同一只读快照中分块读取，不一次性载入内存。库级 SQL/JSONL 写入单个文件；CSV 会创建文件夹并按表拆分文件。" : "Redis 逐键流式读取并保持类型与 TTL；中途失败会明确报告已读取数量。" : supportsSql ? "导入按批流式读取，不把整份文件装入内存；所有批次仍在单一事务中，任一步失败全部回滚。" : "Redis 逐键流式读取并原子替换；中途失败会明确报告已写入数量。"}</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="start-transfer">${isExport ? "开始导出" : "开始导入"}</button></div>
    `);
  }

  /* ---------- schema compare ---------- */

  const compareState = {
    source: { connectionId: null, schema: null },
    target: { connectionId: null, schema: null },
    running: false,
    result: null,
    showSame: false,
  };

  function protocolQuote(protocol, name) {
    if (protocol === "mysql") return `\`${String(name).replace(/`/g, "``")}\``;
    return `"${String(name).replace(/"/g, "\"\"")}"`;
  }

  function protocolQualified(protocol, schema, table) {
    if (protocol === "sqlite" || !schema) return protocolQuote(protocol, table);
    return `${protocolQuote(protocol, schema)}.${protocolQuote(protocol, table)}`;
  }

  function sqlConnections() {
    return savedConnections.filter((connection) => protocolFamily(connection.protocol) === "sql");
  }

  function initCompareState() {
    const connections = sqlConnections();
    const valid = (id) => connections.some((connection) => connection.id === id);
    if (!valid(compareState.source.connectionId)) {
      const preferred = activeFamily === "sql" && valid(activeConnectionId) ? activeConnectionId : connections[0]?.id || null;
      compareState.source = { connectionId: preferred, schema: preferred === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(preferred) };
    }
    if (!valid(compareState.target.connectionId)) {
      compareState.target = { connectionId: compareState.source.connectionId, schema: null };
    }
    for (const side of [compareState.source, compareState.target]) {
      if (side.connectionId && !side.schema) side.schema = defaultSchemaFor(side.connectionId);
    }
  }

  function compareSideHtml(side, key, label) {
    const connection = connectionLookup.get(side.connectionId);
    const protocol = connection?.protocol || "unknown";
    const schemas = side.connectionId ? metaFor(side.connectionId).schemas || [] : [];
    const options = [...new Set([...(side.schema ? [side.schema] : []), ...schemas])];
    return `<div class="compare-side ${key}">
      <div class="compare-side-label">${label}</div>
      <div class="compare-side-body">
        ${connection ? protocolBadgeHtml(protocol) : '<span class="protocol-badge unknown">?</span>'}
        <label class="compare-field"><small>连接</small><select data-compare-connection="${key}">${sqlConnections().map((item) => `<option value="${escapeHtml(item.id)}"${item.id === side.connectionId ? " selected" : ""}>${escapeHtml(item.name)}</option>`).join("")}</select></label>
        <label class="compare-field"><small>${protocol === "postgres" ? "模式" : "数据库"}</small><select data-compare-schema="${key}"${protocol === "sqlite" ? " disabled" : ""}>${options.length ? options.map((name) => `<option value="${escapeHtml(name)}"${name === side.schema ? " selected" : ""}>${escapeHtml(name)}</option>`).join("") : '<option value="">加载中…</option>'}</select></label>
      </div>
    </div>`;
  }

  function renderCompareView() {
    const view = $("#view-compare");
    if (!view) return;
    initCompareState();
    const connections = sqlConnections();
    view.innerHTML = `
      <div class="view-heading">
        <div><div class="eyebrow">SCHEMA DIFF</div><h2>结构比较</h2><p>对比两个库的表、列、索引和外键，生成让<b>目标</b>与<b>源</b>一致的同步脚本。比较只读取元数据，脚本经你勾选、审阅并确认后才会执行。</p></div>
      </div>
      ${modeStripHtml("compare")}
      ${connections.length ? `
        <div class="compare-bar">
          ${compareSideHtml(compareState.source, "source", "源 · 以它为准")}
          <button class="compare-swap" data-action="compare-swap" title="交换源与目标">⇄</button>
          ${compareSideHtml(compareState.target, "target", "目标 · 将被修改")}
          <button class="primary-button compare-go" data-action="compare-run-now"${compareState.running ? " disabled" : ""}><svg><use href="#i-play"/></svg>${compareState.running ? "比较中…" : "开始比较"}</button>
        </div>
        <div id="compare-results" class="compare-results">${compareResultsHtml()}</div>`
        : '<div class="compare-placeholder"><strong>还没有 SQL 连接</strong><p>结构比较需要至少一个 MySQL、PostgreSQL 或 SQLite 连接（源和目标可以是同一连接下的两个库）。</p><button class="primary-button" data-action="new-connection">新建连接</button></div>'}`;
    colorizeCompareScript();
    for (const side of [compareState.source, compareState.target]) {
      if (backendAvailable && side.connectionId && !metaFor(side.connectionId).schemas && connectionProtocol(side.connectionId) !== "sqlite") {
        loadSchemas(side.connectionId).then(() => {
          if ($("#view-compare")?.classList.contains("active")) renderCompareView();
        }).catch((error) => showOperationError(error, "读取数据库列表失败"));
      }
    }
  }

  const DIFF_LABELS = {
    create_table: "创建表", drop_table: "删除表", create_view: "视图", drop_view: "删除视图",
    add_column: "新增列", modify_column: "修改列", drop_column: "删除列",
    primary_key: "主键", add_index: "新增索引", modify_index: "重建索引", drop_index: "删除索引",
    add_fk: "新增外键", modify_fk: "重建外键", drop_fk: "删除外键", manual: "需手工处理",
  };
  const DESTRUCTIVE_DIFFS = new Set(["drop_table", "drop_view", "drop_column"]);

  function compareResultsHtml() {
    if (compareState.running) return '<div class="compare-placeholder"><div class="tree-loading">正在读取两侧的表、列、索引和外键…</div></div>';
    const result = compareState.result;
    if (!result) {
      return `<div class="compare-placeholder"><strong>选择源和目标后点击“开始比较”</strong><p>· 源和目标可以是不同服务器，也可以是同一连接下的两个库（例如 shop_dev → shop_prod）。<br>· 默认只勾选不会丢数据的变更；删除表/删除列需要你手动勾选。<br>· 同协议才可直接执行；跨协议只生成脚本供参考。</p></div>`;
    }
    if (result.error) return `<div class="compare-placeholder danger"><strong>比较失败</strong><p>${escapeHtml(result.error)}</p></div>`;
    const counts = { source_only: 0, changed: 0, target_only: 0, same: 0 };
    for (const table of result.tables) counts[table.status] += 1;
    const visible = result.tables.filter((table) => compareState.showSame || table.status !== "same");
    const statusText = { source_only: "仅源中有", changed: "有差异", target_only: "仅目标中有", same: "一致" };
    const tablesHtml = visible.map((table) => `
      <div class="diff-table ${table.status}${table.status === "same" ? "" : " open"}" data-diff-table="${escapeHtml(table.name)}">
        <button class="diff-table-head" data-diff-toggle="${escapeHtml(table.name)}">
          <span class="tree-chevron"><svg><use href="#i-chevron"/></svg></span>
          <svg class="tree-icon ${table.isView ? "view" : "table"}"><use href="#i-${table.isView ? "view" : "table"}"/></svg>
          <strong>${escapeHtml(table.name)}</strong>
          <span class="diff-status ${table.status}">${statusText[table.status]}</span>
          <span class="diff-count">${table.items.length ? `${table.items.length} 项` : ""}</span>
        </button>
        <div class="diff-items">${table.items.map((item) => `
          <label class="diff-item ${item.kind}${item.ops.length ? "" : " manual"}">
            <input type="checkbox" data-diff-item="${item.id}"${item.checked ? " checked" : ""}${item.ops.length ? "" : " disabled"} />
            <span class="diff-op">${escapeHtml(DIFF_LABELS[item.kind] || item.kind)}</span>
            <code class="diff-name">${escapeHtml(item.name)}</code>
            <span class="diff-detail">${escapeHtml(item.detail || "")}</span>
            ${DESTRUCTIVE_DIFFS.has(item.kind) ? '<span class="diff-warn">会丢失数据</span>' : ""}
          </label>`).join("") || '<div class="diff-none">结构一致</div>'}
        </div>
      </div>`).join("");
    return `
      <div class="compare-summary">
        <span class="compare-chip source_only"><b>${counts.source_only}</b>仅源中有</span>
        <span class="compare-chip changed"><b>${counts.changed}</b>有差异</span>
        <span class="compare-chip target_only"><b>${counts.target_only}</b>仅目标中有</span>
        <span class="compare-chip same"><b>${counts.same}</b>一致</span>
        <span class="toolbar-spacer"></span>
        <label class="compare-toggle"><input type="checkbox" data-compare-show-same${compareState.showSame ? " checked" : ""} />显示一致的表</label>
        <button class="ghost-button" data-action="compare-check" data-mode="safe">仅安全变更</button>
        <button class="ghost-button" data-action="compare-check" data-mode="all">全选</button>
        <button class="ghost-button" data-action="compare-check" data-mode="none">全不选</button>
      </div>
      <div class="compare-split">
        <div class="compare-tree">${tablesHtml || '<div class="compare-placeholder"><strong>两侧结构一致</strong><p>没有发现表、列、索引或外键差异。</p></div>'}</div>
        <div class="compare-script" id="compare-script">${compareScriptHtml()}</div>
      </div>`;
  }

  function compareSelectedStatements() {
    const result = compareState.result;
    if (!result?.tables) return [];
    const ops = [];
    let order = 0;
    for (const table of result.tables) {
      for (const item of table.items) {
        if (!item.checked) continue;
        for (const op of item.ops) ops.push({ ...op, order: order += 1 });
      }
    }
    return ops.sort((a, b) => a.phase - b.phase || a.order - b.order).map((op) => op.sql);
  }

  function compareScriptHtml() {
    const result = compareState.result;
    if (!result?.tables) return "";
    const statements = compareSelectedStatements();
    const target = connectionLookup.get(result.targetId);
    const script = statements.length ? `${statements.join(";\n\n")};` : "-- 没有勾选任何变更";
    const executable = statements.length && result.sameProtocol;
    return `
      <div class="compare-script-head">
        <div><strong>同步脚本</strong><span>${statements.length} 条语句 · 在 ${escapeHtml(target?.name || "目标")}${result.targetSchema && result.targetProtocol !== "sqlite" ? ` / ${escapeHtml(result.targetSchema)}` : ""} 上执行</span></div>
        <div class="compare-script-actions">
          <button class="tiny-icon" data-action="copy-schema-sync" title="复制脚本"${statements.length ? "" : " disabled"}><svg><use href="#i-copy"/></svg></button>
          <button class="ghost-button" data-action="compare-open-query"${statements.length ? "" : " disabled"}>在查询中打开</button>
          <button class="danger-button" data-action="execute-schema-sync"${executable ? "" : " disabled"}>执行同步</button>
        </div>
      </div>
      <pre class="compare-sql" id="compare-sync-sql">${escapeHtml(script)}</pre>
      ${result.notes.length ? `<div class="compare-notes">${result.notes.map((note) => `<div><svg><use href="#i-warning"/></svg>${escapeHtml(note)}</div>`).join("")}</div>` : ""}
      ${result.sameProtocol ? (result.targetProtocol === "mysql" ? '<div class="compare-notes info"><div><svg><use href="#i-shield"/></svg>MySQL 的 DDL 会隐式提交：脚本逐条执行，中途失败时之前的语句已生效，会提示失败位置。</div></div>' : '<div class="compare-notes info"><div><svg><use href="#i-shield"/></svg>脚本在一个事务中执行，任一语句失败会整体回滚。</div></div>')
        : '<div class="compare-notes"><div><svg><use href="#i-warning"/></svg>源与目标协议不同，列类型未做转换，脚本仅供参考，不能直接执行。</div></div>'}`;
  }

  function refreshCompareScript() {
    const panel = $("#compare-script");
    if (panel) panel.innerHTML = compareScriptHtml();
    colorizeCompareScript();
  }

  function colorizeCompareScript() {
    const pre = $("#compare-sync-sql");
    const protocol = compareState.result?.targetProtocol;
    if (!pre || !protocol || !window.__luxbaseColorizeSql) return;
    const text = pre.textContent;
    window.__luxbaseColorizeSql(text, protocol)
      .then((html) => {
        if (pre.isConnected && pre.textContent === text) pre.innerHTML = html;
      })
      .catch(() => {});
  }

  function normalizeColumnType(type, protocol) {
    let text = String(type || "").trim().toLowerCase().replace(/\s+/g, " ");
    if (protocol === "mysql") text = text.replace(/^(tinyint|smallint|mediumint|int|integer|bigint)\((?!1\))\d+\)/, "$1");
    return text;
  }

  function normalizeDefault(value) {
    if (value === null || value === undefined) return null;
    return String(value).trim().replace(/^'(.*)'$/s, "$1");
  }

  function normalizeExtra(extra) {
    return String(extra || "").toLowerCase().replace(/default_generated/g, "").replace(/\s+/g, " ").trim();
  }

  function columnChanges(source, target, protocol) {
    const changes = [];
    if (normalizeColumnType(source.dataType, protocol) !== normalizeColumnType(target.dataType, protocol)) changes.push(`类型 ${target.dataType} → ${source.dataType}`);
    if (Boolean(source.nullable) !== Boolean(target.nullable)) changes.push(source.nullable ? "改为可空" : "改为 NOT NULL");
    if (normalizeDefault(source.defaultValue) !== normalizeDefault(target.defaultValue)) changes.push(`默认值 ${target.defaultValue ?? "无"} → ${source.defaultValue ?? "无"}`);
    if (protocol !== "sqlite" && normalizeExtra(source.extra) !== normalizeExtra(target.extra)) changes.push(`属性 ${target.extra || "无"} → ${source.extra || "无"}`);
    if (String(source.comment || "") !== String(target.comment || "")) changes.push("注释不同");
    return changes;
  }

  function pgColumnDefinition(column, { forCreate = false } = {}) {
    const designerShape = designerColumn(column);
    const serial = forCreate && /^nextval\(/i.test(String(column.defaultValue || ""));
    if (serial) {
      const base = String(designerShape.base).toLowerCase();
      designerShape.base = base === "bigint" ? "bigserial" : base === "smallint" ? "smallserial" : "serial";
      designerShape.length = "";
      designerShape.defaultValue = "";
    }
    let definition = genericColumnDefinition(designerShape, "postgres");
    if (/identity/i.test(column.extra || "")) definition += " GENERATED BY DEFAULT AS IDENTITY";
    return definition;
  }

  function columnDefinitionFor(protocol, column, options = {}) {
    if (protocol === "mysql") return mysqlColumnDefinition(designerColumn(column), "mysql");
    if (protocol === "postgres") return pgColumnDefinition(column, options);
    return genericColumnDefinition(designerColumn(column), protocol, options);
  }

  function primaryKeyColumns(table) {
    return (table.columns || []).filter((column) => column.isPrimaryKey).sort((a, b) => a.ordinal - b.ordinal).map((column) => column.name);
  }

  function isPrimaryIndex(table, index) {
    if (index.name === "PRIMARY") return true;
    const keys = primaryKeyColumns(table);
    return Boolean(index.unique && keys.length && /_pkey$/.test(index.name) && index.columns.join(",") === keys.join(","));
  }

  function computeSchemaDiff(source, target, context) {
    const { protocol, sourceProtocol, schema: targetSchema } = context;
    const q = (name) => protocolQuote(protocol, name);
    const qt = (name) => protocolQualified(protocol, targetSchema, name);
    const cols = (names) => names.map(q).join(", ");
    const notes = [];
    let sequence = 0;
    const item = (kind, name, detail, ops, checked = !DESTRUCTIVE_DIFFS.has(kind)) => ({ id: `d${sequence += 1}`, kind, name, detail, ops, checked: ops.length ? checked : false });
    const sourceTables = new Map((source.tables || []).map((table) => [table.name, table]));
    const targetTables = new Map((target.tables || []).map((table) => [table.name, table]));
    const tables = [];

    const createIndexSql = (tableName, index) => `CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${q(index.name)} ON ${qt(tableName)} (${cols(index.columns)})`;
    const dropIndexSql = (tableName, index) => protocol === "mysql"
      ? `DROP INDEX ${q(index.name)} ON ${qt(tableName)}`
      : protocol === "postgres" ? `DROP INDEX ${qt(index.name)}` : `DROP INDEX ${q(index.name)}`;
    const addForeignKeySql = (tableName, key) => `ALTER TABLE ${qt(tableName)} ADD CONSTRAINT ${q(key.name)} FOREIGN KEY (${cols(key.columns)}) REFERENCES ${qt(key.referencedTable)} (${cols(key.referencedColumns)})`;
    const dropForeignKeySql = (tableName, key) => `ALTER TABLE ${qt(tableName)} ${protocol === "mysql" ? "DROP FOREIGN KEY" : "DROP CONSTRAINT"} ${q(key.name)}`;
    const indexShape = (index) => `${index.columns.join(",")}|${index.unique ? "U" : ""}`;
    const keyShape = (key) => `${key.columns.join(",")}|${key.referencedTable}|${key.referencedColumns.join(",")}`;
    const hasExpression = (index) => index.columns.some((column) => column === "(expression)" || /[()]/.test(column));

    for (const [name, sourceTable] of sourceTables) {
      const targetTable = targetTables.get(name);
      const isView = /VIEW/i.test(sourceTable.tableType || "");
      if (!targetTable) {
        if (isView) {
          tables.push({ name, isView, status: "source_only", items: [item("create_view", name, "视图定义需在“查看 DDL”中复制后手工创建", [])] });
          continue;
        }
        const keys = primaryKeyColumns(sourceTable);
        const integerPk = protocol === "sqlite" && keys.length === 1 && /^integer$/i.test(sourceTable.columns.find((column) => column.name === keys[0])?.dataType || "");
        const definitions = sourceTable.columns.map((column) => columnDefinitionFor(protocol, column, { forCreate: true, inlinePrimaryKey: integerPk && column.name === keys[0] }));
        if (keys.length && !integerPk) definitions.push(`PRIMARY KEY (${cols(keys)})`);
        if (protocol === "sqlite") {
          for (const key of sourceTable.foreignKeys || []) definitions.push(`FOREIGN KEY (${cols(key.columns)}) REFERENCES ${q(key.referencedTable)} (${cols(key.referencedColumns)})`);
        }
        const ops = [{ phase: 2, sql: `CREATE TABLE ${qt(name)} (\n  ${definitions.join(",\n  ")}\n)` }];
        for (const index of sourceTable.indexes || []) {
          if (isPrimaryIndex(sourceTable, index) || /^sqlite_autoindex/.test(index.name)) continue;
          if (hasExpression(index)) {
            notes.push(`${name}.${index.name} 是表达式索引，需手工创建。`);
            continue;
          }
          ops.push({ phase: 4, sql: createIndexSql(name, index) });
        }
        if (protocol !== "sqlite") for (const key of sourceTable.foreignKeys || []) ops.push({ phase: 5, sql: addForeignKeySql(name, key) });
        if (protocol === "postgres") {
          for (const column of sourceTable.columns) if (column.comment) ops.push({ phase: 3, sql: `COMMENT ON COLUMN ${qt(name)}.${q(column.name)} IS ${sqlLiteral(column.comment, protocol)}` });
        }
        tables.push({ name, isView, status: "source_only", items: [item("create_table", name, `${sourceTable.columns.length} 列 · ${(sourceTable.indexes || []).length} 索引 · ${(sourceTable.foreignKeys || []).length} 外键`, ops)] });
        continue;
      }
      if (isView || /VIEW/i.test(targetTable.tableType || "")) {
        tables.push({ name, isView: true, status: "same", items: [] });
        continue;
      }

      const items = [];
      const sourceColumns = new Map(sourceTable.columns.map((column) => [column.name, column]));
      const targetColumns = new Map(targetTable.columns.map((column) => [column.name, column]));
      sourceTable.columns.forEach((column, index) => {
        const existing = targetColumns.get(column.name);
        if (!existing) {
          if (protocol === "sqlite" && (column.isPrimaryKey || (!column.nullable && column.defaultValue == null))) {
            items.push(item("add_column", column.name, `${column.dataType} · SQLite 不能直接添加主键或无默认值的 NOT NULL 列`, []));
            return;
          }
          const position = protocol === "mysql" ? (index === 0 ? " FIRST" : ` AFTER ${q(sourceTable.columns[index - 1].name)}`) : "";
          const ops = [{ phase: 3, sql: `ALTER TABLE ${qt(name)} ADD COLUMN ${columnDefinitionFor(protocol, { ...column, isPrimaryKey: false })}${position}` }];
          if (protocol === "postgres" && column.comment) ops.push({ phase: 3, sql: `COMMENT ON COLUMN ${qt(name)}.${q(column.name)} IS ${sqlLiteral(column.comment, protocol)}` });
          items.push(item("add_column", column.name, `${column.dataType}${column.nullable ? "" : " NOT NULL"}`, ops));
          return;
        }
        const changes = columnChanges(column, existing, protocol);
        if (!changes.length) return;
        const ops = [];
        if (protocol === "mysql") {
          ops.push({ phase: 3, sql: `ALTER TABLE ${qt(name)} MODIFY COLUMN ${columnDefinitionFor(protocol, column)}` });
        } else if (protocol === "postgres") {
          const column_ = q(column.name);
          if (normalizeColumnType(column.dataType, protocol) !== normalizeColumnType(existing.dataType, protocol)) ops.push({ phase: 3, sql: `ALTER TABLE ${qt(name)} ALTER COLUMN ${column_} TYPE ${column.dataType}` });
          if (Boolean(column.nullable) !== Boolean(existing.nullable)) ops.push({ phase: 3, sql: `ALTER TABLE ${qt(name)} ALTER COLUMN ${column_} ${column.nullable ? "DROP NOT NULL" : "SET NOT NULL"}` });
          if (normalizeDefault(column.defaultValue) !== normalizeDefault(existing.defaultValue)) {
            ops.push({ phase: 3, sql: column.defaultValue == null ? `ALTER TABLE ${qt(name)} ALTER COLUMN ${column_} DROP DEFAULT` : `ALTER TABLE ${qt(name)} ALTER COLUMN ${column_} SET DEFAULT ${defaultExpression(column.defaultValue, protocol, column.extra)}` });
          }
          if (String(column.comment || "") !== String(existing.comment || "")) ops.push({ phase: 3, sql: `COMMENT ON COLUMN ${qt(name)}.${column_} IS ${column.comment ? sqlLiteral(column.comment, protocol) : "NULL"}` });
        } else {
          notes.push(`SQLite 无法原地修改列 ${name}.${column.name}（${changes.join("，")}），请在“设计表”中重建。`);
        }
        items.push(item("modify_column", column.name, changes.join("；"), ops));
      });
      for (const column of targetTable.columns) {
        if (sourceColumns.has(column.name)) continue;
        items.push(item("drop_column", column.name, `${column.dataType} · 源中不存在`, protocol === "sqlite" && column.isPrimaryKey ? [] : [{ phase: 3, sql: `ALTER TABLE ${qt(name)} DROP COLUMN ${q(column.name)}` }]));
      }

      const sourceKeys = primaryKeyColumns(sourceTable);
      const targetKeys = primaryKeyColumns(targetTable);
      if (sourceKeys.join(",") !== targetKeys.join(",")) {
        const ops = [];
        if (protocol === "mysql") {
          if (targetKeys.length) ops.push({ phase: 1, sql: `ALTER TABLE ${qt(name)} DROP PRIMARY KEY` });
          if (sourceKeys.length) ops.push({ phase: 4, sql: `ALTER TABLE ${qt(name)} ADD PRIMARY KEY (${cols(sourceKeys)})` });
        } else if (protocol === "postgres") {
          const pkIndex = (targetTable.indexes || []).find((index) => isPrimaryIndex(targetTable, index));
          if (targetKeys.length) ops.push({ phase: 1, sql: `ALTER TABLE ${qt(name)} DROP CONSTRAINT ${q(pkIndex?.name || `${name}_pkey`)}` });
          if (sourceKeys.length) ops.push({ phase: 4, sql: `ALTER TABLE ${qt(name)} ADD PRIMARY KEY (${cols(sourceKeys)})` });
        } else {
          notes.push(`SQLite 表 ${name} 的主键不同，需要重建表。`);
        }
        items.push(item("primary_key", sourceKeys.join(", ") || "（无）", `目标 ${targetKeys.join(", ") || "无主键"} → 源 ${sourceKeys.join(", ") || "无主键"}`, ops));
      }

      const sourceIndexes = new Map((sourceTable.indexes || []).filter((index) => !isPrimaryIndex(sourceTable, index) && !/^sqlite_autoindex/.test(index.name)).map((index) => [index.name, index]));
      const targetIndexes = new Map((targetTable.indexes || []).filter((index) => !isPrimaryIndex(targetTable, index) && !/^sqlite_autoindex/.test(index.name)).map((index) => [index.name, index]));
      for (const [indexName, index] of sourceIndexes) {
        const existing = targetIndexes.get(indexName);
        if (existing && indexShape(existing) === indexShape(index)) continue;
        if (hasExpression(index)) {
          items.push(item(existing ? "modify_index" : "add_index", indexName, "表达式索引，需手工处理", []));
          continue;
        }
        const ops = existing ? [{ phase: 1, sql: dropIndexSql(name, existing) }] : [];
        ops.push({ phase: 4, sql: createIndexSql(name, index) });
        items.push(item(existing ? "modify_index" : "add_index", indexName, `${index.unique ? "UNIQUE " : ""}(${index.columns.join(", ")})${existing ? ` ← 目标 (${existing.columns.join(", ")})` : ""}`, ops));
      }
      for (const [indexName, index] of targetIndexes) {
        if (sourceIndexes.has(indexName)) continue;
        items.push(item("drop_index", indexName, `(${index.columns.join(", ")}) · 源中不存在`, [{ phase: 1, sql: dropIndexSql(name, index) }]));
      }

      const sourceForeignKeys = new Map((sourceTable.foreignKeys || []).map((key) => [key.name, key]));
      const targetForeignKeys = new Map((targetTable.foreignKeys || []).map((key) => [key.name, key]));
      const sqliteKeys = protocol === "sqlite";
      for (const [keyName, key] of sourceForeignKeys) {
        const existing = targetForeignKeys.get(keyName) || (sqliteKeys ? [...targetForeignKeys.values()].find((candidate) => keyShape(candidate) === keyShape(key)) : null);
        if (existing && keyShape(existing) === keyShape(key)) continue;
        const ops = sqliteKeys ? [] : [...(existing ? [{ phase: 0, sql: dropForeignKeySql(name, existing) }] : []), { phase: 5, sql: addForeignKeySql(name, key) }];
        items.push(item(existing ? "modify_fk" : "add_fk", keyName, `(${key.columns.join(", ")}) → ${key.referencedTable}(${key.referencedColumns.join(", ")})${sqliteKeys ? " · SQLite 需重建表" : ""}`, ops));
      }
      for (const [keyName, key] of targetForeignKeys) {
        if (sourceForeignKeys.has(keyName) || (sqliteKeys && [...sourceForeignKeys.values()].some((candidate) => keyShape(candidate) === keyShape(key)))) continue;
        items.push(item("drop_fk", keyName, `(${key.columns.join(", ")}) → ${key.referencedTable} · 源中不存在${sqliteKeys ? " · SQLite 需重建表" : ""}`, sqliteKeys ? [] : [{ phase: 0, sql: dropForeignKeySql(name, key) }]));
      }
      tables.push({ name, isView: false, status: items.length ? "changed" : "same", items });
    }
    for (const [name, targetTable] of targetTables) {
      if (sourceTables.has(name)) continue;
      const isView = /VIEW/i.test(targetTable.tableType || "");
      tables.push({ name, isView, status: "target_only", items: [item(isView ? "drop_view" : "drop_table", name, "源中不存在", [{ phase: 6, sql: `DROP ${isView ? "VIEW" : "TABLE"} ${qt(name)}` }])] });
    }
    const rank = { changed: 0, source_only: 1, target_only: 2, same: 3 };
    tables.sort((a, b) => rank[a.status] - rank[b.status] || a.name.localeCompare(b.name));
    if (sourceProtocol !== protocol) notes.unshift(`源是 ${protocolMeta[sourceProtocol]?.label || sourceProtocol}，目标是 ${protocolMeta[protocol]?.label || protocol}：列类型按源原样输出。`);
    return { tables, notes };
  }

  async function runSchemaCompare() {
    const { source, target } = compareState;
    const sourceConnection = connectionLookup.get(source.connectionId);
    const targetConnection = connectionLookup.get(target.connectionId);
    if (!sourceConnection || !targetConnection) {
      toast("请选择源和目标", "两侧都需要一个 SQL 连接。", "danger");
      return;
    }
    const sourceSchema = sourceConnection.protocol === "sqlite" ? "main" : source.schema;
    const targetSchema = targetConnection.protocol === "sqlite" ? "main" : target.schema;
    if (!sourceSchema || !targetSchema) {
      toast("请选择数据库", "源和目标都需要选择具体的数据库或模式。", "danger");
      return;
    }
    if (source.connectionId === target.connectionId && sourceSchema === targetSchema) {
      toast("源和目标相同", "请为目标选择另一个数据库，或另一个连接。", "danger");
      return;
    }
    compareState.running = true;
    renderCompareView();
    const detail = (connectionId, schema) => backendCall("sql_metadata", {
      connectionId,
      request: { connectionId, schema, table: null, includeTables: true, includeColumns: true },
    }).then((snapshot) => (snapshot.databases || []).find((database) => database.name === schema) || snapshot.databases?.[0] || { tables: [] });
    try {
      const started = performance.now();
      const [sourceDetail, targetDetail] = await Promise.all([detail(source.connectionId, sourceSchema), detail(target.connectionId, targetSchema)]);
      const diff = computeSchemaDiff(sourceDetail, targetDetail, { protocol: targetConnection.protocol, sourceProtocol: sourceConnection.protocol, schema: targetSchema });
      compareState.result = {
        ...diff,
        sourceId: source.connectionId,
        targetId: target.connectionId,
        targetSchema,
        targetProtocol: targetConnection.protocol,
        sameProtocol: sourceConnection.protocol === targetConnection.protocol,
        elapsed: Math.round(performance.now() - started),
      };
    } catch (error) {
      compareState.result = { error: errorMessage(error) };
    } finally {
      compareState.running = false;
      renderCompareView();
    }
  }

  function executeSchemaSync() {
    const result = compareState.result;
    const statements = compareSelectedStatements();
    if (!result?.sameProtocol || !statements.length) {
      toast("没有可执行的同步脚本", "请先比较并勾选要同步的变更。", "danger");
      return;
    }
    const target = connectionLookup.get(result.targetId);
    confirmDangerous(
      `SYNC SCHEMA ${target?.name || ""}`,
      `${target?.name || "目标连接"}${result.targetProtocol === "sqlite" ? "" : ` / ${result.targetSchema}`}\n${statements.length} 条 DDL\n${statements.slice(0, 8).join(";\n")}${statements.length > 8 ? "\n…" : ""}`,
      async () => {
        const op = operationId();
        try {
          await backendCall("sql_apply_ddl", {
            connectionId: result.targetId,
            operationId: op,
            statements: statements.map((sql, index) => ({ connectionId: result.targetId, sql, params: [], operationId: `${op}-${index + 1}`, schema: result.targetSchema })),
          });
          toast("结构同步完成", `${statements.length} 条语句已在 ${target?.name || "目标"} 上执行，正在重新比较。`);
        } catch (error) {
          showOperationError(error, "结构同步失败");
        } finally {
          afterDdl(result.targetId);
          refreshAuditOnly();
          await runSchemaCompare();
        }
      },
    );
  }

  /* ---------- data transfer & row sync ---------- */

  const MODE_HELP = [
    {
      key: "transfer", action: "view-transfer", title: "数据传输", tag: "一次性搬运",
      text: "按勾选的表整体复制结构与数据，不比对差异。目标表可以追加、清空后写入或删除重建。适合搬库、建副本、把任意 SQL 源写进一个 SQLite 文件。",
    },
    {
      key: "sync", action: "view-sync", title: "数据同步", tag: "按主键追平",
      text: "逐行比对同引擎的两张表，只写有差异的行：源有目标无就新增，内容不同就更新，目标多出才删除（默认不勾选）。不会改动表结构。",
    },
    {
      key: "compare", action: "view-compare", title: "结构比较", tag: "只生成 DDL",
      text: "只比较表、列、索引与外键的定义，生成让目标与源一致的 DDL 脚本，不复制任何行数据。",
    },
  ];

  const TRANSFER_MODES = [
    { value: "append", label: "追加写入", hint: "保留目标已有数据，只把源表的行插入进去" },
    { value: "truncate", label: "清空后写入", hint: "先清空目标表，再写入源表的全部行" },
    { value: "recreate", label: "删除重建", hint: "删除目标表，按源表结构重建后再写入" },
  ];
  const TRANSFER_PHASE_LABELS = { prepare: "准备目标结构", copy: "复制数据", finished: "收尾" };
  const TRANSFER_TABLE_STATUS_LABELS = { pending: "等待", running: "复制中", done: "完成", failed: "失败", skipped: "跳过", cancelled: "已停止" };
  const TRANSFER_ACTION_LABELS = { created: "已建表", truncated: "已清空", recreated: "已重建", append: "追加" };
  const JOB_STATUS_LABELS = { running: "进行中", done: "已完成", failed: "失败", cancelled: "已停止" };
  const SYNC_PHASE_LABELS = { source: "扫描源表并比对目标", target: "扫描目标表找多余行", finished: "生成计划" };
  const SYNC_KIND_LABELS = { insert: "新增行", update: "更新行", delete: "删除行" };
  const MAX_SYNC_ROWS_SHOWN = 500;
  const MAX_SYNC_SQL_SHOWN = 300;

  function modeStripHtml(current) {
    return `<div class="mode-strip">${MODE_HELP.map((card) => `
      <button class="mode-card${card.key === current ? " current" : ""}" data-action="${card.action}" title="切换到${card.title}">
        <span class="mode-tag">${card.tag}</span>
        <h4>${card.title}</h4>
        <p>${card.text}</p>
      </button>`).join("")}</div>`;
  }

  function sqlSchemaFor(side) {
    const connection = connectionLookup.get(side.connectionId);
    if (!connection) return side.schema || null;
    return connection.protocol === "sqlite" ? "main" : side.schema || null;
  }

  function secondsOf(value) {
    return `${((Number(value) || 0) / 1000).toFixed(1)} 秒`;
  }

  function baseTables(connectionId, schema) {
    if (!connectionId || !schema) return Promise.resolve([]);
    return loadTables(connectionId, schema).then((tables) => tables.filter((table) => !/VIEW/i.test(table.type || "")));
  }

  const transferState = {
    source: { connectionId: null, schema: null },
    target: { connectionId: null, schema: null },
    available: [],
    targetTables: [],
    tables: [],
    filter: "",
    mode: "append",
    createMissing: true,
    createIndexes: true,
    disableIndexes: false,
    disableChecks: false,
    batchSize: 500,
    job: null,
    operationId: null,
    timer: 0,
  };

  const syncState = {
    source: { connectionId: null, schema: null, table: null },
    target: { connectionId: null, schema: null, table: null },
    available: { source: [], target: [] },
    maxRows: 200000,
    job: null,
    plan: null,
    selected: new Set(),
    operationId: null,
    timer: 0,
  };

  function initTransferState() {
    const connections = sqlConnections();
    const valid = (id) => connections.some((connection) => connection.id === id);
    if (!valid(transferState.source.connectionId)) {
      const preferred = activeFamily === "sql" && valid(activeConnectionId) ? activeConnectionId : connections[0]?.id || null;
      transferState.source = {
        connectionId: preferred,
        schema: preferred === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(preferred),
      };
      transferState.tables = [];
      transferState.available = [];
    }
    if (!valid(transferState.target.connectionId)) transferState.target = { connectionId: transferState.source.connectionId, schema: null };
    for (const side of [transferState.source, transferState.target]) {
      if (side.connectionId && !side.schema) side.schema = defaultSchemaFor(side.connectionId);
    }
  }

  function initSyncState() {
    const connections = sqlConnections();
    const valid = (id) => connections.some((connection) => connection.id === id);
    if (!valid(syncState.source.connectionId)) {
      const preferred = activeFamily === "sql" && valid(activeConnectionId) ? activeConnectionId : connections[0]?.id || null;
      syncState.source = {
        connectionId: preferred,
        schema: preferred === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(preferred),
        table: activeFamily === "sql" && preferred === activeConnectionId ? activeTable : null,
      };
    }
    if (!valid(syncState.target.connectionId)) syncState.target = { connectionId: syncState.source.connectionId, schema: null, table: null };
    for (const key of ["source", "target"]) {
      const side = syncState[key];
      if (side.connectionId && !side.schema) side.schema = defaultSchemaFor(side.connectionId);
    }
  }

  function transferPickerRows() {
    const needle = transferState.filter.trim().toLowerCase();
    const existing = new Set(transferState.targetTables.map((table) => table.name));
    const visible = transferState.available.filter((table) => !needle || table.name.toLowerCase().includes(needle));
    if (!visible.length) {
      return `<div class="picker-empty">${transferState.available.length ? "没有匹配当前过滤条件的表。" : "该库里没有可读的表（视图不参与数据传输）。"}</div>`;
    }
    return visible.map((table) => `
      <label class="picker-row">
        <input type="checkbox" data-transfer-table="${escapeHtml(table.name)}"${transferState.tables.includes(table.name) ? " checked" : ""} />
        <span>${escapeHtml(table.name)}</span>
        <span class="picker-target">${existing.has(table.name) ? "目标已存在" : transferState.createMissing ? "将新建" : "目标缺失"}</span>
      </label>`).join("");
  }

  function transferTargetSummary() {
    const existing = new Set(transferState.targetTables.map((table) => table.name));
    const missing = transferState.tables.filter((name) => !existing.has(name));
    if (!transferState.tables.length) return "目标表默认与源表同名。在左侧勾选表后，这里会显示哪些表需要新建。";
    const parts = [`同名目标表：已存在 <b>${transferState.tables.length - missing.length}</b> 张 · 需新建 <b>${missing.length}</b> 张。`];
    if (missing.length && !transferState.createMissing) parts.push("未勾选“创建缺失的表”，缺失的表会被跳过。");
    if (transferState.mode === "append") parts.push("追加模式遇到相同主键时该表会失败并记录错误，其余表继续。");
    if (transferState.mode === "truncate") parts.push("清空后写入会先 TRUNCATE 目标表，原有数据不可恢复。");
    if (transferState.mode === "recreate") parts.push("删除重建会 DROP 目标表并按源结构重建，外键与触发器不会跟着重建。");
    return parts.join(" ");
  }

  function transferSideHtml(side, key, label) {
    const connection = connectionLookup.get(side.connectionId);
    const protocol = connection?.protocol || "unknown";
    const schemas = side.connectionId ? metaFor(side.connectionId).schemas || [] : [];
    const current = sqlSchemaFor(side);
    const options = [...new Set([current, ...schemas].filter(Boolean))];
    return `<div class="compare-side ${key}">
      <div class="compare-side-label">${label}</div>
      <div class="transfer-side-body">
        <div class="compare-side-body">
          ${connection ? protocolBadgeHtml(protocol) : '<span class="protocol-badge unknown">?</span>'}
          <label class="compare-field"><small>连接</small><select data-transfer-connection="${key}">${sqlConnections().map((item) => `<option value="${escapeHtml(item.id)}"${item.id === side.connectionId ? " selected" : ""}>${escapeHtml(item.name)}</option>`).join("")}</select></label>
          <label class="compare-field"><small>${protocol === "postgres" ? "模式" : "数据库"}</small><select data-transfer-schema="${key}"${protocol === "sqlite" ? " disabled" : ""}>${options.length ? options.map((name) => `<option value="${escapeHtml(name)}"${name === current ? " selected" : ""}>${escapeHtml(name)}</option>`).join("") : '<option value="">加载中…</option>'}</select></label>
        </div>
        ${key === "source" ? `
          <div class="table-picker">
            <div class="picker-head">
              <b id="transfer-picker-count">已选 ${transferState.tables.length} / ${transferState.available.length}</b>
              <input data-transfer-filter placeholder="过滤表名" value="${escapeHtml(transferState.filter)}" />
              <button class="mini-button" data-action="transfer-tables-all">全选可见</button>
              <button class="mini-button" data-action="transfer-tables-none">清空</button>
            </div>
            <div class="picker-list" id="transfer-picker-list">${transferPickerRows()}</div>
          </div>`
        : `<div class="picker-note" id="transfer-target-summary">${transferTargetSummary()}</div>`}
      </div>
    </div>`;
  }

  function transferJobHtml() {
    const job = transferState.job;
    if (!job) {
      return `<div class="compare-placeholder"><strong>选好源表与目标后点击“开始传输”</strong><p>· 传输在桌面后台按块读写，不会把整张表装入内存，中途可以停止。<br>· 目标可以是同引擎的另一个库，也可以是任意 SQL 源写入一个 SQLite 文件；跨 MySQL / PostgreSQL 引擎不支持。<br>· 每张表独立提交批次，失败的表会在下表里单独报错，不会连累其他表。</p></div>`;
    }
    const running = job.status === "running";
    const total = Number(job.tablesTotal) || 0;
    const percent = total ? Math.round(((Number(job.tablesDone) || 0) * 100) / total) : running ? 3 : 100;
    const rows = (job.tables || []).map((table) => `
      <tr>
        <td>${escapeHtml(table.sourceTable)}</td>
        <td>${escapeHtml(table.targetTable)}</td>
        <td class="state ${escapeHtml(table.status)}">${escapeHtml(TRANSFER_TABLE_STATUS_LABELS[table.status] || table.status)}${table.action ? ` · ${escapeHtml(TRANSFER_ACTION_LABELS[table.action] || table.action)}` : ""}${table.notes?.length ? `<span class="cell-note">${escapeHtml(table.notes.join(" · "))}</span>` : ""}</td>
        <td class="num">${formatCount(table.rowsRead) ?? "—"}</td>
        <td class="num">${formatCount(table.rowsWritten) ?? "—"}</td>
        <td class="num">${table.durationMs ? `${(table.durationMs / 1000).toFixed(1)}s` : "—"}</td>
        <td class="state error-text">${escapeHtml(table.error || "")}</td>
      </tr>`).join("");
    return `<div class="job-panel">
      <div class="job-progress">
        <div class="job-progress-top">
          <strong>${escapeHtml(JOB_STATUS_LABELS[job.status] || job.status)} · ${escapeHtml(TRANSFER_PHASE_LABELS[job.phase] || job.phase)}</strong>
          <span>${total ? `${formatCount(job.tablesDone)}/${formatCount(job.tablesTotal)} 张表` : ""} · ${escapeHtml(secondsOf(job.elapsedMs))}</span>
        </div>
        <div class="job-bar${job.status === "failed" ? " failed" : running ? " running" : ""}"><i style="width:${percent}%"></i></div>
        ${(() => {
          const totalRows = Number(job.totalRows) || 0;
          const read = Number(job.rowsRead) || 0;
          const written = Number(job.rowsWritten) || 0;
          const elapsedSec = Math.max(0.1, (Number(job.elapsedMs) || 0) / 1000);
          const rate = Math.round(written / elapsedSec);
          const percentRows = totalRows ? Math.min(100, Math.round((read * 100) / totalRows)) : null;
          const etaSec = rate > 0 && totalRows > read ? Math.round((totalRows - read) / rate) : null;
          const runningTable = (job.tables || []).find((table) => table.status === "running");
          const tablePct = runningTable?.totalRows ? Math.min(100, Math.round((runningTable.rowsRead * 100) / runningTable.totalRows)) : null;
          return `<div class="job-rows-progress">
            <div class="job-bar slim${job.status === "failed" ? " failed" : running ? " running" : ""}"><i style="width:${percentRows ?? 0}%"></i></div>
            <div class="job-stats"><span>${formatCount(read)}${totalRows ? `/${formatCount(totalRows)}` : ""} 行（读取）</span><span>${formatCount(rate)}/s</span><span>${etaSec != null ? `约 ${secondsOf(etaSec * 1000)} 后完成` : running ? "估算中…" : "已完成"}</span></div>
            ${runningTable ? `<div class="picker-note">正在复制 ${escapeHtml(runningTable.sourceTable)} · ${formatCount(runningTable.rowsRead)}/${runningTable.totalRows ? formatCount(runningTable.totalRows) : "?"} 行${tablePct != null ? `（${tablePct}%）` : ""}</div>` : ""}
          </div>`;
        })()}
        <div class="job-stats">
          <span class="compare-chip"><b>${formatCount(job.rowsRead) ?? "0"}</b>读取行</span>
          <span class="compare-chip same"><b>${formatCount(job.rowsWritten) ?? "0"}</b>写入行</span>
          <span class="toolbar-spacer"></span>
          ${running
            ? '<button class="danger-button" data-action="transfer-stop"><svg><use href="#i-stop"/></svg>停止</button>'
            : '<button class="ghost-button" data-action="transfer-clear">清除结果</button>'}
        </div>
        ${job.sourceProtocol && job.targetProtocol ? `<div class="picker-note">${escapeHtml(job.sourceName)}（${escapeHtml(job.sourceProtocol)}）→ ${escapeHtml(job.targetName)}（${escapeHtml(job.targetProtocol)}）· 模式：${escapeHtml(TRANSFER_MODES.find((mode) => mode.value === transferState.mode)?.label || transferState.mode)}${job.error ? ` · ${escapeHtml(job.error)}` : ""}</div>` : ""}
      </div>
      <div class="job-scroll"><table class="job-table" data-resize-key="transfer">
        <thead><tr><th>源表<span class="col-resizer" data-gcol="0"></span></th><th>目标表<span class="col-resizer" data-gcol="1"></span></th><th>状态<span class="col-resizer" data-gcol="2"></span></th><th class="num">读取<span class="col-resizer" data-gcol="3"></span></th><th class="num">写入<span class="col-resizer" data-gcol="4"></span></th><th class="num">耗时<span class="col-resizer" data-gcol="5"></span></th><th>错误<span class="col-resizer" data-gcol="6"></span></th></tr></thead>
        <tbody>${rows}</tbody>
      </table></div>
    </div>`;
  }

  function renderTransferProgress() {
    const panel = $("#transfer-job");
    if (panel) panel.innerHTML = transferJobHtml();
    applyGenericColWidths();
  }

  function renderTransferPicker() {
    const list = $("#transfer-picker-list");
    if (list) list.innerHTML = transferPickerRows();
    const count = $("#transfer-picker-count");
    if (count) count.textContent = `已选 ${transferState.tables.length} / ${transferState.available.length}`;
    const summary = $("#transfer-target-summary");
    if (summary) summary.innerHTML = transferTargetSummary();
  }

  function renderTransferView() {
    const view = $("#view-transfer");
    if (!view) return;
    initTransferState();
    const connections = sqlConnections();
    const running = transferState.job?.status === "running";
    if (!connections.length) {
      view.innerHTML = `
        <div class="view-heading"><div><div class="eyebrow">DATA TRANSFER</div><h2>数据传输</h2><p>整表复制结构与数据，不比对差异。</p></div></div>
        ${modeStripHtml("transfer")}
        <div class="compare-placeholder"><strong>还没有 SQL 连接</strong><p>数据传输在 MySQL、PostgreSQL 与 SQLite 之间进行。</p><button class="primary-button" data-action="new-connection">新建连接</button></div>`;
      return;
    }
    view.innerHTML = `
      <div class="view-heading">
        <div><div class="eyebrow">DATA TRANSFER</div><h2>数据传输</h2><p>整表搬运：复制你勾选的表的结构与全部行数据，不做差异比对，也不会逐行核对目标里已有的内容。只想追平行数据请用<b>数据同步</b>，只想改结构请用<b>结构比较</b>。</p></div>
      </div>
      ${modeStripHtml("transfer")}
      <div class="compare-bar">
        ${transferSideHtml(transferState.source, "source", "源 · 勾选要搬走的表")}
        ${transferSideHtml(transferState.target, "target", "目标 · 数据写入这里")}
      </div>
      <div class="compare-summary">
        <div class="mode-choices">${TRANSFER_MODES.map((mode) => `
          <label class="mode-choice" title="${mode.hint}"><input type="radio" name="transfer-mode" data-transfer-mode="${mode.value}"${transferState.mode === mode.value ? " checked" : ""} />${mode.label}</label>`).join("")}
        </div>
        <span class="toolbar-spacer"></span>
        <div class="transfer-options">
          <label><input type="checkbox" data-transfer-option="createMissing"${transferState.createMissing ? " checked" : ""} />创建缺失的表</label>
          <label><input type="checkbox" data-transfer-option="createIndexes"${transferState.createIndexes ? " checked" : ""} />创建二级索引</label>
          <label title="大数据量提速：复制期间删除目标二级索引，完成后自动重建"><input type="checkbox" data-transfer-option="disableIndexes"${transferState.disableIndexes ? " checked" : ""} />停用目标索引（完成后重建）</label>
          <label title="大数据量提速：MySQL 关闭 UNIQUE / 外键检查，SQLite 关闭外键"><input type="checkbox" data-transfer-option="disableChecks"${transferState.disableChecks ? " checked" : ""} />禁用唯一/外键检查</label>
          <label>批量行数<input type="number" data-transfer-batch min="50" max="5000" step="50" value="${escapeHtml(String(transferState.batchSize))}" /></label>
          <button class="primary-button" data-action="transfer-start"${running ? " disabled" : ""}><svg><use href="#i-play"/></svg>${running ? "传输中…" : "开始传输"}</button>
        </div>
      </div>
      <div id="transfer-job">${transferJobHtml()}</div>`;
    loadTransferTables();
  }

  function loadTransferTables() {
    const sourceSchema = sqlSchemaFor(transferState.source);
    const targetSchema = sqlSchemaFor(transferState.target);
    baseTables(transferState.source.connectionId, sourceSchema).then((tables) => {
      if (sqlSchemaFor(transferState.source) !== sourceSchema) return;
      transferState.available = tables;
      transferState.tables = transferState.tables.filter((name) => tables.some((table) => table.name === name));
      if (currentWorkspace() === "transfer") renderTransferPicker();
    }).catch((error) => showOperationError(error, "读取源表列表失败"));
    baseTables(transferState.target.connectionId, targetSchema).then((tables) => {
      if (sqlSchemaFor(transferState.target) !== targetSchema) return;
      transferState.targetTables = tables;
      if (currentWorkspace() === "transfer") renderTransferPicker();
    }).catch(() => { transferState.targetTables = []; });
    for (const side of [transferState.source, transferState.target]) {
      if (side.connectionId && !metaFor(side.connectionId).schemas && connectionProtocol(side.connectionId) !== "sqlite") {
        loadSchemas(side.connectionId).then(() => {
          if (currentWorkspace() === "transfer") renderTransferView();
        }).catch((error) => showOperationError(error, "读取数据库列表失败"));
      }
    }
  }

  function stopTransferPolling() {
    if (transferState.timer) window.clearInterval(transferState.timer);
    transferState.timer = 0;
  }

  function pollTransferProgress() {
    stopTransferPolling();
    transferState.timer = window.setInterval(async () => {
      if (!transferState.operationId) {
        stopTransferPolling();
        return;
      }
      let job = null;
      try {
        job = await backendCall("transfer_status", { operationId: transferState.operationId });
      } catch (error) {
        stopTransferPolling();
        transferState.job = { status: "failed", phase: "prepare", tables: [], error: errorMessage(error) };
        if (currentWorkspace() === "transfer") renderTransferView();
        showOperationError(error, "读取传输进度失败");
        return;
      }
      transferState.job = job;
      const serialized = JSON.stringify(job);
      if (serialized !== transferState.lastSnapshot) {
        transferState.lastSnapshot = serialized;
        if (currentWorkspace() === "transfer") renderTransferProgress();
      }
      if (job.status !== "running") {
        stopTransferPolling();
        onTransferFinished(job);
      }
    }, 800);
  }

  function onTransferFinished(job) {
    if (job.status === "done") {
      toast("数据传输完成", `${formatCount(job.tablesTotal)} 张表 · 写入 ${formatCount(job.rowsWritten)} 行 · ${secondsOf(job.elapsedMs)}。`);
    } else if (job.status === "cancelled") {
      toast("传输已停止", `已写入 ${formatCount(job.rowsWritten) ?? "0"} 行。已完成的表是完整的，正在复制的表只保留了已写入的部分。`, "danger");
    } else {
      toast("传输未完成", job.error || "部分表复制失败，请在下表查看错误列。", "danger");
    }
    afterDdl(transferState.target.connectionId);
    transferState.targetTables = [];
    refreshAuditOnly();
    if (currentWorkspace() === "transfer") {
      renderTransferView();
      loadTransferTables();
    }
  }

  async function runDataTransfer() {
    const sourceConnection = connectionLookup.get(transferState.source.connectionId);
    const targetConnection = connectionLookup.get(transferState.target.connectionId);
    const sourceSchema = sqlSchemaFor(transferState.source);
    const targetSchema = sqlSchemaFor(transferState.target);
    if (!sourceConnection || !targetConnection) {
      toast("请选择源和目标", "两侧都需要一个 SQL 连接。", "danger");
      return;
    }
    if (!transferState.tables.length) {
      toast("没有勾选表", "请至少勾选一张要传输的表。", "danger");
      return;
    }
    if (!sourceSchema || !targetSchema) {
      toast("请选择数据库", "源和目标都需要确定数据库或模式。", "danger");
      return;
    }
    if (sourceConnection.protocol !== targetConnection.protocol && targetConnection.protocol !== "sqlite") {
      toast("不支持的引擎组合", `${sourceConnection.protocol} → ${targetConnection.protocol}：数据传输只做同引擎复制，以及任意 SQL 源写入 SQLite。跨引擎的表结构请用结构比较手工处理。`, "danger");
      return;
    }
    if (transferState.mode === "append"
      && transferState.source.connectionId === transferState.target.connectionId
      && sourceSchema === targetSchema) {
      toast("源库与目标库相同", "追加模式会把数据写回同一批表。请改用另一个库、另一台服务器，或选择清空后写入。", "danger");
      return;
    }
    const request = {
      sourceConnectionId: transferState.source.connectionId,
      targetConnectionId: transferState.target.connectionId,
      targetSchema,
      tables: transferState.tables.map((table) => ({ schema: sourceSchema, table })),
      mode: transferState.mode,
      createMissing: transferState.createMissing,
      createIndexes: transferState.createIndexes,
      disableIndexes: Boolean(transferState.disableIndexes),
      disableChecks: Boolean(transferState.disableChecks),
      batchSize: transferState.batchSize,
      operationId: operationId(),
    };
    const launch = async () => {
      try {
        transferState.job = await backendCall("transfer_start", { request });
        transferState.operationId = request.operationId;
        pollTransferProgress();
      } catch (error) {
        transferState.operationId = null;
        transferState.job = { status: "failed", phase: "prepare", tables: [], error: errorMessage(error) };
        showOperationError(error, "无法开始传输");
      }
      if (currentWorkspace() === "transfer") renderTransferView();
    };
    if (transferState.mode === "append") {
      await launch();
      return;
    }
    confirmDangerous(
      `TRANSFER ${transferState.mode.toUpperCase()} ${targetConnection.name}`,
      `${targetConnection.name}${targetSchema ? ` / ${targetSchema}` : ""}\n模式：${TRANSFER_MODES.find((mode) => mode.value === transferState.mode)?.label}\n将受影响的目标表：${request.tables.slice(0, 12).map((item) => item.table).join(", ")}${request.tables.length > 12 ? ` …共 ${request.tables.length} 张` : ""}`,
      launch,
    );
  }

  function renderSyncSelectors() {
    for (const key of ["source", "target"]) {
      const select = $(`#sync-table-${key}`);
      if (!select) continue;
      const side = syncState[key];
      const names = [...new Set([side.table, ...syncState.available[key].map((table) => table.name)].filter(Boolean))];
      select.innerHTML = names.length
        ? names.map((name) => `<option value="${escapeHtml(name)}"${name === side.table ? " selected" : ""}>${escapeHtml(name)}</option>`).join("")
        : '<option value="">加载中…</option>';
    }
  }

  function loadSyncTables(key) {
    const side = syncState[key];
    const schema = sqlSchemaFor(side);
    if (!side.connectionId || !schema) {
      syncState.available[key] = [];
      return;
    }
    baseTables(side.connectionId, schema).then((tables) => {
      if (sqlSchemaFor(syncState[key]) !== schema) return;
      syncState.available[key] = tables;
      if (syncState[key].table && !tables.some((table) => table.name === syncState[key].table)) syncState[key].table = null;
      if (currentWorkspace() === "sync") renderSyncSelectors();
    }).catch((error) => showOperationError(error, "读取表列表失败"));
  }

  function syncSideHtml(side, key, label) {
    const connection = connectionLookup.get(side.connectionId);
    const protocol = connection?.protocol || "unknown";
    const schemas = side.connectionId ? metaFor(side.connectionId).schemas || [] : [];
    const current = sqlSchemaFor(side);
    const options = [...new Set([current, ...schemas].filter(Boolean))];
    return `<div class="compare-side ${key}">
      <div class="compare-side-label">${label}</div>
      <div class="transfer-side-body">
        <div class="compare-side-body">
          ${connection ? protocolBadgeHtml(protocol) : '<span class="protocol-badge unknown">?</span>'}
          <label class="compare-field"><small>连接</small><select data-sync-connection="${key}">${sqlConnections().map((item) => `<option value="${escapeHtml(item.id)}"${item.id === side.connectionId ? " selected" : ""}>${escapeHtml(item.name)}</option>`).join("")}</select></label>
          <label class="compare-field"><small>${protocol === "postgres" ? "模式" : "数据库"}</small><select data-sync-schema="${key}"${protocol === "sqlite" ? " disabled" : ""}>${options.length ? options.map((name) => `<option value="${escapeHtml(name)}"${name === current ? " selected" : ""}>${escapeHtml(name)}</option>`).join("") : '<option value="">加载中…</option>'}</select></label>
        </div>
        <label class="compare-field"><small>表（需要主键）</small><select id="sync-table-${key}" data-sync-table="${key}"><option value="">加载中…</option></select></label>
      </div>
    </div>`;
  }

  function syncProgressHtml() {
    const job = syncState.job;
    const percent = job.phase === "target" ? 78 : job.phase === "finished" ? 96 : 34;
    return `<div class="job-panel">
      <div class="job-progress">
        <div class="job-progress-top">
          <strong>正在比对 · ${escapeHtml(SYNC_PHASE_LABELS[job.phase] || job.phase)}</strong>
          <span>${escapeHtml(secondsOf(job.elapsedMs))}</span>
        </div>
        <div class="job-bar running"><i style="width:${percent}%"></i></div>
        <div class="job-stats">
          <span class="compare-chip"><b>${formatCount(job.sourceRows) ?? "0"}</b>源表已扫描</span>
          <span class="compare-chip"><b>${formatCount(job.targetRows) ?? "0"}</b>目标已匹配</span>
          <span class="compare-chip same"><b>${formatCount(job.identical) ?? "0"}</b>行一致</span>
          <span class="compare-chip source_only"><b>${formatCount(job.inserts) ?? "0"}</b>待新增</span>
          <span class="compare-chip changed"><b>${formatCount(job.updates) ?? "0"}</b>待更新</span>
          <span class="compare-chip target_only"><b>${formatCount(job.deletes) ?? "0"}</b>待删除</span>
          <span class="toolbar-spacer"></span>
          <button class="danger-button" data-action="sync-plan-stop"><svg><use href="#i-stop"/></svg>停止</button>
        </div>
        <div class="picker-note">进度按已扫描行数与 ${formatCount(syncState.maxRows)} 行上限估算；比对只读取数据，不写入任何一张表。</div>
      </div>
    </div>`;
  }

  function syncOpHtml(op) {
    const selected = syncState.selected.has(op.id);
    const detail = op.kind === "update" ? (op.columns || []).join(", ") : "";
    const tail = op.kind === "update" ? `${(op.columns || []).length} 列不同` : op.kind === "insert" ? "目标缺少" : "源中不存在";
    return `<label class="sync-op ${op.kind}${selected ? "" : " off"}">
      <input type="checkbox" data-sync-operation="${op.id}"${selected ? " checked" : ""} />
      <span class="op-key">${escapeHtml(op.key)}</span>
      <span class="op-detail">${escapeHtml(detail)}</span>
      <span class="diff-count">${tail}</span>
    </label>`;
  }

  function syncGroupHtml(kind, ops) {
    if (!ops.length) return "";
    const picked = ops.filter((op) => syncState.selected.has(op.id)).length;
    const shown = ops.slice(0, MAX_SYNC_ROWS_SHOWN);
    return `<div class="sync-group ${kind}" data-sync-kind="${kind}">
      <div class="sync-group-head">
        <input type="checkbox" data-sync-group="${kind}"${picked === ops.length ? " checked" : ""} />
        <strong>${SYNC_KIND_LABELS[kind]}</strong>
        <span class="diff-count">${picked} / ${ops.length}</span>
      </div>
      <div class="sync-ops">${shown.map(syncOpHtml).join("")}${ops.length > shown.length ? `<div class="picker-empty">仅列出前 ${MAX_SYNC_ROWS_SHOWN} 条；勾选左侧复选框会作用于全部 ${ops.length} 条。</div>` : ""}</div>
    </div>`;
  }

  function syncPlanOperations(kind) {
    return (syncState.plan?.operations || []).filter((op) => op.kind === kind);
  }

  function syncChipsHtml() {
    const plan = syncState.plan;
    if (!plan) return "";
    return `
      <span class="compare-chip"><b>${formatCount(plan.sourceRows)}</b>源表行</span>
      <span class="compare-chip"><b>${formatCount(plan.targetRows)}</b>目标行</span>
      <span class="compare-chip same"><b>${formatCount(plan.identical)}</b>行一致</span>
      <span class="compare-chip source_only"><b>${syncPlanOperations("insert").length}</b>待新增</span>
      <span class="compare-chip changed"><b>${syncPlanOperations("update").length}</b>待更新</span>
      <span class="compare-chip target_only"><b>${syncPlanOperations("delete").length}</b>待删除</span>`;
  }

  function syncScriptHtml() {
    const plan = syncState.plan;
    if (!plan) return "";
    const ops = plan.operations.filter((op) => syncState.selected.has(op.id));
    const deletes = ops.filter((op) => op.kind === "delete").length;
    const preview = ops.slice(0, MAX_SYNC_SQL_SHOWN).map((op) => op.previewSql);
    const script = preview.length
      ? `${preview.join(";\n\n")};${ops.length > preview.length ? `\n-- 还有 ${ops.length - preview.length} 条未显示` : ""}`
      : "-- 没有勾选任何差异";
    const target = `${plan.target.connectionName}${plan.target.schema && plan.target.protocol !== "sqlite" ? ` / ${plan.target.schema}` : ""}.${plan.target.table}`;
    return `
      <div class="compare-script-head">
        <div><strong>将要执行的语句</strong><span>${ops.length} 条 · 写入 ${escapeHtml(target)}${deletes ? ` · 含 ${deletes} 条删除` : ""}</span></div>
        <div class="compare-script-actions">
          <button class="tiny-icon" data-action="copy-sync-sql" title="复制语句"${ops.length ? "" : " disabled"}><svg><use href="#i-copy"/></svg></button>
          <button class="ghost-button" data-action="sync-check" data-mode="none">全不选</button>
          <button class="${deletes ? "danger-button" : "primary-button"}" data-action="sync-apply"${ops.length ? "" : " disabled"}>执行所选</button>
        </div>
      </div>
      <pre class="compare-sql">${escapeHtml(script)}</pre>
      ${plan.skippedColumns?.length ? `<div class="compare-notes"><div><svg><use href="#i-warning"/></svg>列 ${escapeHtml(plan.skippedColumns.join(", "))} 只存在于一侧，未参与比对，也不会被写入。</div></div>` : ""}
      ${plan.notes?.length ? plan.notes.map((note) => `<div class="compare-notes"><div><svg><use href="#i-warning"/></svg>${escapeHtml(note)}</div>`).join("") : ""}
      <div class="compare-notes info">
        <div><svg><use href="#i-shield"/></svg>执行时使用绑定参数并放在同一个事务里，任一语句失败整体回滚；上面的文本仅供审阅，复制出去的内容是字面值。</div>
      </div>`;
  }

  function syncChipsBarHtml() {
    return `${syncChipsHtml()}
      <span class="toolbar-spacer"></span>
      <button class="ghost-button" data-action="sync-check" data-mode="safe">仅新增与更新</button>
      <button class="ghost-button" data-action="sync-check" data-mode="all">全选差异</button>
      <button class="ghost-button" data-action="sync-check" data-mode="none">全不选</button>
      <button class="ghost-button" data-action="sync-plan-start"><svg><use href="#i-refresh"/></svg>重新比对</button>`;
  }

  function syncResultsHtml() {
    const job = syncState.job;
    if (job?.status === "running") return syncProgressHtml();
    if (job?.status === "failed") {
      return `<div class="compare-placeholder danger"><strong>比对失败</strong><p>${escapeHtml(job.error || "未知错误")}<br>数据同步要求两侧同引擎、同一张表有主键（最多 6 个主键列），且不是视图。整表搬运请改用数据传输。</p><button class="ghost-button" data-action="sync-plan-start">重新比对</button></div>`;
    }
    if (job?.status === "cancelled") {
      return `<div class="compare-placeholder"><strong>比对已停止</strong><p>源表已扫描 ${formatCount(job.sourceRows) ?? "0"} 行，目标表 ${formatCount(job.targetRows) ?? "0"} 行。差异计划未生成，可以重新比对。</p><button class="ghost-button" data-action="sync-plan-start">重新比对</button></div>`;
    }
    const plan = syncState.plan;
    if (!plan) {
      return `<div class="compare-placeholder"><strong>选好两张表后点击“生成差异计划”</strong><p>· 两侧必须是同一引擎（MySQL→MySQL、PostgreSQL→PostgreSQL、SQLite→SQLite），并且都定义了主键。<br>· 按主键逐行比对：源有目标无 → 新增；主键相同但列值不同 → 只更新变化的列；目标多出的行 → 删除，默认不勾选。<br>· 比对只读取行数据；执行才写入，并需要一个可停止的操作编号与 30 分钟内有效的计划。</p></div>`;
    }
    return `
      <div class="compare-summary" id="sync-chips">${syncChipsBarHtml()}</div>
      <div class="sync-split">
        <div class="sync-list">
          ${syncGroupHtml("insert", plan.operations.filter((op) => op.kind === "insert"))}
          ${syncGroupHtml("update", plan.operations.filter((op) => op.kind === "update"))}
          ${syncGroupHtml("delete", plan.operations.filter((op) => op.kind === "delete"))}
          ${plan.operations.length ? "" : '<div class="compare-placeholder"><strong>两张表的数据一致</strong><p>没有需要新增、更新或删除的行。</p></div>'}
        </div>
        <div class="compare-script" id="sync-script">${syncScriptHtml()}</div>
      </div>`;
  }

  function renderSyncResults() {
    const view = $("#view-sync");
    if (!view) return;
    const slot = $("#sync-results");
    if (slot) {
      slot.innerHTML = syncResultsHtml();
      renderSyncSelectors();
      return;
    }
    renderSyncView();
  }

  function refreshSyncSelection() {
    const script = $("#sync-script");
    if (script) script.innerHTML = syncScriptHtml();
    const chips = $("#sync-chips");
    if (chips) chips.innerHTML = syncChipsBarHtml();
    for (const kind of ["insert", "update", "delete"]) {
      const group = $(`.sync-group[data-sync-kind="${kind}"]`);
      if (!group) continue;
      const ops = syncPlanOperations(kind);
      const picked = ops.filter((op) => syncState.selected.has(op.id)).length;
      const count = $(".diff-count", group);
      if (count) count.textContent = `${picked} / ${ops.length}`;
      const head = $("input[data-sync-group]", group);
      if (head) head.checked = picked === ops.length;
      for (const input of $$("input[data-sync-operation]", group)) {
        const on = syncState.selected.has(Number(input.dataset.syncOperation));
        input.checked = on;
        input.closest(".sync-op")?.classList.toggle("off", !on);
      }
    }
  }

  function renderSyncView() {
    const view = $("#view-sync");
    if (!view) return;
    initSyncState();
    if (!sqlConnections().length) {
      view.innerHTML = `
        <div class="view-heading"><div><div class="eyebrow">ROW SYNC</div><h2>数据同步</h2><p>按主键逐行追平两张同引擎的表。</p></div></div>
        ${modeStripHtml("sync")}
        <div class="compare-placeholder"><strong>还没有 SQL 连接</strong><p>数据同步在 MySQL、PostgreSQL 或 SQLite 之间按主键比对行数据。</p><button class="primary-button" data-action="new-connection">新建连接</button></div>`;
      return;
    }
    view.innerHTML = `
      <div class="view-heading">
        <div><div class="eyebrow">ROW SYNC</div><h2>数据同步</h2><p>差异驱动：按主键逐行比对，只把有差异的行写入目标，不改表结构、不整表搬运。要把整张表搬过去请用<b>数据传输</b>，要改结构请用<b>结构比较</b>。</p></div>
      </div>
      ${modeStripHtml("sync")}
      <div class="compare-bar">
        ${syncSideHtml(syncState.source, "source", "源 · 以它为准")}
        <button class="compare-swap" data-action="sync-swap" title="交换源与目标">⇄</button>
        ${syncSideHtml(syncState.target, "target", "目标 · 将被写入")}
        <label class="compare-field"><small>扫描行数上限</small><input type="number" data-sync-max-rows min="1" max="100000000" step="10000" value="${escapeHtml(String(syncState.maxRows))}" /></label>
        <button class="primary-button compare-go" data-action="sync-plan-start"><svg><use href="#i-play"/></svg>生成差异计划</button>
      </div>
      <div id="sync-results" class="compare-results">${syncResultsHtml()}</div>`;
    renderSyncSelectors();
    loadSyncTables("source");
    loadSyncTables("target");
    for (const key of ["source", "target"]) {
      const side = syncState[key];
      if (side.connectionId && !metaFor(side.connectionId).schemas && connectionProtocol(side.connectionId) !== "sqlite") {
        loadSchemas(side.connectionId).then(() => {
          if (currentWorkspace() === "sync") renderSyncView();
        }).catch((error) => showOperationError(error, "读取数据库列表失败"));
      }
    }
  }

  function stopSyncPolling() {
    if (syncState.timer) window.clearInterval(syncState.timer);
    syncState.timer = 0;
  }

  function applySyncPlan(plan) {
    syncState.plan = plan;
    syncState.selected = new Set((plan.operations || []).filter((op) => op.selected).map((op) => op.id));
    renderSyncResults();
    const deletes = (plan.operations || []).filter((op) => op.kind === "delete").length;
    toast("差异计划已生成", `${formatCount(plan.sourceRows)} 行源数据 · ${syncState.plan.operations.length} 处差异${deletes ? `，其中 ${deletes} 条删除默认未勾选` : ""}。`);
  }

  function pollSyncProgress() {
    stopSyncPolling();
    syncState.timer = window.setInterval(async () => {
      if (!syncState.operationId) {
        stopSyncPolling();
        return;
      }
      let job = null;
      try {
        job = await backendCall("sync_plan_status", { operationId: syncState.operationId });
      } catch (error) {
        stopSyncPolling();
        syncState.job = { status: "failed", error: errorMessage(error) };
        if (currentWorkspace() === "sync") renderSyncResults();
        showOperationError(error, "读取比对进度失败");
        return;
      }
      syncState.job = job;
      const serialized = JSON.stringify(job);
      if (currentWorkspace() === "sync" && job.status === "running" && serialized !== syncState.lastSnapshot) {
        syncState.lastSnapshot = serialized;
        renderSyncResults();
      }
      if (job.status !== "running") {
        stopSyncPolling();
        if (job.status === "done" && job.planId) {
          try {
            applySyncPlan(await backendCall("sync_get_plan", { planId: job.planId }));
          } catch (error) {
            syncState.job = { status: "failed", error: `${errorMessage(error)}（计划可能已超过 30 分钟有效期）` };
            showOperationError(error, "读取差异计划失败");
          }
        } else if (job.status === "cancelled") {
          renderSyncResults();
          toast("比对已停止", "未生成差异计划，已有数据没有被修改。", "danger");
        } else {
          if (currentWorkspace() === "sync") renderSyncResults();
          toast("比对失败", job.error || "差异计划未能生成。", "danger");
        }
        refreshAuditOnly();
      }
    }, 800);
  }

  async function startSyncPlan() {
    const sourceConnection = connectionLookup.get(syncState.source.connectionId);
    const targetConnection = connectionLookup.get(syncState.target.connectionId);
    if (!sourceConnection || !targetConnection) {
      toast("请选择源和目标", "两侧都需要一个 SQL 连接。", "danger");
      return;
    }
    if (sourceConnection.protocol !== targetConnection.protocol) {
      toast("只支持同引擎比对", `${sourceConnection.protocol} → ${targetConnection.protocol}：数据同步按同一套主键与类型语义逐行比对，跨引擎请使用数据传输。`, "danger");
      return;
    }
    const sourceSchema = sqlSchemaFor(syncState.source);
    const targetSchema = sqlSchemaFor(syncState.target);
    if (!syncState.source.table || !syncState.target.table) {
      toast("请选择两张表", "源和目标都需要指定一张表，且该表必须有主键。", "danger");
      return;
    }
    const maxRows = Math.min(5_000_000, Math.max(1, Math.round(Number(syncState.maxRows) || 200000)));
    syncState.maxRows = maxRows;
    stopSyncPolling();
    syncState.plan = null;
    syncState.selected = new Set();
    const request = {
      sourceConnectionId: syncState.source.connectionId,
      sourceSchema,
      sourceTable: syncState.source.table,
      targetConnectionId: syncState.target.connectionId,
      targetSchema,
      targetTable: syncState.target.table,
      maxRows,
      operationId: operationId(),
    };
    syncState.job = { status: "running", phase: "source", sourceRows: 0, targetRows: 0, identical: 0, inserts: 0, updates: 0, deletes: 0, elapsedMs: 0 };
    renderSyncResults();
    try {
      syncState.job = await backendCall("sync_plan_start", { request });
      syncState.operationId = request.operationId;
      pollSyncProgress();
    } catch (error) {
      syncState.operationId = null;
      syncState.job = { status: "failed", error: errorMessage(error) };
      showOperationError(error, "无法开始比对");
    }
    if (currentWorkspace() === "sync") renderSyncResults();
  }

  function executeSyncPlan() {
    const plan = syncState.plan;
    const ids = [...syncState.selected];
    if (!plan || !ids.length) {
      toast("没有勾选差异", "请先勾选要执行的行差异。", "danger");
      return;
    }
    const byId = new Map(plan.operations.map((op) => [op.id, op]));
    const missing = ids.filter((id) => !byId.has(id));
    const picked = ids.filter((id) => byId.has(id));
    if (missing.length) syncState.selected = new Set(picked);
    const deletes = picked.filter((id) => byId.get(id).kind === "delete").length;
    const inserts = picked.filter((id) => byId.get(id).kind === "insert").length;
    const updates = picked.filter((id) => byId.get(id).kind === "update").length;
    const target = `${plan.target.connectionName}${plan.target.schema && plan.target.protocol !== "sqlite" ? ` / ${plan.target.schema}` : ""}.${plan.target.table}`;
    const run = async () => {
      try {
        const result = await backendCall("sync_apply", {
          request: { planId: plan.planId, operationIds: picked, operationId: operationId(), confirmedDangerous: true },
        });
        toast("数据同步完成", `新增 ${formatCount(result.inserts)} · 更新 ${formatCount(result.updates)} · 删除 ${formatCount(result.deletes)} · 影响 ${formatCount(result.rowsAffected)} 行 · ${secondsOf(result.durationMs)}。`);
      } catch (error) {
        showOperationError(error, "执行差异失败");
        throw error;
      }
      syncState.plan = null;
      syncState.job = null;
      syncState.selected = new Set();
      invalidateTableMeta(plan.target.connectionId, plan.target.schema, plan.target.table);
      refreshAuditOnly();
      renderSyncResults();
    };
    if (!deletes) {
      run().catch(() => {});
      return;
    }
    confirmDangerous(
      `SYNC DATA ${plan.target.connectionName}`,
      `${target}\n新增 ${inserts} 行 · 更新 ${updates} 行 · 删除 ${deletes} 行\n主键示例：${picked.slice(0, 6).map((id) => byId.get(id).key).join("、")}${picked.length > 6 ? ` …共 ${picked.length} 条` : ""}`,
      run,
    );
  }

  async function copySyncSql() {
    const plan = syncState.plan;
    const ops = plan?.operations.filter((op) => syncState.selected.has(op.id)) || [];
    if (!ops.length) {
      toast("没有可复制的语句", "请先勾选要执行的行差异。", "danger");
      return;
    }
    const copied = await copyToClipboard(textCopyPayload(`${ops.map((op) => `${op.previewSql};`).join("\n\n")}\n`));
    if (copied) toast("已复制", `${ops.length} 条语句。复制出的是字面值，应用内执行时才使用绑定参数。`);
    else toast("复制失败", "系统剪贴板不可用。", "danger");
  }

  /* ---------- end data transfer & row sync ---------- */

  function sqlStringLiteral(value) {
    return `'${String(value).replace(/'/g, "''")}'`;
  }



  function mysqlAccountParts(value) {
    const text = String(value || "").trim();
    const separator = text.lastIndexOf("@");
    if (separator <= 0 || separator === text.length - 1) throw new Error("MySQL 账户格式应为 user@host。");
    return { user: text.slice(0, separator), host: text.slice(separator + 1) };
  }

  function mysqlScope(value) {
    const text = String(value || "").trim();
    if (text === "*.*") return "*.*";
    const parts = text.split(".");
    if (parts.length !== 2 || !parts[0] || !parts[1]) throw new Error("作用域格式应为 database.table 或 *.*。");
    return `${designerIdentifier(parts[0])}.${parts[1] === "*" ? "*" : designerIdentifier(parts[1])}`;
  }

  function buildIdentitySql(action) {
    const privilege = $("#identity-privilege")?.value || "SELECT";
    if (activeProtocol === "mysql") {
      const account = mysqlAccountParts($("#identity-account")?.value);
      const scope = mysqlScope($("#identity-scope")?.value);
      return `${action} ${privilege} ON ${scope} ${action === "GRANT" ? "TO" : "FROM"} ${sqlStringLiteral(account.user)}@${sqlStringLiteral(account.host)}`;
    }
    if (activeProtocol === "postgres") {
      const role = designerIdentifier($("#identity-role")?.value);
      const object = String($("#identity-object")?.value || "").trim();
      const parts = object.split(".");
      if (parts.length !== 2 || !parts[0] || !parts[1]) throw new Error("PostgreSQL 对象格式应为 schema.table。");
      return `${action} ${privilege} ON TABLE ${designerIdentifier(parts[0])}.${designerIdentifier(parts[1])} ${action === "GRANT" ? "TO" : "FROM"} ${role}`;
    }
    throw new Error("当前协议不支持服务器级用户权限编辑。");
  }

  function updateIdentityPreview() {
    const preview = $("#identity-sql-preview");
    if (!preview) return;
    try {
      preview.value = `${buildIdentitySql("GRANT")};`;
      const error = $("#identity-form-error");
      if (error) error.hidden = true;
    } catch (error) {
      preview.value = "";
      const box = $("#identity-form-error");
      if (box) {
        box.hidden = false;
        box.textContent = errorMessage(error);
      }
    }
  }

  function executeIdentityChange(action) {
    try {
      const sql = buildIdentitySql(action);
      confirmDangerous(`${action} 权限`, `${activeConnection?.name || "SQL"}\n${sql}\n\n权限变更会立即生效并写入审计。`, async () => {
        await backendCall("sql_execute", {
          connectionId: activeConnectionId,
          request: {
            connectionId: activeConnectionId,
            sql,
            params: [],
            operationId: operationId(),
            pageSize: null,
            offset: null,
          },
        });
        toast("权限已更新", `${action} 已执行并写入审计日志。`);
        await usersModal();
      });
    } catch (error) {
      showOperationError(error, "权限变更未执行");
    }
  }

  function mysqlIdentityLiteral(value) {
    return `'${String(value ?? "")
      .replace(/\\/g, "\\\\")
      .replace(/\0/g, "\\0")
      .replace(/\n/g, "\\n")
      .replace(/\r/g, "\\r")
      .replace(/\x1a/g, "\\Z")
      .replace(/'/g, "''")}'`;
  }

  function postgresIdentityLiteral(value) {
    return `E'${String(value ?? "")
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "''")}'`;
  }

  function identityPassword() {
    const password = $("#identity-new-password")?.value || "";
    const confirmation = $("#identity-confirm-password")?.value || "";
    if (password.length < 8) throw new Error("密码至少需要 8 个字符。");
    if (password !== confirmation) throw new Error("两次输入的密码不一致。");
    return password;
  }

  function mysqlLifecycleTarget() {
    const user = String($("#identity-mysql-user")?.value || "").trim();
    const host = String($("#identity-mysql-host")?.value || "%").trim() || "%";
    if (!user) throw new Error("请填写 MySQL 用户名或角色名。");
    if (user.length > 32) throw new Error("MySQL 用户名不能超过 32 个字符。");
    if (host.length > 255) throw new Error("MySQL 主机名不能超过 255 个字符。");
    return `${mysqlIdentityLiteral(user)}@${mysqlIdentityLiteral(host)}`;
  }

  function postgresLifecycleRole() {
    const role = String($("#identity-pg-role")?.value || "").trim();
    if (!role) throw new Error("请填写 PostgreSQL 角色名。");
    if (role.length > 63) throw new Error("PostgreSQL 角色名不能超过 63 个字符。");
    return designerIdentifier(role);
  }

  function identityAuditSafeLabel(sql) {
    return String(sql)
      .replace(/\b(PASSWORD|IDENTIFIED\s+BY)\s*=?\s*E?'(?:''|\\.|[^'])*'/gi, "$1 '<已隐藏>'")
      .replace(/\bIDENTIFIED\s+WITH\s+[^ ]+\s+BY\s+E?'(?:''|\\.|[^'])*'/gi, "IDENTIFIED WITH <已隐藏>");
  }

  function executeLifecycleSql(title, summary, sql, successText) {
    confirmDangerous(title, `${activeConnection?.name || "SQL"}\n${summary}\n\n账户与权限变更会立即生效；密码不会写入审计摘要。`, async () => {
      await backendCall("sql_execute", {
        connectionId: activeConnectionId,
        request: {
          connectionId: activeConnectionId,
          sql,
          params: [],
          operationId: operationId(),
          pageSize: null,
          offset: null,
        },
      });
      toast("账户已更新", successText);
      await usersModal();
    });
  }

  function createIdentityAccount() {
    try {
      if (activeProtocol === "mysql") {
        const target = mysqlLifecycleTarget();
        const kind = $("#identity-mysql-kind")?.value || "user";
        if (kind === "role") {
          const sql = `CREATE ROLE ${target}`;
          executeLifecycleSql("CREATE ROLE", `${target} · 无登录密码`, sql, "MySQL 角色已创建。");
          return;
        }
        const password = identityPassword();
        const sql = `CREATE USER ${target} IDENTIFIED BY ${mysqlIdentityLiteral(password)}`;
        executeLifecycleSql("CREATE USER", `${target} · 密码已隐藏`, sql, "MySQL 用户已创建。");
        return;
      }
      if (activeProtocol === "postgres") {
        const role = postgresLifecycleRole();
        const canLogin = Boolean($("#identity-pg-login")?.checked);
        const password = canLogin ? identityPassword() : "";
        const sql = `CREATE ROLE ${role} ${canLogin ? "LOGIN" : "NOLOGIN"}${canLogin ? ` PASSWORD ${postgresIdentityLiteral(password)}` : ""}`;
        executeLifecycleSql("CREATE ROLE", `${role} · ${canLogin ? "可登录 · 密码已隐藏" : "不可登录"}`, sql, "PostgreSQL 角色已创建。");
        return;
      }
      throw new Error("当前协议不支持 SQL 账户生命周期操作。");
    } catch (error) {
      showOperationError(error, "账户创建未执行");
    }
  }

  function resetIdentityPassword() {
    try {
      if (activeProtocol === "mysql") {
        if (($("#identity-mysql-kind")?.value || "user") === "role") {
          throw new Error("MySQL 角色不用于登录，不能轮换登录密码。");
        }
        const password = identityPassword();
        const target = mysqlLifecycleTarget();
        executeLifecycleSql("ALTER USER", `${target} · 密码已隐藏`, `ALTER USER ${target} IDENTIFIED BY ${mysqlIdentityLiteral(password)}`, "MySQL 用户密码已轮换。");
        return;
      }
      if (activeProtocol === "postgres") {
        const password = identityPassword();
        const role = postgresLifecycleRole();
        executeLifecycleSql("ALTER ROLE", `${role} · 密码已隐藏`, `ALTER ROLE ${role} WITH PASSWORD ${postgresIdentityLiteral(password)}`, "PostgreSQL 角色密码已轮换。");
        return;
      }
      throw new Error("当前协议不支持密码轮换。");
    } catch (error) {
      showOperationError(error, "密码轮换未执行");
    }
  }

  function dropIdentityAccount() {
    try {
      if (activeProtocol === "mysql") {
        const target = mysqlLifecycleTarget();
        const kind = $("#identity-mysql-kind")?.value || "user";
        const keyword = kind === "role" ? "DROP ROLE" : "DROP USER";
        executeLifecycleSql(keyword, `${target} · 账户及其授权将删除`, `${keyword} ${target}`, `${kind === "role" ? "MySQL 角色" : "MySQL 用户"}已删除。`);
        return;
      }
      if (activeProtocol === "postgres") {
        const role = postgresLifecycleRole();
        executeLifecycleSql("DROP ROLE", `${role} · 角色及其授权将删除`, `DROP ROLE ${role}`, "PostgreSQL 角色已删除。");
        return;
      }
      throw new Error("当前协议不支持账户删除。");
    } catch (error) {
      showOperationError(error, "账户删除未执行");
    }
  }

  function redisAclUsername() {
    const username = String($("#identity-acl-user")?.value || "").trim();
    if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(username)) {
      throw new Error("Redis ACL 用户名需为 1-64 位字母、数字、点、下划线或短横线。");
    }
    return username;
  }

  async function runRedisAclCommand(args, title, summary, successText) {
    confirmDangerous(title, `${activeConnection?.name || "Redis"}\n${summary}\n\nACL 变更会立即生效，密码参数不会写入审计摘要。`, async () => {
      const op = operationId();
      activeRedisOperationId = op;
      try {
        await backendCall("redis_command", {
          connectionId: activeConnectionId,
          request: {
            connectionId: activeConnectionId,
            command: "ACL",
            args,
            operationId: op,
            confirmedDangerous: true,
          },
        });
      } finally {
        if (activeRedisOperationId === op) activeRedisOperationId = null;
      }
      toast("Redis ACL 已更新", successText);
      await usersModal();
    });
  }

  function redisAclPassword() {
    const password = $("#identity-acl-password")?.value || "";
    const confirmation = $("#identity-acl-confirm")?.value || "";
    if (password.length < 8) throw new Error("Redis ACL 密码至少需要 8 个字符。");
    if (password !== confirmation) throw new Error("两次输入的 Redis ACL 密码不一致。");
    return password;
  }

  function applyRedisAclUser() {
    try {
      const username = redisAclUsername();
      const password = redisAclPassword();
      const preset = $("#identity-acl-preset")?.value || "read";
      const commands = preset === "full" ? "+@all" : preset === "write" ? "+@write" : "+@read";
      const args = ["SETUSER", username, "reset", "on", `>${password}`, "~*", commands];
      void runRedisAclCommand(
        args,
        "ACL SETUSER",
        `用户 ${username} · ${preset === "full" ? "全部命令" : preset === "write" ? "写入命令" : "只读命令"} · 密码已隐藏`,
        `ACL 用户 ${username} 已应用。`,
      );
    } catch (error) {
      showOperationError(error, "Redis ACL 用户未更新");
    }
  }

  function resetRedisAclPassword() {
    try {
      const username = redisAclUsername();
      const password = redisAclPassword();
      void runRedisAclCommand(
        ["SETUSER", username, "resetpass", `>${password}`],
        "ACL SETUSER",
        `用户 ${username} · 重置密码 · 密码已隐藏`,
        `ACL 用户 ${username} 的密码已轮换。`,
      );
    } catch (error) {
      showOperationError(error, "Redis ACL 密码未更新");
    }
  }

  function toggleRedisAclUser(enabled) {
    try {
      const username = redisAclUsername();
      void runRedisAclCommand(
        ["SETUSER", username, enabled ? "on" : "off"],
        "ACL SETUSER",
        `用户 ${username} · ${enabled ? "启用" : "禁用"}`,
        `ACL 用户 ${username} 已${enabled ? "启用" : "禁用"}。`,
      );
    } catch (error) {
      showOperationError(error, "Redis ACL 状态未更新");
    }
  }

  function deleteRedisAclUser() {
    try {
      const username = redisAclUsername();
      void runRedisAclCommand(
        ["DELUSER", username],
        "ACL DELUSER",
        `用户 ${username} · 删除 ACL 用户及其权限`,
        `ACL 用户 ${username} 已删除。`,
      );
    } catch (error) {
      showOperationError(error, "Redis ACL 用户未删除");
    }
  }

  async function usersModal() {
    if (!activeConnectionId) {
      toast("没有活动连接", "请先选择连接后再管理用户与权限。", "danger");
      return;
    }
    try {
      const snapshot = await backendCall("list_identities", { connectionId: activeConnectionId });
      const sqlEditable = activeProtocol === "mysql" || activeProtocol === "postgres";
      const privileges = activeProtocol === "mysql"
        ? ["SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "ALTER", "INDEX", "EXECUTE", "ALL PRIVILEGES"]
        : ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL PRIVILEGES"];
      const lifecycle = activeProtocol === "mysql" ? `
        <div class="section-title" style="margin-top:14px"><span>账户与角色生命周期</span><span>立即生效 · 二次确认</span></div>
        <div class="form-grid">
          <label>对象类型<select id="identity-mysql-kind"><option value="user">登录用户</option><option value="role">角色</option></select></label>
          <label>用户名 / 角色名<input id="identity-mysql-user" autocomplete="off" placeholder="report_reader" /></label>
          <label>主机<input id="identity-mysql-host" value="%" autocomplete="off" /></label>
          <label>新密码<input id="identity-new-password" type="password" autocomplete="new-password" placeholder="至少 8 个字符" /></label>
          <label>确认密码<input id="identity-confirm-password" type="password" autocomplete="new-password" /></label>
        </div>
        <div class="form-note"><svg><use href="#i-shield"/></svg>密码仅用于本次 DDL，执行确认框和只追加审计中都会隐藏。</div>
        <div class="modal-footer"><button class="secondary-button" data-action="identity-create">创建</button><button class="secondary-button" data-action="identity-reset-password">轮换密码</button><button class="danger-button" data-action="identity-drop">删除</button></div>
      ` : activeProtocol === "postgres" ? `
        <div class="section-title" style="margin-top:14px"><span>角色生命周期</span><span>立即生效 · 二次确认</span></div>
        <div class="form-grid">
          <label>角色名<input id="identity-pg-role" autocomplete="off" placeholder="report_reader" /></label>
          <label class="full" style="display:flex;flex-direction:row;align-items:center;gap:7px"><input id="identity-pg-login" type="checkbox" style="width:13px;height:13px" />允许登录（创建时设置密码）</label>
          <label>新密码<input id="identity-new-password" type="password" autocomplete="new-password" placeholder="至少 8 个字符" /></label>
          <label>确认密码<input id="identity-confirm-password" type="password" autocomplete="new-password" /></label>
        </div>
        <div class="form-note"><svg><use href="#i-shield"/></svg>密码仅用于本次 DDL，执行确认框和只追加审计中都会隐藏。</div>
        <div class="modal-footer"><button class="secondary-button" data-action="identity-create">创建角色</button><button class="secondary-button" data-action="identity-reset-password">轮换密码</button><button class="danger-button" data-action="identity-drop">删除角色</button></div>
      ` : activeProtocol === "redis" ? `
        <div class="section-title" style="margin-top:14px"><span>Redis ACL 用户</span><span>ACL SETUSER / DELUSER</span></div>
        <div class="form-grid">
          <label>ACL 用户名<input id="identity-acl-user" autocomplete="off" placeholder="report_reader" /></label>
          <label>权限模板<select id="identity-acl-preset"><option value="read">只读（+@read）</option><option value="write">写入（+@write）</option><option value="full">全部命令（+@all）</option></select></label>
          <label>新密码<input id="identity-acl-password" type="password" autocomplete="new-password" placeholder="至少 8 个字符" /></label>
          <label>确认密码<input id="identity-acl-confirm" type="password" autocomplete="new-password" /></label>
        </div>
        <div class="form-note"><svg><use href="#i-shield"/></svg>默认 key 模式为 ~*；ACL 密码在确认框和审计摘要中隐藏。</div>
        <div class="modal-footer"><button class="secondary-button" data-action="acl-apply">应用用户与权限</button><button class="secondary-button" data-action="acl-reset-password">轮换密码</button><button class="secondary-button" data-action="acl-enable">启用</button><button class="secondary-button" data-action="acl-disable">禁用</button><button class="danger-button" data-action="acl-delete">删除用户</button></div>
      ` : "";
      showModal("用户与权限", "identity & lifecycle", `
        <div class="unsupported-banner" style="display:flex;margin-bottom:12px"><svg><use href="#i-shield"/></svg><div><strong>${sqlEditable || activeProtocol === "redis" ? "身份查看、生命周期与权限" : "只读身份信息"}</strong><p>${escapeHtml(snapshot.note || "当前仅展示身份与权限元数据。")}${activeProtocol === "sqlite" ? " SQLite 是嵌入文件数据库，不支持服务器用户或角色。" : ""}</p></div></div>
        <div class="hash-editor" style="width:100%">
          <div class="hash-head" style="grid-template-columns:repeat(${Math.max(1, snapshot.columns.length)}, minmax(0, 1fr))">${snapshot.columns.map((column) => `<span>${escapeHtml(column)}</span>`).join("")}</div>
          ${snapshot.rows.map((row) => `<div class="hash-row" style="grid-template-columns:repeat(${Math.max(1, snapshot.columns.length)}, minmax(0, 1fr))">${row.map((cell) => `<span>${formatCell(cell)}</span>`).join("")}</div>`).join("") || '<div class="empty-table">没有可见的身份记录</div>'}
        </div>
        ${lifecycle}
        ${sqlEditable ? `
          <div class="section-title" style="margin-top:14px"><span>权限变更</span><span>先生成 SQL，再二次确认</span></div>
          <div class="form-grid">
            <label>权限<select id="identity-privilege">${privileges.map((item) => `<option value="${item}">${item}</option>`).join("")}</select></label>
            ${activeProtocol === "mysql"
              ? `<label>账户<input id="identity-account" placeholder="user@host" /></label><label>作用域<input id="identity-scope" placeholder="database.table 或 *.*" /></label>`
              : `<label>角色<input id="identity-role" placeholder="role_name" /></label><label>对象<input id="identity-object" placeholder="schema.table" /></label>`}
            <label class="full">SQL 预览<textarea id="identity-sql-preview" class="modal-textarea" readonly></textarea></label>
          </div>
          <div id="identity-form-error" class="form-note danger-note" role="alert" hidden></div>
          <div class="modal-footer"><button class="secondary-button" data-action="preview-identity">生成 SQL</button><button class="secondary-button" data-action="revoke-identity">审阅 REVOKE</button><button class="danger-button" data-action="grant-identity">审阅 GRANT</button></div>
        ` : activeProtocol === "redis" ? "" : '<div class="modal-footer"><button class="secondary-button" data-close-modal>关闭</button></div>'}
      `);
      if (sqlEditable) {
        ["identity-account", "identity-scope", "identity-role", "identity-object", "identity-privilege"].forEach((id) => {
          $("#" + id)?.addEventListener("input", updateIdentityPreview);
        });
        updateIdentityPreview();
      }
    } catch (error) {
      showOperationError(error, "用户与权限读取失败");
    }
  }

  function setActiveConnectionState(connection) {
    // Preserve the outgoing connection's ES workspace so switching back restores it.
    if (esState.connectionId && esState.index && connection?.id !== esState.connectionId) {
      esSessions.set(esState.connectionId, captureEsSession());
    }
    activeConnectionId = connection?.id || null;
    activeConnection = connection || null;
    activeProtocol = connection?.protocol || "mysql";
    activeFamily = protocolFamily(activeProtocol);
    $$(".connection-row").forEach((row) => row.classList.toggle("active-connection", row.dataset.connectionSelect === activeConnectionId));
    const redisTab = $('.tool-tab[data-workspace="redis"]');
    if (redisTab) redisTab.hidden = !redisTabOpen && activeFamily !== "kv";
    const esTab = $('.tool-tab[data-workspace="es"]');
    if (esTab) esTab.hidden = !esTabOpen && activeFamily !== "search";
    resetInspectorHealth(connection);
    const statusConnection = $(".status-connection");
    if (statusConnection) statusConnection.innerHTML = `<span class="health-dot ${connection ? "" : "muted"}"></span>${escapeHtml(connection?.name || "未选择连接")}`;
    const breadcrumb = $(".breadcrumb");
    if (breadcrumb && connection) {
      breadcrumb.innerHTML = `<span class="connection-crumb">${protocolBadgeHtml(connection.protocol)}${escapeHtml(connection.name)}</span>`;
    }
    const titleContext = $("#title-context");
    if (titleContext) titleContext.textContent = connection ? `${connection.name} · ${protocolMeta[connection.protocol]?.label || connection.protocol} · ${connection.hostSummary}` : "未连接";
    const inspectorName = $("#inspector-connection-name");
    if (inspectorName) inspectorName.textContent = connection?.name || "未选择连接";
    const inspectorBadge = $(".inspector-protocol .protocol-badge");
    if (inspectorBadge) {
      inspectorBadge.className = protocolBadgeClass(connection?.protocol);
      inspectorBadge.innerHTML = connection ? protocolGlyphHtml(connection.protocol) : "?";
    }
    const inspectorStatus = $("#inspector-connection-status");
    if (inspectorStatus) inspectorStatus.innerHTML = connection
      ? '<span class="health-dot"></span>已打开 · 健康检查中'
      : '<span class="health-dot muted"></span>等待连接';
    const securityStack = $("#inspector-security");
    if (securityStack) {
      securityStack.innerHTML = connection
        ? `<span><svg><use href="#i-lock"/></svg>${connection.tlsEnabled ? "TLS 已配置" : "TLS 未启用"}</span><span><svg><use href="#i-shield"/></svg>${connection.hasSsh ? "SSH 隧道" : "无 SSH 隧道"}</span><span><svg><use href="#i-check"/></svg>健康检查</span>`
        : '<span><svg><use href="#i-lock"/></svg>安全状态待读取</span>';
    }
    const inspectorFoot = $("#inspector-foot-text");
    if (inspectorFoot) inspectorFoot.textContent = connection ? `${connection.name} · 健康检查` : "尚未打开连接";
    if (activeFamily === "sql" && window.__luxbaseSetSqlDialect) {
      window.__luxbaseSetSqlDialect(activeProtocol);
    }
  }

  async function selectConnection(connectionId) {
    const connection = connectionLookup.get(connectionId);
    if (!connection) throw new Error("连接不存在，可能已被删除。");
    if (connectionId !== activeConnectionId) {
      activeTable = null;
      activeSchema = null;
      activePrimaryKey = null;
    }
    setActiveConnectionState(connection);
    const boundTab = activeSqlTab();
    if (activeFamily === "sql" && boundTab?.kind === "query" && !boundTab.connectionId) {
      boundTab.connectionId = connection.id;
      boundTab.schema = defaultSchemaFor(connection.id);
      renderQueryContext(boundTab);
      renderBreadcrumbForTab(boundTab);
      renderSqlTabs();
      persistSqlTabs();
    }
    if (activeFamily === "sql") {
      const sameScope = objectsState.connectionId === connection.id;
      setObjectsScope(connection.id, sameScope ? objectsState.schema : defaultSchemaFor(connection.id));
    } else {
      setObjectsScope(connection.id, null, { show: false });
      showWorkspace("redis");
    }
    const group = $(`.connection-group[data-connection-id="${CSS.escape(connectionId)}"]`);
    if (!group) return;
    const children = $(".tree-children", group);
    const row = $(".connection-row", group);
    if (children && (children.hidden || group.dataset.loaded !== "true")) {
      children.hidden = false;
      $(".tree-chevron", row)?.classList.add("open");
      await loadConnectionTree(group);
    }
    if (group.dataset.loaded === "true") {
      const health = $(".health-dot", row);
      if (health) {
        health.classList.remove("muted");
        health.classList.add("online");
      }
      if (activeFamily === "sql") {
        await refreshPoolMetrics();
      } else {
        try {
          const info = await getRedisInfo(connectionId);
          const inspectorStatus = $("#inspector-connection-status");
          if (inspectorStatus) {
            inspectorStatus.innerHTML = `<span class="health-dot online"></span>PING 正常 · ${info.connectedClients ?? "—"} 客户端`;
          }
        } catch (error) {
          const inspectorStatus = $("#inspector-connection-status");
          if (inspectorStatus) inspectorStatus.innerHTML = `<span class="health-dot muted"></span>${escapeHtml(errorMessage(error))}`;
        }
      }
    }
  }

  async function loadConnectionTree(group) {
    if (!group || group.dataset.loaded === "true") return;
    const connectionId = group.dataset.connectionId;
    const connection = connectionLookup.get(connectionId);
    const children = $(".tree-children", group);
    if (!connection || !children) return;
    children.innerHTML = '<div class="tree-loading">正在读取元数据…</div>';
    if (protocolFamily(connection.protocol) === "search") {
      try {
        const indices = await backendCall("es_indices", { connectionId, operationId: operationId() });
        esState.indices = indices || [];
        esState.connectionId = connectionId;
        children.innerHTML = (indices || []).map((index) => `
          <button class="tree-row" data-es-index="${escapeHtml(index.name)}" title="${escapeHtml(index.name)} · 单击打开索引浏览">
            <span class="tree-spacer"></span>
            <svg class="tree-icon table"><use href="#i-grid"/></svg>
            <span class="tree-label">${escapeHtml(index.name)}</span>
            <span class="tree-meta">${index.docsCount != null ? formatCount(index.docsCount) : ""}</span>
          </button>
        `).join("") || '<div class="tree-empty">集群中没有索引</div>';
        group.dataset.loaded = "true";
        $(".connection-row .health-dot", group)?.classList.replace("muted", "online");
        const inspectorStatus = $("#inspector-connection-status");
        if (inspectorStatus) inspectorStatus.innerHTML = `<span class="health-dot online"></span>集群可达 · ${(indices || []).length} 个索引`;
        if (objectsState.connectionId === connectionId && currentWorkspace() === "objects") renderObjectsView();
      } catch (error) {
        children.innerHTML = `<div class="tree-error">${escapeHtml(errorMessage(error))}</div>`;
        showOperationError(error, "Elasticsearch 连接失败");
      }
      return;
    }
    if (protocolFamily(connection.protocol) === "kv") {
      try {
        const info = await getRedisInfo(connectionId, { force: true });
        const databases = connection.redisMode === "cluster"
          ? [0]
          : Array.from({ length: 16 }, (_, index) => index);
        const keyspace = new Map((info.keyspace || []).map((entry) => [entry.db, entry.keys]));
        children.innerHTML = databases.map((db) => `
          <button class="tree-row" data-redis-db="${db}">
            <span class="tree-chevron"><svg><use href="#i-chevron"/></svg></span>
            <svg class="tree-icon db"><use href="#i-database"/></svg>
            <span>DB ${db}${db === connection.redisDb ? " · 默认" : ""}</span><span class="tree-meta" data-redis-db-count="${db}">${connection.redisMode === "cluster" ? "CLUSTER" : `${keyspace.get(db) ?? 0} keys`}</span>
          </button>
          <div class="tree-grandchildren redis-key-list" data-redis-key-list="${db}" hidden></div>
        `).join("");
        group.dataset.loaded = "true";
        const inspectorStatus = $("#inspector-connection-status");
        if (inspectorStatus) {
          inspectorStatus.innerHTML = `<span class="health-dot online"></span>INFO 正常 · ${info.connectedClients ?? "—"} 客户端`;
        }
      } catch (error) {
        children.innerHTML = `<div class="tree-error">${escapeHtml(errorMessage(error))}</div>`;
        showOperationError(error, "Redis 连接失败");
      }
      return;
    }
    try {
      const [schemas, drafts] = await Promise.all([
        loadSchemas(connectionId),
        backendCall("list_query_drafts", { connectionId }).catch(() => []),
      ]);
      replaceConnectionDrafts(connectionId, drafts);
      const draftRows = (drafts || []).map((draft) => `
        <button class="tree-row" data-open-draft="${escapeHtml(String(draft.id))}">
          <span class="tree-spacer"></span>
          <svg class="tree-icon query"><use href="#i-query"/></svg>
          <span>${escapeHtml(draft.name)}</span>
          <span class="tree-meta">${escapeHtml(new Date(draft.updatedAt).toLocaleDateString("zh-CN"))}</span>
        </button>
      `).join("");
      const cachedTables = metaFor(connectionId).tables;
      if (objectsState.connectionId === connectionId && currentWorkspace() === "objects") renderObjectsView();
      children.innerHTML = `
        ${draftRows ? `<div class="database-node"><button class="tree-row" data-toggle-drafts><span class="tree-chevron open"><svg><use href="#i-chevron"/></svg></span><svg class="tree-icon query"><use href="#i-query"/></svg><span>已保存查询</span><span class="tree-meta">${drafts.length}</span></button><div class="tree-grandchildren">${draftRows}</div></div>` : ""}
        ${schemas.map((name) => `
        <div class="database-node">
          <button class="tree-row" data-sql-database="${escapeHtml(name)}" title="${escapeHtml(name)}">
            <span class="tree-chevron"><svg><use href="#i-chevron"/></svg></span>
            <svg class="tree-icon db"><use href="#i-database"/></svg>
            <span class="tree-label">${escapeHtml(name)}</span><span class="tree-meta">${cachedTables.has(name) ? cachedTables.get(name).length : ""}</span>
          </button>
          <div class="tree-grandchildren" hidden></div>
        </div>
      `).join("") || '<div class="empty-table">没有可见的数据库</div>'}`;
      group.dataset.loaded = "true";
      $(".connection-row .health-dot", group)?.classList.replace("muted", "online");
      const preferred = defaultSchemaFor(connectionId);
      const target = $$("[data-sql-database]", children).find((button) => button.dataset.sqlDatabase === preferred)
        || (schemas.length === 1 ? $("[data-sql-database]", children) : null);
      if (target) loadDatabaseObjects(target);
    } catch (error) {
      children.innerHTML = `<div class="tree-error">${escapeHtml(errorMessage(error))}</div>`;
      showOperationError(error, "元数据读取失败");
    }
  }

  function tableTreeRow(schema, table) {
    const isView = /view/i.test(table.type || "");
    const kind = isView ? "view" : "table";
    return `<button class="tree-row" data-open-table="${escapeHtml(table.name)}" data-table-schema="${escapeHtml(schema)}" data-object-type="${escapeHtml(table.type || kind)}" title="${escapeHtml(table.name)} · 单击查看结构，双击打开数据"><span class="tree-spacer"></span><svg class="tree-icon ${kind}"><use href="#i-${kind}"/></svg><span class="tree-label">${escapeHtml(table.name)}</span></button>`;
  }

  async function loadDatabaseObjects(button) {
    const node = button?.closest(".database-node");
    const children = node ? $(".tree-grandchildren", node) : null;
    const schema = button?.dataset.sqlDatabase;
    if (!node || !children || !schema || node.dataset.objectsLoaded === "true") return;
    const group = node.closest(".connection-group");
    const connectionId = group?.dataset.connectionId;
    if (!connectionId) return;
    children.hidden = false;
    $(".tree-chevron", button)?.classList.add("open");
    if (!metaFor(connectionId).tables.has(schema)) children.innerHTML = '<div class="tree-loading">正在读取库对象…</div>';
    try {
      const tables = await loadTables(connectionId, schema);
      const tableRows = tables.filter((table) => !/view/i.test(table.type || ""));
      const viewRows = tables.filter((table) => /view/i.test(table.type || ""));
      children.innerHTML = tables.length
        ? `${tableRows.map((table) => tableTreeRow(schema, table)).join("")}${viewRows.length ? `<div class="tree-section-label">视图 · ${viewRows.length}</div>${viewRows.map((table) => tableTreeRow(schema, table)).join("")}` : ""}`
        : '<div class="tree-empty">此库没有表或视图</div>';
      node.dataset.objectsLoaded = "true";
      const count = $(".tree-meta", button);
      if (count) count.textContent = String(tables.length);
      applyConnectionFilter();
    } catch (error) {
      children.innerHTML = `<div class="tree-error">${escapeHtml(errorMessage(error))}</div>`;
      showOperationError(error, "库对象读取失败");
    }
  }

  async function loadRedisKeys(db = 0, append = false) {
    requireSavedConnection("kv");
    if (redisScanState.loading) return false;
    redisScanState.loading = true;
    const list = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"] [data-redis-key-list="${db}"]`);
    if (!list) {
      redisScanState.loading = false;
      return false;
    }
    list.hidden = false;
    try {
      activeRedisOperationId = operationId();
      // SCAN's COUNT is only a hint: on large keyspaces a call can return an empty batch.
      // Keep advancing the cursor until we actually have keys (or the cursor wraps) so a
      // populated DB never renders as an empty, "unopenable" node.
      let cursor = append ? (redisDbCursors.get(db) ?? 0) : 0;
      let keys = [];
      let scanned = 0;
      let guard = 0;
      for (;;) {
        const result = await backendCall("redis_scan", {
          connectionId: activeConnectionId,
          request: {
            connectionId: activeConnectionId,
            cursor,
            pattern: redisScanState.pattern,
            count: redisScanState.count,
            typeFilter: null,
            db,
            operationId: activeRedisOperationId,
          },
        });
        keys = keys.concat(result.keys);
        scanned += result.scanned;
        cursor = result.cursor;
        redisDbCursors.set(db, cursor);
        if (cursor === 0 || keys.length > 0 || ++guard >= 64) break;
      }
      redisScanState.cursor = cursor;
      redisScanState.db = db;
      if (!append) list.innerHTML = "";
      const rows = keys.map((key) => redisKeyRow(key));
      list.insertAdjacentHTML("beforeend", rows.join(""));
      let scanMore = $(".scan-more", list);
      if (!scanMore) {
        list.insertAdjacentHTML("beforeend", `<button class="scan-more" data-action="scan-more"><svg><use href="#i-refresh"/></svg>继续 SCAN <span></span></button>`);
        scanMore = $(".scan-more", list);
      }
      scanMore.hidden = cursor === 0;
      scanMore.querySelector("span").textContent = `cursor ${cursor}`;
      const dbButton = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"] [data-redis-db="${db}"]`);
      const count = dbButton ? $("[data-redis-db-count]", dbButton) : null;
      if (count) {
        const total = await backendCall("redis_dbsize", { connectionId: activeConnectionId, db, operationId: operationId() }).catch(() => null);
        count.textContent = total != null ? `${total} keys` : `${scanned} keys`;
      }
      refreshAuditOnly();
      return true;
    } catch (error) {
      list.innerHTML = `<div class="tree-error">${escapeHtml(errorMessage(error))}<button class="scan-more" data-action="redis-retry" data-db="${db}"><svg><use href="#i-refresh"/></svg>重试</button></div>`;
      showOperationError(error, "SCAN 失败");
      return false;
    } finally {
      redisScanState.loading = false;
      activeRedisOperationId = null;
    }
  }

  function redisKeyRow(key) {
    const type = typeClass[key.keyType] || "unknown";
    const ttl = key.ttlSeconds === -1
      ? "∞"
      : key.ttlSeconds < 0
        ? "已失效"
        : key.ttlSeconds >= 3600
          ? `${Math.round(key.ttlSeconds / 3600)}h`
          : `${Math.max(1, Math.round(key.ttlSeconds / 60))}m`;
    const size = key.length != null ? `${key.length}${["hash", "set", "zset"].includes(key.keyType) ? "f" : key.keyType === "list" ? "i" : ""}` : key.sizeBytes != null ? formatBytes(key.sizeBytes) : "";
    const label = redisKeyLabel(key.key, key.keyBase64);
    return `<button class="tree-row redis-key" data-open-redis="${escapeHtml(key.key)}" data-key-type="${escapeHtml(key.keyType)}"${key.keyBase64 ? ` data-key-b64="${escapeHtml(key.keyBase64)}"` : ""} title="${escapeHtml(key.key)}"><span class="tree-spacer"></span><span class="type-icon ${type}">${typeLetter[type] || "?"}</span><span class="tree-label redis-key-name">${escapeHtml(label)}</span>${key.keyBase64 ? '<span class="key-bin-badge">bin</span>' : ""}<span class="ttl-chip">${ttl}${size ? ` · ${escapeHtml(size)}` : ""}</span></button>`;
  }

  async function getRedisInfo(connectionId, { force = false, ttlMs = 10000 } = {}) {
    const cached = redisInfoCache.get(connectionId);
    if (!force && cached && Date.now() - cached.at < ttlMs) return cached.info;
    const info = await backendCall("redis_info", { connectionId, operationId: operationId() });
    redisInfoCache.set(connectionId, { at: Date.now(), info });
    return info;
  }

  function renderRedisInfoGrid(value, info) {
    const infoGrid = $(".key-info-grid");
    if (!infoGrid || !value) return;
    infoGrid.innerHTML = `<div><span>键</span><code title="${escapeHtml(value.key)}">${escapeHtml(value.key)}</code></div><div><span>原始类型</span><code title="TYPE 命令返回值">${escapeHtml(value.keyType)}</code></div><div><span>TTL</span><code>${escapeHtml(String(value.ttlSeconds))}</code></div><div><span>编码</span><code>${escapeHtml(value.encoding || "—")}</code></div><div><span>内存占用</span><code>${escapeHtml(formatBytes(value.sizeBytes))}</code></div><div><span>实例内存</span><code>${info ? escapeHtml(formatBytes(info.memoryUsedBytes)) : "—"}</code></div><div><span>命中率</span><code>${info?.hitRate == null ? "—" : `${(info.hitRate * 100).toFixed(2)}%`}</code></div><div><span>主从状态</span><code>${info ? `${escapeHtml(info.replicationRole || "未知")} · 副本 ${info.connectedReplicas ?? 0}` : "—"}</code></div>`;
  }

  async function openRedisKey(key, fallbackType = "string", db = currentRedisDb, keyBase64 = currentRedisKeyB64) {
    const op = operationId();
    activeRedisOperationId = op;
    // Show the workspace and a loading placeholder immediately so slow reads never look frozen.
    showWorkspace("redis");
    setRedisTab("value");
    const loadingPanel = $('.redis-value-panel[data-redis-content="value"]');
    if (loadingPanel) loadingPanel.innerHTML = '<div class="tree-loading">正在读取键值…</div>';
    try {
      requireSavedConnection("kv");
      const value = await backendCall("redis_get", {
        connectionId: activeConnectionId,
        key,
        keyBase64: keyBase64 || null,
        db,
        operationId: op,
      });
      currentRedisValue = value;
      currentRedisDb = db;
      currentRedisKeyB64 = keyBase64 || null;
      currentRedisConn = activeConnectionId;
      redisBinaryEncoding = null;
      redisValueView = null;
      redisSessions.set(activeConnectionId, { db, key, b64: keyBase64 || null });
      lastRedisConn = activeConnectionId;
      redisTabOpen = true;
      const redisTabEl = $('.tool-tab[data-workspace="redis"]');
      if (redisTabEl) redisTabEl.hidden = false;
      contextRedisKey = null;
      $("#redis-key-name").textContent = redisKeyLabel(key, keyBase64);
      $("#redis-key-name").title = key;
      const breadcrumb = $(".key-breadcrumb");
      if (breadcrumb) breadcrumb.innerHTML = `Redis <span>/</span> DB ${db}`;
      const type = typeClass[value.keyType] || fallbackType;
      const keyIcon = $(".redis-key-title .type-icon");
      if (keyIcon) {
        keyIcon.className = `type-icon ${type} large`;
        keyIcon.textContent = typeLetter[type] || "?";
      }
      updateRedisMeta(value);
      renderRedisEditor(value);
      // Instance-wide INFO is heavy; render key fields now and load INFO lazily on the info tab.
      renderRedisInfoGrid(value, redisInfoCache.get(activeConnectionId)?.info || null);
      showWorkspace("redis");
      setRedisTab("value");
      $$(".redis-key").forEach((row) => row.classList.toggle("selected-key", row.dataset.openRedis === key));
    } catch (error) {
      showOperationError(error, "Redis 键读取失败");
    } finally {
      if (activeRedisOperationId === op) activeRedisOperationId = null;
    }
  }

  function updateRedisMeta(value) {
    const strip = $(".key-meta-strip");
    if (!strip) return;
    const ttl = value.ttlSeconds < 0 ? "永久" : `${value.ttlSeconds}s`;
    const size = value.sizeBytes == null ? "未知" : `${value.sizeBytes} B`;
    const length = value.value?.type === "hash" ? Object.keys(value.value.data).length
      : value.value?.type === "list" || value.value?.type === "set" || value.value?.type === "zset" ? value.value.data.length
      : value.value?.type === "stream" ? value.value.data.length : null;
    strip.innerHTML = `<span><small>类型</small><b>${escapeHtml(value.keyType)}</b></span><span><small>TTL</small><b class="green-text">${escapeHtml(ttl)}</b></span><span><small>内存</small><b>${escapeHtml(size)}</b></span><span><small>编码</small><b>${escapeHtml(value.encoding || "—")}</b></span><span><small>${length == null ? "长度" : "元素"}</small><b>${length == null ? "—" : length}</b></span>`;
  }

  function redisRawBytes(b64) {
    const bin = atob(b64);
    const bytes = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
    return bytes;
  }

  function detectRedisEncoding(b64) {
    const bytes = redisRawBytes(b64);
    const printableRatio = (text) => {
      if (!text.length) return 0;
      let good = 0;
      for (const ch of text) {
        const code = ch.codePointAt(0);
        if (code >= 32 && code !== 127) good++;
      }
      return good / text.length;
    };
    for (const enc of ["utf-8", "gbk"]) {
      try {
        const text = new TextDecoder(enc, { fatal: true }).decode(bytes);
        if (printableRatio(text) >= 0.95) return enc;
      } catch { /* try next */ }
    }
    return "strings";
  }

  function decodeRedisRaw(b64, enc) {
    const bytes = redisRawBytes(b64);
    if (enc === "hex") {
      const lines = [];
      for (let i = 0; i < bytes.length; i += 16) {
        const slice = bytes.slice(i, i + 16);
        const hex = [...slice].map((b) => b.toString(16).padStart(2, "0")).join(" ");
        const ascii = [...slice].map((b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : ".")).join("");
        lines.push(`${i.toString(16).padStart(8, "0")}  ${hex.padEnd(47)}  ${ascii}`);
      }
      return lines.join("\n");
    }
    if (enc === "strings") {
      // Extract printable runs (like `strings`) so serialized/binary payloads show their
      // readable fragments instead of an opaque hex dump.
      const runs = [];
      let current = "";
      for (const byte of bytes) {
        if (byte >= 32 && byte < 127) current += String.fromCharCode(byte);
        else {
          if (current.length >= 4) runs.push(current);
          current = "";
        }
      }
      if (current.length >= 4) runs.push(current);
      return runs.length ? runs.join("\n") : "(无可打印文本片段，可切换 Hex / Base64 查看原始字节)";
    }
    if (enc === "base64") return b64;
    try { return new TextDecoder(enc).decode(bytes); } catch { return new TextDecoder("utf-8").decode(bytes); }
  }

  function redisKeyLabel(key, b64) {
    if (!b64) return key;
    const cleaned = String(key).replace(/[\u0000-\u001f\ufffd]/g, "");
    const match = cleaned.match(/[A-Za-z_][A-Za-z0-9_:.$\-]{5,}/);
    return match ? match[0] : (cleaned || key);
  }

  function looksLikeJson(text) {
    const t = String(text || "").trim();
    if (!t || (t[0] !== "{" && t[0] !== "[")) return false;
    try { JSON.parse(t); return true; } catch { return false; }
  }

  function redisBinaryHtml(value) {
    const enc = redisBinaryEncoding || detectRedisEncoding(value.rawBase64);
    redisBinaryEncoding = enc;
    const bytes = redisRawBytes(value.rawBase64);
    const options = [["utf-8", "UTF-8"], ["gbk", "GBK"], ["latin1", "Latin-1"], ["strings", "可读片段"], ["hex", "Hex"], ["base64", "Base64"]]
      .map(([v, label]) => `<option value="${v}"${v === enc ? " selected" : ""}>${label}</option>`).join("");
    return `<div class="value-help binary"><svg><use href="#i-warning"/></svg><span>该值为非 UTF-8 二进制（${bytes.length} 字节），已按 ${enc.toUpperCase()} 只读预览；如需修改请用命令控制台，避免误改损坏数据。</span></div>
      <div class="redis-binary-bar"><label>编码<select id="redis-binary-encoding">${options}</select></label><button class="ghost-button" data-action="redis-copy-binary"><svg><use href="#i-copy"/></svg>复制</button></div>
      <pre class="redis-binary-pre" id="redis-binary-pre">${escapeHtml(decodeRedisRaw(value.rawBase64, enc))}</pre>`;
  }

  function renderRedisEditor(value) {
    const panel = $('.redis-value-panel[data-redis-content="value"]');
    if (!panel) return;
    const body = value.value;
    let editor = "";
    if (body.type === "string" && value.rawBase64) {
      editor = redisBinaryHtml(value);
    } else if (body.type === "string") {
      const isJson = looksLikeJson(body.data);
      const view = redisValueView || (isJson ? "preview" : "edit");
      redisValueView = view;
      const content = (view === "preview" && isJson)
        ? `<pre class="redis-json-pre">${highlightJson(JSON.parse(body.data))}</pre>`
        : `<textarea class="modal-textarea redis-string-editor" data-redis-editor="string" spellcheck="false">${escapeHtml(body.data)}</textarea>`;
      editor = `<div class="value-help"><svg><use href="#i-shield"/></svg><span>字符串编辑器写入时会原子替换整键，并保留当前 TTL。</span></div>${content}`;
    } else if (body.type === "hash") {
      const rows = Object.entries(body.data).map(([field, fieldValue], index) => `<div class="hash-row" data-hash-row="${index}"><input class="field redis-field-name" value="${escapeHtml(field)}" /><input class="field-value redis-field-value" value="${escapeHtml(fieldValue)}" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>`).join("");
      editor = `<div class="hash-editor"><div class="hash-head"><span>字段</span><span>值</span><span>操作</span></div>${rows}<button class="add-field" data-action="redis-add-row"><svg><use href="#i-plus"/></svg>新增字段</button></div>`;
    } else if (body.type === "list") {
      editor = `<div class="hash-editor"><div class="hash-head"><span>索引</span><span>值</span><span>操作</span></div>${body.data.map((item) => `<div class="hash-row" data-list-index="${item.index}"><span class="field">${item.index}</span><input class="field-value redis-list-value" value="${escapeHtml(item.value)}" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>`).join("")}<button class="add-field" data-action="redis-add-row"><svg><use href="#i-plus"/></svg>新增元素</button></div>`;
    } else if (body.type === "set") {
      editor = `<div class="value-help"><svg><use href="#i-shield"/></svg><span>Set 编辑器按行去重；空行不会写入。</span></div><textarea class="modal-textarea redis-string-editor" data-redis-editor="set" spellcheck="false">${escapeHtml(body.data.join("\n"))}</textarea>`;
    } else if (body.type === "zset") {
      editor = `<div class="hash-editor"><div class="hash-head" style="grid-template-columns:1fr 120px 70px"><span>成员</span><span>Score</span><span>操作</span></div>${body.data.map((item) => `<div class="hash-row" data-zset-row style="grid-template-columns:1fr 120px 70px"><input class="field redis-zset-member" value="${escapeHtml(item.member)}" /><input class="field-value redis-zset-score" type="number" step="any" value="${item.score}" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>`).join("")}<button class="add-field" data-action="redis-add-row"><svg><use href="#i-plus"/></svg>新增成员</button></div>`;
    } else if (body.type === "stream") {
      editor = `<div class="value-help"><svg><use href="#i-lock"/></svg><span>Stream 只读展示最近 100 条记录。当前版本不支持直接编辑 stream。</span></div><div class="hash-editor">${body.data.map((entry) => `<div class="hash-row" style="grid-template-columns:180px 1fr"><span class="field">${escapeHtml(entry.id)}</span><code>${escapeHtml(JSON.stringify(entry.fields))}</code></div>`).join("") || '<div class="empty-table">Stream 为空</div>'}</div>`;
    } else {
      editor = `<div class="unsupported-banner" style="display:flex"><svg><use href="#i-warning"/></svg><div><strong>当前键类型不支持编辑</strong><p>${escapeHtml(body.data || "未知 Redis 类型")}</p></div></div>`;
    }
    const editable = !["stream", "unsupported"].includes(body.type) && !value.rawBase64;
    const isJsonString = body.type === "string" && !value.rawBase64 && looksLikeJson(body.data);
    const toggle = isJsonString
      ? `<div class="redis-view-toggle"><button class="${redisValueView === "preview" ? "active" : ""}" data-action="redis-view-preview">高亮预览</button><button class="${redisValueView === "edit" ? "active" : ""}" data-action="redis-view-edit">编辑</button></div>`
      : "";
    const toolbar = `<div class="redis-value-toolbar">${toggle}<button class="ghost-button" data-action="redis-copy-value"><svg><use href="#i-copy"/></svg>复制值</button></div>`;
    const savebar = editable
      ? '<div class="redis-savebar"><span>修改仅在本机暂存，应用后原子替换整键</span><div class="redis-savebar-actions"><button class="ghost-button" data-action="redis-discard">放弃</button><button class="primary-button small" data-action="redis-save">应用更改</button></div></div>'
      : "";
    panel.innerHTML = `${toolbar}${editor}${savebar}`;
  }

  function collectRedisValue() {
    if (!currentRedisValue) throw new Error("没有已加载的 Redis 键。");
    const value = structuredClone(currentRedisValue);
    if (value.value.type === "string") {
      value.value.data = $('[data-redis-editor="string"]')?.value ?? currentRedisValue.value.data;
    } else if (value.value.type === "hash") {
      const fields = {};
      $$("[data-hash-row]").forEach((row) => {
        const name = $(".redis-field-name", row)?.value?.trim();
        if (!name) return;
        if (Object.prototype.hasOwnProperty.call(fields, name)) throw new Error(`Hash 字段重复：${name}`);
        fields[name] = $(".redis-field-value", row)?.value || "";
      });
      value.value.data = fields;
    } else if (value.value.type === "list") {
      value.value.data = $$("[data-list-index]").map((row, index) => ({ index, value: $(".redis-list-value", row)?.value || "" }));
    } else if (value.value.type === "set") {
      value.value.data = [...new Set(($('[data-redis-editor="set"]')?.value || "").split("\n").map((item) => item.trim()).filter(Boolean))];
    } else if (value.value.type === "zset") {
      value.value.data = $$("[data-zset-row]").map((row) => ({
        member: $(".redis-zset-member", row)?.value || "",
        score: Number($(".redis-zset-score", row)?.value),
      })).filter((item) => item.member && Number.isFinite(item.score));
    }
    return value;
  }

  async function runRedisConsole(confirmed = false) {
    const input = $("#redis-console-input");
    const line = input?.value.trim() || "";
    if (!line) return;
    requireSavedConnection("kv");
    const { command, args } = parseRedisCommand(line);
    const output = $("#redis-console-output");
    const append = (html) => {
      if (!output) return;
      output.insertAdjacentHTML("beforeend", html);
      output.scrollTop = output.scrollHeight;
    };
    append(`<div class="rc-cmd">&gt; ${escapeHtml(line)}</div>`);
    try {
      const result = await backendCall("redis_command", {
        connectionId: activeConnectionId,
        request: { connectionId: activeConnectionId, command, args, operationId: operationId(), confirmedDangerous: confirmed },
      });
      append(`<pre class="rc-out">${highlightJson(result.value)}</pre><div class="rc-meta">${result.durationMs} ms</div>`);
      if (input) input.value = "";
      refreshAuditOnly();
    } catch (error) {
      const message = errorMessage(error);
      if (!confirmed && /second-step confirmation/i.test(message)) {
        confirmDangerous(command.toUpperCase(), `${activeConnection?.name || "Redis"} · DB ${currentRedisDb}\n${line}\n该命令被危险命令护栏标记，需二次确认。`, () => runRedisConsole(true));
        return;
      }
      append(`<div class="rc-err">${escapeHtml(message)}</div>`);
    }
  }

  async function saveRedisValue() {
    try {
      requireSavedConnection("kv");
      const value = collectRedisValue();
      activeRedisOperationId = operationId();
      await backendCall("redis_set", {
        connectionId: activeConnectionId,
        db: currentRedisDb,
        value,
        keyBase64: currentRedisKeyB64,
        operationId: activeRedisOperationId,
      });
      toast("Redis 键已保存", `${value.key} · ${value.keyType} 已原子替换。`);
      await openRedisKey(value.key, value.keyType, currentRedisDb, currentRedisKeyB64);
      refreshAuditOnly();
    } catch (error) {
      showOperationError(error, "Redis 保存失败");
    } finally {
      activeRedisOperationId = null;
    }
  }

  function redisCreateHelp(type) {
    return {
      string: ["字符串值", "直接填写字符串，允许换行。"],
      hash: ["Hash 字段（JSON 对象）", '{\n  "field": "value"\n}'],
      list: ["List 元素（每行一个）", "first\nsecond"],
      set: ["Set 成员（每行一个，自动去重）", "member-a\nmember-b"],
      zset: ["Sorted Set（每行：score + 空格 + member）", "1.5 member-a\n2 member-b"],
    }[type];
  }

  function updateRedisCreateEditor() {
    const type = $("#redis-create-type")?.value || "string";
    const help = redisCreateHelp(type);
    const label = $("#redis-create-value-label");
    const input = $("#redis-create-value");
    const note = $("#redis-create-format");
    if (label) label.textContent = help[0];
    if (input) input.placeholder = help[1];
    if (note) note.textContent = help[1].replace(/\n/g, "；");
  }

  function redisCreateModal() {
    try {
      requireSavedConnection("kv");
      const db = activeConnection?.redisMode === "cluster" ? 0 : (redisScanState.db ?? activeConnection?.redisDb ?? 0);
      showModal("新建 Redis 键", "CREATE KEY WITHOUT OVERWRITE", `
        <div class="form-grid">
          <label class="full">键名<input id="redis-create-key" spellcheck="false" placeholder="例如 cache:user:1001" /></label>
          <label>类型<select id="redis-create-type"><option value="string">String</option><option value="hash">Hash</option><option value="list">List</option><option value="set">Set</option><option value="zset">Sorted Set</option></select></label>
          <label>数据库<input id="redis-create-db" type="number" min="0" value="${db}" ${activeConnection?.redisMode === "cluster" ? "disabled" : ""} /></label>
          <label>TTL（秒）<input id="redis-create-ttl" type="number" min="-1" step="1" value="-1" /></label>
          <label class="full"><span id="redis-create-value-label">字符串值</span><textarea id="redis-create-value" class="modal-textarea redis-create-value" spellcheck="false"></textarea></label>
        </div>
        <div id="redis-create-format" class="form-note">直接填写字符串，允许换行。</div>
        <div class="form-note"><svg><use href="#i-shield"/></svg>创建操作是原子的：如果键名已经存在，Redis 中的原值和 TTL 都不会改变。</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="redis-create-save">创建键</button></div>
      `, { wide: true });
      $("#redis-create-key")?.focus();
    } catch (error) {
      showOperationError(error, "无法新建 Redis 键");
    }
  }

  function redisCreateBody(type, text) {
    if (type === "string") return { type, data: text };
    if (type === "hash") {
      let value;
      try { value = JSON.parse(text); } catch { throw new Error("Hash 必须填写为 JSON 对象。"); }
      if (!value || Array.isArray(value) || typeof value !== "object") throw new Error("Hash 必须填写为 JSON 对象。");
      for (const [field, fieldValue] of Object.entries(value)) {
        if (!field || typeof fieldValue !== "string") throw new Error("Hash 的字段名不能为空，字段值必须是字符串。");
      }
      return { type, data: value };
    }
    const lines = text.split("\n").map((item) => item.trim()).filter(Boolean);
    if (!lines.length) throw new Error(`${type.toUpperCase()} 至少需要一个元素。`);
    if (type === "list") return { type, data: lines.map((value, index) => ({ index, value })) };
    if (type === "set") return { type, data: [...new Set(lines)] };
    const members = new Set();
    const data = lines.map((line) => {
      const match = /^(\S+)\s+([\s\S]+)$/.exec(line);
      const score = Number(match?.[1]);
      const member = match?.[2]?.trim();
      if (!match || !Number.isFinite(score) || !member) throw new Error(`Sorted Set 行格式错误：${line}`);
      if (members.has(member)) throw new Error(`Sorted Set 成员重复：${member}`);
      members.add(member);
      return { member, score };
    });
    return { type, data };
  }

  async function createRedisKey() {
    requireSavedConnection("kv");
    const key = $("#redis-create-key")?.value || "";
    const type = $("#redis-create-type")?.value || "string";
    const ttl = Number($("#redis-create-ttl")?.value);
    const db = activeConnection?.redisMode === "cluster" ? 0 : Number($("#redis-create-db")?.value);
    if (!key) throw new Error("键名不能为空。");
    if (!Number.isInteger(ttl) || (ttl !== -1 && ttl <= 0)) throw new Error("TTL 必须是 -1（永久）或正整数秒。");
    if (!Number.isInteger(db) || db < 0) throw new Error("Redis DB 编号必须是非负整数。");
    const value = { key, keyType: type, ttlSeconds: ttl, encoding: null, sizeBytes: null, value: redisCreateBody(type, $("#redis-create-value")?.value || "") };
    const op = operationId();
    activeRedisOperationId = op;
    try {
      await backendCall("redis_create", { connectionId: activeConnectionId, db, value, operationId: op });
      closeModal();
      redisScanState = { cursor: 0, pattern: "*", count: 300, db, loading: false };
      const list = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"] [data-redis-key-list="${db}"]`);
      if (list) await loadRedisKeys(db, false);
      await openRedisKey(key, type, db, null);
      toast("Redis 键已创建", `${key} · ${type}${ttl === -1 ? " · 永久" : ` · TTL ${ttl}s`}`);
      refreshAuditOnly();
    } catch (error) {
      if (serverCommitNeedsRefresh(error)) {
        closeModal();
        redisScanState = { cursor: 0, pattern: "*", count: 300, db, loading: false };
        const list = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"] [data-redis-key-list="${db}"]`);
        if (list) await loadRedisKeys(db, false);
        await openRedisKey(key, type, db, null).catch(() => {});
      }
      throw error;
    } finally {
      activeRedisOperationId = null;
    }
  }

  /* ---------- Elasticsearch ---------- */

  function esConnection() {
    const connection = connectionLookup.get(esState.connectionId || activeConnectionId);
    return connection && protocolFamily(connection.protocol) === "search" ? connection : null;
  }

  function requireEsConnection() {
    const connection = esConnection();
    if (!connection) throw new Error("请先选择并打开一个 Elasticsearch 连接。");
    return connection;
  }

  function esHealthClass(health) {
    return health === "green" ? "green" : health === "yellow" ? "yellow" : health === "red" ? "red" : "unknown";
  }

  async function refreshEsIndices() {
    const connection = requireEsConnection();
    const indices = await backendCall("es_indices", { connectionId: connection.id, operationId: operationId() });
    esState.indices = indices || [];
    esState.connectionId = connection.id;
    esState.indicesConnectionId = connection.id;
    return esState.indices;
  }

  async function renderEsObjectsView(connection) {
    const list = $("#objects-list");
    if (!list) return;
    esState.connectionId = connection.id;
    if (!esState.indices.length || esState.indicesConnectionId !== connection.id) {
      list.innerHTML = '<div class="objects-empty"><div class="objects-hint"><strong>正在读取索引…</strong></div></div>';
      try { await refreshEsIndices(); }
      catch (error) {
        objectsEmpty(`<div class="objects-hint"><strong>无法读取索引</strong><span>${escapeHtml(errorMessage(error))}</span><div class="objects-hint-actions"><button class="primary-button" data-action="es-objects-refresh">重试</button></div></div>`);
        return;
      }
    }
    const indices = esState.indices;
    const rows = indices.map((index) => `
      <tr class="object-row" data-es-index="${escapeHtml(index.name)}" data-connection-id="${escapeHtml(connection.id)}">
        <td><span class="object-name"><svg class="tree-icon table"><use href="#i-grid"/></svg>${escapeHtml(index.name)}</span></td>
        <td><span class="es-health ${esHealthClass(index.health)}">${escapeHtml(index.health || "—")}</span></td>
        <td>${escapeHtml(index.status || "—")}</td>
        <td class="numeric">${formatCount(index.docsCount) ?? "—"}</td>
        <td class="numeric">${index.storeBytes != null ? formatBytes(index.storeBytes) : "—"}</td>
        <td class="numeric">${index.primaryShards ?? "—"}</td>
        <td class="numeric">${index.replicaShards ?? "—"}</td>
        <td>${index.aliases?.length ? escapeHtml(index.aliases.join(", ")) : "—"}</td>
      </tr>`).join("");
    list.innerHTML = `
      <div class="es-objects-toolbar">
        <button class="ot-button" data-action="es-open-index" data-needs-selection disabled><svg><use href="#i-grid"/></svg>打开索引</button>
        <button class="ot-button" data-action="es-create-index"><svg><use href="#i-plus"/></svg>新建索引</button>
        <button class="ot-button" data-action="es-delete-index" data-needs-selection disabled><svg><use href="#i-warning"/></svg>删除索引</button>
        <button class="ot-button" data-action="es-objects-refresh"><svg><use href="#i-refresh"/></svg>刷新</button>
        <span class="toolbar-spacer"></span>
        <span class="es-objects-count">${indices.length} 个索引</span>
      </div>
      <div class="object-table-wrap">
        <table class="object-table es-object-table" data-resize-key="es-indices">
          <colgroup><col style="width:220px"><col style="width:80px"><col style="width:80px"><col style="width:90px"><col style="width:100px"><col style="width:80px"><col style="width:70px"><col></colgroup>
          <thead><tr><th>名称<span class="col-resizer" data-gcol="0"></span></th><th>健康<span class="col-resizer" data-gcol="1"></span></th><th>状态<span class="col-resizer" data-gcol="2"></span></th><th class="numeric">文档数<span class="col-resizer" data-gcol="3"></span></th><th class="numeric">容量<span class="col-resizer" data-gcol="4"></span></th><th class="numeric">主分片<span class="col-resizer" data-gcol="5"></span></th><th class="numeric">副本<span class="col-resizer" data-gcol="6"></span></th><th>别名<span class="col-resizer" data-gcol="7"></span></th></tr></thead>
          <tbody>${rows || '<tr><td colspan="8" class="empty-table">集群中没有索引。点击“新建索引”创建第一个索引。</td></tr>'}</tbody>
        </table>
      </div>`;
    setStatusCenter(`${connection.name} · ${indices.length} 个索引`);
    $$(".es-object-table .object-row", list).forEach((row) => {
      row.addEventListener("click", () => {
        $$(".es-object-table .object-row", list).forEach((item) => item.classList.toggle("selected", item === row));
        $$(".es-objects-toolbar [data-needs-selection]", list).forEach((button) => { button.disabled = false; });
      });
      row.addEventListener("dblclick", () => openEsIndex(row.dataset.esIndex));
    });
    applyGenericColWidths();
  }

  function selectedEsIndex() {
    return $(".es-object-table .object-row.selected")?.dataset.esIndex || esState.index || null;
  }

  function captureEsSession() {
    return {
      index: esState.index, tab: esState.tab, dsl: esState.dsl, size: esState.size, from: esState.from,
      total: esState.total, totalRelation: esState.totalRelation, hits: esState.hits, tookMs: esState.tookMs,
      aggregations: esState.aggregations, mapping: esState.mapping, aliases: esState.aliases,
      docCols: { ...esState.docCols },
    };
  }

  async function openEsIndex(name) {
    const connection = requireEsConnection();
    const prevConn = esState.connectionId;
    if (prevConn && prevConn !== connection.id && esState.index) esSessions.set(prevConn, captureEsSession());
    const saved = esSessions.get(connection.id);
    esState.connectionId = connection.id;
    if (saved && saved.index === name) {
      Object.assign(esState, saved, { connectionId: connection.id, docCols: { ...saved.docCols } });
    } else {
      esState.index = name;
      esState.from = 0;
      esState.hits = [];
      esState.total = 0;
      esState.currentDoc = null;
      esState.mapping = null;
      esState.aliases = [];
      esState.tab = "docs";
    }
    lastEsConn = connection.id;
    esTabOpen = true;
    const esTabEl = $('.tool-tab[data-workspace="es"]');
    if (esTabEl) esTabEl.hidden = false;
    showWorkspace("es");
    if (!saved || saved.index !== name) await esSearch();
  }

  function renderEsView() {
    const view = $("#view-es");
    if (!view) return;
    const connection = esConnection();
    if (!connection) {
      view.innerHTML = '<div class="objects-empty"><div class="objects-hint"><strong>Elasticsearch 浏览器</strong><span>在左侧选择一个 Elasticsearch 连接并展开索引，或从对象列表打开索引。</span></div></div>';
      return;
    }
    const index = esState.index;
    const tab = esState.tab;
    const tabs = [["docs", "文档浏览"], ["mapping", "Mapping"], ["aliases", "别名"], ["rest", "REST 控制台"]]
      .map(([key, label]) => `<button class="es-tab${tab === key ? " active" : ""}" data-es-tab="${key}">${label}</button>`).join("");
    view.innerHTML = `
      <div class="es-head">
        <div class="es-title"><span class="protocol-badge es">ES</span><div><div class="key-breadcrumb">${escapeHtml(connection.name)} <span>/</span> 索引</div><h2 title="${escapeHtml(index || "")}">${index ? escapeHtml(index) : "未选择索引"}</h2></div></div>
        <div class="es-head-actions">
          <button class="ghost-button" data-action="es-new-doc" ${index ? "" : "disabled"}><svg><use href="#i-plus"/></svg>新建文档</button>
          <button class="ghost-button" data-action="es-refresh"><svg><use href="#i-refresh"/></svg>刷新</button>
          <button class="ghost-button" data-action="es-create-index"><svg><use href="#i-plus"/></svg>新建索引</button>
          <button class="ghost-button danger-text" data-action="es-delete-index" ${index ? "" : "disabled"}>删除索引</button>
        </div>
      </div>
      <div class="es-tabs">${tabs}</div>
      <div class="es-body">${index ? esPanelHtml(tab) : '<div class="empty-table">从左侧树或对象列表选择一个索引开始浏览。</div>'}</div>`;
    if (index) {
      if (tab === "docs") renderEsDocsResults();
      if (tab === "mapping") renderEsMapping();
      if (tab === "aliases") renderEsAliases();
      if (tab === "rest") renderEsRestResponse();
    }
  }

  function esPanelHtml(tab) {
    if (tab === "docs") {
      return `
        <div class="es-query-bar">
          <textarea id="es-dsl" class="es-dsl" spellcheck="false" placeholder='Query DSL，例如 { "query": { "match_all": {} } }'>${escapeHtml(esState.dsl)}</textarea>
          <div class="es-query-controls">
            <label>每页<select id="es-size">${[20, 50, 100, 200].map((n) => `<option value="${n}"${n === esState.size ? " selected" : ""}>${n}</option>`).join("")}</select></label>
            <button class="primary-button small" data-action="es-search"><svg><use href="#i-search"/></svg>搜索</button>
            <button class="ghost-button" data-action="es-match-all">match_all</button>
          </div>
        </div>
        <div id="es-docs-results" class="es-docs-results"></div>`;
    }
    if (tab === "mapping") {
      return `<div class="es-panel-head"><strong>索引 Mapping</strong><button class="ghost-button" data-action="es-refresh-mapping"><svg><use href="#i-refresh"/></svg>刷新</button></div><pre id="es-mapping-pre" class="es-json"></pre>`;
    }
    if (tab === "aliases") {
      return `<div class="es-panel-head"><strong>别名</strong><button class="ghost-button" data-action="es-add-alias"><svg><use href="#i-plus"/></svg>添加别名</button></div><div id="es-aliases-list" class="es-aliases-list"></div>`;
    }
    return `
      <div class="es-rest-bar">
        <select id="es-rest-method">${["GET", "POST", "PUT", "DELETE", "HEAD"].map((m) => `<option value="${m}"${m === esState.restMethod ? " selected" : ""}>${m}</option>`).join("")}</select>
        <input id="es-rest-path" spellcheck="false" value="${escapeHtml(esState.restPath)}" placeholder="/_cluster/health" />
        <button class="primary-button small" data-action="es-send-rest"><svg><use href="#i-play"/></svg>发送</button>
      </div>
      <div class="es-resize-wrap" id="es-rest-body-wrap">
        <textarea id="es-rest-body" class="es-dsl es-rest-body" spellcheck="false" placeholder="请求体 JSON（GET 可留空）">${escapeHtml(esState.restBody)}</textarea>
        <div class="v-resizer" data-vresize="es-rest-body-wrap" title="拖动调整请求体高度"></div>
      </div>
      <div class="es-panel-head"><strong>响应</strong><span id="es-rest-status" class="j-status"></span><button class="ghost-button" data-action="es-copy-rest"><svg><use href="#i-copy"/></svg>复制响应</button></div>
      <div class="es-resize-wrap es-response-wrap" id="es-rest-response-wrap">
        <pre id="es-rest-response" class="es-json es-rest-response"></pre>
        <div class="v-resizer" data-vresize="es-rest-response-wrap" title="拖动调整响应高度"></div>
      </div>`;
  }

  function setEsTab(name) {
    esState.tab = name;
    renderEsView();
    if (!esState.index) return;
    if (name === "mapping" && !esState.mapping) esLoadMapping();
    if (name === "aliases" && !esState.aliases.length) esLoadAliases();
  }

  function esDocWidths(avail) {
    const c = esState.docCols;
    const num = 48;
    const source = c.source ?? Math.max(240, avail - num - c.id - c.score);
    return { num, id: c.id, score: c.score, source, total: num + c.id + c.score + source };
  }

  function renderEsDocsResults() {
    const box = $("#es-docs-results");
    if (!box) return;
    const hits = esState.hits || [];
    const rows = hits.map((hit, index) => {
      return `<tr class="es-doc-row" data-es-doc-id="${escapeHtml(hit.id)}" title="点击查看 / 编辑文档">
        <td class="row-number">${esState.from + index + 1}</td>
        <td class="es-doc-id">${escapeHtml(hit.id)}</td>
        <td class="numeric">${hit.score != null ? hit.score.toFixed(2) : "—"}</td>
        <td class="es-doc-source">${highlightJson(hit.source, 0)}</td>
      </tr>`;
    }).join("");
    const page = Math.floor(esState.from / esState.size) + 1;
    const hasPrev = esState.from > 0;
    const hasNext = esState.from + esState.size < esState.total;
    const w = esDocWidths(box.clientWidth || 900);
    box.innerHTML = `
      <div class="es-results-meta">${esState.tookMs != null ? `命中 <b>${formatCount(esState.total)}</b>${esState.totalRelation === "gte" ? "+" : ""} 条 · took ${esState.tookMs} ms · 第 ${page} 页` : "执行搜索后在这里显示文档。"}</div>
      <div class="object-table-wrap es-doc-wrap">
        <table class="object-table es-doc-table" style="width:${w.total}px">
          <colgroup><col style="width:${w.num}px"><col style="width:${w.id}px"><col style="width:${w.score}px"><col style="width:${w.source}px"></colgroup>
          <thead><tr><th class="row-number">#</th><th>_id<span class="col-resizer" data-es-col="id"></span></th><th class="numeric">_score<span class="col-resizer" data-es-col="score"></span></th><th>_source<span class="col-resizer" data-es-col="source"></span></th></tr></thead>
          <tbody>${rows || '<tr><td colspan="4" class="empty-table">没有命中文档。</td></tr>'}</tbody>
        </table>
      </div>
      <div class="es-pager">
        <button class="ghost-button" data-action="es-prev" ${hasPrev ? "" : "disabled"}>‹ 上一页</button>
        <button class="ghost-button" data-action="es-next" ${hasNext ? "" : "disabled"}>下一页 ›</button>
        ${esState.aggregations ? '<span class="es-aggs-note">含聚合结果，可在 REST 控制台查看完整响应。</span>' : ""}
      </div>`;
    $$(".es-doc-row", box).forEach((row) => row.addEventListener("click", () => openEsDoc(row.dataset.esDocId)));
  }

  const genericColWidths = new Map();
  let justDraggedColumn = false;

  function applyGenericColWidths() {
    $$("table[data-resize-key]").forEach((table) => {
      const widths = genericColWidths.get(table.dataset.resizeKey);
      if (!widths) return;
      const cols = table.querySelectorAll("colgroup col");
      const ths = table.querySelectorAll("thead th");
      widths.forEach((width, index) => {
        const target = cols[index] || ths[index];
        if (width && target) target.style.width = `${width}px`;
      });
      table.style.tableLayout = "fixed";
      const cells = cols.length ? [...cols] : [...ths];
      const total = cells.reduce((sum, cell) => sum + (parseFloat(cell.style.width) || cell.getBoundingClientRect().width), 0);
      table.style.width = `${Math.max(table.parentElement?.clientWidth || 0, Math.round(total))}px`;
    });
  }

  function initGenericColumnResize() {
    document.addEventListener("mousedown", (event) => {
      const handle = event.target.closest?.(".col-resizer[data-gcol]");
      if (!handle) return;
      event.preventDefault();
      event.stopPropagation();
      const table = handle.closest("table");
      const th = handle.closest("th");
      const key = table?.dataset.resizeKey;
      if (!key || !th) return;
      const index = Number(handle.dataset.gcol);
      const cols = table.querySelectorAll("colgroup col");
      const target = cols[index] || th;
      const startX = event.clientX;
      const startWidth = th.getBoundingClientRect().width;
      const move = (moveEvent) => {
        const width = Math.max(60, Math.round(startWidth + (moveEvent.clientX - startX)));
        target.style.width = `${width}px`;
        table.style.tableLayout = "fixed";
        const widths = genericColWidths.get(key) || [];
        widths[index] = width;
        genericColWidths.set(key, widths);
        const cells = cols.length ? [...cols] : [...table.querySelectorAll("thead th")];
        const total = cells.reduce((sum, cell) => sum + (parseFloat(cell.style.width) || cell.getBoundingClientRect().width), 0);
        table.style.width = `${Math.max(table.parentElement?.clientWidth || 0, Math.round(total))}px`;
      };
      const up = () => {
        justDraggedColumn = true;
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
      };
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  }

  function initEsColumnResize() {
    document.addEventListener("mousedown", (event) => {
      const handle = event.target.closest?.(".es-doc-table .col-resizer");
      if (!handle) return;
      event.preventDefault();
      const key = handle.dataset.esCol;
      const table = handle.closest("table");
      const wrap = table.parentElement;
      const colIndex = { id: 1, score: 2, source: 3 }[key];
      const startX = event.clientX;
      const startWidth = key === "source" ? esDocWidths(wrap.clientWidth).source : esState.docCols[key];
      const move = (moveEvent) => {
        esState.docCols[key] = Math.max(60, Math.round(startWidth + (moveEvent.clientX - startX)));
        const w = esDocWidths(wrap.clientWidth);
        const cols = table.querySelectorAll("colgroup col");
        cols[0].style.width = `${w.num}px`;
        cols[1].style.width = `${w.id}px`;
        cols[2].style.width = `${w.score}px`;
        cols[3].style.width = `${w.source}px`;
        table.style.width = `${w.total}px`;
      };
      const up = () => {
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
      };
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  }

  function initVerticalResize() {
    document.addEventListener("mousedown", (event) => {
      const handle = event.target.closest?.(".v-resizer");
      if (!handle) return;
      event.preventDefault();
      const target = document.getElementById(handle.dataset.vresize);
      if (!target) return;
      const startY = event.clientY;
      const startHeight = target.getBoundingClientRect().height;
      const move = (moveEvent) => {
        target.style.flex = "none";
        target.style.height = `${Math.max(80, Math.round(startHeight + (moveEvent.clientY - startY)))}px`;
      };
      const up = () => {
        document.removeEventListener("mousemove", move);
        document.removeEventListener("mouseup", up);
      };
      document.addEventListener("mousemove", move);
      document.addEventListener("mouseup", up);
    });
  }

  async function esSearch() {
    const connection = requireEsConnection();
    if (!esState.index) return;
    const dslText = $("#es-dsl")?.value ?? esState.dsl;
    esState.dsl = dslText;
    esState.size = Number($("#es-size")?.value || esState.size);
    let body;
    try { body = dslText.trim() ? JSON.parse(dslText) : {}; }
    catch { toast("Query DSL 无效", "请输入合法的 JSON 查询体。", "danger"); return; }
    esState.loading = true;
    const resultsBox = $("#es-docs-results");
    if (resultsBox) resultsBox.innerHTML = '<div class="tree-loading">正在搜索…</div>';
    try {
      const result = await backendCall("es_search", {
        connectionId: connection.id,
        request: { connectionId: connection.id, index: esState.index, body, from: esState.from, size: esState.size, operationId: operationId() },
      });
      esState.hits = result.hits || [];
      esState.total = result.total || 0;
      esState.totalRelation = result.totalRelation || "eq";
      esState.tookMs = result.tookMs;
      esState.aggregations = result.aggregations || null;
      renderEsDocsResults();
      refreshAuditOnly();
    } catch (error) {
      showOperationError(error, "Elasticsearch 搜索失败");
    } finally {
      esState.loading = false;
    }
  }

  async function esLoadMapping() {
    const connection = requireEsConnection();
    if (!esState.index) return;
    const pre = $("#es-mapping-pre");
    if (pre) pre.textContent = "正在读取 Mapping…";
    try {
      esState.mapping = await backendCall("es_mapping", { connectionId: connection.id, name: esState.index, operationId: operationId() });
      renderEsMapping();
    } catch (error) {
      if (pre) pre.textContent = errorMessage(error);
      showOperationError(error, "Mapping 读取失败");
    }
  }

  function highlightJson(value, indent = 2) {
    const json = JSON.stringify(value, null, indent);
    if (json == null) return "";
    const token = /("(?:\\.|[^"\\])*"(?:\s*:)?|\b(?:true|false|null)\b|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g;
    let out = "";
    let last = 0;
    let match;
    while ((match = token.exec(json))) {
      out += escapeHtml(json.slice(last, match.index));
      const tok = match[0];
      if (tok[0] === '"') {
        const quoted = tok.replace(/\s*:$/, "");
        const isKey = quoted.length < tok.length;
        out += `<span class="${isKey ? "j-key" : "j-str"}">${escapeHtml(quoted)}</span>`;
        if (isKey) out += escapeHtml(tok.slice(quoted.length));
      } else {
        const cls = tok === "true" || tok === "false" ? "j-bool" : tok === "null" ? "j-null" : "j-num";
        out += `<span class="${cls}">${escapeHtml(tok)}</span>`;
      }
      last = token.lastIndex;
    }
    out += escapeHtml(json.slice(last));
    return out;
  }

  function renderEsMapping() {
    const pre = $("#es-mapping-pre");
    if (!pre) return;
    if (esState.mapping) pre.innerHTML = highlightJson(esState.mapping);
    else pre.textContent = "点击“刷新”读取 Mapping。";
  }

  async function esLoadAliases() {
    const connection = requireEsConnection();
    try {
      const all = await backendCall("es_aliases", { connectionId: connection.id, operationId: operationId() });
      esState.aliases = (all || []).filter((alias) => alias.index === esState.index);
      renderEsAliases();
    } catch (error) {
      showOperationError(error, "别名读取失败");
    }
  }

  function renderEsAliases() {
    const box = $("#es-aliases-list");
    if (!box) return;
    const aliases = esState.aliases || [];
    box.innerHTML = aliases.length ? aliases.map((alias) => `
      <div class="es-alias-row">
        <span class="es-alias-name">${escapeHtml(alias.alias)}</span>
        <span class="es-alias-meta">${alias.isWriteIndex ? "写索引" : ""}${alias.filter ? " · 带过滤" : ""}</span>
        <button class="ghost-button danger-text" data-action="es-remove-alias" data-alias="${escapeHtml(alias.alias)}">移除</button>
      </div>`).join("") : '<div class="empty-table">该索引没有别名。</div>';
  }

  async function openEsDoc(id) {
    const connection = requireEsConnection();
    try {
      const doc = await backendCall("es_get_document", { connectionId: connection.id, index: esState.index, id, operationId: operationId() });
      esState.currentDoc = doc;
      esDocView = null;
      esDocModal(doc);
    } catch (error) {
      showOperationError(error, "文档读取失败");
    }
  }

  function esDocModal(doc) {
    esDocModalDoc = doc;
    const isNew = !doc;
    const source = doc ? JSON.stringify(doc.source, null, 2) : "{\n  \n}";
    const isJson = !isNew && looksLikeJson(source);
    const view = isNew ? "edit" : (esDocView || "preview");
    esDocView = view;
    const toggle = isJson
      ? `<div class="redis-view-toggle"><button class="${view === "preview" ? "active" : ""}" data-action="es-doc-view-preview">高亮预览</button><button class="${view === "edit" ? "active" : ""}" data-action="es-doc-view-edit">编辑</button></div>`
      : "";
    const sourceField = (view === "preview" && isJson)
      ? `<pre class="redis-json-pre es-doc-source-pre">${highlightJson(doc.source)}</pre>`
      : `<textarea id="es-doc-source" class="modal-textarea es-doc-source-editor" spellcheck="false">${escapeHtml(source)}</textarea>`;
    showModal(isNew ? "新建文档" : `文档 ${doc.id}`, isNew ? "INDEX DOCUMENT" : "EDIT DOCUMENT", `
      <div class="form-grid">
        <label class="full">_id${isNew ? "（留空则自动生成）" : ""}<input id="es-doc-id" value="${escapeHtml(doc?.id || "")}" ${isNew ? "" : "readonly"} spellcheck="false" /></label>
        <div class="full">
          <div class="redis-value-toolbar">${toggle}<button class="ghost-button" data-action="es-copy-doc"><svg><use href="#i-copy"/></svg>复制 _source</button></div>
          ${sourceField}
        </div>
      </div>
      ${doc && !doc.found ? '<div class="form-note danger-note">该文档不存在（可能已被删除）。</div>' : ""}
      <div class="form-note"><svg><use href="#i-shield"/></svg>保存会整文档替换（PUT _doc）；删除需二次确认并写入只追加审计。</div>
      <div class="modal-footer">
        <button class="secondary-button" data-close-modal>取消</button>
        ${isNew ? "" : '<button class="danger-button" data-action="es-delete-doc">删除文档</button>'}
        <button class="primary-button" data-action="es-save-doc">${isNew ? "创建文档" : "保存文档"}</button>
      </div>`, { wide: true });
  }

  async function esSaveDoc() {
    const connection = requireEsConnection();
    const id = $("#es-doc-id")?.value.trim() || "";
    const editor = $("#es-doc-source");
    let source;
    try { source = editor ? JSON.parse(editor.value || "{}") : (esDocModalDoc?.source ?? {}); }
    catch { throw new Error("_source 必须是合法 JSON。"); }
    if (!source || Array.isArray(source) || typeof source !== "object") throw new Error("_source 必须是 JSON 对象。");
    const doc = await backendCall("es_put_document", { connectionId: connection.id, index: esState.index, id, source, operationId: operationId() });
    closeModal();
    toast("文档已保存", `${doc.index}/${doc.id} · version ${doc.version ?? "—"}`);
    await esSearch();
    refreshAuditOnly();
  }

  function esDeleteDoc() {
    const connection = esConnection();
    const doc = esState.currentDoc;
    if (!connection || !doc) return;
    confirmDangerous(`DELETE ${doc.index}/${doc.id}`, `${connection.name}\nDELETE /${doc.index}/_doc/${doc.id}\n影响：永久删除该文档。`, async () => {
      await backendCall("es_delete_document", { connectionId: connection.id, index: doc.index, id: doc.id, confirmedDangerous: true, operationId: operationId() });
      closeModal();
      toast("文档已删除", `${doc.index}/${doc.id} · 操作已写入只追加审计。`, "danger");
      esState.currentDoc = null;
      await esSearch();
      refreshAuditOnly();
    });
  }

  function esCreateIndexModal() {
    requireEsConnection();
    showModal("新建索引", "CREATE INDEX", `
      <div class="form-grid">
        <label class="full">索引名称<input id="es-create-name" spellcheck="false" placeholder="例如 logs-2026" /></label>
        <label class="full">settings / mappings（JSON，可留空）<textarea id="es-create-body" class="modal-textarea" spellcheck="false" placeholder='{ "settings": { "number_of_shards": 1 }, "mappings": { "properties": {} } }'></textarea></label>
      </div>
      <div class="form-note"><svg><use href="#i-shield"/></svg>名称不能包含空格或 \\ / * ? " &lt; &gt; | , #，且不能以 - _ + 开头。</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="es-create-index-save">创建索引</button></div>`, { wide: true });
    $("#es-create-name")?.focus();
  }

  async function esCreateIndex() {
    const connection = requireEsConnection();
    const name = $("#es-create-name")?.value.trim() || "";
    if (!name) throw new Error("请填写索引名称。");
    const bodyText = $("#es-create-body")?.value.trim() || "";
    let body = null;
    if (bodyText) {
      try { body = JSON.parse(bodyText); } catch { throw new Error("settings / mappings 必须是合法 JSON。"); }
    }
    await backendCall("es_create_index", { connectionId: connection.id, name, body: body ?? {}, operationId: operationId() });
    closeModal();
    toast("索引已创建", name);
    await refreshEsIndices();
    const group = $(`.connection-group[data-connection-id="${CSS.escape(connection.id)}"]`);
    if (group) { group.dataset.loaded = "false"; await loadConnectionTree(group); }
    if (currentWorkspace() === "objects") renderObjectsView();
    refreshAuditOnly();
  }

  function esDeleteIndex(name = null) {
    const connection = esConnection();
    const target = name || selectedEsIndex();
    if (!connection || !target) { toast("没有选中的索引", "请先选择一个索引。", "danger"); return; }
    confirmDangerous(`DELETE INDEX ${target}`, `${connection.name}\nDELETE /${target}\n影响：永久删除该索引及其全部文档、mapping 与别名。`, async () => {
      await backendCall("es_delete_index", { connectionId: connection.id, name: target, confirmedDangerous: true, operationId: operationId() });
      toast("索引已删除", `${target} · 操作已写入只追加审计。`, "danger");
      if (esState.index === target) { esState.index = null; esState.hits = []; }
      await refreshEsIndices();
      const group = $(`.connection-group[data-connection-id="${CSS.escape(connection.id)}"]`);
      if (group) { group.dataset.loaded = "false"; await loadConnectionTree(group); }
      if (currentWorkspace() === "objects") renderObjectsView();
      if (currentWorkspace() === "es") renderEsView();
      refreshAuditOnly();
    });
  }

  function esAddAliasModal() {
    requireEsConnection();
    if (!esState.index) { toast("没有打开的索引", "请先打开一个索引。", "danger"); return; }
    showModal("添加别名", "ADD ALIAS", `
      <div class="form-grid">
        <label class="full">别名名称<input id="es-alias-name" spellcheck="false" placeholder="例如 logs-current" /></label>
        <label class="check-line full"><input id="es-alias-write" type="checkbox" />设为写索引（is_write_index）</label>
      </div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="es-add-alias-save">添加</button></div>`);
    $("#es-alias-name")?.focus();
  }

  async function esAddAlias() {
    const connection = requireEsConnection();
    const alias = $("#es-alias-name")?.value.trim() || "";
    if (!alias) throw new Error("请填写别名名称。");
    const isWrite = Boolean($("#es-alias-write")?.checked);
    const add = { add: { index: esState.index, alias, is_write_index: isWrite } };
    await backendCall("es_alias_modify", { connectionId: connection.id, actions: [add], confirmedDangerous: true, operationId: operationId() });
    closeModal();
    toast("别名已添加", `${alias} → ${esState.index}`);
    await esLoadAliases();
    refreshAuditOnly();
  }

  function esRemoveAlias(alias) {
    const connection = esConnection();
    if (!connection || !esState.index) return;
    confirmDangerous(`REMOVE ALIAS ${alias}`, `${connection.name}\n移除别名 ${alias} → ${esState.index}\n影响：依赖该别名的读写会失效。`, async () => {
      await backendCall("es_alias_modify", { connectionId: connection.id, actions: [{ remove: { index: esState.index, alias } }], confirmedDangerous: true, operationId: operationId() });
      toast("别名已移除", `${alias} · 操作已写入只追加审计。`, "danger");
      await esLoadAliases();
      refreshAuditOnly();
    });
  }

  async function esSendRest() {
    const connection = requireEsConnection();
    const method = $("#es-rest-method")?.value || "GET";
    const path = $("#es-rest-path")?.value.trim() || "/";
    const bodyText = $("#es-rest-body")?.value.trim() || "";
    esState.restMethod = method;
    esState.restPath = path;
    esState.restBody = bodyText;
    let body = null;
    if (bodyText) {
      try { body = JSON.parse(bodyText); } catch { toast("请求体无效", "请输入合法 JSON 或留空。", "danger"); return; }
    }
    const send = async (confirmed) => {
      const pre = $("#es-rest-response");
      if (pre) pre.textContent = "请求中…";
      try {
        const result = await backendCall("es_rest", { connectionId: connection.id, request: { connectionId: connection.id, method, path, body, operationId: operationId(), confirmedDangerous: confirmed } });
        esState.restResult = result;
        renderEsRestResponse();
        refreshAuditOnly();
      } catch (error) {
        if (pre) pre.textContent = errorMessage(error);
        showOperationError(error, "REST 请求失败");
      }
    };
    if (method === "DELETE") {
      confirmDangerous(`DELETE ${path}`, `${connection.name}\nDELETE ${path}\n影响：可能删除索引、文档或集群资源。`, () => send(true));
    } else {
      await send(false);
    }
  }

  function renderEsRestResponse() {
    const pre = $("#es-rest-response");
    const status = $("#es-rest-status");
    if (!pre) return;
    const result = esState.restResult;
    if (!result) {
      pre.textContent = "发送请求后在这里显示原样响应。";
      if (status) status.textContent = "";
      return;
    }
    if (status) status.textContent = `HTTP ${result.status} · ${result.durationMs} ms`;
    pre.innerHTML = highlightJson(result.body);
  }

  function placeContextMenu(event, html) {
    event.preventDefault();
    contextMenu.innerHTML = html;
    contextMenu.hidden = false;
    contextMenu.style.left = "0px";
    contextMenu.style.top = "0px";
    const { width, height } = contextMenu.getBoundingClientRect();
    contextMenu.style.left = `${Math.max(6, Math.min(event.clientX, window.innerWidth - width - 6))}px`;
    contextMenu.style.top = `${Math.max(6, Math.min(event.clientY, window.innerHeight - height - 6))}px`;
    // Menus are keyboard surfaces: items carry menuitem semantics, the first enabled item takes
    // focus, and arrows/Home/End cycle the enabled items without leaving the menu.
    $$("button", contextMenu).forEach((button) => button.setAttribute("role", "menuitem"));
    $$("button:not([disabled])", contextMenu)[0]?.focus({ preventScroll: true });
  }

  contextMenu.addEventListener("keydown", (event) => {
    const items = $$("button:not([disabled])", contextMenu);
    if (!items.length) return;
    const index = Math.max(0, items.indexOf(document.activeElement));
    const steps = { ArrowDown: 1, ArrowUp: -1, Home: -index, End: items.length - 1 - index };
    const step = steps[event.key];
    if (step === undefined) return;
    event.preventDefault();
    items[(index + step + items.length) % items.length].focus({ preventScroll: true });
  });

  const menuButton = (action, icon, label, extra = "") => `<button data-action="${action}"${extra}><svg><use href="#i-${icon}"/></svg>${label}</button>`;

  function showConnectionContext(event, protocol, connectionId = null) {
    const family = protocolFamily(protocol);
    if (connectionId) setActiveConnectionState(connectionLookup.get(connectionId));
    const name = connectionLookup.get(connectionId)?.name || "连接";
    const lifecycle = `<span class="context-label">${escapeHtml(name)}</span>${menuButton("reconnect", "refresh", "连接 / 重连")}${menuButton("test-saved-connection", "check", "测试连接")}${menuButton("edit-connection", "settings", "编辑连接")}${menuButton("disconnect", "close", "断开连接")}<span class="context-sep"></span>`;
    const items = lifecycle + (family === "kv"
      ? `${menuButton("console-open", "terminal", "命令控制台")}${menuButton("redis-create", "plus", "新建键")}${menuButton("redis-scan", "search", "SCAN 键空间")}${menuButton("users", "user", "ACL 用户")}${menuButton("export", "export", "导出匹配键")}<span class="context-sep"></span><button class="danger-item" data-action="delete-connection"><svg><use href="#i-warning"/></svg>删除连接</button>`
      : family === "search"
      ? `${menuButton("es-open-browser", "grid", "打开索引浏览器")}${menuButton("es-create-index", "plus", "新建索引")}${menuButton("es-objects-refresh", "refresh", "刷新索引")}<span class="context-sep"></span><button class="danger-item" data-action="delete-connection"><svg><use href="#i-warning"/></svg>删除连接</button>`
      : `${menuButton("new-query", "query", "新建查询")}${menuButton("console-open", "terminal", "命令控制台")}${menuButton("compare-from-schema", "compare", "结构比较")}${menuButton("users", "user", "用户与权限")}<span class="context-sep"></span><button class="danger-item" data-action="delete-connection"><svg><use href="#i-warning"/></svg>删除连接</button>`);
    placeContextMenu(event, items);
  }

  function showEsIndexContext(event, esRow) {
    const group = esRow.closest(".connection-group");
    const connectionId = group?.dataset.connectionId || esRow.dataset.connectionId || activeConnectionId;
    if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
    esState.connectionId = connectionId;
    esState.index = esRow.dataset.esIndex;
    $$(".tree-row.selected").forEach((row) => row.classList.remove("selected"));
    esRow.classList.add("selected");
    placeContextMenu(event, `
      <span class="context-label">${escapeHtml(esState.index)}</span>
      ${menuButton("es-open-index", "grid", "打开文档浏览")}
      ${menuButton("es-context-mapping", "table", "查看 Mapping")}
      ${menuButton("es-context-aliases", "key", "查看别名")}
      ${menuButton("es-new-doc", "plus", "新建文档")}
      <span class="context-sep"></span>
      <button class="danger-item" data-action="es-delete-index"><svg><use href="#i-warning"/></svg>删除索引</button>`);
  }

  function qualifiedTableName(target) {
    const protocol = connectionProtocol(target.connectionId) || "mysql";
    const quote = (part) => protocol === "mysql" ? `\`${String(part).replace(/`/g, "``")}\`` : `"${String(part).replace(/"/g, '""')}"`;
    return target.schema ? `${quote(target.schema)}.${quote(target.table)}` : quote(target.table);
  }

  function showTableContext(event, tableRow) {
    const group = tableRow.closest(".connection-group");
    const connectionId = tableRow.dataset.connectionId || group?.dataset.connectionId || activeConnectionId;
    if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
    activeTable = {
      schema: tableRow.dataset.tableSchema || "",
      table: tableRow.dataset.openTable,
      connectionId,
      objectType: tableRow.dataset.objectType || "table",
    };
    activeSchema = activeTable.schema || activeSchema;
    $$(".tree-row.selected").forEach((row) => row.classList.remove("selected"));
    tableRow.classList.add("selected");
    const isView = /view/i.test(activeTable.objectType);
    const isMaterialized = /materialized/i.test(activeTable.objectType);
    placeContextMenu(event, `
      <span class="context-label">${escapeHtml(activeTable.table)}</span>
      ${menuButton("open-table", isView ? "view" : "table", isView ? "打开视图" : "打开表")}
      ${menuButton("query-table", "query", isView ? "查询此视图" : "查询此表")}
      ${isView ? (isMaterialized ? "" : menuButton("view-edit", "settings", "编辑视图")) : menuButton("table-designer", "table", "设计表")}
      ${menuButton("show-object-info", "search", "查看结构与 DDL")}
      ${menuButton("copy-table-name", "copy", "复制名称")}
      ${menuButton("copy-table-qualified", "copy", "复制限定名（库.表）")}
      ${menuButton("export", "export", "导出数据")}
      <span class="context-sep"></span>
      ${menuButton("new-query-here", "query", "在此库新建查询")}
      ${menuButton("refresh-database", "refresh", "刷新对象列表")}
      <span class="context-sep"></span><button class="danger-item" data-action="${isView ? "view-drop" : "drop-table"}"><svg><use href="#i-warning"/></svg>${isView ? "删除视图" : "删除表"}</button>`);
  }

  function showDatabaseContext(event, databaseRow) {
    const group = databaseRow.closest(".connection-group");
    const connectionId = databaseRow.dataset.connectionId || group?.dataset.connectionId || activeConnectionId;
    if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
    activeSchema = databaseRow.dataset.sqlDatabase;
    activeTable = null;
    const canManage = activeProtocol === "mysql" || activeProtocol === "postgres";
    placeContextMenu(event, `
      <span class="context-label">${escapeHtml(activeSchema)}</span>
      ${menuButton("new-query-here", "query", "在此库新建查询")}
      ${menuButton("console-open", "terminal", "在命令控制台中打开")}
      ${menuButton("compare-from-schema", "compare", "以此库为源做结构比较")}
      ${menuButton("export", "export", "导出整个库")}
      ${canManage ? `<span class="context-sep"></span>${menuButton("namespace-create", "plus", activeProtocol === "postgres" ? "新建模式" : "新建数据库")}<button class="danger-item" data-action="namespace-drop"><svg><use href="#i-warning"/></svg>${activeProtocol === "postgres" ? "删除模式" : "删除数据库"}</button>` : ""}
      <span class="context-sep"></span>
      ${menuButton("refresh-database", "refresh", "刷新对象列表")}`);
  }

  function showDraftContext(event, draftRow) {
    const id = draftRow.dataset.openDraft || draftRow.dataset.draftRow;
    const draft = queryDrafts.get(String(id));
    if (!draft) return;
    const extra = ` data-draft-id="${escapeHtml(String(draft.id))}"`;
    placeContextMenu(event, `
      <span class="context-label">${escapeHtml(draft.name)}</span>
      ${menuButton("draft-open", "query", "打开查询", extra)}
      ${menuButton("draft-rename", "settings", "重命名", extra)}
      <span class="context-sep"></span>
      <button class="danger-item" data-action="draft-delete"${extra}><svg><use href="#i-warning"/></svg>删除保存的查询</button>`);
  }

  async function scanMore(db = redisScanState.db) {
    try {
      await loadRedisKeys(db, true);
    } catch (error) {
      showOperationError(error, "SCAN 失败");
    }
  }

  function parseRedisCommand(command) {
    const parts = [];
    let current = "";
    let quote = null;
    for (let index = 0; index < command.length; index += 1) {
      const char = command[index];
      if (quote) {
        if (char === quote) quote = null;
        else if (char === "\\" && index + 1 < command.length) current += command[++index];
        else current += char;
      } else if (char === "'" || char === '"') {
        quote = char;
      } else if (/\s/.test(char)) {
        if (current) {
          parts.push(current);
          current = "";
        }
      } else {
        current += char;
      }
    }
    if (quote) throw new Error("Redis 命令引号未闭合。");
    if (current) parts.push(current);
    if (!parts.length) throw new Error("请输入 Redis 命令。");
    return { command: parts[0], args: parts.slice(1) };
  }

  function isDangerousRedisCommand(command, args) {
    const upper = command.toUpperCase();
    const subcommand = String(args[0] || "").toUpperCase();
    if ([
      "FLUSHALL", "FLUSHDB", "SHUTDOWN", "SWAPDB", "MOVE", "RESTORE", "MIGRATE",
      "REPLICAOF", "SLAVEOF", "FAILOVER", "RENAME", "GETDEL", "EVAL", "EVALSHA", "FCALL",
    ].includes(upper)) return true;
    if (upper === "CONFIG" && ["SET", "REWRITE"].includes(subcommand)) return true;
    if (upper === "ACL" && ["SETUSER", "DELUSER", "LOAD", "SAVE"].includes(subcommand)) return true;
    if (upper === "SCRIPT" && subcommand === "FLUSH") return true;
    if (upper === "FUNCTION" && ["FLUSH", "DELETE", "LOAD", "RESTORE"].includes(subcommand)) return true;
    if (upper === "MODULE" && ["LOAD", "UNLOAD"].includes(subcommand)) return true;
    if (upper === "XGROUP" && subcommand === "DESTROY") return true;
    if (["DEL", "UNLINK"].includes(upper) && args.length > 0) return true;
    return false;
  }

  /* ---------- command console (mysql / psql / redis-cli style) ---------- */

  const consoleState = { connectionId: null, schema: null, history: [], historyIndex: 0, busy: false, ready: false };

  function consoleConnection() {
    return connectionLookup.get(consoleState.connectionId) || null;
  }

  function consolePrompt() {
    const connection = consoleConnection();
    if (!connection) return ">";
    if (connection.protocol === "redis") return `${connection.hostSummary || "redis"}>`;
    if (connection.protocol === "sqlite") return "sqlite>";
    if (connection.protocol === "postgres") return `${consoleState.schema || "postgres"}=>`;
    return `mysql${consoleState.schema ? ` ${consoleState.schema}` : ""}>`;
  }

  function consoleHelp(protocol) {
    if (protocol === "redis") {
      return [
        "直接输入 Redis 命令，按 ↵ 执行，例如：",
        "  GET user:1      HGETALL user:1      SCAN 0 MATCH user:* COUNT 100",
        "  TTL key         INFO memory          SELECT 2",
        "危险命令（FLUSHDB、DEL、CONFIG SET 等）会先要求确认；KEYS 已禁用，请用 SCAN。",
        "控制台命令：help 查看帮助 · clear 清屏",
      ].join("\n");
    }
    return [
      "输入 SQL 并按 ↵ 执行（⇧↵ 换行）；一次输入多条语句时按顺序执行，每条结果分别显示。",
      "  SELECT * FROM orders LIMIT 10;",
      "  UPDATE orders SET status = 'paid' WHERE id = 1;",
      "包含写操作的多条语句在一个事务中执行，任一失败整体回滚。结果最多显示 200 行。",
      "控制台命令：",
      "  use <库名>      切换当前数据库/模式        \\l   列出数据库",
      "  \\dt            列出当前库的表              \\d <表名>   查看表结构",
      "  clear           清屏                        help  查看帮助",
    ].join("\n");
  }

  function displayWidth(text) {
    let width = 0;
    for (const char of String(text)) width += /[\u1100-\u115f\u2e80-\ua4cf\uac00-\ud7a3\uf900-\ufaff\ufe30-\ufe4f\uff00-\uff60\uffe0-\uffe6]|[\u{1f300}-\u{1faff}]/u.test(char) ? 2 : 1;
    return width;
  }

  function fitWidth(text, width) {
    let result = "";
    let used = 0;
    for (const char of String(text)) {
      const size = displayWidth(char);
      if (used + size > width - 1 && used + size > width) break;
      if (used + size > width) break;
      result += char;
      used += size;
    }
    return { text: result, width: used };
  }

  function asciiTable(columns, rows) {
    const MAX = 48;
    const header = columns.map((column) => String(column.name ?? ""));
    const body = rows.map((row) => columns.map((_, index) => {
      const value = row[index];
      return value === null || value === undefined ? "NULL" : cellText(value).replace(/\r?\n/g, "↵");
    }));
    const numeric = columns.map((column, index) => isNumericColumn(column, rows[0]?.[index]));
    const widths = header.map((name, index) => Math.min(MAX, Math.max(displayWidth(name), ...body.map((row) => displayWidth(row[index])))));
    const line = `+${widths.map((width) => "-".repeat(width + 2)).join("+")}+`;
    const format = (cells, alignRight) => `|${cells.map((cell, index) => {
      let { text, width } = fitWidth(cell, widths[index]);
      if (text.length < cell.length) {
        ({ text, width } = fitWidth(cell, widths[index] - 1));
        text += "…";
        width += 1;
      }
      const pad = " ".repeat(Math.max(0, widths[index] - width));
      return ` ${alignRight?.[index] ? pad + text : text + pad} `;
    }).join("|")}|`;
    return [line, format(header), line, ...body.map((row) => format(row, numeric)), line].join("\n");
  }

  function formatRespValue(value, indent = "") {
    if (value === null || value === undefined) return "(nil)";
    if (typeof value === "number") return Number.isInteger(value) ? `(integer) ${value}` : `(double) ${value}`;
    if (typeof value === "boolean") return value ? "(true)" : "(false)";
    if (typeof value === "string") return /^(OK|PONG|QUEUED)$/.test(value) ? value : JSON.stringify(value);
    if (Array.isArray(value)) {
      if (!value.length) return "(empty array)";
      const width = String(value.length).length;
      return value.map((item, index) => {
        const label = `${String(index + 1).padStart(width)}) `;
        const nested = formatRespValue(item, `${indent}${" ".repeat(label.length)}`);
        return `${index ? indent : ""}${label}${nested}`;
      }).join("\n");
    }
    if (typeof value === "object") {
      if ("error" in value && Object.keys(value).length === 1) return `(error) ${value.error}`;
      const entries = Object.entries(value);
      if (!entries.length) return "(empty map)";
      return entries.map(([key, item], index) => `${index ? indent : ""}${index + 1}# ${JSON.stringify(key)} => ${formatRespValue(item, `${indent}   `)}`).join("\n");
    }
    return String(value);
  }

  function renderConsoleView() {
    const view = $("#view-console");
    if (!view) return;
    const connections = savedConnections;
    if (!consoleState.connectionId || !connectionLookup.has(consoleState.connectionId)) {
      consoleState.connectionId = activeConnectionId || connections[0]?.id || null;
      consoleState.schema = activeConnectionId && consoleState.connectionId === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(consoleState.connectionId);
    }
    const connection = consoleConnection();
    const protocol = connection?.protocol || null;
    const isSql = protocol && protocolFamily(protocol) === "sql";
    if (!consoleState.ready) {
      view.innerHTML = `
        <div class="console-head">
          <div class="console-title"><div class="eyebrow">COMMAND CONSOLE</div><h2>命令控制台</h2><p>像 mysql / psql / redis-cli 一样逐条输入命令，结果以纯文本显示。适合快速查看数据、执行运维命令；编写和保存复杂 SQL 请使用查询编辑器。</p></div>
          <div class="console-controls">
            <label class="console-select"><span class="protocol-badge unknown" id="console-badge">?</span><select id="console-connection" aria-label="控制台连接"></select></label>
            <label class="console-select" id="console-schema-wrap"><select id="console-schema" aria-label="当前数据库"></select></label>
            <button class="console-button" data-action="console-clear" title="清屏 (⌃L)">清屏</button>
          </div>
        </div>
        <div class="console-shell" id="console-shell">
          <div class="console-output" id="console-output"></div>
          <div class="console-line current"><span class="prompt" id="console-prompt">&gt;</span><textarea class="console-input" id="console-input" rows="1" spellcheck="false" autocapitalize="off" autocomplete="off" aria-label="控制台输入"></textarea></div>
        </div>
        <div class="console-bar"><span id="console-mode"><b>—</b></span><span class="guard-chip" id="console-guard"><svg><use href="#i-shield"/></svg>危险命令执行前需确认</span><span class="console-hint">↵ 执行 · ⇧↵ 换行 · ↑↓ 历史 · 输入 help 查看帮助</span></div>`;
      consoleState.ready = true;
      bindConsoleInput();
      if (connection) consolePrint(`<span class="console-muted">${escapeHtml(`已连接到 ${connection.name}（${protocolMeta[protocol]?.label || protocol}）。输入 help 查看用法。`)}</span>`);
    }
    const select = $("#console-connection");
    if (select) {
      select.innerHTML = connections.length
        ? connections.map((item) => `<option value="${escapeHtml(item.id)}"${item.id === consoleState.connectionId ? " selected" : ""}>${escapeHtml(item.name)} · ${escapeHtml(protocolMeta[item.protocol]?.label || item.protocol)}</option>`).join("")
        : '<option value="">还没有连接</option>';
    }
    const badge = $("#console-badge");
    if (badge) {
      badge.className = protocolBadgeClass(protocol);
      badge.innerHTML = protocol ? protocolGlyphHtml(protocol) : "?";
    }
    const schemaWrap = $("#console-schema-wrap");
    const schemaSelect = $("#console-schema");
    if (schemaWrap) schemaWrap.hidden = !isSql || protocol === "sqlite";
    if (schemaSelect && isSql && protocol !== "sqlite") {
      const cached = metaFor(consoleState.connectionId).schemas;
      const names = [...new Set([...(consoleState.schema ? [consoleState.schema] : []), ...(cached || [])])];
      schemaSelect.innerHTML = `<option value="">${protocol === "postgres" ? "默认模式" : "默认数据库"}</option>${names.map((name) => `<option value="${escapeHtml(name)}"${name === consoleState.schema ? " selected" : ""}>${escapeHtml(name)}</option>`).join("")}`;
      if (!cached && backendAvailable) loadSchemas(consoleState.connectionId).then(() => renderConsoleView()).catch(() => {});
    }
    const prompt = $("#console-prompt");
    if (prompt) prompt.textContent = consolePrompt();
    const mode = $("#console-mode");
    if (mode) mode.innerHTML = protocol === "redis" ? "<b>RESP</b> redis-cli 风格输出" : isSql ? `<b>${escapeHtml(protocolMeta[protocol]?.label || "SQL")}</b> 文本表格输出 · 最多 200 行` : "<b>—</b> 请选择连接";
    const guard = $("#console-guard");
    if (guard) guard.lastChild.textContent = protocol === "redis" ? "FLUSHDB / DEL / CONFIG SET 等需确认 · KEYS 已禁用" : "多条写语句在一个事务中执行";
  }

  function consolePrint(html) {
    const output = $("#console-output");
    if (!output) return;
    output.insertAdjacentHTML("beforeend", html);
    while (output.childElementCount > 400) output.firstElementChild.remove();
    const shell = $("#console-shell");
    if (shell) shell.scrollTop = shell.scrollHeight;
  }

  function consoleEcho(command) {
    consolePrint(`<div class="console-entry"><span class="prompt">${escapeHtml(consolePrompt())}</span><span class="command-text">${escapeHtml(command)}</span></div>`);
  }

  function consoleBlock(text, tone = "") {
    consolePrint(`<pre class="console-result${tone ? ` ${tone}` : ""}">${escapeHtml(text)}</pre>`);
  }

  async function runConsoleSql(command) {
    const connection = consoleConnection();
    const text = command.trim();
    const meta = /^(use\s+|\\c\s+)([`"]?)([^`";\s]+)\2\s*;?$/i.exec(text);
    if (meta) {
      consoleState.schema = meta[3];
      consoleBlock(connection.protocol === "postgres" ? `search_path 已切换到 "${meta[3]}"` : "Database changed", "muted");
      renderConsoleView();
      return;
    }
    if (/^\\l\s*;?$/i.test(text)) {
      const schemas = await loadSchemas(connection.id);
      consoleBlock(`${asciiTable([{ name: connection.protocol === "postgres" ? "Schema" : "Database" }], schemas.map((name) => [name]))}\n${schemas.length} rows`);
      return;
    }
    if (/^\\dt\s*;?$/i.test(text)) {
      const schema = consoleState.schema || defaultSchemaFor(connection.id);
      if (!schema) throw new Error("请先用 use <库名> 选择数据库。");
      const tables = await loadTables(connection.id, schema);
      consoleBlock(`${asciiTable([{ name: "Name" }, { name: "Type" }], tables.map((table) => [table.name, table.type]))}\n${tables.length} rows · ${schema}`);
      return;
    }
    const describe = /^\\d\s+([`"]?)([^`";\s]+)\1\s*;?$/i.exec(text);
    if (describe) {
      const parts = describe[2].split(".");
      const schema = parts.length > 1 ? parts[0] : consoleState.schema || defaultSchemaFor(connection.id);
      const meta = await loadTableMeta(connection.id, schema, parts[parts.length - 1]);
      consoleBlock(`${asciiTable([{ name: "Column" }, { name: "Type" }, { name: "Null" }, { name: "Key" }, { name: "Default" }, { name: "Extra" }], (meta.columns || []).map((column) => [column.name, column.dataType, column.nullable ? "YES" : "NO", column.isPrimaryKey ? "PRI" : "", column.defaultValue ?? null, column.extra || ""]))}\n${(meta.indexes || []).map((index) => `${index.unique ? "UNIQUE " : ""}INDEX ${index.name} (${index.columns.join(", ")})`).join("\n")}`.trimEnd());
      return;
    }
    const statements = splitSqlStatements(text);
    if (!statements.length) return;
    if (parameterCount(text) > 0) throw new Error("控制台不支持 ? / $1 参数占位符，请在查询编辑器中执行带参数的语句。");
    let allowPartialCommit = false;
    if (statements.length > 1 && connection.protocol === "mysql" && statements.some((statement) => DDL_STATEMENT.test(stripLeadingComments(statement)))) {
      allowPartialCommit = await confirmChoice({
        title: "逐条执行",
        eyebrow: "MYSQL IMPLICIT COMMIT",
        body: "这些语句包含 DDL。MySQL 的 DDL 会隐式提交，将逐条自动提交执行，中途失败时之前的语句不会回滚。",
        confirmLabel: "逐条执行",
      });
      if (!allowPartialCommit) {
        consoleBlock("已取消", "muted");
        return;
      }
    }
    const op = operationId();
    activeSqlOperationId = op;
    const script = await backendCall("sql_run_script", {
      connectionId: connection.id,
      request: {
        connectionId: connection.id,
        operationId: op,
        pageSize: 200,
        allowPartialCommit,
        statements: statements.map((statement, index) => ({ connectionId: connection.id, sql: statement, params: [], operationId: `${op}-${index + 1}`, schema: consoleState.schema || null })),
      },
    });
    for (const item of script.results || []) {
      if (item.returnsRows) {
        const count = `${item.rows.length}${item.hasMore ? "+" : ""} row${item.rows.length === 1 ? "" : "s"} in set (${item.durationMs} ms)${item.hasMore ? " · 只显示前 200 行" : ""}`;
        consoleBlock(item.columns.length ? `${asciiTable(item.columns, item.rows)}\n${count}` : count);
      } else {
        consoleBlock(`Query OK, ${item.rowsAffected} row${item.rowsAffected === 1 ? "" : "s"} affected (${item.durationMs} ms)${item.lastInsertId ? ` · last insert id ${item.lastInsertId}` : ""}`, "muted");
      }
    }
    if (script.failure) {
      consoleBlock(`ERROR (语句 ${script.failure.index + 1}/${statements.length}): ${script.failure.message}${script.transactional ? "\n事务已回滚，本次输入的修改均未生效。" : ""}`, "error");
    } else if (script.transactional && statements.length > 1) {
      consoleBlock(`事务已提交 · ${statements.length} 条语句`, "muted");
    }
    if (statements.some((statement) => DDL_STATEMENT.test(stripLeadingComments(statement)))) afterDdl(connection.id);
  }

  async function runConsoleRedis(command, confirmed = false) {
    const connection = consoleConnection();
    const parsed = parseRedisCommand(command.trim());
    if (!confirmed && isDangerousRedisCommand(parsed.command, parsed.args)) {
      const ok = await confirmChoice({
        title: "确认危险命令",
        eyebrow: "REDIS GUARDRAIL",
        body: `即将在 <b>${escapeHtml(connection.name)}</b> 上执行 <code>${escapeHtml(command.trim())}</code>，可能造成不可逆的数据变更。`,
        confirmLabel: "确认执行",
        danger: true,
      });
      if (!ok) {
        consoleBlock("已取消", "muted");
        return;
      }
      confirmed = true;
    }
    activeRedisOperationId = operationId();
    try {
      const result = await backendCall("redis_command", {
        connectionId: connection.id,
        request: { connectionId: connection.id, command: parsed.command, args: parsed.args, operationId: activeRedisOperationId, confirmedDangerous: confirmed },
      });
      consoleBlock(formatRespValue(result.value));
    } finally {
      activeRedisOperationId = null;
    }
  }

  async function consoleSubmit() {
    const input = $("#console-input");
    if (!input || consoleState.busy) return;
    const command = input.value;
    if (!command.trim()) return;
    input.value = "";
    autosizeConsoleInput();
    if (consoleState.history[consoleState.history.length - 1] !== command) consoleState.history.push(command);
    if (consoleState.history.length > 200) consoleState.history.shift();
    consoleState.historyIndex = consoleState.history.length;
    consoleEcho(command);
    const keyword = command.trim().toLowerCase().replace(/;$/, "");
    const connection = consoleConnection();
    if (keyword === "clear" || keyword === "cls") {
      $("#console-output").innerHTML = "";
      return;
    }
    if (keyword === "help" || keyword === "\\?" || keyword === "\\h") {
      consoleBlock(consoleHelp(connection?.protocol), "muted");
      return;
    }
    if (!connection) {
      consoleBlock("请先在右上角选择一个连接。", "error");
      return;
    }
    if (!backendAvailable) {
      consoleBlock("桌面后端未连接，当前窗口只能预览界面。", "error");
      return;
    }
    consoleState.busy = true;
    $(".console-line.current")?.classList.add("busy");
    const started = performance.now();
    try {
      if (connection.protocol === "redis") await runConsoleRedis(command);
      else await runConsoleSql(command);
    } catch (error) {
      consoleBlock(`ERROR: ${errorMessage(error)}`, "error");
    } finally {
      consoleState.busy = false;
      activeSqlOperationId = null;
      $(".console-line.current")?.classList.remove("busy");
      if (performance.now() - started > 400) refreshAuditOnly();
      input.focus();
    }
  }

  function autosizeConsoleInput() {
    const input = $("#console-input");
    if (!input) return;
    input.style.height = "auto";
    input.style.height = `${Math.min(220, input.scrollHeight)}px`;
  }

  function bindConsoleInput() {
    const input = $("#console-input");
    if (!input || input.dataset.bound === "true") return;
    input.dataset.bound = "true";
    input.addEventListener("input", autosizeConsoleInput);
    input.addEventListener("keydown", (event) => {
      if (event.isComposing) return;
      if (event.key === "Enter" && !event.shiftKey && !event.altKey) {
        event.preventDefault();
        event.stopPropagation();
        consoleSubmit();
      } else if (event.key === "l" && event.ctrlKey) {
        event.preventDefault();
        $("#console-output").innerHTML = "";
      } else if (event.key === "Escape" && input.value) {
        event.preventDefault();
        input.value = "";
        autosizeConsoleInput();
      } else if ((event.key === "ArrowUp" || event.key === "ArrowDown") && !input.value.includes("\n")) {
        if (!consoleState.history.length) return;
        event.preventDefault();
        const delta = event.key === "ArrowUp" ? -1 : 1;
        consoleState.historyIndex = Math.max(0, Math.min(consoleState.history.length, consoleState.historyIndex + delta));
        input.value = consoleState.history[consoleState.historyIndex] || "";
        autosizeConsoleInput();
        input.setSelectionRange(input.value.length, input.value.length);
      }
    });
    $("#console-shell")?.addEventListener("mouseup", () => {
      if (!window.getSelection()?.toString()) input.focus({ preventScroll: true });
    });
  }

  function openConsole(connectionId = null) {
    if (connectionId && connectionId !== consoleState.connectionId) {
      consoleState.connectionId = connectionId;
      consoleState.schema = connectionId === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(connectionId);
      if (consoleState.ready) consolePrint(`<span class="console-muted">${escapeHtml(`— 切换到 ${connectionLookup.get(connectionId)?.name || "连接"} —`)}</span>`);
    }
    showWorkspace("console");
  }

  /* ===================== SQL workspace: tabs, metadata, completion, grid ===================== */
  const SQL_TABS_KEY = "luxbase.sqlTabs.v1";
  const LAYOUT_KEY = "luxbase.layout.v1";
  const THEME_KEY = "luxbase.theme.v1"; // ui/theme-preload.js reads this same key before mount
  let themeRepaintTimer = 0;
  const GRID_ROW_HEIGHT = 28;
  const ROWNUM_WIDTH = 54;
  const NUMERIC_TYPE = /INT|DECIMAL|NUMERIC|FLOAT|DOUBLE|REAL|NUMBER|SERIAL|MONEY/i;
  const READ_STATEMENT = /^(SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|PRAGMA|VALUES|TABLE)\b/i;
  const DDL_STATEMENT = /^(CREATE|ALTER|DROP|RENAME|TRUNCATE|COMMENT|GRANT|REVOKE)\b/i;
  const sqlTabs = [];
  let activeSqlTabId = null;
  let persistTimer = null;
  let pendingClipboard = null;
  let gridDrag = null;
  let columnResize = null;
  let panelResize = null;
  let cellEditor = null;
  let lastCellDouble = null;
  const pendingCellEdits = new Map();
  let blobTarget = null;
  let gridScrollFrame = 0;
  let designer = null;
  const metaCache = new Map();
  const layout = {
    sidebarWidth: 260,
    inspectorWidth: 320,
    sidebarCollapsed: false,
    inspectorCollapsed: false,
    editorHeight: 260,
  };

  function connectionProtocol(connectionId) {
    return connectionLookup.get(connectionId)?.protocol || null;
  }

  function defaultSchemaFor(connectionId) {
    const connection = connectionLookup.get(connectionId);
    if (!connection) return null;
    if (connection.protocol === "postgres") return "public";
    if (connection.protocol === "sqlite") return "main";
    return connection.database || null;
  }

  function quoteIdent(name, protocol) {
    const text = String(name ?? "");
    return protocol === "mysql" ? `\`${text.replace(/`/g, "``")}\`` : `"${text.replace(/"/g, "\"\"")}"`;
  }

  function stripLeadingComments(sql) {
    let text = String(sql || "").trimStart();
    for (;;) {
      if (text.startsWith("--") || text.startsWith("#")) {
        const end = text.indexOf("\n");
        text = end < 0 ? "" : text.slice(end + 1).trimStart();
      } else if (text.startsWith("/*")) {
        const end = text.indexOf("*/");
        text = end < 0 ? "" : text.slice(end + 2).trimStart();
      } else {
        return text;
      }
    }
  }

  function isReadSql(sql) {
    return READ_STATEMENT.test(stripLeadingComments(sql));
  }

  function summarizeSql(sql, limit = 160) {
    const text = String(sql || "").replace(/\s+/g, " ").trim();
    return text.length > limit ? `${text.slice(0, limit)}…` : text;
  }

  /* ---------- metadata cache (schemas → tables → columns, DDL) ---------- */

  function metaFor(connectionId) {
    let entry = metaCache.get(connectionId);
    if (!entry) {
      entry = { schemas: null, schemasPromise: null, tables: new Map(), tablePromises: new Map(), tableMeta: new Map(), columnPromises: new Map(), ddl: new Map(), stats: new Map(), statsPromises: new Map() };
      metaCache.set(connectionId, entry);
    }
    return entry;
  }

  function rememberSnapshot(connectionId, snapshot, { schemaList = false } = {}) {
    const entry = metaFor(connectionId);
    if (schemaList) entry.schemas = (snapshot.databases || []).map((database) => database.name);
    for (const database of snapshot.databases || []) {
      if (database.tables?.length) {
        entry.tables.set(database.name, database.tables.map((table) => ({ name: table.name, type: table.tableType })));
        for (const table of database.tables) {
          if (table.columns?.length) entry.tableMeta.set(`${table.schema}.${table.name}`, table);
        }
      }
    }
  }

  function loadSchemas(connectionId) {
    const entry = metaFor(connectionId);
    if (entry.schemas) return Promise.resolve(entry.schemas);
    if (!entry.schemasPromise) {
      entry.schemasPromise = backendCall("sql_metadata", { connectionId, request: { connectionId, schema: null, table: null } })
        .then((snapshot) => {
          rememberSnapshot(connectionId, snapshot, { schemaList: true });
          return entry.schemas;
        })
        .finally(() => { entry.schemasPromise = null; });
    }
    return entry.schemasPromise;
  }

  function loadTables(connectionId, schema) {
    const entry = metaFor(connectionId);
    if (!schema) return Promise.resolve([]);
    if (entry.tables.has(schema)) return Promise.resolve(entry.tables.get(schema));
    if (!entry.tablePromises.has(schema)) {
      entry.tablePromises.set(schema, backendCall("sql_metadata", { connectionId, request: { connectionId, schema, table: null, includeTables: true } })
        .then((snapshot) => {
          const database = (snapshot.databases || []).find((item) => item.name === schema) || snapshot.databases?.[0];
          const tables = (database?.tables || []).map((table) => ({ name: table.name, type: table.tableType }));
          entry.tables.set(schema, tables);
          return tables;
        })
        .finally(() => entry.tablePromises.delete(schema)));
    }
    return entry.tablePromises.get(schema);
  }

  function loadTableMeta(connectionId, schema, table) {
    const entry = metaFor(connectionId);
    const key = `${schema}.${table}`;
    if (entry.tableMeta.has(key)) return Promise.resolve(entry.tableMeta.get(key));
    if (!entry.columnPromises.has(key)) {
      const pending = backendCall("sql_metadata", { connectionId, request: { connectionId, schema, table } })
        .then((snapshot) => {
          const found = (snapshot.databases || [])
            .flatMap((database) => database.tables || [])
            .find((item) => item.name === table && (item.schema === schema || !schema));
          if (!found) throw new Error(`驱动没有返回 ${schema ? `${schema}.` : ""}${table} 的结构。`);
          if (entry.columnPromises.get(key) === pending) entry.tableMeta.set(key, found);
          return found;
        })
        .finally(() => {
          if (entry.columnPromises.get(key) === pending) entry.columnPromises.delete(key);
        });
      entry.columnPromises.set(key, pending);
    }
    return entry.columnPromises.get(key);
  }

  // Structure changed: drop cached metadata and reload the database nodes already open in the tree.
  function afterDdl(connectionId) {
    metaCache.delete(connectionId);
    if (objectsState.connectionId === connectionId && currentWorkspace() === "objects") renderObjectsView();
    const group = $(`.connection-group[data-connection-id="${CSS.escape(connectionId || "")}"]`);
    if (!group) return;
    $$(".database-node", group).forEach((node) => {
      if (node.dataset.objectsLoaded !== "true") return;
      delete node.dataset.objectsLoaded;
      const button = $("[data-sql-database]", node);
      if (button) loadDatabaseObjects(button);
    });
  }

  function invalidateTableMeta(connectionId, schema, table) {
    const entry = metaFor(connectionId);
    entry.tableMeta.delete(`${schema}.${table}`);
    entry.columnPromises.delete(`${schema}.${table}`);
    entry.ddl.delete(`${schema}.${table}`);
  }

  /* ---------- SQL completion (called from Monaco) ---------- */

  function unquoteIdent(text) {
    return String(text || "").trim().replace(/^[`"[]|[`"\]]$/g, "");
  }

  function tableReferences(sql) {
    const references = [];
    const clause = /\b(FROM|JOIN|UPDATE|INTO)\b([\s\S]*?)(?=\b(?:FROM|JOIN|WHERE|GROUP|ORDER|HAVING|LIMIT|UNION|ON|USING|SET|VALUES|RETURNING|LEFT|RIGHT|INNER|OUTER|CROSS|FULL|NATURAL|SELECT)\b|;|$)/gi;
    const reserved = new Set(["WHERE", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "SET", "VALUES", "GROUP", "ORDER", "LIMIT", "AS", "USING", "CROSS", "FULL", "NATURAL", "UNION"]);
    let match;
    while ((match = clause.exec(sql))) {
      for (const part of match[2].split(",")) {
        const item = /^\s*((?:[`"]?[\w$]+[`"]?\s*\.\s*)?[`"]?[\w$]+[`"]?)(?:\s+(?:AS\s+)?([`"]?[\w$]+[`"]?))?/i.exec(part);
        if (!item || part.trim().startsWith("(")) continue;
        const pieces = item[1].split(".").map(unquoteIdent);
        const alias = item[2] && !reserved.has(item[2].toUpperCase()) ? unquoteIdent(item[2]) : null;
        references.push({ schema: pieces.length > 1 ? pieces[0] : null, table: pieces[pieces.length - 1], alias });
      }
    }
    return references;
  }

  async function sqlCompletions({ textBefore = "", textAfter = "" } = {}) {
    const tab = activeSqlTab();
    if (!backendAvailable || !tab || tab.kind !== "query" || !tab.connectionId) return [];
    const connectionId = tab.connectionId;
    const schema = tab.schema || defaultSchemaFor(connectionId);
    const statementStart = textBefore.lastIndexOf(";") + 1;
    const statementEnd = textAfter.indexOf(";");
    const statement = textBefore.slice(statementStart) + (statementEnd < 0 ? textAfter : textAfter.slice(0, statementEnd));
    const references = tableReferences(statement);
    const columnItems = (table, columns, prefix) => columns.map((column) => ({
      label: column.name,
      kind: "column",
      detail: `${prefix || table.name || ""} · ${column.dataType}${column.isPrimaryKey ? " · PK" : ""}`,
      sortText: `0${column.name}`,
    }));
    const qualified = /([`"]?[\w$]+[`"]?)\.([\w$]*)$/.exec(textBefore);
    if (qualified) {
      const qualifier = unquoteIdent(qualified[1]).toLowerCase();
      const aliased = references.find((item) => item.alias?.toLowerCase() === qualifier);
      const schemas = await loadSchemas(connectionId).catch(() => []);
      const schemaMatch = aliased ? null : schemas.find((name) => name.toLowerCase() === qualifier);
      const reference = aliased || (schemaMatch ? null : references.find((item) => item.table.toLowerCase() === qualifier));
      if (reference) {
        const meta = await loadTableMeta(connectionId, reference.schema || schema, reference.table).catch(() => null);
        return meta ? columnItems(meta, meta.columns || [], reference.alias || reference.table) : [];
      }
      if (schemaMatch) {
        const tables = await loadTables(connectionId, schemaMatch).catch(() => []);
        return tables.map((table) => ({ label: table.name, kind: /VIEW/i.test(table.type) ? "view" : "table", detail: `${schemaMatch} · ${/VIEW/i.test(table.type) ? "视图" : "表"}`, sortText: `0${table.name}` }));
      }
      const tables = await loadTables(connectionId, schema).catch(() => []);
      const table = tables.find((item) => item.name.toLowerCase() === qualifier);
      if (table) {
        const meta = await loadTableMeta(connectionId, schema, table.name).catch(() => null);
        return meta ? columnItems(meta, meta.columns || [], table.name) : [];
      }
      return [];
    }
    const expectsTable = /\b(FROM|JOIN|INTO|UPDATE|TABLE|DESCRIBE|DESC)\s+[\w$`"]*$/i.test(textBefore);
    const items = [];
    const [tables, schemas] = await Promise.all([
      loadTables(connectionId, schema).catch(() => []),
      loadSchemas(connectionId).catch(() => []),
    ]);
    for (const table of tables) {
      items.push({ label: table.name, kind: /VIEW/i.test(table.type) ? "view" : "table", detail: `${schema} · ${/VIEW/i.test(table.type) ? "视图" : "表"}`, sortText: `${expectsTable ? "0" : "2"}${table.name}` });
    }
    for (const name of schemas) {
      items.push({ label: name, kind: "schema", detail: connectionProtocol(connectionId) === "postgres" ? "模式" : "数据库", sortText: `${expectsTable ? "1" : "3"}${name}` });
    }
    if (!expectsTable) {
      const metas = await Promise.all(references.slice(0, 6).map((reference) => loadTableMeta(connectionId, reference.schema || schema, reference.table)
        .then((meta) => ({ meta, reference })).catch(() => null)));
      for (const item of metas.filter(Boolean)) {
        items.push(...columnItems(item.meta, item.meta.columns || [], item.reference.alias || item.reference.table));
      }
    }
    return items;
  }
  window.__luxbaseSqlCompletions = sqlCompletions;

  /* ---------- tabs ---------- */

  function emptyGrid(source = "query") {
    return {
      columns: [], rows: [], offset: 0, hasMore: false, source, connectionId: null, widths: [],
      durationMs: null, executed: false, selection: null, focus: null, scrollTop: 0, scrollLeft: 0,
      baseRows: null, filterText: "", affected: null, sql: null, params: [], schema: null,
    };
  }

  function nextQueryTitle() {
    const used = new Set(sqlTabs.filter((tab) => tab.kind === "query").map((tab) => tab.title));
    let index = 1;
    while (used.has(`查询 ${index}`)) index += 1;
    return `查询 ${index}`;
  }

  function createSqlTab(options = {}) {
    const kind = options.kind || "query";
    const connectionId = options.connectionId !== undefined ? options.connectionId : activeConnectionId;
    const tab = {
      id: options.id || `tab-${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`,
      kind,
      title: options.title || (kind === "query" ? nextQueryTitle() : options.table),
      connectionId: connectionId || null,
      schema: options.schema ?? null,
      table: options.table || null,
      objectType: options.objectType || "table",
      sql: options.sql ?? "",
      dirty: Boolean(options.dirty),
      draftId: options.draftId || null,
      grid: emptyGrid(kind === "table" ? "table" : "query"),
      results: [],
      activeResult: 0,
      resultTab: "grid",
      messages: [],
      running: false,
      operationId: null,
      primaryKeys: [],
      pageSize: kind === "table" ? 100 : 500,
      filter: "",
      orderBy: "",
      filterConditions: [],
      filterLogic: "AND",
      filterRaw: "",
    };
    if (kind === "query" && !tab.schema && tab.connectionId) tab.schema = defaultSchemaFor(tab.connectionId);
    const activeIndex = sqlTabs.findIndex((item) => item.id === activeSqlTabId);
    if (options.append || activeIndex < 0) sqlTabs.push(tab);
    else sqlTabs.splice(activeIndex + 1, 0, tab);
    return tab;
  }

  function activeSqlTab() {
    return sqlTabs.find((tab) => tab.id === activeSqlTabId) || null;
  }

  function tabDialect(tab) {
    return connectionProtocol(tab?.connectionId) || activeProtocol || "mysql";
  }

  function renderSqlTabs() {
    const strip = $("#sql-tab-strip");
    if (!strip) return;
    const sqlVisible = $("#view-sql")?.classList.contains("active");
    strip.innerHTML = sqlTabs.map((tab) => {
      const connection = connectionLookup.get(tab.connectionId);
      const isView = tab.kind === "table" && /view/i.test(tab.objectType || "");
      const icon = tab.kind === "table"
        ? `<svg class="tab-svg ${isView ? "view" : "table"}"><use href="#i-${isView ? "view" : "table"}"/></svg>`
        : '<svg class="tab-svg query"><use href="#i-query"/></svg>';
      const detail = tab.kind === "table"
        ? `${tab.schema ? `${tab.schema}.` : ""}${tab.table} · ${connection?.name || ""}`
        : `${tab.title}${connection ? ` · ${connection.name}${tab.schema ? ` / ${tab.schema}` : ""}` : " · 未选择连接"}`;
      const active = sqlVisible && tab.id === activeSqlTabId;
      return `<button class="workspace-tab sql-tab${active ? " active" : ""}${tab.running ? " running" : ""}" data-sql-tab="${escapeHtml(tab.id)}" title="${escapeHtml(detail)}">${icon}<span class="tab-title">${escapeHtml(tab.title)}</span>${tab.kind === "query" && tab.dirty ? '<span class="tab-dirty" title="未保存为草稿"></span>' : ""}<span class="tab-close" data-close-sql-tab="${escapeHtml(tab.id)}" title="关闭 (⌘W)">×</span></button>`;
    }).join("");
    const active = strip.querySelector(".sql-tab.active");
    if (active) active.scrollIntoView({ block: "nearest", inline: "nearest" });
    updateTabStripNav();
    const stripMenu = $(".tab-strip-menu");
    if (stripMenu) stripMenu.hidden = sqlTabs.length === 0;
  }

  function updateTabStripNav() {
    const strip = $("#sql-tab-strip");
    if (!strip) return;
    const overflow = strip.scrollWidth > strip.clientWidth + 2;
    const left = $('[data-tab-scroll="-1"]');
    const right = $('[data-tab-scroll="1"]');
    if (left) { left.hidden = !overflow; left.disabled = strip.scrollLeft <= 2; }
    if (right) { right.hidden = !overflow; right.disabled = strip.scrollLeft + strip.clientWidth >= strip.scrollWidth - 2; }
  }

  function syncGridIntoTab() {
    const tab = activeSqlTab();
    if (!tab) return;
    const wrap = $(".grid-wrap");
    if (wrap) {
      gridState.scrollTop = wrap.scrollTop;
      gridState.scrollLeft = wrap.scrollLeft;
    }
    tab.grid = gridState;
  }

  function activateSqlTab(id, options = {}) {
    const tab = sqlTabs.find((entry) => entry.id === id);
    if (!tab) return;
    cancelCellEditor();
    if (activeSqlTabId && activeSqlTabId !== id) syncGridIntoTab();
    activeSqlTabId = id;
    gridState = tab.grid;
    const connection = tab.connectionId ? connectionLookup.get(tab.connectionId) : null;
    if (connection && connection.id !== activeConnectionId) setActiveConnectionState(connection);
    if (tab.schema) activeSchema = tab.schema;
    if (tab.kind === "table") {
      activeTable = { schema: tab.schema, table: tab.table, connectionId: tab.connectionId };
      activePrimaryKey = tab.primaryKeys.length === 1 ? tab.primaryKeys[0] : null;
    }
    $("#view-sql")?.classList.toggle("table-mode", tab.kind === "table");
    showWorkspace("sql");
    if (tab.kind === "query") {
      window.__luxbaseEditorOpen?.(tab.id, tab.sql, tabDialect(tab), Boolean(options.focusEditor));
      renderQueryContext(tab);
    } else {
      renderTableToolbar(tab);
    }
    renderResultTabs(tab);
    renderGrid();
    renderMessages(tab);
    setResultTab(tab.resultTab || "grid");
    renderBreadcrumbForTab(tab);
    renderExecutionState(tab);
    if (tab.kind === "table") showObjectInfo({ connectionId: tab.connectionId, schema: tab.schema, table: tab.table, objectType: tab.objectType });
    persistSqlTabs();
  }

  function newQueryTab(options = {}) {
    let connectionId = options.connectionId !== undefined ? options.connectionId : activeConnectionId;
    if (connectionId && protocolFamily(connectionProtocol(connectionId)) !== "sql") connectionId = null;
    const schema = options.schema !== undefined
      ? options.schema
      : (connectionId && connectionId === activeConnectionId && activeSchema ? activeSchema : defaultSchemaFor(connectionId));
    const tab = createSqlTab({ kind: "query", connectionId, schema, sql: options.sql || "", title: options.title, draftId: options.draftId, dirty: Boolean(options.sql && !options.draftId) });
    activateSqlTab(tab.id, { focusEditor: true });
    return tab;
  }

  function closeSqlTab(id, { force = false } = {}) {
    const index = sqlTabs.findIndex((tab) => tab.id === id);
    if (index < 0) return;
    const tab = sqlTabs[index];
    if (tab.running && !force) {
      toast("该标签页仍在执行", "请先停止查询，再关闭标签页。", "danger");
      return;
    }
    if (!force && tab.kind === "query" && tab.dirty && tab.sql.trim()) {
      showModal("关闭查询", "UNSAVED QUERY", `
        <p class="modal-text">“${escapeHtml(tab.title)}”中的 SQL 还没有保存为草稿，关闭后无法恢复。</p>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="secondary-button danger-text" data-action="close-tab-discard" data-tab="${escapeHtml(tab.id)}">不保存，直接关闭</button><button class="primary-button" data-action="close-tab-save" data-tab="${escapeHtml(tab.id)}">保存为草稿</button></div>
      `);
      return;
    }
    sqlTabs.splice(index, 1);
    if (activeSqlTabId === id) {
      activeSqlTabId = null;
      const next = sqlTabs[index] || sqlTabs[index - 1];
      if (next) activateSqlTab(next.id);
      else showWorkspace("objects");
    }
    window.__luxbaseEditorClose?.(id);
    renderSqlTabs();
    persistSqlTabs();
  }

  let tabMenuTargetId = null;
  let pendingBatchClose = null;

  function tabMenuHtml(tab) {
    const index = sqlTabs.findIndex((item) => item.id === tab.id);
    const left = index;
    const right = sqlTabs.length - index - 1;
    const others = sqlTabs.length - 1;
    const count = (n) => (n ? `（${n}）` : "");
    return `
      ${tab.kind === "query" ? '<button data-action="tab-save"><svg><use href="#i-save"/></svg>保存为草稿<kbd>⌘S</kbd></button><span class="context-sep"></span>' : ""}
      <button data-action="tab-close"><svg><use href="#i-close"/></svg>关闭<kbd>⌘W</kbd></button>
      <button data-action="tab-close-others"${others ? "" : " disabled"}><svg><use href="#i-close"/></svg>关闭其他${count(others)}</button>
      <button data-action="tab-close-left"${left ? "" : " disabled"}><svg><use href="#i-close"/></svg>关闭左侧${count(left)}</button>
      <button data-action="tab-close-right"${right ? "" : " disabled"}><svg><use href="#i-close"/></svg>关闭右侧${count(right)}</button>
      <span class="context-sep"></span>
      <button class="danger-item" data-action="tab-close-all"><svg><use href="#i-warning"/></svg>关闭全部${count(sqlTabs.length)}</button>`;
  }

  function openTabMenu(event, id) {
    const tab = sqlTabs.find((item) => item.id === id);
    if (!tab) return;
    tabMenuTargetId = id;
    placeContextMenu(event, tabMenuHtml(tab));
  }

  // Bulk close asks once for the whole batch instead of once per tab: unsaved drafts can be kept
  // (auto-named after the tab title) or discarded, and running queries always block the batch.
  function closeTabBatch(ids, label) {
    const tabs = ids.map((id) => sqlTabs.find((item) => item.id === id)).filter(Boolean);
    if (!tabs.length) return;
    if (tabs.some((tab) => tab.running)) {
      toast("仍有查询在执行", "请先停止执行中的查询，再批量关闭标签页。", "danger");
      return;
    }
    const dirty = tabs.filter((tab) => tab.kind === "query" && tab.dirty && tab.sql.trim());
    if (dirty.length) {
      pendingBatchClose = { ids: tabs.map((tab) => tab.id), label };
      showModal("批量关闭查询", "UNSAVED QUERIES", `
        <p class="modal-text">${label}将关闭 ${tabs.length} 个标签页，其中 ${dirty.length} 个查询还没有保存为草稿：</p>
        <p class="modal-text muted-text">${dirty.map((tab) => escapeHtml(tab.title)).join("、")}</p>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="secondary-button danger-text" data-action="batch-close-discard">不保存，直接关闭</button><button class="primary-button" data-action="batch-close-save">保存草稿并关闭</button></div>
      `);
      return;
    }
    performBatchClose(tabs.map((tab) => tab.id), label);
  }

  function performBatchClose(ids, label) {
    const removed = new Set(ids);
    const firstIndex = sqlTabs.findIndex((tab) => removed.has(tab.id));
    for (const id of ids) {
      const index = sqlTabs.findIndex((tab) => tab.id === id);
      if (index >= 0) sqlTabs.splice(index, 1);
      window.__luxbaseEditorClose?.(id);
    }
    if (removed.has(activeSqlTabId)) {
      activeSqlTabId = null;
      const next = sqlTabs[Math.min(firstIndex, sqlTabs.length - 1)] || sqlTabs[firstIndex - 1];
      if (next) activateSqlTab(next.id);
      else showWorkspace("objects");
    }
    renderSqlTabs();
    persistSqlTabs();
    toast(label, `已关闭 ${ids.length} 个标签页。`);
  }

  async function saveTabsThenClose(ids, label) {
    const connectionIds = [...new Set(ids.map((id) => sqlTabs.find((t) => t.id === id)?.connectionId).filter(Boolean))];
    for (const id of ids) {
      const tab = sqlTabs.find((item) => item.id === id);
      if (!tab || tab.kind !== "query" || !tab.dirty || !tab.sql.trim()) continue;
      if (!tab.connectionId) {
        toast("无法保存草稿", `“${tab.title}”没有选择连接，已保留该标签页。`, "danger");
        return;
      }
      try {
        const draft = await backendCall("save_query_draft", {
          connectionId: tab.connectionId,
          name: tab.title,
          sqlText: tab.sql,
          draftId: tab.draftId ? Number(tab.draftId) : null,
        });
        tab.title = draft.name;
        tab.draftId = String(draft.id);
        tab.dirty = false;
        queryDrafts.set(String(draft.id), draft);
      } catch (error) {
        showOperationError(error, `保存“${tab.title}”失败，批量关闭已中止`);
        return;
      }
    }
    performBatchClose(ids, label);
    for (const connectionId of connectionIds) await refreshDraftUi(connectionId).catch(() => {});
  }

  function persistSqlTabs() {
    clearTimeout(persistTimer);
    persistTimer = setTimeout(() => {
      try {
        const tabs = sqlTabs.filter((tab) => tab.kind === "query").map((tab) => ({
          id: tab.id, title: tab.title, connectionId: tab.connectionId, schema: tab.schema, sql: tab.sql, dirty: tab.dirty, draftId: tab.draftId,
        }));
        localStorage.setItem(SQL_TABS_KEY, JSON.stringify({ active: activeSqlTabId, tabs }));
      } catch {
        // Storage can be unavailable in a locked-down WebView; tabs still work for the session.
      }
    }, 300);
  }

  function restoreSqlTabs() {
    let saved = null;
    try {
      saved = JSON.parse(localStorage.getItem(SQL_TABS_KEY) || "null");
    } catch {
      saved = null;
    }
    for (const item of saved?.tabs || []) {
      createSqlTab({ ...item, kind: "query", append: true });
    }
    // No default query tab on startup: the objects workspace is the landing view, and a
    // query tab is only created when the user asks for one (⌘T / 新建查询 / open table).
    const target = sqlTabs.find((tab) => tab.id === saved?.active) || sqlTabs[0];
    if (target) activateSqlTab(target.id);
  }

  window.__luxbaseEditorChanged = (id, value) => {
    const tab = sqlTabs.find((entry) => entry.id === id);
    if (!tab || tab.sql === value) return;
    tab.sql = value;
    if (!tab.dirty) {
      tab.dirty = true;
      renderSqlTabs();
      if (tab.id === activeSqlTabId) renderQueryContext(tab);
    }
    persistSqlTabs();
  };

  window.addEventListener("luxbase:editor-ready", () => {
    const tab = activeSqlTab();
    if (tab?.kind === "query") window.__luxbaseEditorOpen?.(tab.id, tab.sql, tabDialect(tab), false);
  });

  function renderBreadcrumbForTab(tab) {
    const breadcrumb = $(".breadcrumb");
    if (!breadcrumb) return;
    const connection = connectionLookup.get(tab.connectionId);
    const parts = [connection
      ? `<span class="connection-crumb">${protocolBadgeHtml(connection.protocol)}${escapeHtml(connection.name)}</span>`
      : '<span class="connection-crumb"><span class="protocol-badge unknown">?</span>未选择连接</span>'];
    if (tab.schema && connectionProtocol(tab.connectionId) !== "sqlite") parts.push(`<span>${escapeHtml(tab.schema)}</span>`);
    parts.push(`<strong>${escapeHtml(tab.kind === "table" ? tab.table : tab.title)}</strong>`);
    breadcrumb.innerHTML = parts.join('<span class="crumb-sep">/</span>');
  }

  function renderQueryContext(tab) {
    const title = $("#query-title");
    if (title) title.textContent = tab.title;
    const state = $("#query-state");
    if (state) state.textContent = tab.draftId && !tab.dirty ? "已保存为草稿" : tab.dirty ? "未保存 · 已暂存在本机" : "新查询";
    const connectionSelect = $("#query-connection");
    if (connectionSelect) {
      const options = savedConnections.filter((connection) => protocolFamily(connection.protocol) === "sql");
      connectionSelect.innerHTML = `<option value="">选择连接…</option>${options.map((connection) => `<option value="${escapeHtml(connection.id)}"${connection.id === tab.connectionId ? " selected" : ""}>${escapeHtml(connection.name)}</option>`).join("")}`;
    }
    const schemaSelect = $("#query-schema");
    if (schemaSelect) {
      const protocol = connectionProtocol(tab.connectionId);
      const cached = tab.connectionId ? metaFor(tab.connectionId).schemas : null;
      const schemas = [...new Set([...(cached || []), ...(tab.schema ? [tab.schema] : [])])];
      schemaSelect.innerHTML = `<option value="">${tab.connectionId ? (protocol === "postgres" ? "默认模式" : "默认数据库") : "—"}</option>${schemas.map((name) => `<option value="${escapeHtml(name)}"${name === tab.schema ? " selected" : ""}>${escapeHtml(name)}</option>`).join("")}`;
      schemaSelect.disabled = !tab.connectionId || protocol === "sqlite";
      schemaSelect.title = protocol === "postgres" ? "查询使用的 search_path 模式" : "查询默认使用的数据库";
      if (backendAvailable && tab.connectionId && !cached && protocol !== "sqlite") {
        loadSchemas(tab.connectionId).then(() => {
          if (activeSqlTabId === tab.id) renderQueryContext(tab);
        }).catch(() => {});
      }
    }
  }

  function renderTableToolbar(tab) {
    const filter = $("#table-filter");
    if (filter) filter.value = tab.filterRaw ?? tab.filter ?? "";
    const order = $("#table-order");
    if (order) order.value = tab.orderBy || "";
    const label = $("#table-toolbar-title");
    if (label) label.textContent = `${tab.objectType && /view/i.test(tab.objectType) ? "视图" : "表"} ${tab.table}`;
    renderFilterConditions(tab);
    updateFilterButton(tab);
  }

  const FILTER_OPERATORS = [
    { value: "=", label: "=", needsValue: true },
    { value: "!=", label: "≠", needsValue: true },
    { value: ">", label: ">", needsValue: true },
    { value: ">=", label: "≥", needsValue: true },
    { value: "<", label: "<", needsValue: true },
    { value: "<=", label: "≤", needsValue: true },
    { value: "LIKE", label: "LIKE", needsValue: true },
    { value: "NOT LIKE", label: "NOT LIKE", needsValue: true },
    { value: "IN", label: "IN（逗号分隔）", needsValue: true },
    { value: "NOT IN", label: "NOT IN（逗号分隔）", needsValue: true },
    { value: "IS NULL", label: "IS NULL", needsValue: false },
    { value: "IS NOT NULL", label: "IS NOT NULL", needsValue: false },
  ];

  function filterColumnNames(tab) {
    const columns = tab?.grid?.columns?.length ? tab.grid.columns : gridState.columns;
    return (columns || []).map((column) => column.name).filter(Boolean);
  }

  function filterConditionRowHtml(condition, columns, isFirst = false) {
    const columnOptions = columns.map((name) => `<option value="${escapeHtml(name)}"${name === condition.column ? " selected" : ""}>${escapeHtml(name)}</option>`).join("");
    const opOptions = FILTER_OPERATORS.map((op) => `<option value="${escapeHtml(op.value)}"${op.value === (condition.op || "=") ? " selected" : ""}>${escapeHtml(op.label)}</option>`).join("");
    const op = FILTER_OPERATORS.find((item) => item.value === (condition.op || "=")) || FILTER_OPERATORS[0];
    const connector = isFirst
      ? '<span class="filter-connector-first">WHERE</span>'
      : `<select class="filter-connector" title="与上一条件的连接符；OR 开启新分组"><option value="AND"${(condition.connector || "AND") === "AND" ? " selected" : ""}>AND</option><option value="OR"${condition.connector === "OR" ? " selected" : ""}>OR</option></select>`;
    return `<div class="filter-condition" data-filter-condition>
      ${connector}
      <select class="filter-column" title="列"><option value="">选择列…</option>${columnOptions}</select>
      <select class="filter-op" title="操作符">${opOptions}</select>
      <input class="filter-value" spellcheck="false" placeholder="值" value="${escapeHtml(condition.value ?? "")}" ${op.needsValue ? "" : "disabled"} />
      <button class="filter-remove" data-action="filter-remove-condition" title="移除条件">×</button>
    </div>`;
  }

  function renderFilterConditions(tab) {
    const box = $("#filter-conditions");
    if (!box) return;
    const columns = filterColumnNames(tab);
    const conditions = tab?.filterConditions?.length ? tab.filterConditions : [];
    box.innerHTML = conditions.map((condition, index) => filterConditionRowHtml(condition, columns, index === 0)).join("")
      || '<div class="filter-empty">还没有条件；点击“添加条件”或直接填写自定义 WHERE。</div>';
  }

  function collectFilterConditions() {
    return $$("[data-filter-condition]").map((row, index) => ({
      connector: index === 0 ? "AND" : ($(".filter-connector", row)?.value || "AND"),
      column: $(".filter-column", row)?.value || "",
      op: $(".filter-op", row)?.value || "=",
      value: $(".filter-value", row)?.value ?? "",
    }));
  }

  function filterLiteral(rawValue) {
    const text = String(rawValue ?? "").trim();
    if (text !== "" && Number.isFinite(Number(text))) return text;
    return `'${text.replace(/'/g, "''")}'`;
  }

  function buildFilterCondition(condition, protocol) {
    if (!condition.column) return "";
    const ident = quoteIdent(condition.column, protocol);
    const op = condition.op || "=";
    if (op === "IS NULL" || op === "IS NOT NULL") return `${ident} ${op}`;
    const raw = String(condition.value ?? "").trim();
    if (raw === "") return "";
    if (op === "IN" || op === "NOT IN") {
      const items = raw.split(",").map((item) => item.trim()).filter(Boolean).map(filterLiteral);
      if (!items.length) return "";
      return `${ident} ${op} (${items.join(", ")})`;
    }
    return `${ident} ${op} ${filterLiteral(raw)}`;
  }

  // Mixed AND/OR: an OR connector starts a new parenthesised group; conditions inside a group
  // are AND-ed. Produces e.g. (a = 1 AND b > 2) OR (c = 3), matching SQL precedence intuitively.
  function buildFilterWhere(conditions, protocol) {
    const groups = [];
    let current = [];
    for (const condition of conditions) {
      const part = buildFilterCondition(condition, protocol);
      if (!part) continue;
      if (condition.connector === "OR" && current.length) {
        groups.push(current);
        current = [];
      }
      current.push(part);
    }
    if (current.length) groups.push(current);
    return groups.map((group) => (group.length > 1 ? `(${group.join(" AND ")})` : group[0])).join(" OR ");
  }

  function updateFilterButton(tab) {
    const button = $("#table-filter-button");
    if (!button) return;
    const active = Boolean((tab?.filter || "").trim() || (tab?.orderBy || "").trim());
    button.classList.toggle("filter-active", active);
    const dot = $(".filter-active-dot", button);
    if (dot) dot.hidden = !active;
  }

  function toggleFilterPanel(force) {
    const panel = $("#table-filter-panel");
    const button = $("#table-filter-button");
    if (!panel) return;
    const open = force ?? panel.hidden;
    panel.hidden = !open;
    if (button) button.setAttribute("aria-expanded", String(open));
    if (open) {
      // The toolbar scrolls horizontally (overflow-x), which would clip an absolutely
      // positioned panel; anchor it to the button with fixed coordinates instead.
      const rect = button.getBoundingClientRect();
      const width = Math.min(520, window.innerWidth * 0.82);
      const left = Math.max(8, Math.min(rect.left, window.innerWidth - width - 8));
      const top = Math.min(rect.bottom + 6, window.innerHeight - 120);
      panel.style.left = `${left}px`;
      panel.style.top = `${top}px`;
      const tab = activeSqlTab();
      renderFilterConditions(tab);
      $(".filter-value", panel)?.focus({ preventScroll: true });
    }
  }

  async function applyTableFilter() {
    const tab = activeSqlTab();
    if (!tab || tab.kind !== "table") return;
    const protocol = connectionProtocol(tab.connectionId) || "mysql";
    const raw = $("#table-filter")?.value.trim() || "";
    const conditions = collectFilterConditions();
    tab.filterConditions = conditions.filter((condition) => condition.column);
    tab.filterRaw = raw;
    tab.orderBy = $("#table-order")?.value.trim() || "";
    if (raw) {
      tab.filter = raw;
    } else {
      tab.filter = buildFilterWhere(tab.filterConditions, protocol);
    }
    toggleFilterPanel(false);
    updateFilterButton(tab);
    await loadTableData(tab, 0);
  }

  /* ---------- execution ---------- */

  function renderExecutionState(tab) {
    const time = $(".execution-time");
    if (!time || !tab) return;
    if (tab.running) time.innerHTML = '<span class="live-dot running"></span>执行中…';
    else if (tab.grid.executed && tab.grid.durationMs != null) time.innerHTML = `<span class="live-dot"></span>${tab.grid.durationMs} ms`;
    else time.innerHTML = '<span class="live-dot idle"></span>—';
    renderSqlTabs();
  }

  function addMessage(tab, message) {
    tab.messages.unshift({ ...message, at: new Date() });
    if (tab.messages.length > 200) tab.messages.length = 200;
    if (tab.id === activeSqlTabId) renderMessages(tab);
  }

  function renderMessages(tab) {
    const panel = $('[data-result-content="messages"]');
    const badge = $('[data-result-tab="messages"] .result-count');
    if (badge) badge.textContent = String(tab?.messages.length || 0);
    if (!panel) return;
    if (!tab?.messages.length) {
      panel.innerHTML = '<div class="empty-table">执行查询后在这里显示每条语句的结果、影响行数和耗时。</div>';
      return;
    }
    panel.innerHTML = tab.messages.map((message, index) => `
      <div class="message-line${message.ok ? "" : " error"}${message.skipped ? " skipped" : ""}${message.summary ? " summary" : ""}">
        <span class="message-index">${escapeHtml(message.label || String(tab.messages.length - index))}</span>
        <div><strong>${message.summary ? "" : message.skipped ? "未执行 · " : message.ok ? "成功 · " : "失败 · "}${escapeHtml(message.text || "")}</strong>${message.sql ? `<code>${escapeHtml(summarizeSql(message.sql, 400))}</code>` : ""}</div>
        <span>${escapeHtml(message.at.toLocaleTimeString("zh-CN", { hour12: false }))}${message.durationMs != null ? ` · ${escapeHtml(message.durationMs)} ms` : ""}</span>
      </div>`).join("");
  }

  function applyResultToTab(tab, result, source = "query", extra = {}) {
    clearPendingForTab(tab.id);
    const columns = result.columns || [];
    const rows = result.rows || [];
    const grid = {
      ...emptyGrid(source),
      columns,
      rows,
      offset: result.page?.offset || 0,
      hasMore: Boolean(result.page?.hasMore),
      connectionId: tab.connectionId,
      widths: measureColumnWidths(columns, rows),
      durationMs: result.durationMs ?? null,
      executed: true,
      schema: tab.schema,
      ...extra,
    };
    tab.grid = grid;
    tab.results = [grid];
    tab.activeResult = 0;
    if (source === "query") detectQueryEditTarget(tab, grid).catch(() => {});
    if (tab.id === activeSqlTabId) {
      gridState = grid;
      if (source !== "query" || columns.length || !extra.affected) tab.resultTab = "grid";
      renderResultTabs(tab);
      renderGrid();
      setResultTab(tab.resultTab || "grid");
    }
  }

  function renderResultTabs(tab) {
    const host = $(".result-tabs");
    if (!host) return;
    const active = tab?.resultTab || "grid";
    const results = tab?.results?.length > 1 ? tab.results : null;
    const gridTabs = results
      ? results.map((grid, index) => `<button class="result-tab${active === "grid" && (tab.activeResult || 0) === index ? " active" : ""}" data-result-tab="grid" data-result-index="${index}" title="${escapeHtml(`第 ${grid.statementIndex + 1} 条语句 · ${summarizeSql(grid.sql, 200)}`)}">结果 ${index + 1} <span class="result-count">${grid.rows.length}${grid.hasMore ? "+" : ""}</span></button>`).join("")
      : `<button class="result-tab${active === "grid" ? " active" : ""}" data-result-tab="grid">结果 <span class="result-count">${tab?.grid?.rows.length || 0}</span></button>`;
    host.innerHTML = `${gridTabs}<button class="result-tab${active === "messages" ? " active" : ""}" data-result-tab="messages">消息 <span class="result-count gray">${tab?.messages.length || 0}</span></button>${tab?.kind === "table" ? "" : `<button class="result-tab${active === "plan" ? " active" : ""}" data-result-tab="plan">执行计划</button>`}`;
  }

  function activeGridTabCount() {
    const tab = activeSqlTab();
    return $(`.result-tab[data-result-tab="grid"][data-result-index="${tab?.activeResult || 0}"] .result-count`)
      || $('.result-tab[data-result-tab="grid"] .result-count');
  }

  function selectResult(tab, index) {
    if (!tab?.results?.[index]) return;
    cancelCellEditor();
    syncGridIntoTab();
    tab.activeResult = index;
    tab.grid = tab.results[index];
    tab.resultTab = "grid";
    gridState = tab.grid;
    renderGrid();
    setResultTab("grid");
  }

  function applyScriptResult(tab, statements, script) {
    const grids = [];
    const messages = [];
    const total = statements.length;
    for (const item of script.results || []) {
      const sql = statements[item.index];
      const label = `${item.index + 1}/${total}`;
      if (item.returnsRows) {
        const pageable = !script.transactional || script.committed;
        grids.push({
          ...emptyGrid("query"),
          columns: item.columns,
          rows: item.rows,
          offset: 0,
          hasMore: Boolean(item.hasMore),
          connectionId: tab.connectionId,
          widths: measureColumnWidths(item.columns, item.rows),
          durationMs: item.durationMs,
          executed: true,
          schema: tab.schema,
          sql: pageable ? sql : null,
          params: [],
          statementIndex: item.index,
        });
        messages.push({ ok: true, sql, label, text: `返回 ${item.rows.length}${item.hasMore ? "+" : ""} 行 → 结果 ${grids.length}`, durationMs: item.durationMs });
      } else {
        messages.push({ ok: true, sql, label, text: `影响 ${item.rowsAffected} 行${item.lastInsertId ? ` · 自增 ID ${item.lastInsertId}` : ""}`, durationMs: item.durationMs });
      }
    }
    if (script.failure) {
      messages.push({ ok: false, sql: statements[script.failure.index], label: `${script.failure.index + 1}/${total}`, text: script.failure.message });
      for (let index = script.failure.index + 1; index < total; index += 1) {
        messages.push({ ok: false, skipped: true, sql: statements[index], label: `${index + 1}/${total}`, text: "前一条语句失败，已停止" });
      }
    }
    const affected = (script.results || []).reduce((sum, item) => sum + Number(item.rowsAffected || 0), 0);
    if (script.transactional) {
      messages.push({ ok: Boolean(script.committed), summary: true, label: "TX", text: script.committed ? `事务已提交 · ${total} 条语句 · 共影响 ${affected} 行` : "事务已回滚，本次脚本的所有修改均未生效" , durationMs: script.durationMs });
    } else {
      messages.push({ ok: !script.failure, summary: true, label: "Σ", text: `${(script.results || []).length}/${total} 条语句已执行${script.failure && (script.results || []).some((item) => !item.returnsRows) ? "（自动提交模式，失败前的语句已生效）" : ""}`, durationMs: script.durationMs });
    }
    for (const message of messages.reverse()) addMessage(tab, message);
    if (!grids.length) {
      grids.push({ ...emptyGrid("query"), connectionId: tab.connectionId, executed: true, durationMs: script.durationMs, affected, schema: tab.schema });
    }
    tab.results = grids;
    tab.activeResult = 0;
    tab.grid = grids[0];
    tab.resultTab = script.failure || !grids[0].columns.length ? "messages" : "grid";
    if (tab.id === activeSqlTabId) {
      gridState = tab.grid;
      renderResultTabs(tab);
      renderGrid();
      renderMessages(tab);
      setResultTab(tab.resultTab);
    }
  }

  function renderQueryResult(result, source = "query") {
    const tab = activeSqlTab();
    if (tab) applyResultToTab(tab, result, source);
  }

  function sqlRequestFor(tab, sql, params, operation, page = {}) {
    return {
      connectionId: tab.connectionId,
      sql,
      params,
      operationId: operation,
      pageSize: page.pageSize ?? null,
      offset: page.offset ?? null,
      schema: tab.schema || null,
    };
  }

  async function runQuery() {
    const tab = activeSqlTab();
    if (!tab) return;
    if (tab.kind === "table") {
      await loadTableData(tab, 0);
      return;
    }
    if (tab.running) {
      toast("查询仍在执行", "请先停止当前查询或等待它完成。", "danger");
      return;
    }
    let sql = "";
    try {
      if (!backendAvailable) throw new Error("桌面后端未连接，当前窗口只能预览界面。");
      if (!tab.connectionId) throw new Error("请先在编辑器上方选择一个 SQL 连接。");
      const connection = connectionLookup.get(tab.connectionId);
      if (!connection || protocolFamily(connection.protocol) !== "sql") throw new Error("当前标签页绑定的不是 SQL 连接。");
      if (connection.id !== activeConnectionId) setActiveConnectionState(connection);
      const selected = window.__luxbaseGetSqlSelection?.() || "";
      sql = selected || window.__luxbaseGetSql?.() || tab.sql || "";
      if (!sql.trim()) throw new Error("SQL 编辑器为空。");
      const statements = splitSqlStatements(sql);
      if (!statements.length) throw new Error("没有可执行的 SQL。");
      if (statements.length > 1 && parameterCount(sql) > 0) {
        throw new Error("多语句脚本暂不支持位置参数；请逐条执行带参数的语句。");
      }
      const params = statements.length === 1 ? await parseBoundParameters(parameterCount(statements[0])) : [];
      let allowPartialCommit = false;
      if (statements.length > 1 && connection.protocol === "mysql" && statements.some((statement) => DDL_STATEMENT.test(stripLeadingComments(statement)))) {
        allowPartialCommit = await confirmChoice({
          title: "逐条执行脚本",
          eyebrow: "MYSQL IMPLICIT COMMIT",
          body: `脚本包含 DDL（CREATE / ALTER / DROP 等）。MySQL 执行 DDL 会隐式提交，无法把 ${statements.length} 条语句放进同一个事务。<br><br>继续后将<b>逐条自动提交</b>执行；如果中途某条失败，之前已执行的语句不会回滚。`,
          confirmLabel: "逐条执行",
        });
        if (!allowPartialCommit) return;
      }
      tab.running = true;
      tab.operationId = operationId();
      activeSqlOperationId = tab.operationId;
      renderExecutionState(tab);
      const op = tab.operationId;
      if (statements.length === 1 && isReadSql(statements[0])) {
        const result = await backendCall("sql_fetch", { connectionId: tab.connectionId, request: sqlRequestFor(tab, statements[0], params, op, { pageSize: 500, offset: 0 }) });
        applyResultToTab(tab, result, "query", { sql: statements[0], params });
        addMessage(tab, { ok: true, sql: statements[0], text: `返回 ${result.rows.length}${result.page?.hasMore ? "+" : ""} 行`, durationMs: result.durationMs });
      } else if (statements.length === 1) {
        const result = await backendCall("sql_execute", { connectionId: tab.connectionId, request: sqlRequestFor(tab, statements[0], params, op) });
        applyResultToTab(tab, { columns: [], rows: [], page: { offset: 0, hasMore: false }, durationMs: result.durationMs }, "query", { affected: result.rowsAffected, sql: statements[0] });
        addMessage(tab, { ok: true, sql: statements[0], text: `影响 ${result.rowsAffected} 行${result.lastInsertId ? ` · 自增 ID ${result.lastInsertId}` : ""}`, durationMs: result.durationMs });
        tab.resultTab = "messages";
        if (tab.id === activeSqlTabId) setResultTab("messages");
        if (DDL_STATEMENT.test(stripLeadingComments(statements[0]))) afterDdl(tab.connectionId);
      } else {
        const script = await backendCall("sql_run_script", {
          connectionId: tab.connectionId,
          request: {
            connectionId: tab.connectionId,
            operationId: op,
            pageSize: 500,
            allowPartialCommit,
            statements: statements.map((statement, index) => sqlRequestFor(tab, statement, [], `${op}-${index + 1}`)),
          },
        });
        applyScriptResult(tab, statements, script);
        if (statements.some((statement) => DDL_STATEMENT.test(stripLeadingComments(statement)))) afterDdl(tab.connectionId);
        if (script.failure) {
          toast(script.transactional ? "脚本失败，事务已回滚" : "脚本在中途停止", `第 ${script.failure.index + 1} 条语句：${script.failure.message}`, "danger");
        }
      }
    } catch (error) {
      if (sql) addMessage(tab, { ok: false, sql, text: errorMessage(error) });
      showOperationError(error, "查询未执行");
      if (tab.messages.length && tab.id === activeSqlTabId && !tab.grid.executed) {
        tab.resultTab = "messages";
        setResultTab("messages");
      }
    } finally {
      tab.running = false;
      tab.operationId = null;
      activeSqlOperationId = null;
      renderExecutionState(tab);
      if (tab.connectionId) refreshPoolMetrics();
      refreshAuditOnly();
    }
  }

  async function runTransactionBlock() {
    const tab = activeSqlTab();
    if (!tab || tab.kind !== "query") {
      toast("事务块执行", "请在查询标签页中编写要整体提交的语句。", "danger");
      return;
    }
    const selected = window.__luxbaseGetSqlSelection?.() || "";
    const sql = selected || window.__luxbaseGetSql?.() || "";
    const statements = splitSqlStatements(sql);
    if (!statements.length) {
      toast("事务块为空", "没有可执行的语句。", "danger");
      return;
    }
    if (statements.length === 1 && isReadSql(statements[0])) {
      await runQuery();
      return;
    }
    if (parameterCount(sql) > 0) {
      toast("事务块未提交", "事务块中的绑定参数需要逐语句输入，当前请在单语句查询中使用参数。", "danger");
      return;
    }
    tab.running = true;
    tab.operationId = operationId();
    activeSqlOperationId = tab.operationId;
    renderExecutionState(tab);
    try {
      const results = await backendCall("sql_execute_batch", {
        connectionId: tab.connectionId,
        request: {
          connectionId: tab.connectionId,
          operationId: tab.operationId,
          wrapInTransaction: true,
          statements: statements.map((statement, index) => sqlRequestFor(tab, statement, [], `${tab.operationId}-${index + 1}`)),
        },
      });
      const total = results.reduce((sum, item) => sum + Number(item.rowsAffected || 0), 0);
      results.forEach((item, index) => addMessage(tab, { ok: true, sql: statements[index], text: `影响 ${item.rowsAffected} 行`, durationMs: item.durationMs }));
      tab.resultTab = "messages";
      setResultTab("messages");
      toast("事务块已提交", `${results.length} 条语句全部成功 · 影响 ${total} 行。`);
    } catch (error) {
      addMessage(tab, { ok: false, sql, text: `${errorMessage(error)}（整个事务已回滚）` });
      showOperationError(error, "事务块未提交");
    } finally {
      tab.running = false;
      tab.operationId = null;
      activeSqlOperationId = null;
      renderExecutionState(tab);
      refreshAuditOnly();
    }
  }

  async function loadGridPage(direction) {
    const tab = activeSqlTab();
    if (!tab || !gridState.executed) return;
    const pageSize = tab.pageSize || 500;
    const nextOffset = direction === "next" ? gridState.offset + pageSize : Math.max(0, gridState.offset - pageSize);
    if (tab.kind === "table") {
      await loadTableData(tab, nextOffset);
      return;
    }
    if (!gridState.sql) {
      toast("无法翻页", "当前结果不是单条查询的结果集。", "danger");
      return;
    }
    tab.running = true;
    tab.operationId = operationId();
    activeSqlOperationId = tab.operationId;
    renderExecutionState(tab);
    try {
      const result = await backendCall("sql_fetch", { connectionId: tab.connectionId, request: sqlRequestFor(tab, gridState.sql, gridState.params || [], tab.operationId, { pageSize, offset: nextOffset }) });
      applyResultToTab(tab, result, "query", { sql: gridState.sql, params: gridState.params || [] });
    } catch (error) {
      showOperationError(error, "结果分页失败");
    } finally {
      tab.running = false;
      tab.operationId = null;
      activeSqlOperationId = null;
      renderExecutionState(tab);
    }
  }

  async function loadTablePrimaryKeys(tab) {
    try {
      const meta = await loadTableMeta(tab.connectionId, tab.schema, tab.table);
      tab.primaryKeys = (meta.columns || []).filter((column) => column.isPrimaryKey).map((column) => column.name);
      if (tab.id === activeSqlTabId) activePrimaryKey = tab.primaryKeys.length === 1 ? tab.primaryKeys[0] : null;
    } catch {
      tab.primaryKeys = [];
    }
  }

  async function loadTableData(tab, offset = 0) {
    if (tab.running) return;
    clearPendingForTab(tab.id);
    tab.running = true;
    tab.operationId = operationId();
    activeSqlOperationId = tab.operationId;
    renderExecutionState(tab);
    try {
      const result = await backendCall("sql_table_preview", {
        connectionId: tab.connectionId,
        schema: tab.schema || "",
        table: tab.table,
        limit: tab.pageSize,
        offset,
        operationId: tab.operationId,
        filter: tab.filter || null,
        orderBy: tab.orderBy || null,
      });
      applyResultToTab(tab, result, "table");
      addMessage(tab, { ok: true, sql: `SELECT * FROM ${tab.table}${tab.filter ? ` WHERE ${tab.filter}` : ""}${tab.orderBy ? ` ORDER BY ${tab.orderBy}` : ""}`, text: `读取 ${result.rows.length}${result.page?.hasMore ? "+" : ""} 行`, durationMs: result.durationMs });
    } catch (error) {
      addMessage(tab, { ok: false, sql: `SELECT * FROM ${tab.table}`, text: errorMessage(error) });
      showOperationError(error, "表数据读取失败");
    } finally {
      tab.running = false;
      tab.operationId = null;
      activeSqlOperationId = null;
      renderExecutionState(tab);
    }
  }

  async function openTableTab(connectionId, schema, table, objectType = "table") {
    const connection = connectionLookup.get(connectionId);
    if (!connection) {
      toast("连接不存在", "请刷新连接列表后重试。", "danger");
      return;
    }
    if (connection.id !== activeConnectionId) setActiveConnectionState(connection);
    let tab = sqlTabs.find((item) => item.kind === "table" && item.connectionId === connectionId && item.schema === schema && item.table === table);
    const fresh = !tab;
    if (!tab) tab = createSqlTab({ kind: "table", title: table, connectionId, schema, table, objectType });
    activateSqlTab(tab.id);
    if (fresh) {
      await Promise.all([loadTablePrimaryKeys(tab), loadTableData(tab, 0)]);
      if (tab.id === activeSqlTabId) activePrimaryKey = tab.primaryKeys.length === 1 ? tab.primaryKeys[0] : null;
    }
  }

  /* ---------- result grid ---------- */

  function base64ToHex(base64) {
    try {
      const binary = atob(base64);
      let hex = "";
      for (let index = 0; index < binary.length; index += 1) hex += binary.charCodeAt(index).toString(16).padStart(2, "0");
      return hex.toUpperCase();
    } catch {
      return "";
    }
  }

  function cellText(value) {
    if (value === null || value === undefined) return "";
    if (typeof value === "object") {
      if ("bytesBase64" in value) return `0x${base64ToHex(value.bytesBase64)}`;
      if ("unsupportedCellType" in value) return `(${value.unsupportedCellType})`;
      return JSON.stringify(value);
    }
    return String(value);
  }

  function formatCell(cell) {
    if (cell === null || cell === undefined) return '<span class="cell-null">NULL</span>';
    if (typeof cell === "object") {
      if ("bytesBase64" in cell) {
        const bytes = Math.floor((cell.bytesBase64.length * 3) / 4) - (cell.bytesBase64.match(/=+$/)?.[0].length || 0);
        return `<span class="cell-binary">(BLOB) ${bytes} 字节</span>`;
      }
      if ("unsupportedCellType" in cell) return `<span class="cell-binary">(${escapeHtml(cell.unsupportedCellType)})</span>`;
      return escapeHtml(JSON.stringify(cell));
    }
    const text = String(cell);
    return escapeHtml(text.length > 400 ? `${text.slice(0, 400)}…` : text);
  }

  function isNumericColumn(column, sample) {
    return NUMERIC_TYPE.test(column?.typeName || "") || typeof sample === "number";
  }

  function measureColumnWidths(columns, rows) {
    if (!columns.length) return [];
    const canvas = measureColumnWidths.canvas || (measureColumnWidths.canvas = document.createElement("canvas"));
    const context = canvas.getContext("2d");
    const fontFamily = getComputedStyle(document.body).fontFamily || "sans-serif";
    return columns.map((column, columnIndex) => {
      context.font = `600 12px ${fontFamily}`;
      let width = context.measureText(column.name).width + 30;
      context.font = `11px ${fontFamily}`;
      width = Math.max(width, context.measureText(column.typeName || "").width + 24);
      context.font = `12px ${fontFamily}`;
      for (let rowIndex = 0; rowIndex < Math.min(rows.length, 120); rowIndex += 1) {
        const text = cellText(rows[rowIndex]?.[columnIndex]).slice(0, 60) || "NULL";
        width = Math.max(width, context.measureText(text).width + 22);
      }
      return Math.round(Math.min(Math.max(width, 70), 360));
    });
  }

  function normalizedSelection() {
    const selection = gridState.selection;
    const rowCount = gridState.rows.length;
    const columnCount = gridState.columns.length;
    if (!selection || !rowCount || !columnCount) return null;
    if (selection.mode === "all") return { r0: 0, r1: rowCount - 1, c0: 0, c1: columnCount - 1 };
    if (selection.mode === "rows") return { r0: Math.min(selection.r1, selection.r2), r1: Math.max(selection.r1, selection.r2), c0: 0, c1: columnCount - 1 };
    if (selection.mode === "columns") return { r0: 0, r1: rowCount - 1, c0: Math.min(selection.c1, selection.c2), c1: Math.max(selection.c1, selection.c2) };
    return {
      r0: Math.max(0, Math.min(selection.r1, selection.r2)),
      r1: Math.min(rowCount - 1, Math.max(selection.r1, selection.r2)),
      c0: Math.max(0, Math.min(selection.c1, selection.c2)),
      c1: Math.min(columnCount - 1, Math.max(selection.c1, selection.c2)),
    };
  }

  function selectedRowIndexes() {
    const selection = normalizedSelection();
    if (!selection) return [];
    return Array.from({ length: selection.r1 - selection.r0 + 1 }, (_, index) => selection.r0 + index);
  }

  function emptyGridMessage() {
    const tab = activeSqlTab();
    if (gridState.executed && gridState.affected != null) return `执行成功，影响 ${escapeHtml(gridState.affected)} 行。详情见“消息”。`;
    if (gridState.executed) return "语句已执行，没有返回结果集。";
    if (tab?.running) return "正在执行…";
    if (tab?.kind === "table") return "正在读取表数据…";
    return "编写 SQL 后按 ⌘↵ 执行；结果会显示在这里。";
  }

  function renderGrid() {
    const wrap = $(".grid-wrap");
    const table = $(".data-grid");
    if (!wrap || !table) return;
    const { columns, rows } = gridState;
    if (gridState.widths.length !== columns.length) gridState.widths = measureColumnWidths(columns, rows);
    let colgroup = $("colgroup", table);
    if (!colgroup) {
      colgroup = document.createElement("colgroup");
      table.prepend(colgroup);
    }
    colgroup.innerHTML = `<col style="width:${ROWNUM_WIDTH}px">${gridState.widths.map((width) => `<col style="width:${width}px">`).join("")}`;
    table.style.width = columns.length ? `${ROWNUM_WIDTH + gridState.widths.reduce((sum, width) => sum + width, 0)}px` : "100%";
    table.classList.toggle("is-empty", !columns.length);
    const thead = $("thead", table);
    if (thead) {
      thead.innerHTML = columns.length
        ? `<tr><th class="row-number corner" data-grid-corner title="全选">#</th>${columns.map((column, index) => `<th data-c="${index}" class="${isNumericColumn(column, rows[0]?.[index]) ? "numeric" : ""}" title="${escapeHtml(column.name)}${column.typeName ? ` · ${escapeHtml(column.typeName)}` : ""}"><div class="th-inner"><span class="th-name">${escapeHtml(column.name)}</span><span class="col-type">${escapeHtml(column.typeName || "")}</span></div><span class="col-resizer" data-col-resize="${index}" title="拖动调整列宽，双击自适应"></span></th>`).join("")}</tr>`
        : '<tr><th class="row-number corner"></th><th>结果</th></tr>';
    }
    wrap.scrollTop = gridState.scrollTop || 0;
    wrap.scrollLeft = gridState.scrollLeft || 0;
    renderGridBody();
    renderGridFooter();
    const count = activeGridTabCount();
    if (count) count.textContent = `${rows.length}${gridState.hasMore ? "+" : ""}`;
  }

  function renderGridBody() {
    const wrap = $(".grid-wrap");
    const tbody = $(".data-grid tbody");
    if (!wrap || !tbody) return;
    const { columns, rows } = gridState;
    const colspan = columns.length + 1;
    if (!columns.length) {
      tbody.innerHTML = `<tr><td colspan="${Math.max(2, colspan)}" class="empty-table">${emptyGridMessage()}</td></tr>`;
      return;
    }
    if (!rows.length) {
      tbody.innerHTML = `<tr><td class="row-number"></td><td colspan="${columns.length}" class="empty-table">没有返回行。</td></tr>`;
      return;
    }
    const viewport = Math.ceil((wrap.clientHeight || 600) / GRID_ROW_HEIGHT);
    const start = Math.max(0, Math.floor(wrap.scrollTop / GRID_ROW_HEIGHT) - 10);
    const end = Math.min(rows.length, start + viewport + 24);
    const selection = normalizedSelection();
    const numeric = columns.map((column, index) => isNumericColumn(column, rows[0]?.[index]));
    const focus = gridState.focus;
    let html = start ? `<tr class="virtual-spacer" style="height:${start * GRID_ROW_HEIGHT}px"><td colspan="${colspan}"></td></tr>` : "";
    for (let rowIndex = start; rowIndex < end; rowIndex += 1) {
      const row = rows[rowIndex] || [];
      const rowSelected = selection && rowIndex >= selection.r0 && rowIndex <= selection.r1;
      html += `<tr data-r="${rowIndex}"${rowSelected ? ' class="row-in-selection"' : ""}><td class="row-number" data-row-head="${rowIndex}">${gridState.offset + rowIndex + 1}</td>`;
      for (let columnIndex = 0; columnIndex < columns.length; columnIndex += 1) {
        const value = row[columnIndex];
        let classes = numeric[columnIndex] ? "numeric" : "";
        if (rowSelected && columnIndex >= selection.c0 && columnIndex <= selection.c1) classes += " sel";
        if (focus && focus.r === rowIndex && focus.c === columnIndex) classes += " focus";
        if (pendingCellEdits.has(`${activeSqlTabId}|${rowIndex}|${columnIndex}`)) classes += " cell-pending";
        html += `<td data-c="${columnIndex}"${classes ? ` class="${classes.trim()}"` : ""}>${formatCell(value)}</td>`;
      }
      html += "</tr>";
    }
    if (end < rows.length) html += `<tr class="virtual-spacer" style="height:${(rows.length - end) * GRID_ROW_HEIGHT}px"><td colspan="${colspan}"></td></tr>`;
    tbody.innerHTML = html;
    $$(".data-grid thead th[data-c]").forEach((th) => {
      const index = Number(th.dataset.c);
      th.classList.toggle("col-in-selection", Boolean(selection && index >= selection.c0 && index <= selection.c1));
    });
  }

  function renderGridFooter() {
    const tab = activeSqlTab();
    updatePendingBar();
    const footer = $(".grid-footer");
    if (!footer) return;
    const { rows, offset, hasMore, executed, durationMs } = gridState;
    const pageSize = tab?.pageSize || 500;
    const selection = normalizedSelection();
    const selectionText = selection ? ` · 已选 ${selection.r1 - selection.r0 + 1} 行 × ${selection.c1 - selection.c0 + 1} 列` : "";
    const status = executed
      ? `<span class="stream-icon"></span>${rows.length ? `第 ${offset + 1}–${offset + rows.length} 行${hasMore ? "（还有更多）" : ""}` : "0 行"}${durationMs != null ? ` · ${durationMs} ms` : ""}${selectionText}`
      : '<span class="stream-icon idle"></span>尚未执行';
    const sizes = tab?.kind === "table" ? [100, 500, 1000, 5000] : [500];
    footer.innerHTML = `<span class="grid-status">${status}</span><span class="grid-pager"><button data-grid-page="prev" ${!executed || offset === 0 ? "disabled" : ""} title="上一页">‹</button><b>${Math.floor(offset / pageSize) + 1}</b><button data-grid-page="next" ${hasMore ? "" : "disabled"} title="下一页">›</button><span>每页</span>${sizes.length > 1 ? `<select data-page-size aria-label="每页行数">${sizes.map((size) => `<option value="${size}"${size === pageSize ? " selected" : ""}>${size}</option>`).join("")}</select>` : `<span>${pageSize}</span>`}<span>行</span></span>`;
  }

  function refreshSelection() {
    renderGridBody();
    renderGridFooter();
  }

  function cellAt(target) {
    const td = target?.closest?.(".data-grid tbody td[data-c]");
    if (!td) return null;
    const row = td.parentElement;
    return { r: Number(row.dataset.r), c: Number(td.dataset.c), td };
  }

  function ensureCellVisible(rowIndex, columnIndex) {
    const wrap = $(".grid-wrap");
    if (!wrap) return;
    const header = $(".data-grid thead")?.offsetHeight || 36;
    const top = rowIndex * GRID_ROW_HEIGHT;
    if (top < wrap.scrollTop) wrap.scrollTop = top;
    else if (top + GRID_ROW_HEIGHT > wrap.scrollTop + wrap.clientHeight - header) wrap.scrollTop = top + GRID_ROW_HEIGHT - wrap.clientHeight + header;
    const left = ROWNUM_WIDTH + gridState.widths.slice(0, columnIndex).reduce((sum, width) => sum + width, 0);
    const right = left + (gridState.widths[columnIndex] || 0);
    if (left < wrap.scrollLeft + ROWNUM_WIDTH) wrap.scrollLeft = left - ROWNUM_WIDTH;
    else if (right > wrap.scrollLeft + wrap.clientWidth) wrap.scrollLeft = right - wrap.clientWidth;
  }

  function onGridMouseDown(event) {
    if (event.button !== 0 || event.target.closest(".cell-editor")) return;
    const wrap = $(".grid-wrap");
    const resizer = event.target.closest("[data-col-resize]");
    if (resizer) {
      const index = Number(resizer.dataset.colResize);
      columnResize = { index, startX: event.clientX, startWidth: gridState.widths[index] || 120 };
      document.body.classList.add("col-resizing");
      event.preventDefault();
      return;
    }
    if (cellEditor) commitCellEditor(true);
    if (!gridState.columns.length) return;
    if (event.target.closest("[data-grid-corner]")) {
      gridState.selection = { mode: "all" };
      refreshSelection();
      wrap?.focus({ preventScroll: true });
      event.preventDefault();
      return;
    }
    const header = event.target.closest(".data-grid thead th[data-c]");
    if (header) {
      const index = Number(header.dataset.c);
      const anchor = event.shiftKey && gridState.selection?.mode === "columns" ? gridState.selection.c1 : index;
      gridState.selection = { mode: "columns", c1: anchor, c2: index };
      gridDrag = { mode: "columns" };
      refreshSelection();
      wrap?.focus({ preventScroll: true });
      event.preventDefault();
      return;
    }
    const rowHead = event.target.closest("[data-row-head]");
    if (rowHead) {
      const index = Number(rowHead.dataset.rowHead);
      const anchor = event.shiftKey && gridState.selection?.mode === "rows" ? gridState.selection.r1 : index;
      gridState.selection = { mode: "rows", r1: anchor, r2: index };
      gridState.focus = { r: index, c: gridState.focus?.c ?? 0 };
      gridDrag = { mode: "rows" };
      refreshSelection();
      wrap?.focus({ preventScroll: true });
      event.preventDefault();
      return;
    }
    const cell = cellAt(event.target);
    if (!cell) return;
    if (event.detail >= 2) {
      // The first click rebuilds tbody for selection, which detaches the second click's
      // target and can suppress the browser's dblclick event. Enter editing straight from
      // the second mousedown instead of relying on dblclick delivery.
      gridDrag = null;
      lastCellDouble = { r: cell.r, c: cell.c, at: Date.now() };
      gridState.focus = { r: cell.r, c: cell.c };
      gridState.selection = { mode: "cells", r1: cell.r, c1: cell.c, r2: cell.r, c2: cell.c };
      event.preventDefault();
      beginCellEdit(cell.r, cell.c);
      return;
    }
    if (event.shiftKey && gridState.focus) {
      const anchor = gridState.selection?.mode === "cells" ? { r: gridState.selection.r1, c: gridState.selection.c1 } : gridState.focus;
      gridState.selection = { mode: "cells", r1: anchor.r, c1: anchor.c, r2: cell.r, c2: cell.c };
    } else {
      gridState.selection = { mode: "cells", r1: cell.r, c1: cell.c, r2: cell.r, c2: cell.c };
    }
    gridState.focus = { r: cell.r, c: cell.c };
    gridDrag = { mode: "cells" };
    refreshSelection();
    wrap?.focus({ preventScroll: true });
    event.preventDefault();
  }

  function onDocumentMouseMove(event) {
    if (columnResize) {
      const width = Math.max(48, Math.round(columnResize.startWidth + event.clientX - columnResize.startX));
      gridState.widths[columnResize.index] = width;
      const col = $$(".data-grid colgroup col")[columnResize.index + 1];
      if (col) col.style.width = `${width}px`;
      const table = $(".data-grid");
      if (table) table.style.width = `${ROWNUM_WIDTH + gridState.widths.reduce((sum, value) => sum + value, 0)}px`;
      return;
    }
    if (panelResize) {
      onPanelResize(event);
      return;
    }
    if (!gridDrag || !gridState.selection) return;
    const target = document.elementFromPoint(event.clientX, event.clientY);
    const selection = gridState.selection;
    if (gridDrag.mode === "cells") {
      const cell = cellAt(target);
      if (cell && (selection.r2 !== cell.r || selection.c2 !== cell.c)) {
        selection.r2 = cell.r;
        selection.c2 = cell.c;
        gridState.focus = { r: cell.r, c: cell.c };
        refreshSelection();
      }
    } else if (gridDrag.mode === "rows") {
      const head = target?.closest?.("[data-row-head]");
      if (head && selection.r2 !== Number(head.dataset.rowHead)) {
        selection.r2 = Number(head.dataset.rowHead);
        refreshSelection();
      }
    } else if (gridDrag.mode === "columns") {
      const header = target?.closest?.(".data-grid thead th[data-c]");
      if (header && selection.c2 !== Number(header.dataset.c)) {
        selection.c2 = Number(header.dataset.c);
        refreshSelection();
      }
    }
  }

  function onDocumentMouseUp() {
    if (columnResize) document.body.classList.remove("col-resizing");
    if (panelResize) {
      document.body.classList.remove("panel-resizing");
      saveLayout();
    }
    gridDrag = null;
    columnResize = null;
    panelResize = null;
  }

  function autoFitColumn(index) {
    const column = gridState.columns[index];
    if (!column) return;
    const [width] = measureColumnWidths([column], gridState.rows.slice(0, 2000).map((row) => [row[index]]));
    gridState.widths[index] = Math.min(Math.max(width, 70), 720);
    renderGrid();
  }

  function onGridDoubleClick(event) {
    const resizer = event.target.closest("[data-col-resize]");
    if (resizer) {
      autoFitColumn(Number(resizer.dataset.colResize));
      return;
    }
    const cell = cellAt(event.target);
    if (cell) {
      if (cellEditor && cellEditor.r === cell.r && cellEditor.c === cell.c) return;
      if (lastCellDouble && lastCellDouble.r === cell.r && lastCellDouble.c === cell.c && Date.now() - lastCellDouble.at < 500) return;
      beginCellEdit(cell.r, cell.c);
    }
  }

  function onGridKeyDown(event) {
    if (event.target.closest(".cell-editor") || !gridState.columns.length) return;
    const modifier = event.metaKey || event.ctrlKey;
    const key = event.key.toLowerCase();
    if (modifier && key === "c") {
      event.preventDefault();
      event.stopPropagation();
      copyGridSelection(event.shiftKey ? "tsv-header" : "tsv");
      return;
    }
    if (modifier && key === "a") {
      event.preventDefault();
      event.stopPropagation();
      gridState.selection = { mode: "all" };
      refreshSelection();
      return;
    }
    const pageRows = Math.max(1, Math.floor(($(".grid-wrap")?.clientHeight || 300) / GRID_ROW_HEIGHT) - 2);
    const moves = { ArrowUp: [-1, 0], ArrowDown: [1, 0], ArrowLeft: [0, -1], ArrowRight: [0, 1], PageUp: [-pageRows, 0], PageDown: [pageRows, 0], Tab: [0, event.shiftKey ? -1 : 1] };
    if (moves[event.key]) {
      event.preventDefault();
      const focus = gridState.focus || { r: 0, c: 0 };
      const [dr, dc] = moves[event.key];
      const next = {
        r: Math.min(gridState.rows.length - 1, Math.max(0, focus.r + (modifier ? dr * gridState.rows.length : dr))),
        c: Math.min(gridState.columns.length - 1, Math.max(0, focus.c + (modifier ? dc * gridState.columns.length : dc))),
      };
      if (event.shiftKey && event.key !== "Tab") {
        const anchor = gridState.selection?.mode === "cells" ? { r: gridState.selection.r1, c: gridState.selection.c1 } : focus;
        gridState.selection = { mode: "cells", r1: anchor.r, c1: anchor.c, r2: next.r, c2: next.c };
      } else {
        gridState.selection = { mode: "cells", r1: next.r, c1: next.c, r2: next.r, c2: next.c };
      }
      gridState.focus = next;
      ensureCellVisible(next.r, next.c);
      refreshSelection();
      return;
    }
    if ((event.key === "Enter" || event.key === "F2") && gridState.focus) {
      event.preventDefault();
      beginCellEdit(gridState.focus.r, gridState.focus.c);
      return;
    }
    if (event.key === "Escape") {
      gridState.selection = null;
      refreshSelection();
    }
  }

  function onGridContextMenu(event) {
    const cell = cellAt(event.target);
    const rowHead = event.target.closest("[data-row-head]");
    const header = event.target.closest(".data-grid thead th[data-c]");
    if (!cell && !rowHead && !header) return;
    event.preventDefault();
    event.stopPropagation();
    const selection = normalizedSelection();
    if (cell && !(selection && cell.r >= selection.r0 && cell.r <= selection.r1 && cell.c >= selection.c0 && cell.c <= selection.c1)) {
      gridState.selection = { mode: "cells", r1: cell.r, c1: cell.c, r2: cell.r, c2: cell.c };
      gridState.focus = { r: cell.r, c: cell.c };
    } else if (rowHead && !(selection && Number(rowHead.dataset.rowHead) >= selection.r0 && Number(rowHead.dataset.rowHead) <= selection.r1)) {
      gridState.selection = { mode: "rows", r1: Number(rowHead.dataset.rowHead), r2: Number(rowHead.dataset.rowHead) };
    } else if (header && !(selection && Number(header.dataset.c) >= selection.c0 && Number(header.dataset.c) <= selection.c1)) {
      gridState.selection = { mode: "columns", c1: Number(header.dataset.c), c2: Number(header.dataset.c) };
    }
    refreshSelection();
    const tab = activeSqlTab();
    const editable = Boolean(editTargetFor(tab)?.primaryKeys?.length);
    contextMenu.innerHTML = `
      ${cell ? '<button data-action="cell-details"><svg><use href="#i-info"/></svg>查看完整内容</button>' : ""}
      <button data-grid-copy="tsv"><svg><use href="#i-copy"/></svg>复制<kbd>⌘C</kbd></button>
      <button data-grid-copy="tsv-header"><svg><use href="#i-copy"/></svg>复制（含列名）<kbd>⇧⌘C</kbd></button>
      <span class="context-sep"></span>
      <span class="context-label">复制为</span>
      <button data-grid-copy="csv">CSV</button>
      <button data-grid-copy="json">JSON</button>
      <button data-grid-copy="markdown">Markdown 表格</button>
      <button data-grid-copy="insert">INSERT 语句</button>
      ${tab?.kind === "table" && tab.primaryKeys.length ? '<button data-grid-copy="update">UPDATE 语句</button>' : ""}
      <button data-grid-copy="in-list">逗号分隔值（IN 列表）</button>
      <button data-grid-copy="columns">列名</button>
      ${editable ? '<span class="context-sep"></span><button data-grid-edit><svg><use href="#i-settings"/></svg>编辑单元格<kbd>↵</kbd></button><button data-grid-set-null>设置为 NULL</button><button class="danger-item" data-action="batch-delete"><svg><use href="#i-warning"/></svg>删除所选行</button>' : ""}
      <span class="context-sep"></span>
      <button data-action="export-query-result"><svg><use href="#i-export"/></svg>导出结果…</button>
      <button data-grid-select-all>全选<kbd>⌘A</kbd></button>
      ${tab?.kind === "table" && !/view/i.test(tab.objectType) ? '<button data-action="insert-row">新增行</button><button data-action="duplicate-row">复制所选行</button>' : ""}
      ${tab?.kind === "table" ? '<button data-action="table-refresh"><svg><use href="#i-refresh"/></svg>刷新</button>' : ""}`;
    contextMenu.hidden = false;
    const rect = contextMenu.getBoundingClientRect();
    contextMenu.style.left = `${Math.min(event.clientX, window.innerWidth - rect.width - 8)}px`;
    contextMenu.style.top = `${Math.min(event.clientY, window.innerHeight - rect.height - 8)}px`;
  }

  /* ---------- copy formats ---------- */

  function selectionMatrix() {
    const selection = normalizedSelection() || (gridState.rows.length && gridState.columns.length
      ? { r0: 0, r1: gridState.rows.length - 1, c0: 0, c1: gridState.columns.length - 1 }
      : null);
    if (!selection) return null;
    return {
      columns: gridState.columns.slice(selection.c0, selection.c1 + 1),
      rows: gridState.rows.slice(selection.r0, selection.r1 + 1).map((row) => row.slice(selection.c0, selection.c1 + 1)),
      rowIndexes: Array.from({ length: selection.r1 - selection.r0 + 1 }, (_, index) => selection.r0 + index),
    };
  }

  function tsvField(value) {
    const text = cellText(value);
    return /[\t\n\r"]/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
  }

  function csvField(value) {
    if (value === null || value === undefined) return "";
    const text = cellText(value);
    return /[",\n\r]/.test(text) || /^\s|\s$/.test(text) ? `"${text.replace(/"/g, "\"\"")}"` : text;
  }

  function jsonValue(value) {
    if (value && typeof value === "object" && "bytesBase64" in value) return `0x${base64ToHex(value.bytesBase64)}`;
    return value === undefined ? null : value;
  }

  function sqlLiteral(value, protocol) {
    if (value === null || value === undefined) return "NULL";
    if (typeof value === "number") return Number.isFinite(value) ? String(value) : "NULL";
    if (typeof value === "boolean") return protocol === "postgres" ? (value ? "TRUE" : "FALSE") : (value ? "1" : "0");
    if (typeof value === "object" && "bytesBase64" in value) {
      const hex = base64ToHex(value.bytesBase64);
      return protocol === "postgres" ? `'\\x${hex}'::bytea` : `X'${hex}'`;
    }
    let text = typeof value === "object" ? JSON.stringify(value) : String(value);
    if (protocol === "mysql") text = text.replace(/\\/g, "\\\\");
    return `'${text.replace(/'/g, "''")}'`;
  }

  function htmlTable(names, rows, withHeader) {
    const head = withHeader ? `<thead><tr>${names.map((name) => `<th>${escapeHtml(name)}</th>`).join("")}</tr></thead>` : "";
    const body = rows.map((row) => `<tr>${row.map((value) => `<td>${escapeHtml(cellText(value))}</td>`).join("")}</tr>`).join("");
    return `<table>${head}<tbody>${body}</tbody></table>`;
  }

  function copyTargetTable(protocol) {
    const tab = activeSqlTab();
    if (tab?.kind === "table") return protocol === "sqlite" ? quoteIdent(tab.table, protocol) : `${quoteIdent(tab.schema, protocol)}.${quoteIdent(tab.table, protocol)}`;
    const from = gridState.sql ? tableReferences(gridState.sql)[0] : null;
    if (from) return from.schema ? `${quoteIdent(from.schema, protocol)}.${quoteIdent(from.table, protocol)}` : quoteIdent(from.table, protocol);
    return quoteIdent("table_name", protocol);
  }

  function formatSelection(format) {
    const matrix = selectionMatrix();
    if (!matrix) return null;
    const { columns, rows, rowIndexes } = matrix;
    const names = columns.map((column) => column.name);
    const protocol = connectionProtocol(gridState.connectionId) || activeProtocol;
    switch (format) {
      case "tsv":
        return { text: rows.map((row) => row.map(tsvField).join("\t")).join("\n"), html: htmlTable(names, rows, false), label: "制表符分隔文本" };
      case "tsv-header":
        return { text: [names.join("\t"), ...rows.map((row) => row.map(tsvField).join("\t"))].join("\n"), html: htmlTable(names, rows, true), label: "带列名的表格" };
      case "csv":
        return { text: [names.map(csvField).join(","), ...rows.map((row) => row.map(csvField).join(","))].join("\n"), label: "CSV" };
      case "json":
        return { text: JSON.stringify(rows.map((row) => Object.fromEntries(names.map((name, index) => [name, jsonValue(row[index])]))), null, 2), label: "JSON" };
      case "markdown": {
        const escape = (text) => text.replace(/\|/g, "\\|").replace(/\n/g, " ");
        return {
          text: [`| ${names.map(escape).join(" | ")} |`, `| ${names.map(() => "---").join(" | ")} |`, ...rows.map((row) => `| ${row.map((value) => escape(value === null || value === undefined ? "NULL" : cellText(value))).join(" | ")} |`)].join("\n"),
          label: "Markdown 表格",
        };
      }
      case "insert": {
        const table = copyTargetTable(protocol);
        const columnList = names.map((name) => quoteIdent(name, protocol)).join(", ");
        return { text: rows.map((row) => `INSERT INTO ${table} (${columnList}) VALUES (${row.map((value) => sqlLiteral(value, protocol)).join(", ")});`).join("\n"), label: "INSERT 语句" };
      }
      case "update": {
        const tab = activeSqlTab();
        const keys = tab?.primaryKeys || [];
        const keyIndexes = keys.map((key) => gridState.columns.findIndex((column) => column.name === key));
        if (!keys.length || keyIndexes.some((index) => index < 0)) return null;
        const table = copyTargetTable(protocol);
        const text = rowIndexes.map((rowIndex, index) => {
          const fullRow = gridState.rows[rowIndex];
          const editable = names.map((name, column) => ({ name, column })).filter((item) => !keys.includes(item.name));
          const targets = editable.length ? editable : names.map((name, column) => ({ name, column }));
          const assignments = targets.map((item) => `${quoteIdent(item.name, protocol)} = ${sqlLiteral(rows[index][item.column], protocol)}`).join(", ");
          const where = keys.map((key, keyPosition) => `${quoteIdent(key, protocol)} = ${sqlLiteral(fullRow[keyIndexes[keyPosition]], protocol)}`).join(" AND ");
          return `UPDATE ${table} SET ${assignments} WHERE ${where};`;
        }).join("\n");
        return { text, label: "UPDATE 语句" };
      }
      case "in-list":
        return { text: rows.flat().map((value) => sqlLiteral(value, protocol)).join(", "), label: "逗号分隔值" };
      case "columns":
        return { text: names.join(", "), label: "列名" };
      default:
        return null;
    }
  }

  function textareaCopy(text) {
    const textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.setAttribute("readonly", "");
    textarea.style.position = "fixed";
    textarea.style.opacity = "0";
    document.body.appendChild(textarea);
    textarea.select();
    let ok = false;
    try {
      ok = document.execCommand("copy");
    } catch {
      ok = false;
    }
    textarea.remove();
    return ok;
  }

  async function copyToClipboard({ text, html = null }) {
    pendingClipboard = { text, html, done: false };
    try {
      document.execCommand("copy");
    } catch {
      // Fall through to the async clipboard API below.
    }
    const handled = pendingClipboard?.done;
    pendingClipboard = null;
    if (handled) return true;
    if (navigator.clipboard?.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        return true;
      } catch {
        // Older WebKit builds reject the async API outside secure contexts.
      }
    }
    return textareaCopy(text);
  }

  async function copyGridSelection(format) {
    const payload = formatSelection(format);
    if (!payload) {
      toast("没有可复制的内容", format === "update" ? "UPDATE 语句需要表的主键列出现在结果中。" : "请先执行查询或打开表。", "danger");
      return;
    }
    const ok = await copyToClipboard(payload);
    const lines = payload.text ? payload.text.split("\n").length : 0;
    if (ok) toast("已复制", `${payload.label} · ${lines} 行`);
    else toast("复制失败", "系统剪贴板不可用。", "danger");
  }

  function textCopyPayload(text) {
    return { text, html: null };
  }

  /* ---------- in-place cell editing ---------- */

  function editTargetFor(tab) {
    if (!tab) return null;
    if (tab.kind === "table") return { table: tab.table, schema: tab.schema, primaryKeys: tab.primaryKeys || [] };
    return tab.queryEdit || null;
  }

  async function detectQueryEditTarget(tab, grid) {
    tab.queryEdit = null;
    if (tab.kind !== "query" || !grid?.sql || !grid.columns?.length) return;
    const sql = stripLeadingComments(grid.sql).trim().replace(/;+\s*$/, "");
    if (!/^select\b/i.test(sql) || /\bjoin\b|\bunion\b/i.test(sql)) return;
    const match = sql.match(/\bfrom\s+(`[^`]+`|"[^"]+"|\[[^\]]+\]|[A-Za-z0-9_$]+)(?:\s*\.\s*(`[^`]+`|"[^"]+"|\[[^\]]+\]|[A-Za-z0-9_$]+))?/i);
    if (!match) return;
    if (/^\s*,/.test(sql.slice(match.index + match[0].length))) return;
    const unquote = (s) => s.replace(/^[`"\[]/, "").replace(/[`"\]]$/, "");
    const table = unquote(match[2] || match[1]);
    const schema = match[2] ? unquote(match[1]) : (tab.schema || defaultSchemaFor(tab.connectionId));
    try {
      const meta = await loadTableMeta(tab.connectionId, schema, table);
      const primaryKeys = (meta.columns || []).filter((column) => column.isPrimaryKey).map((column) => column.name);
      if (!primaryKeys.length) return;
      const names = new Set(grid.columns.map((column) => column.name));
      if (!primaryKeys.every((key) => names.has(key))) return;
      tab.queryEdit = { table, schema, primaryKeys };
    } catch {
      tab.queryEdit = null;
    }
  }

  function coerceCell(value, previous) {
    if (value === null) return null;
    return typeof previous === "number" && /^-?\d+(\.\d+)?$/.test(value) && Number.isSafeInteger(Number(value)) ? Number(value) : value;
  }

  function blobBytesFromValue(value) {
    if (!value || typeof value !== "object" || !("bytesBase64" in value)) return null;
    return value.bytesBase64;
  }

  function hexDump(bytes) {
    const lines = [];
    for (let i = 0; i < bytes.length; i += 16) {
      const slice = bytes.slice(i, i + 16);
      const hex = [...slice].map((b) => b.toString(16).padStart(2, "0")).join(" ");
      const ascii = [...slice].map((b) => (b >= 32 && b < 127 ? String.fromCharCode(b) : ".")).join("");
      lines.push(`${i.toString(16).padStart(8, "0")}  ${hex.padEnd(47)}  ${ascii}`);
    }
    return lines.join("\n");
  }

  function hexToBytes(text) {
    const clean = String(text || "").replace(/0x/gi, "").replace(/\s+/g, "");
    if (!clean) return "";
    if (!/^[0-9a-fA-F]+$/.test(clean) || clean.length % 2 !== 0) throw new Error("HEX 需为偶数长度的十六进制字符。");
    const out = new Uint8Array(clean.length / 2);
    for (let i = 0; i < out.length; i++) out[i] = parseInt(clean.substr(i * 2, 2), 16);
    let binary = "";
    out.forEach((b) => { binary += String.fromCharCode(b); });
    return binary;
  }

  function blobEditorModal(rowIndex, columnIndex) {
    blobTarget = { r: rowIndex, c: columnIndex };
    const tab = activeSqlTab();
    const value = gridState.rows[rowIndex]?.[columnIndex];
    const column = gridState.columns[columnIndex];
    const b64 = blobBytesFromValue(value);
    const bytes = b64 ? (() => { const bin = atob(b64); const arr = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) arr[i] = bin.charCodeAt(i); return arr; })() : null;
    const editable = Boolean(editTargetFor(tab)?.primaryKeys?.length);
    const textPreview = bytes ? new TextDecoder("utf-8").decode(bytes) : "";
    showModal(`BLOB 单元格 · ${column.name}`, "BINARY CELL", `
      <p class="modal-text">第 ${gridState.offset + rowIndex + 1} 行 · ${escapeHtml(column.typeName || "BLOB")} · ${bytes ? `${bytes.length} 字节` : "NULL"}${editable ? " · 保存后写入数据库" : " · 只读查询结果"}</p>
      <div class="blob-grid">
        <div><span class="blob-label">HEX 视图</span><pre class="blob-pre">${bytes ? escapeHtml(hexDump(bytes)) : "NULL"}</pre></div>
        <div><span class="blob-label">文本预览（UTF-8 有损）</span><pre class="blob-pre">${escapeHtml(textPreview)}</pre></div>
      </div>
      <label class="full">HEX 编辑（仅十六进制，可含空格；保存写回二进制列）<textarea id="blob-hex-editor" class="modal-textarea" spellcheck="false">${escapeHtml(b64 ? base64ToHex(b64) : "")}</textarea></label>
      <label class="confirm-check"><input id="blob-null" type="checkbox" ${value == null ? "checked" : ""} ${editable ? "" : "disabled"} />NULL</label>
      <div class="modal-footer"><button class="secondary-button" data-action="blob-copy-hex">复制 HEX</button><button class="secondary-button" data-action="blob-copy-b64">复制 Base64</button><span class="toolbar-spacer"></span><button class="secondary-button" data-close-modal>关闭</button>${editable ? '<button class="primary-button" data-action="blob-save">保存修改</button>' : ""}</div>
    `, { wide: true });
  }

  async function saveBlobCell(rowIndex, columnIndex) {
    const text = $("#blob-hex-editor")?.value ?? "";
    const isNull = Boolean($("#blob-null")?.checked);
    let payload = null;
    if (!isNull) {
      const binary = hexToBytes(text);
      payload = { bytesBase64: btoa(binary) };
    }
    if (await updateGridCell(rowIndex, columnIndex, payload)) closeModal();
  }

  function beginCellEdit(rowIndex, columnIndex, retried = false) {
    const tab = activeSqlTab();
    let target = editTargetFor(tab);
    if (!target && tab?.kind === "query" && tab.grid?.sql && !retried) {
      if (!tab.queryEditDetecting) {
        tab.queryEditDetecting = detectQueryEditTarget(tab, tab.grid).finally(() => { tab.queryEditDetecting = null; });
      }
      tab.queryEditDetecting.then(() => {
        if (activeSqlTabId === tab.id) beginCellEdit(rowIndex, columnIndex, true);
      });
      return;
    }
    if (!tab || !target) {
      toast("查询结果为只读", "仅单表且结果含主键的查询可编辑（支持表别名；JOIN/UNION/子查询只读）。双击左侧表打开表数据后也可编辑。", "danger");
      return;
    }
    if (!target.primaryKeys.length) {
      toast("无法编辑", "该表没有主键，无法安全定位要修改的行。", "danger");
      return;
    }
    const value = gridState.rows[rowIndex]?.[columnIndex];
    if (value && typeof value === "object" && "bytesBase64" in value) {
      blobEditorModal(rowIndex, columnIndex);
      return;
    }
    if (value && typeof value === "object" && "unsupportedCellType" in value) {
      toast("该列类型暂不支持编辑", "此驱动返回的列类型当前无法写回。", "danger");
      return;
    }
    ensureCellVisible(rowIndex, columnIndex);
    renderGridBody();
    const td = $(`.data-grid tbody tr[data-r="${rowIndex}"] td[data-c="${columnIndex}"]`);
    const wrap = $(".grid-wrap");
    if (!td || !wrap) return;
    const text = value === null || value === undefined ? "" : cellText(value);
    const multiline = text.length > 120 || /\n/.test(text);
    const input = document.createElement(multiline ? "textarea" : "input");
    input.className = "cell-editor";
    input.value = text;
    input.placeholder = value === null || value === undefined ? "NULL" : "";
    input.spellcheck = false;
    const wrapRect = wrap.getBoundingClientRect();
    const rect = td.getBoundingClientRect();
    input.style.left = `${rect.left - wrapRect.left + wrap.scrollLeft}px`;
    input.style.top = `${rect.top - wrapRect.top + wrap.scrollTop}px`;
    input.style.width = `${Math.max(rect.width, multiline ? 320 : 160)}px`;
    input.style.height = `${multiline ? 140 : rect.height}px`;
    wrap.appendChild(input);
    cellEditor = { input, r: rowIndex, c: columnIndex, original: value, tabId: tab.id };
    input.focus();
    input.select();
    input.addEventListener("keydown", (event) => {
      event.stopPropagation();
      if (event.key === "Enter" && !(multiline && event.shiftKey)) {
        event.preventDefault();
        commitCellEditor(true);
        wrap.focus({ preventScroll: true });
      } else if (event.key === "Escape") {
        event.preventDefault();
        cancelCellEditor();
        wrap.focus({ preventScroll: true });
      }
    });
    input.addEventListener("blur", () => commitCellEditor(true));
  }

  function cancelCellEditor() {
    if (!cellEditor) return;
    const editor = cellEditor;
    cellEditor = null;
    editor.input.remove();
  }

  async function commitCellEditor(save) {
    if (!cellEditor) return;
    const editor = cellEditor;
    cellEditor = null;
    const text = editor.input.value;
    editor.input.remove();
    if (!save || editor.tabId !== activeSqlTabId) return;
    const original = editor.original;
    const originalText = original === null || original === undefined ? "" : cellText(original);
    if (text === originalText) return;
    await updateGridCell(editor.r, editor.c, text);
  }

  function gridRowKey(tab, grid, rowIndex) {
    const target = editTargetFor(tab);
    if (!target || !target.primaryKeys.length) throw new Error("该结果没有可定位的主键，无法安全编辑。");
    const row = grid.rows[rowIndex];
    const values = target.primaryKeys.map((key) => {
      const columnIndex = grid.columns.findIndex((column) => column.name === key);
      const staged = pendingCellEdits.get(`${tab.id}|${rowIndex}|${columnIndex}`);
      return staged ? staged.original : row?.[columnIndex];
    });
    if (values.some((value) => value === null || value === undefined)) throw new Error("结果中缺少完整的主键值，请刷新后重试。");
    return { columns: [...target.primaryKeys], values };
  }

  async function updateGridCell(rowIndex, columnIndex, value) {
    const tab = activeSqlTab();
    const target = editTargetFor(tab);
    if (!tab || !target) return false;
    const grid = tab.grid;
    const row = grid.rows[rowIndex];
    const column = grid.columns[columnIndex];
    if (!row || !column) return false;
    try {
      const key = gridRowKey(tab, grid, rowIndex);
      const id = `${tab.id}|${rowIndex}|${columnIndex}`;
      const existing = pendingCellEdits.get(id);
      const original = existing ? existing.original : row[columnIndex];
      pendingCellEdits.set(id, {
        tabId: tab.id, r: rowIndex, c: columnIndex, value, original,
        columnName: column.name, connectionId: tab.connectionId,
        schema: target.schema || "", table: target.table,
        keyColumns: key.columns, keyValues: key.values,
      });
      row[columnIndex] = coerceCell(value, original);
      if (tab.id === activeSqlTabId) { renderGridBody(); updatePendingBar(); }
      return true;
    } catch (error) {
      showOperationError(error, "无法暂存修改");
      return false;
    }
  }

  function updatePendingBar() {
    const bar = $("#grid-pending-bar");
    if (!bar) return;
    const tab = activeSqlTab();
    const count = [...pendingCellEdits.values()].filter((entry) => entry.tabId === tab?.id).length;
    bar.hidden = !count;
    const label = $("#grid-pending-count");
    if (label) label.textContent = `${count} 处未保存的单元格修改`;
  }

  function clearPendingForTab(tabId) {
    for (const [id, entry] of [...pendingCellEdits.entries()]) {
      if (entry.tabId === tabId) pendingCellEdits.delete(id);
    }
  }

  function discardPendingEdits() {
    const tab = activeSqlTab();
    if (!tab) return;
    for (const [id, entry] of [...pendingCellEdits.entries()]) {
      if (entry.tabId !== tab.id) continue;
      const row = tab.grid.rows[entry.r];
      if (row) row[entry.c] = entry.original;
      pendingCellEdits.delete(id);
    }
    renderGridBody();
    updatePendingBar();
  }

  async function applyPendingEdits() {
    const tab = activeSqlTab();
    if (!tab) return;
    const entries = [...pendingCellEdits.entries()].filter(([, entry]) => entry.tabId === tab.id);
    if (!entries.length) return;
    const rowGroups = new Map();
    for (const [id, entry] of entries) {
      if (!rowGroups.has(entry.r)) rowGroups.set(entry.r, []);
      rowGroups.get(entry.r).push([id, entry]);
    }
    const preview = [...rowGroups.values()].map((group) => {
      const first = group[0][1];
      const changes = group.map(([, entry]) => `${escapeHtml(entry.columnName)}：${escapeHtml(cellText(entry.original))} → ${escapeHtml(cellText(entry.value))}`).join("；");
      return `<div class="rc-cmd">第 ${gridState.offset + first.r + 1} 行 · ${changes}</div>`;
    }).join("");
    const confirmed = await confirmChoice({
      title: "应用单元格修改",
      eyebrow: "CONFIRM CELL EDITS",
      body: `<div class="modal-text">将在一个事务中写入 ${rowGroups.size} 行、共 ${entries.length} 处修改到表 ${escapeHtml(entries[0][1].table)}（按主键逐行 UPDATE，任一行失败则全部回滚）：</div>${preview}`,
      confirmLabel: "应用更改",
    });
    if (!confirmed) return;
    const first = entries[0][1];
    const rows = [...rowGroups.values()].map((group) => ({
      keyColumns: group[0][1].keyColumns,
      keyValues: group[0][1].keyValues,
      changes: group.map(([, entry]) => ({ column: entry.columnName, value: entry.value })),
    }));
    const submittedIds = entries.map(([id]) => id);
    try {
      const result = await backendCall("sql_apply_row_edits", {
        connectionId: first.connectionId, schema: first.schema, table: first.table,
        rows, operationId: operationId(),
      });
      for (const id of submittedIds) pendingCellEdits.delete(id);
      renderGridBody();
      updatePendingBar();
      refreshAuditOnly();
      if (Number(result?.rowsAffected ?? rows.length) !== rows.length) {
        toast("修改已提交，但行数不一致", `事务已提交；预计更新 ${rows.length} 行，实际影响 ${result?.rowsAffected ?? 0} 行。部分行可能已被他人修改，请刷新结果核对。`, "danger");
      } else {
        toast("修改已应用", `事务提交成功：${rows.length} 行、${entries.length} 处修改已写入，已记录审计日志。`, "success");
      }
    } catch (error) {
      showOperationError(error, "批量修改已回滚，未写入任何行");
    }
  }

  let pendingRowInsert = null;
  let cellDetailTarget = null;
  let queryExportContext = null;
  let activeQueryExport = null;

  function parseCellInput(text, type, binary = false) {
    if (binary) {
      const hex = String(text).replace(/^0x/i, "").replace(/\s/g, "");
      if (!/^(?:[0-9a-f]{2})*$/i.test(hex)) throw new Error("二进制值必须是偶数位 HEX，例如 0xDEADBEEF。");
      let bytes = "";
      for (let index = 0; index < hex.length; index += 2) bytes += String.fromCharCode(parseInt(hex.slice(index, index + 2), 16));
      return { bytesBase64: btoa(bytes) };
    }
    if (/^jsonb?$/i.test(type || "")) {
      try { return JSON.parse(text); } catch { throw new Error("JSON 格式不正确，请先修正内容。"); }
    }
    return text;
  }

  async function insertRowModal(copy = false) {
    const tab = activeSqlTab();
    if (!tab || tab.kind !== "table" || /view/i.test(tab.objectType)) {
      toast("请先打开表", "在左侧双击一张表后新增数据。", "danger");
      return;
    }
    const sourceIndex = copy ? selectedRowIndexes()[0] : null;
    if (copy && sourceIndex == null) {
      toast("请先选择一行", "复制行会保留字段值，自增字段使用默认值。", "danger");
      return;
    }
    try {
      const metadata = await loadTableMeta(tab.connectionId, tab.schema, tab.table);
      const source = sourceIndex == null ? null : structuredClone(tab.grid.rows[sourceIndex]);
      const protocol = connectionProtocol(tab.connectionId);
      const fields = metadata.columns.map((column) => {
        const generated = /generated/i.test(column.extra || "") && !/default_generated/i.test(column.extra || "");
        const serverGeneratedPrimaryKey = column.isPrimaryKey && (
          column.defaultValue != null || (protocol === "sqlite" && /^integer$/i.test(column.dataType || ""))
        );
        const automatic = generated || /auto_increment|identity/i.test(column.extra || "") || serverGeneratedPrimaryKey;
        const index = tab.grid.columns.findIndex((item) => item.name === column.name);
        const original = source && index >= 0 ? source[index] : undefined;
        const mode = automatic ? "default" : original === null ? "null" : original !== undefined ? "value"
          : column.defaultValue != null || column.nullable ? "default" : "value";
        return { column, generated, mode, value: original == null || automatic ? "" : cellText(original) };
      });
      pendingRowInsert = { tab, fields };
      showModal(`${copy ? "复制" : "新增"}行 · ${tab.table}`, "INSERT ROW", `
        <p class="modal-text">目标：${escapeHtml(connectionLookup.get(tab.connectionId)?.name || "")} / ${escapeHtml(tab.schema || "main")} / ${escapeHtml(tab.table)}${copy ? "；非自增主键请修改后再保存。" : ""}</p>
        <div class="row-insert-grid">${fields.map(({ column, generated, mode, value }, index) => `<div class="row-insert-field" data-insert-field="${index}">
          <div><strong>${escapeHtml(column.name)}${column.isPrimaryKey ? " · PK" : ""}</strong><small>${escapeHtml(column.dataType)}${column.nullable ? "" : " · NOT NULL"}</small></div>
          <select data-insert-mode ${generated ? "disabled" : ""} aria-label="${escapeHtml(column.name)} 写入方式"><option value="default"${mode === "default" ? " selected" : ""}>默认值</option><option value="null"${mode === "null" ? " selected" : ""}${!column.nullable ? " disabled" : ""}>NULL</option><option value="value"${mode === "value" ? " selected" : ""}>填写值</option></select>
          <textarea data-insert-value rows="1" spellcheck="false" ${mode !== "value" ? "disabled" : ""} aria-label="${escapeHtml(column.name)} 值" placeholder="${generated ? "生成列不可手工写入" : /blob|binary|bytea/i.test(column.dataType) ? "HEX，例如 0xDEADBEEF" : "填写字段值"}">${escapeHtml(value)}</textarea>
        </div>`).join("")}</div>
        <div class="form-note">默认值表示不发送该字段，交给数据库计算；NULL 与空字符串是不同值。所有数据使用参数绑定。</div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="insert-row-save">写入一行</button></div>
      `, { wide: true });
    } catch (error) { showOperationError(error, "新增行打开失败"); }
  }

  async function saveInsertedRow() {
    const pending = pendingRowInsert;
    if (!pending) return;
    try {
      const columns = [], values = [];
      $$('[data-insert-field]', modalContent).forEach((row) => {
        const field = pending.fields[Number(row.dataset.insertField)];
        const mode = $('[data-insert-mode]', row).value;
        if (mode === "default") return;
        columns.push(field.column.name);
        values.push(mode === "null" ? null : parseCellInput($('[data-insert-value]', row).value, field.column.dataType, /blob|binary|bytea/i.test(field.column.dataType)));
      });
      const tab = pending.tab;
      await backendCall("sql_insert_row", { connectionId: tab.connectionId, schema: tab.schema || "main", table: tab.table, columns, values, operationId: operationId() });
      pendingRowInsert = null;
      closeModal();
      await loadTableData(tab, tab.grid.offset || 0);
      toast("新增行已保存", "已写入数据库并刷新当前页；数据库默认值与生成列由服务器计算。");
      refreshAuditOnly();
    } catch (error) {
      if (pending?.tab && serverCommitNeedsRefresh(error)) await loadTableData(pending.tab, pending.tab.grid.offset || 0);
      showOperationError(error, "新增行失败");
    }
  }

  function showCellDetails() {
    const focus = gridState.focus;
    const tab = activeSqlTab();
    if (!focus || !tab || !gridState.columns[focus.c]) {
      toast("请先选择单元格", "单击单元格后查看完整内容。", "danger");
      return;
    }
    const value = gridState.rows[focus.r]?.[focus.c];
    const column = gridState.columns[focus.c];
    const binary = Boolean(value && typeof value === "object" && "bytesBase64" in value)
      || /blob|binary|bytea/i.test(column.typeName || "");
    if (binary) {
      blobEditorModal(focus.r, focus.c);
      return;
    }
    const editable = Boolean(editTargetFor(tab)?.primaryKeys?.length);
    cellDetailTarget = { tabId: tab.id, r: focus.r, c: focus.c, binary, type: column.typeName, editable };
    showModal(`单元格 · ${column.name}`, "CELL VALUE", `
      <p class="modal-text">第 ${gridState.offset + focus.r + 1} 行 · ${escapeHtml(column.typeName || "")}${editable ? " · 保存后写入数据库" : " · 只读查询结果"}${binary ? " · HEX" : ""}</p>
      <textarea id="cell-detail-value" class="modal-textarea cell-detail-value" spellcheck="false" ${editable ? (value == null ? "disabled" : "") : "readonly"}>${escapeHtml(value == null ? "" : cellText(value))}</textarea>
      <label class="confirm-check"><input id="cell-detail-null" type="checkbox" ${value == null ? "checked" : ""} ${editable ? "" : "disabled"} />NULL</label>
      <div class="modal-footer"><button class="secondary-button" data-action="cell-detail-copy">复制完整内容</button><button class="secondary-button" data-action="cell-detail-json">格式化 JSON</button><span class="toolbar-spacer"></span><button class="secondary-button" data-close-modal>关闭</button>${editable ? '<button class="primary-button" data-action="cell-detail-save">保存修改</button>' : ""}</div>
    `, { wide: true });
  }

  async function saveCellDetails() {
    const target = cellDetailTarget;
    if (!target?.editable || target.tabId !== activeSqlTabId) {
      toast("编辑目标已切换", "请重新打开原单元格后再保存。", "danger");
      return;
    }
    try {
      const value = $('#cell-detail-null').checked ? null : parseCellInput($('#cell-detail-value').value, target.type, target.binary);
      if (await updateGridCell(target.r, target.c, value)) closeModal();
    } catch (error) { showOperationError(error, "内容格式不正确"); }
  }

  function queryResultSql(tab, grid) {
    if (grid.sql) return grid.sql;
    if (tab.kind !== "table") return null;
    const protocol = connectionProtocol(tab.connectionId);
    const qualified = protocolQualified(protocol, tab.schema || "main", tab.table);
    return `SELECT * FROM ${qualified}${tab.filter ? ` WHERE (${tab.filter})` : ""}${tab.orderBy ? ` ORDER BY ${tab.orderBy}` : ""}`;
  }

  let connImportPreview = null;

  function connectionsExportModal() {
    if (!savedConnections.length) { toast("没有可导出的连接", "先创建并保存连接。", "danger"); return; }
    showModal("批量导出连接", "EXPORT CONNECTIONS", `
      <div class="conn-pick-list">${savedConnections.map((c) => `<label class="conn-pick"><input type="checkbox" data-export-id="${escapeHtml(c.id)}" checked />${protocolBadgeHtml(c.protocol)}<span><b>${escapeHtml(c.name)}</b><small>${escapeHtml(c.hostSummary)}</small></span></label>`).join("")}</div>
      <div class="form-grid">
        <label class="check-line full"><input id="export-secrets" type="checkbox" />包含密码 / SSH 凭据（用口令加密）</label>
        <label class="full" id="export-passphrase-row" hidden>导出口令<input id="export-passphrase" type="password" placeholder="用于加密凭据，导入时需相同口令" autocomplete="new-password" /></label>
        <label class="full">保存位置<div class="file-path-field"><input id="export-conn-path" placeholder="选择保存文件" /><button class="secondary-button" data-action="connections-export-browse">选择…</button></div></label>
      </div>
      <div class="form-note"><svg><use href="#i-lock"/></svg>不含凭据的导出仅含连接配置；含凭据的导出以 AES-256-GCM 口令加密，请妥善保管口令与文件。</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="connections-export-save">导出</button></div>`, { wide: true });
  }

  async function chooseConnectionsExportPath() {
    try {
      const now = new Date();
      const pad = (n) => String(n).padStart(2, "0");
      const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}`;
      const { save } = await import('@tauri-apps/plugin-dialog');
      const path = await save({ defaultPath: `luxbase-connections_${stamp}.json`, filters: [{ name: "JSON", extensions: ["json"] }] });
      if (path && $("#export-conn-path")) $("#export-conn-path").value = path;
    } catch (error) { showOperationError(error, "无法选择保存位置"); }
  }

  async function doExportConnections() {
    const ids = $$("[data-export-id]").filter((input) => input.checked).map((input) => input.dataset.exportId);
    if (!ids.length) { toast("未选择连接", "至少勾选一个连接。", "danger"); return; }
    const path = $("#export-conn-path")?.value.trim();
    if (!path) { toast("请选择保存位置", "点击“选择…”指定导出文件。", "danger"); return; }
    const includeSecrets = Boolean($("#export-secrets")?.checked);
    const passphrase = $("#export-passphrase")?.value || null;
    const count = await backendCall("export_connections", { ids, includeSecrets, passphrase, path });
    closeModal();
    toast("连接已导出", `${count} 个连接 · ${path}${includeSecrets ? " · 凭据已口令加密" : " · 不含凭据"}`);
    refreshAuditOnly();
  }

  function connectionsImportModal() {
    connImportPreview = null;
    showModal("批量导入连接", "IMPORT CONNECTIONS", `
      <div class="form-grid">
        <label class="full">导出文件<div class="file-path-field"><input id="import-conn-path" placeholder="选择 .json 导出文件" /><button class="secondary-button" data-action="connections-import-browse">选择…</button></div></label>
        <label class="full" id="import-passphrase-row" hidden>口令<input id="import-passphrase" type="password" placeholder="导出时设置的口令" autocomplete="current-password" /></label>
        <label class="check-line full"><input id="import-overwrite" type="checkbox" />覆盖已存在的同 ID 连接</label>
      </div>
      <div id="import-preview" class="conn-pick-list"><div class="filter-empty">选择导出文件后在这里预览待导入连接。</div></div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="connections-import-save" disabled>导入</button></div>`, { wide: true });
  }

  async function chooseConnectionsImportPath() {
    try {
      const { open } = await import('@tauri-apps/plugin-dialog');
      const path = await open({ multiple: false, filters: [{ name: "JSON", extensions: ["json"] }] });
      if (path && $("#import-conn-path")) {
        $("#import-conn-path").value = path;
        await previewConnectionsImport();
      }
    } catch (error) { showOperationError(error, "无法选择文件"); }
  }

  async function previewConnectionsImport() {
    const path = $("#import-conn-path")?.value.trim();
    const box = $("#import-preview");
    const saveButton = $('[data-action="connections-import-save"]');
    if (!path || !box) return;
    const passphrase = $("#import-passphrase")?.value || null;
    try {
      connImportPreview = await backendCall("import_connections_preview", { path, passphrase });
      $("#import-passphrase-row").hidden = !connImportPreview.includesSecrets;
      box.innerHTML = connImportPreview.entries.map((entry) => `<label class="conn-pick"><input type="checkbox" data-import-id="${escapeHtml(entry.id)}" checked />${protocolBadgeHtml(entry.protocol)}<span><b>${escapeHtml(entry.name)}</b><small>${escapeHtml(entry.hostSummary)}${entry.hasSecrets ? " · 含凭据" : ""}${entry.exists ? " · 已存在" : ""}</small></span></label>`).join("") || '<div class="filter-empty">文件内没有连接。</div>';
      if (saveButton) saveButton.disabled = !connImportPreview.entries.length;
    } catch (error) {
      connImportPreview = null;
      box.innerHTML = `<div class="filter-empty">${escapeHtml(errorMessage(error))}</div>`;
      if (saveButton) saveButton.disabled = true;
      if (/口令/.test(errorMessage(error))) $("#import-passphrase-row").hidden = false;
    }
  }

  async function doImportConnections() {
    if (!connImportPreview) return;
    const ids = $$("[data-import-id]").filter((input) => input.checked).map((input) => input.dataset.importId);
    if (!ids.length) { toast("未选择连接", "至少勾选一个连接。", "danger"); return; }
    const path = $("#import-conn-path")?.value.trim();
    const passphrase = $("#import-passphrase")?.value || null;
    const overwrite = Boolean($("#import-overwrite")?.checked);
    const result = await backendCall("import_connections_apply", { path, passphrase, ids, overwrite });
    closeModal();
    toast("连接导入完成", `导入 ${result.imported} 个${result.skipped ? `，跳过 ${result.skipped} 个已存在` : ""}${result.failed.length ? `，失败 ${result.failed.length} 个` : ""}`);
    if (result.failed.length) showOperationError(new Error(result.failed.join("；")), "部分连接导入失败");
    await refreshBootstrap();
    refreshAuditOnly();
  }

  function queryExportModal() {
    const tab = activeSqlTab();
    if (!tab || !gridState.columns.length) {
      toast("没有可导出的结果", "先执行查询或打开表；空结果集也可以导出列名。", "danger");
      return;
    }
    const grid = tab.grid;
    const sql = queryResultSql(tab, grid);
    const selection = selectionMatrix();
    queryExportContext = { tab, sql, params: structuredClone(grid.params || []), schema: grid.schema || tab.schema,
      columns: structuredClone(grid.columns), rows: structuredClone(grid.rows),
      selection: normalizedSelection() && selection ? structuredClone(selection) : null };
    showModal("导出查询结果", "QUERY RESULT EXPORT", `
      <p class="modal-text">${escapeHtml(connectionLookup.get(tab.connectionId)?.name || "")} · ${escapeHtml(tab.title)}${sql ? `<br><code>${escapeHtml(summarizeSql(sql, 220))}</code>` : ""}</p>
      <div class="form-grid">
        <label>范围<select id="query-export-scope"><option value="page">当前结果页（${grid.rows.length} 行）</option><option value="selection" ${queryExportContext.selection ? "" : "disabled"}>选中单元格</option><option value="all" ${sql && isReadSql(sql) ? "" : "disabled"}>完整查询（只读快照，流式导出）</option></select></label>
        <label>格式<select id="query-export-format"><option value="csv">CSV（UTF-8）</option><option value="jsonl">JSON Lines</option></select></label>
        <label class="full">保存位置<div class="file-path-field"><input id="query-export-path" placeholder="选择保存文件" /><button class="secondary-button" data-action="query-export-browse">选择…</button></div></label>
      </div><div id="query-export-status" class="form-note">完整导出会重新执行这条只读查询，包含全部结果，不限于已加载的 500 行；参数和数据库保持本次执行的值。</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>关闭</button><button class="secondary-button" data-action="query-export-cancel" hidden>停止导出</button><button class="primary-button" data-action="query-export-save">导出</button></div>
    `);
  }

  async function chooseQueryExportPath() {
    try {
      const format = $('#query-export-format').value;
      const tab = activeSqlTab();
      const base = (tab?.kind === "table" ? tab.table : tab?.title) || "query-result";
      const safeBase = String(base).replace(/[^\w.-]+/g, "_").slice(0, 60) || "query-result";
      const now = new Date();
      const pad = (n) => String(n).padStart(2, "0");
      const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
      const { save } = await import('@tauri-apps/plugin-dialog');
      const path = await save({ defaultPath: `${safeBase}_${stamp}.${format}`, filters: [{ name: format.toUpperCase(), extensions: [format] }] });
      if (path && $('#query-export-path')) $('#query-export-path').value = path;
    } catch (error) { showOperationError(error, "无法选择保存位置"); }
  }

  async function exportQueryResult() {
    const context = queryExportContext;
    if (!context || activeQueryExport) return;
    const path = $('#query-export-path').value.trim();
    if (!path) { toast("请选择保存位置", "点击“选择…”指定导出文件。", "danger"); return; }
    const scope = $('#query-export-scope').value;
    const format = $('#query-export-format').value;
    const op = operationId();
    activeQueryExport = { connectionId: context.tab.connectionId, operationId: op };
    const status = $('#query-export-status');
    const stop = $('[data-action="query-export-cancel"]');
    if (stop) stop.hidden = scope !== "all";
    if (status) status.textContent = "导出中…完整结果可随时停止；当前窗口关闭后导出会继续。";
    try {
      const result = scope === "all" ? await backendCall("sql_export_query", { connectionId: context.tab.connectionId,
        request: { format, path, request: { connectionId: context.tab.connectionId, sql: context.sql, params: context.params, schema: context.schema || null, operationId: op, pageSize: null, offset: null } } })
        : await backendCall("sql_export_result", { connectionId: context.tab.connectionId, format, path,
          columns: scope === "selection" ? context.selection.columns : context.columns,
          rows: scope === "selection" ? context.selection.rows : context.rows });
      if (status?.isConnected) status.textContent = `导出成功 · ${result.rowsExported} 行 · ${result.outputPath}`;
      toast("查询结果已导出", `${result.rowsExported} 行 · ${path}`);
      refreshAuditOnly();
    } catch (error) {
      if (status?.isConnected) status.textContent = errorMessage(error);
      showOperationError(error, "导出未完成");
    } finally {
      activeQueryExport = null;
      if (stop?.isConnected) stop.hidden = true;
    }
  }

  async function stopQueryExport() {
    if (!activeQueryExport) return;
    await backendCall("sql_cancel", { ...activeQueryExport });
  }

  /* ---------- information pane (Navicat-style) ---------- */

  function setInspectorMode(mode) {
    $$("[data-inspector-tab]").forEach((button) => button.classList.toggle("active", button.dataset.inspectorTab === mode));
    $$("[data-inspector-panel]").forEach((panel) => { panel.hidden = panel.dataset.inspectorPanel !== mode; });
    if (mode === "health") refreshPoolMetrics();
  }

  function inspectorMode() {
    return $("[data-inspector-tab].active")?.dataset.inspectorTab || "object";
  }

  const INFO_SYMBOL = { table: "o-table", view: "o-view", database: "o-db", query: "o-query" };

  function formatCount(value) {
    return value == null ? null : Number(value).toLocaleString("zh-CN");
  }

  function formatSize(value) {
    return value == null ? null : formatBytes(value);
  }

  function renderInfoCard({ kind, protocol = null, name, typeLabel, location = [], props = [] }) {
    const empty = $("#info-empty");
    const card = $("#info-card");
    if (!card) return;
    if (empty) empty.hidden = true;
    card.hidden = false;
    const icon = $("#info-icon");
    if (icon) {
      icon.className = `info-icon ${kind}`;
      icon.innerHTML = kind === "connection"
        ? protocolBadgeHtml(protocol)
        : `<svg><use href="#${INFO_SYMBOL[kind] || "o-table"}"/></svg>`;
    }
    $("#object-name").textContent = name;
    $("#object-meta").textContent = typeLabel;
    const actions = $("#object-name-actions");
    if (actions) {
      const show = kind === "table" || kind === "view";
      actions.hidden = !show;
      if (show) {
        actions.innerHTML = `<button data-action="copy-info-name" title="复制表名" aria-label="复制表名"><svg><use href="#i-copy"/></svg></button>`;
      }
    }
    const locationBox = $("#info-location");
    if (locationBox) {
      locationBox.innerHTML = location.map(([symbol, text]) => `<span><svg><use href="#${symbol}"/></svg>${escapeHtml(text)}</span>`).join("");
    }
    renderInfoProps(props);
  }

  function renderInfoProps(props) {
    const box = $("#object-stats");
    if (!box) return;
    box.innerHTML = props
      .filter(([, value]) => value !== null && value !== undefined && value !== "")
      .map(([label, value]) => `<div class="info-prop"><span>${escapeHtml(label)}</span><b title="${escapeHtml(value)}">${escapeHtml(value)}</b></div>`)
      .join("");
  }

  function updateDdlActions(tableSelected, textReady = false, isView = false) {
    $$(".ddl-toolbar [data-action]").forEach((button) => {
      const action = button.dataset.action;
      button.disabled = action === "table-designer" ? !tableSelected || isView
        : action === "refresh-ddl" ? !tableSelected : !textReady;
    });
  }

  function clearInfo(message = "无信息") {
    const empty = $("#info-empty");
    const card = $("#info-card");
    if (card) card.hidden = true;
    if (empty) {
      empty.hidden = false;
      const label = $("span", empty);
      if (label) label.textContent = message;
    }
    const panel = $("#inspector-object");
    if (panel) {
      delete panel.dataset.key;
      delete panel.dataset.ddl;
      delete panel.dataset.table;
      delete panel.dataset.connectionId;
      delete panel.dataset.schema;
    }
    updateDdlActions(false);
    const ddl = $("#object-ddl");
    if (ddl) ddl.innerHTML = '<span class="muted">选择表或视图后显示建表语句。</span>';
    const title = $("#ddl-title");
    if (title) title.textContent = "DDL";
  }

  function statFor(connectionId, schema, table) {
    return metaFor(connectionId).stats.get(schema)?.find((item) => item.name === table) || null;
  }

  function tableProps(protocol, stat) {
    if (!stat) return [];
    if (protocol === "mysql") {
      return [
        ["行（估算）", formatCount(stat.rows)],
        ["引擎", stat.engine],
        ["自动递增", formatCount(stat.autoIncrement)],
        ["数据长度", formatSize(stat.dataBytes)],
        ["索引长度", formatSize(stat.indexBytes)],
        ["排序规则", stat.collation],
        ["创建日期", stat.created],
        ["修改日期", stat.updated],
        ["注释", stat.comment],
      ];
    }
    if (protocol === "postgres") {
      return [
        ["行（估算）", formatCount(stat.rows)],
        ["所有者", stat.owner],
        ["数据大小", formatSize(stat.dataBytes)],
        ["索引大小", formatSize(stat.indexBytes)],
        ["注释", stat.comment],
      ];
    }
    return [["类型", /VIEW/i.test(stat.objectType) ? "视图" : "表"]];
  }

  async function showObjectInfo({ connectionId, schema, table, objectType = "table" }, { refresh = false } = {}) {
    const panel = $("#inspector-object");
    if (!panel || !connectionId || !table) return;
    if (inspectorMode() === "health") setInspectorMode("object");
    const key = `${connectionId}|${schema}|${table}|${operationId()}`;
    panel.dataset.key = key;
    panel.dataset.connectionId = connectionId;
    panel.dataset.schema = schema || "";
    panel.dataset.table = table;
    delete panel.dataset.ddl;
    const isView = /view/i.test(objectType || "");
    updateDdlActions(true, false, isView);
    const connection = connectionLookup.get(connectionId);
    const protocol = connection?.protocol;
    const location = [["t-connection", connection?.name || ""], ...(schema && protocol !== "sqlite" ? [["o-db", schema]] : [])];
    const baseInfo = { kind: isView ? "view" : "table", name: table, typeLabel: isView ? "视图" : "表", location };
    renderInfoCard({ ...baseInfo, props: tableProps(protocol, statFor(connectionId, schema, table)) });
    const title = $("#ddl-title");
    if (title) title.textContent = `${table} · DDL`;
    const ddlView = $("#object-ddl");
    if (ddlView) ddlView.innerHTML = '<span class="muted">正在读取建表语句…</span>';
    const entry = metaFor(connectionId);
    const cacheKey = `${schema}.${table}`;
    if (refresh) invalidateTableMeta(connectionId, schema, table);
    const statsReady = loadObjectStats(connectionId, schema || "main", { refresh }).catch(() => null);
    const metaReady = loadTableMeta(connectionId, schema, table).catch(() => null);
    Promise.all([statsReady, metaReady]).then(([, meta]) => {
      if (panel.dataset.key !== key) return;
      const props = tableProps(protocol, statFor(connectionId, schema, table));
      if (meta) {
        const keys = (meta.columns || []).filter((column) => column.isPrimaryKey).map((column) => column.name);
        props.splice(1, 0, ["列", String((meta.columns || []).length)], ["主键", keys.join(", ") || "无"]);
        if (!isView) props.push(["索引", String((meta.indexes || []).length)], ["外键", String((meta.foreignKeys || []).length)]);
      }
      renderInfoProps(props);
    });
    try {
      let ddl = entry.ddl.get(cacheKey);
      if (!ddl) {
        ddl = await backendCall("sql_table_ddl", { connectionId, schema: schema || "", table });
        if (panel.dataset.key !== key) return;
        entry.ddl.set(cacheKey, ddl);
      }
      if (panel.dataset.key !== key) return;
      panel.dataset.ddl = ddl;
      updateDdlActions(true, true, isView);
      let html = null;
      try {
        html = await window.__luxbaseColorizeSql?.(ddl, connectionProtocol(connectionId));
      } catch {
        html = null;
      }
      if (panel.dataset.key === key && ddlView) ddlView.innerHTML = html || escapeHtml(ddl);
    } catch (error) {
      if (panel.dataset.key === key && ddlView) ddlView.innerHTML = `<span class="danger-text">${escapeHtml(errorMessage(error))}</span>`;
    }
  }

  // Info pane for the object list's current scope when nothing in it is selected.
  function showScopeInfo(schema = objectsState.schema) {
    const connection = connectionLookup.get(objectsState.connectionId);
    if (!connection) {
      clearInfo();
      return;
    }
    const panel = $("#inspector-object");
    if (panel) {
      panel.dataset.key = `${connection.id}|scope|${schema || ""}`;
      delete panel.dataset.ddl;
      delete panel.dataset.table;
    }
    updateDdlActions(false);
    const ddl = $("#object-ddl");
    if (ddl) ddl.innerHTML = '<span class="muted">选择表或视图后显示建表语句。</span>';
    const protocol = connection.protocol;
    if (!schema || protocolFamily(protocol) === "kv") {
      renderInfoCard({
        kind: "connection",
        protocol,
        name: connection.name,
        typeLabel: `${protocolMeta[protocol]?.label || protocol} 连接`,
        location: [],
        props: [
          ["主机", connection.hostSummary],
          ["默认数据库", protocolFamily(protocol) === "kv" ? `DB ${connection.redisDb ?? 0}` : connection.database || "—"],
          ["TLS", connection.tlsEnabled ? "已启用" : "未启用"],
          ["SSH 隧道", connection.hasSsh ? "已配置" : "未使用"],
          ["数据库数", protocolFamily(protocol) === "sql" ? formatCount(metaFor(connection.id).schemas?.length) : null],
        ],
      });
      return;
    }
    const stats = metaFor(connection.id).stats.get(schema);
    const tables = stats?.filter((item) => !/VIEW/i.test(item.objectType)) || [];
    const views = stats?.filter((item) => /VIEW/i.test(item.objectType)) || [];
    const total = stats ? stats.reduce((sum, item) => sum + Number(item.dataBytes || 0) + Number(item.indexBytes || 0), 0) : null;
    renderInfoCard({
      kind: "database",
      name: schema,
      typeLabel: protocol === "postgres" ? "模式" : protocol === "sqlite" ? "SQLite 数据库" : "数据库",
      location: [["t-connection", connection.name]],
      props: [
        ["表", stats ? String(tables.length) : "…"],
        ["视图", stats ? String(views.length) : "…"],
        ["总大小", protocol === "sqlite" || !stats ? null : formatBytes(total)],
        ["已保存的查询", String([...queryDrafts.values()].filter((draft) => draft.connectionId === connection.id).length || 0)],
      ],
    });
  }

  /* ---------- objects page (pinned "对象" tab) ---------- */

  const objectsState = { connectionId: null, schema: null, category: "table", selected: null, filter: "", sort: { key: "name", dir: 1 }, renderToken: 0 };
  const objectTypeahead = { text: "", at: 0 };

  function loadObjectStats(connectionId, schema, { refresh = false } = {}) {
    const entry = metaFor(connectionId);
    if (refresh) {
      entry.stats.delete(schema);
      entry.tables.delete(schema);
      entry.statsPromises.delete(schema);
    }
    if (entry.stats.has(schema)) return Promise.resolve(entry.stats.get(schema));
    if (!entry.statsPromises.has(schema)) {
      const pending = backendCall("sql_object_stats", { connectionId, schema })
        .then((stats) => {
          if (entry.statsPromises.get(schema) === pending) {
            entry.stats.set(schema, stats);
            if (!entry.tables.has(schema)) entry.tables.set(schema, stats.map((item) => ({ name: item.name, type: item.objectType })));
          }
          return stats;
        })
        .finally(() => {
          if (entry.statsPromises.get(schema) === pending) entry.statsPromises.delete(schema);
        });
      entry.statsPromises.set(schema, pending);
    }
    return entry.statsPromises.get(schema);
  }

  function setObjectsScope(connectionId, schema = null, options = {}) {
    const connection = connectionLookup.get(connectionId);
    if (connection?.protocol === "sqlite") schema = "main";
    const changed = objectsState.connectionId !== connectionId || objectsState.schema !== schema;
    objectsState.connectionId = connectionId || null;
    objectsState.schema = schema || null;
    if (options.category) objectsState.category = options.category;
    if (connection && connection.id !== activeConnectionId) setActiveConnectionState(connection);
    activeSchema = schema || null;
    if (changed) {
      activeTable = null;
      activePrimaryKey = null;
      objectsState.selected = options.select || null;
      objectsState.filter = "";
      const input = $("#object-filter");
      if (input) input.value = "";
    } else if (options.select) {
      objectsState.selected = options.select;
    }
    if (options.show !== false) showWorkspace("objects");
    else if (currentWorkspace() === "objects") renderObjectsView();
    if (!objectsState.selected) showScopeInfo();
  }

  function showObjectsCategory(category) {
    if (!objectsState.connectionId && activeConnectionId) objectsState.connectionId = activeConnectionId;
    const connection = connectionLookup.get(objectsState.connectionId);
    if (connection && !objectsState.schema && protocolFamily(connection.protocol) === "sql") {
      objectsState.schema = (connection.id === activeConnectionId ? activeSchema : null) || defaultSchemaFor(connection.id);
    }
    if (connection && connection.id !== activeConnectionId) setActiveConnectionState(connection);
    activeSchema = objectsState.schema;
    activeTable = null;
    activePrimaryKey = null;
    objectsState.category = category;
    objectsState.selected = null;
    showWorkspace("objects");
    showScopeInfo();
  }

  function selectObjectFromTree(target) {
    const connection = connectionLookup.get(target.connectionId);
    const schema = connection?.protocol === "sqlite" ? "main" : target.schema || null;
    const category = target.objectType === "view" ? "view" : "table";
    const sameScope = objectsState.connectionId === target.connectionId && objectsState.schema === schema;
    objectsState.connectionId = target.connectionId;
    objectsState.schema = schema;
    if (!sameScope) {
      objectsState.filter = "";
      const input = $("#object-filter");
      if (input) input.value = "";
    }
    objectsState.category = category;
    objectsState.selected = target.table;
    if (currentWorkspace() === "objects") renderObjectsView();
    showObjectInfo({ connectionId: target.connectionId, schema: target.schema, table: target.table, objectType: target.objectType });
  }

  function objectColumns(protocol, category) {
    if (category === "database") return [{ key: "name", label: "名称", width: 260 }, { key: "tables", label: "对象数", numeric: true, width: 110 }];
    if (category === "query") return [{ key: "name", label: "名称", width: 260 }, { key: "updated", label: "修改日期", width: 180 }, { key: "preview", label: "内容", width: 280 }];
    if (protocol === "mysql") {
      return category === "view"
        ? [{ key: "name", label: "名称", width: 260 }, { key: "collation", label: "排序规则", width: 190 }, { key: "comment", label: "注释", width: 220 }]
        : [{ key: "name", label: "名称", width: 260 }, { key: "rows", label: "行", numeric: true, width: 90 }, { key: "capacity", label: "容量", numeric: true, width: 110 }, { key: "dataBytes", label: "数据长度", numeric: true, width: 110 }, { key: "indexBytes", label: "索引长度", numeric: true, width: 110 }, { key: "engine", label: "引擎", width: 96 }, { key: "updated", label: "修改日期", width: 160 }, { key: "comment", label: "注释", width: 220 }];
    }
    if (protocol === "postgres") {
      return category === "view"
        ? [{ key: "name", label: "名称", width: 260 }, { key: "owner", label: "所有者", width: 150 }, { key: "comment", label: "注释", width: 220 }]
        : [{ key: "name", label: "名称", width: 260 }, { key: "rows", label: "行", numeric: true, width: 110 }, { key: "dataBytes", label: "数据大小", numeric: true, width: 110 }, { key: "indexBytes", label: "索引大小", numeric: true, width: 110 }, { key: "owner", label: "所有者", width: 130 }, { key: "comment", label: "注释", width: 220 }];
    }
    return [{ key: "name", label: "名称", width: 260 }, { key: "objectType", label: "类型", width: 120 }];
  }

  function objectValue(item, key) {
    if (key === "capacity") {
      if (item.dataBytes == null && item.indexBytes == null) return null;
      return Number(item.dataBytes || 0) + Number(item.indexBytes || 0);
    }
    return item[key];
  }

  function objectCell(item, column) {
    const value = objectValue(item, column.key);
    if (column.key === "rows") return formatCount(value) ?? "";
    if (column.key === "dataBytes" || column.key === "indexBytes" || column.key === "capacity") return formatSize(value) ?? "";
    if (column.key === "objectType") return /VIEW/i.test(value || "") ? "视图" : "表";
    return value ?? "";
  }

  function objectsForCategory(stats) {
    const category = objectsState.category;
    return (stats || []).filter((item) => (category === "view" ? /VIEW/i.test(item.objectType) : !/VIEW/i.test(item.objectType)));
  }

  function sortedObjects(items, columns) {
    const { key, dir } = objectsState.sort;
    const column = columns.find((item) => item.key === key) || columns[0];
    const needle = objectsState.filter.trim().toLowerCase();
    const filtered = needle ? items.filter((item) => String(item.name).toLowerCase().includes(needle) || String(item.comment || "").toLowerCase().includes(needle)) : items;
    return [...filtered].sort((a, b) => {
      const left = objectValue(a, column.key);
      const right = objectValue(b, column.key);
      if (column.numeric) return ((Number(left ?? -1)) - Number(right ?? -1)) * dir;
      return String(left ?? "").localeCompare(String(right ?? ""), "zh-CN", { numeric: true }) * dir;
    });
  }

  function updateObjectsToolbar(connection) {
    const category = objectsState.category;
    const sql = connection && protocolFamily(connection.protocol) === "sql";
    const isSearch = connection && protocolFamily(connection.protocol) === "search";
    const toolbar = $(".objects-toolbar");
    if (toolbar) toolbar.hidden = Boolean(isSearch);
    const inSchema = sql && objectsState.schema;
    const selected = Boolean(objectsState.selected);
    const scope = inSchema ? category : "database";
    const labels = {
      table: { open: "打开表", create: "新建表", design: "设计表", drop: "删除表" },
      view: { open: "打开视图", create: "新建视图", design: "编辑视图", drop: "删除视图" },
      query: { open: "打开查询", create: "新建查询", design: "重命名", drop: "删除查询" },
      database: { open: connection?.protocol === "postgres" ? "打开模式" : "打开数据库", create: connection?.protocol === "postgres" ? "新建模式" : "新建数据库", design: "", drop: connection?.protocol === "postgres" ? "删除模式" : "删除数据库" },
    }[scope];
    const namespaceManage = sql && !inSchema && connection.protocol !== "sqlite";
    $$(".objects-toolbar .ot-button").forEach((button) => {
      const action = button.dataset.action;
      let visible = true;
      if (!sql) visible = action === "object-refresh";
      else if (!inSchema) visible = namespaceManage
        ? ["object-open", "object-new", "object-drop", "object-refresh"].includes(action)
        : ["object-open", "object-refresh"].includes(action);
      else if (category === "view") visible = ["object-open", "object-design", "object-new", "object-drop", "export", "object-refresh"].includes(action);
      else if (category === "query") visible = ["object-open", "object-design", "object-new", "object-drop", "query-history", "object-refresh"].includes(action);
      else visible = action !== "query-history";
      button.hidden = !visible;
      button.disabled = button.hasAttribute("data-needs-selection") && !selected;
    });
    const open = $('.objects-toolbar [data-object-label="open"]');
    if (open) open.textContent = labels.open;
    const newButton = $('.objects-toolbar [data-action="object-new"]');
    const designButton = $('.objects-toolbar [data-action="object-design"]');
    const dropButton = $('.objects-toolbar [data-action="object-drop"]');
    if (newButton) newButton.lastChild.textContent = labels.create;
    if (designButton) designButton.lastChild.textContent = labels.design;
    if (dropButton) dropButton.lastChild.textContent = labels.drop;
    $$(".objects-toolbar .ot-sep").forEach((sep) => { sep.hidden = !(inSchema && category === "table"); });
    const search = $(".objects-toolbar .ot-search");
    if (search) search.hidden = !sql;
  }

  function setStatusCenter(text) {
    const center = $("#status-center");
    if (center && backendAvailable) center.textContent = text;
  }

  function renderObjectsScope(connection, schema, counts) {
    const scope = $("#objects-scope");
    if (!scope) return;
    if (!connection) {
      scope.innerHTML = "";
      scope.hidden = true;
      return;
    }
    scope.hidden = false;
    const protocol = connection.protocol;
    if (currentWorkspace() === "objects") {
      const breadcrumb = $("#status-breadcrumb");
      if (breadcrumb) breadcrumb.textContent = schema || "";
    }
    const tabs = schema && protocolFamily(protocol) === "sql"
      ? [["table", "表", counts.table], ["view", "视图", counts.view], ["query", "查询", counts.query]]
          .map(([key, label, count]) => `<button class="scope-tab${objectsState.category === key ? " active" : ""}" data-object-category="${key}">${label}<span>${count ?? ""}</span></button>`).join("")
      : "";
    scope.innerHTML = `
      <div class="scope-path">
        ${protocolBadgeHtml(protocol)}
        ${schema && protocol !== "sqlite" ? `<button class="scope-link" data-objects-scope="connection" title="查看全部数据库">${escapeHtml(connection.name)}</button><span class="crumb-sep">›</span><svg class="scope-db"><use href="#o-db"/></svg><b>${escapeHtml(schema)}</b>` : `<b>${escapeHtml(connection.name)}</b>`}
      </div>
      <div class="scope-tabs">${tabs}</div>`;
  }

  function objectsEmpty(html, modifier = "") {
    const list = $("#objects-list");
    if (list) list.innerHTML = `<div class="objects-empty${modifier ? ` ${modifier}` : ""}">${html}</div>`;
  }

  function welcomeHtml() {
    const descriptions = { mysql: "MySQL 5.7 / 8.x、MariaDB", postgres: "PostgreSQL 12+", sqlite: "本机 .sqlite / .db 文件", redis: "单机、哨兵与集群", elasticsearch: "Elasticsearch 7.x / 8.x、OpenSearch" };
    return `<div class="welcome">
      <div class="brand-lockup"><span class="welcome-mark"><svg aria-hidden="true"><use href="#brand-luxbase"/></svg></span><span class="brand-wordmark"><b>Lux</b>Base</span></div>
      <p>先创建一个连接。连接配置与密码都加密保存在本机的 LuxBase 本地保险库里。</p>
      <div class="welcome-protocols">${Object.entries(protocolMeta).map(([key, meta]) => `<button class="welcome-card" data-action="new-connection" data-protocol="${key}">${protocolBadgeHtml(key)}<b>${meta.label}</b><small>${descriptions[key] || ""}</small></button>`).join("")}</div>
      <div class="welcome-tips"><span><kbd>⌘N</kbd> 新建连接</span><span><kbd>⌘T</kbd> 新建查询</span><span><kbd>⌘K</kbd> 搜索连接与表</span><span><kbd>⇧⌘/</kbd> 全部快捷键</span></div>
      <div class="welcome-credit">charl 开发并构建 · v2.1.0</div>
    </div>`;
  }

  async function renderObjectsView() {
    const list = $("#objects-list");
    if (!list) return;
    const token = ++objectsState.renderToken;
    const connection = connectionLookup.get(objectsState.connectionId) || null;
    if (!connection) objectsState.connectionId = null;
    updateObjectsToolbar(connection);
    syncToolbarState(currentWorkspace());
    if (!savedConnections.length) {
      renderObjectsScope(null);
      objectsEmpty(welcomeHtml());
      setStatusCenter("");
      return;
    }
    if (!connection) {
      renderObjectsScope(null);
      objectsEmpty(`<div class="objects-hint"><strong>选择一个连接</strong><span>在左侧单击连接或数据库，这里会列出它的表、视图和已保存的查询。</span></div>
        <div class="connection-cards">${savedConnections.map((item) => `<button class="connection-card" data-connection-select="${escapeHtml(item.id)}">${protocolBadgeHtml(item.protocol)}<span class="connection-card-copy"><b>${escapeHtml(item.name)}</b><small>${escapeHtml(item.hostSummary)}</small></span><span class="connection-card-proto">${escapeHtml(protocolMeta[item.protocol]?.label || item.protocol)}</span></button>`).join("")}</div>`, "objects-empty-top");
      setStatusCenter(`${savedConnections.length} 个连接`);
      return;
    }
    const protocol = connection.protocol;
    if (protocolFamily(protocol) === "kv") {
      renderObjectsScope(connection, null, {});
      objectsEmpty(`<div class="objects-hint"><strong>Redis 没有表结构</strong><span>在左侧展开 DB 用 SCAN 浏览键，或用命令控制台直接执行 Redis 命令。</span><div class="objects-hint-actions"><button class="primary-button" data-workspace="redis">打开键空间</button><button class="secondary-button" data-action="view-console">命令控制台</button></div></div>`);
      setStatusCenter("");
      return;
    }
    if (protocolFamily(protocol) === "search") {
      renderObjectsScope(connection, null, {});
      renderEsObjectsView(connection);
      return;
    }
    const schema = objectsState.schema;
    const entry = metaFor(connection.id);
    const draftCount = [...queryDrafts.values()].filter((draft) => draft.connectionId === connection.id).length;
    const counts = () => {
      const stats = entry.stats.get(schema);
      return { table: stats ? objectsForCategoryOf(stats, "table").length : null, view: stats ? objectsForCategoryOf(stats, "view").length : null, query: draftCount };
    };
    renderObjectsScope(connection, schema, counts());
    let items;
    let columns;
    let rowKind;
    try {
      if (!schema) {
        if (!entry.schemas) list.innerHTML = '<div class="objects-loading">正在读取数据库列表…</div>';
        const schemas = await loadSchemas(connection.id);
        if (token !== objectsState.renderToken) return;
        items = schemas.map((name) => ({ name, tables: entry.stats.get(name)?.length ?? entry.tables.get(name)?.length ?? null }));
        columns = objectColumns(protocol, "database");
        rowKind = "database";
      } else if (objectsState.category === "query") {
        const drafts = [...queryDrafts.values()].filter((draft) => draft.connectionId === connection.id);
        items = drafts.map((draft) => ({ name: draft.name, id: String(draft.id), updated: new Date(draft.updatedAt).toLocaleString("zh-CN", { hour12: false }), preview: summarizeSql(draft.sqlText, 120) }));
        columns = objectColumns(protocol, "query");
        rowKind = "query";
      } else {
        if (!entry.stats.has(schema)) list.innerHTML = '<div class="objects-loading">正在读取对象…</div>';
        const stats = await loadObjectStats(connection.id, schema);
        if (token !== objectsState.renderToken) return;
        renderObjectsScope(connection, schema, counts());
        items = objectsForCategory(stats);
        columns = objectColumns(protocol, objectsState.category);
        rowKind = objectsState.category;
      }
    } catch (error) {
      if (token !== objectsState.renderToken) return;
      objectsEmpty(`<div class="objects-hint danger"><strong>读取失败</strong><span>${escapeHtml(errorMessage(error))}</span><div class="objects-hint-actions"><button class="secondary-button" data-action="object-refresh">重试</button></div></div>`);
      return;
    }
    const rows = sortedObjects(items, columns);
    if (objectsState.selected && !rows.some((item) => (item.id || item.name) === objectsState.selected)) {
      objectsState.selected = null;
      activeTable = null;
      activePrimaryKey = null;
    }
    if (!objectsState.selected && currentWorkspace() === "objects") showScopeInfo();
    updateObjectsToolbar(connection);
    const symbol = { database: "o-db", table: "o-table", view: "o-view", query: "o-query" }[rowKind];
    const { key: sortKey, dir } = objectsState.sort;
    const emptyText = objectsState.filter
      ? `没有名称包含“${escapeHtml(objectsState.filter)}”的对象`
      : { database: "没有可见的数据库", table: "这个库还没有表", view: "这个库没有视图", query: "这个连接还没有保存过查询。在查询标签页按 ⌘S 保存。" }[rowKind];
    const tableMinWidth = columns.reduce((sum, column) => sum + (column.width || 160), 0);
    list.innerHTML = rows.length ? `
      <table class="object-table" data-resize-key="objects:${rowKind}" style="width:${Math.max(list.clientWidth || 0, tableMinWidth)}px">
        <colgroup>${columns.map((column) => `<col${column.width ? ` style="width:${column.width}px"` : ""}>`).join("")}</colgroup>
        <thead><tr>${columns.map((column, index) => `<th data-object-sort="${column.key}" class="${column.numeric ? "numeric" : ""}${sortKey === column.key ? ` sorted ${dir > 0 ? "asc" : "desc"}` : ""}">${escapeHtml(column.label)}<span class="col-resizer" data-gcol="${index}" title="拖动调整列宽"></span></th>`).join("")}</tr></thead>
        <tbody>${rows.map((item) => {
          const id = item.id || item.name;
          const attrs = rowKind === "database"
            ? `data-sql-database="${escapeHtml(item.name)}" data-connection-id="${escapeHtml(connection.id)}"`
            : rowKind === "query"
              ? `data-draft-row="${escapeHtml(item.id)}"`
              : `data-open-table="${escapeHtml(item.name)}" data-table-schema="${escapeHtml(schema)}" data-object-type="${escapeHtml(item.objectType || rowKind)}" data-connection-id="${escapeHtml(connection.id)}"`;
          return `<tr data-object-row="${escapeHtml(id)}" ${attrs}${objectsState.selected === id ? ' class="selected"' : ""}>${columns.map((column, index) => index === 0
            ? `<td class="obj-name"><svg class="obj-icon"><use href="#${symbol}"/></svg><span title="${escapeHtml(item.name)}">${escapeHtml(item.name)}</span></td>`
            : `<td class="${column.numeric ? "numeric" : ""}${column.key === "comment" || column.key === "preview" ? " muted" : ""}">${escapeHtml(objectCell(item, column))}</td>`).join("")}</tr>`;
        }).join("")}</tbody>
      </table>` : `<div class="objects-empty"><div class="objects-hint"><span>${emptyText}</span>${rowKind === "table" && !objectsState.filter ? '<div class="objects-hint-actions"><button class="primary-button" data-action="object-new">新建表</button></div>' : ""}</div></div>`;
    list.querySelector("tr.selected")?.scrollIntoView({ block: "nearest" });
    const noun = { database: "个数据库", table: "张表", view: "个视图", query: "个查询" }[rowKind];
    setStatusCenter(objectsState.selected ? `已选择 1 个对象 · 共 ${rows.length} ${noun}` : `${rows.length} ${noun}`);
    applyGenericColWidths();
  }

  function objectsForCategoryOf(stats, category) {
    return (stats || []).filter((item) => (category === "view" ? /VIEW/i.test(item.objectType) : !/VIEW/i.test(item.objectType)));
  }

  function selectedObjectRow() {
    return objectsState.selected ? $(`#objects-list tr[data-object-row="${CSS.escape(objectsState.selected)}"]`) : null;
  }

  function selectObjectRow(row, { info = true } = {}) {
    if (!row) return;
    objectsState.selected = row.dataset.objectRow;
    $$("#objects-list tr.selected").forEach((item) => item.classList.remove("selected"));
    row.classList.add("selected");
    row.scrollIntoView({ block: "nearest" });
    const connection = connectionLookup.get(objectsState.connectionId);
    updateObjectsToolbar(connection);
    const total = $$("#objects-list tbody tr").length;
    setStatusCenter(`已选择 1 个对象 · 共 ${total} 个`);
    if (!info) return;
    if (row.dataset.openTable) {
      activeTable = { connectionId: objectsState.connectionId, schema: row.dataset.tableSchema, table: row.dataset.openTable, objectType: row.dataset.objectType };
      activeSchema = row.dataset.tableSchema;
      if (objectsState.connectionId !== activeConnectionId) setActiveConnectionState(connection);
      showObjectInfo(activeTable);
      const group = $(`.connection-group[data-connection-id="${CSS.escape(objectsState.connectionId)}"]`);
      $$(".tree-row.selected").forEach((item) => item.classList.remove("selected"));
      group && $$("[data-open-table]", group).find((item) => item.dataset.openTable === activeTable.table && item.dataset.tableSchema === activeTable.schema)?.classList.add("selected");
    } else if (row.dataset.sqlDatabase) {
      activeTable = null;
      showScopeInfo(row.dataset.sqlDatabase);
    } else if (row.dataset.draftRow) {
      const draft = queryDrafts.get(row.dataset.draftRow);
      if (draft) {
        clearInfo();
        const panel = $("#inspector-object");
        panel.dataset.key = `query|${draft.connectionId}|${draft.id}|${operationId()}`;
        panel.dataset.connectionId = draft.connectionId;
        panel.dataset.schema = objectsState.schema || "";
        panel.dataset.ddl = draft.sqlText;
        activeTable = null;
        activePrimaryKey = null;
        renderInfoCard({ kind: "query", name: draft.name, typeLabel: "已保存的查询", location: [["t-connection", connection?.name || ""]], props: [["修改日期", new Date(draft.updatedAt).toLocaleString("zh-CN", { hour12: false })], ["长度", `${draft.sqlText.length} 字符`]] });
        const ddl = $("#object-ddl");
        if (ddl) ddl.textContent = draft.sqlText;
        $("#ddl-title").textContent = `${draft.name} · SQL`;
        updateDdlActions(false, true);
      }
    }
  }

  function openObjectRow(row) {
    if (!row) return;
    if (row.dataset.openTable) {
      openTableTab(objectsState.connectionId, row.dataset.tableSchema, row.dataset.openTable, row.dataset.objectType);
    } else if (row.dataset.sqlDatabase) {
      setObjectsScope(objectsState.connectionId, row.dataset.sqlDatabase);
      const group = $(`.connection-group[data-connection-id="${CSS.escape(objectsState.connectionId)}"]`);
      const button = group && $$("[data-sql-database]", group).find((item) => item.dataset.sqlDatabase === row.dataset.sqlDatabase);
      if (button) loadDatabaseObjects(button);
    } else if (row.dataset.draftRow) {
      const draft = queryDrafts.get(row.dataset.draftRow);
      const existing = draft && sqlTabs.find((tab) => tab.draftId === String(draft.id));
      if (existing) activateSqlTab(existing.id, { focusEditor: true });
      else if (draft) newQueryTab({ connectionId: draft.connectionId, schema: objectsState.schema, sql: draft.sqlText, title: draft.name, draftId: String(draft.id) });
    }
  }

  function openSelectedObject() {
    const row = selectedObjectRow();
    if (row) openObjectRow(row);
    else toast("请先选择对象", "在列表中单击一个对象，或直接双击打开。", "danger");
  }

  function selectObjectAsActiveTable() {
    const row = selectedObjectRow();
    if (!row?.dataset.openTable) {
      toast("请先选择一张表", "在对象列表中单击要操作的表。", "danger");
      return false;
    }
    const connection = connectionLookup.get(objectsState.connectionId);
    if (connection && connection.id !== activeConnectionId) setActiveConnectionState(connection);
    activeTable = { connectionId: objectsState.connectionId, schema: row.dataset.tableSchema, table: row.dataset.openTable, objectType: row.dataset.objectType };
    activeSchema = row.dataset.tableSchema;
    return true;
  }

  async function refreshObjects() {
    const connectionId = objectsState.connectionId;
    if (!connectionId) return;
    const entry = metaFor(connectionId);
    if (objectsState.schema) {
      await loadObjectStats(connectionId, objectsState.schema, { refresh: true }).catch(() => null);
      const group = $(`.connection-group[data-connection-id="${CSS.escape(connectionId)}"]`);
      const button = group && $$("[data-sql-database]", group).find((item) => item.dataset.sqlDatabase === objectsState.schema);
      const node = button?.closest(".database-node");
      if (node?.dataset.objectsLoaded === "true") {
        delete node.dataset.objectsLoaded;
        loadDatabaseObjects(button);
      }
    } else {
      entry.schemas = null;
    }
    if (objectsState.category === "query") {
      const drafts = await backendCall("list_query_drafts", { connectionId }).catch(() => []);
      replaceConnectionDrafts(connectionId, drafts);
    }
    await renderObjectsView();
    if (!objectsState.selected) showScopeInfo();
  }

  function bindObjectsList() {
    const list = $("#objects-list");
    if (!list || list.dataset.bound === "true") return;
    list.dataset.bound = "true";
    list.addEventListener("mousedown", (event) => {
      if (event.target.closest(".col-resizer")) return;
      const header = event.target.closest("th[data-object-sort]");
      if (header) {
        const key = header.dataset.objectSort;
        objectsState.sort = { key, dir: objectsState.sort.key === key ? -objectsState.sort.dir : 1 };
        renderObjectsView();
        return;
      }
      const row = event.target.closest("tr[data-object-row]");
      if (row) selectObjectRow(row);
      else if (event.target.closest(".object-table") || event.target === list) {
        objectsState.selected = null;
        activeTable = null;
        activePrimaryKey = null;
        $$("#objects-list tr.selected").forEach((item) => item.classList.remove("selected"));
        updateObjectsToolbar(connectionLookup.get(objectsState.connectionId));
        showScopeInfo();
      }
    });
    list.addEventListener("dblclick", (event) => {
      const row = event.target.closest("tr[data-object-row]");
      if (!row) return;
      event.stopPropagation();
      openObjectRow(row);
    });
    list.addEventListener("keydown", (event) => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey) return;
      const rows = $$("#objects-list tbody tr[data-object-row]");
      if (!rows.length) return;
      event.stopPropagation();
      const index = rows.indexOf(selectedObjectRow());
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        const next = event.key === "ArrowDown" ? Math.min(rows.length - 1, index + 1) : Math.max(0, index < 0 ? 0 : index - 1);
        selectObjectRow(rows[next]);
      } else if (event.key === "Enter") {
        event.preventDefault();
        openObjectRow(rows[index] || rows[0]);
      } else if ((event.key === "Backspace" || event.key === "Delete") && rows[index]?.dataset.openTable && objectsState.category === "table") {
        event.preventDefault();
        if (selectObjectAsActiveTable()) runAction("drop-table");
      } else if (event.key.length === 1 && !event.metaKey && !event.ctrlKey && !event.altKey) {
        const now = Date.now();
        objectTypeahead.text = now - objectTypeahead.at > 900 ? event.key.toLowerCase() : objectTypeahead.text + event.key.toLowerCase();
        objectTypeahead.at = now;
        const match = rows.find((row) => row.dataset.objectRow.toLowerCase().startsWith(objectTypeahead.text));
        if (match) selectObjectRow(match);
      }
    });
    initEsColumnResize();
    initVerticalResize();
    initGenericColumnResize();
    // ES doc table widths are computed from the container; recompute when the window or
    // sidebar changes size so the table always fills the available width.
    let lastEsViewWidth = 0;
    const esViewEl = $("#view-es");
    if (esViewEl && typeof ResizeObserver !== "undefined") {
      new ResizeObserver((entries) => {
        const width = entries[0].contentRect.width;
        if (Math.abs(width - lastEsViewWidth) < 4) return;
        lastEsViewWidth = width;
        if (currentWorkspace() === "es" && esState.tab === "docs" && esState.index) renderEsDocsResults();
      }).observe(esViewEl);
    }
    let esResizeTimer = null;
    window.addEventListener("resize", () => {
      toggleFilterPanel(false);
      clearTimeout(esResizeTimer);
      esResizeTimer = setTimeout(() => {
        if (currentWorkspace() === "es" && esState.tab === "docs" && esState.index) renderEsDocsResults();
      }, 120);
    });
    let objectFilterTimer = null;
    $("#object-filter")?.addEventListener("input", (event) => {
      objectsState.filter = event.target.value;
      clearTimeout(objectFilterTimer);
      objectFilterTimer = setTimeout(() => renderObjectsView(), 150);
    });
    $("#object-filter")?.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        list.focus();
        const first = $("#objects-list tbody tr[data-object-row]");
        if (first) selectObjectRow(first);
      } else if (event.key === "Escape" && event.target.value) {
        event.stopPropagation();
        event.target.value = "";
        objectsState.filter = "";
        renderObjectsView();
      }
    });
  }

  /* ---------- view and database/schema management ---------- */

  let objectDdlEditor = null;
  let namespaceEditor = null;

  function viewDefinitionFromDdl(ddl) {
    const statement = splitSqlStatements(ddl || "")[0] || ddl || "";
    const match = /\bAS\s+([\s\S]*?)\s*;?\s*$/i.exec(statement);
    return match?.[1]?.trim() || "";
  }

  async function viewEditorModal(edit = false, draft = null) {
    const connectionId = draft?.connectionId || objectsState.connectionId || activeTable?.connectionId;
    const connection = connectionLookup.get(connectionId);
    const schema = draft?.schema || objectsState.schema || activeTable?.schema || defaultSchemaFor(connectionId);
    if (!connection || protocolFamily(connection.protocol) !== "sql" || !schema) {
      toast("无法编辑视图", "请先选择 SQL 连接中的数据库或模式。", "danger");
      return;
    }
    const selected = selectedObjectRow();
    const originalName = draft?.originalName || (edit ? (selected?.dataset.openTable || activeTable?.table) : null);
    const objectType = draft?.objectType || selected?.dataset.objectType || activeTable?.objectType || "view";
    if (edit && /materialized/i.test(objectType)) {
      toast("物化视图保持只读", "当前版本不会用普通 VIEW 语法替换 PostgreSQL 物化视图；请在查询编辑器中执行专用 DDL。", "danger");
      return;
    }
    if (edit && !originalName) {
      toast("请先选择视图", "在视图列表中选择要编辑的视图。", "danger");
      return;
    }
    let definition = draft?.definition || "";
    if (edit && !draft) {
      try {
        const ddl = await backendCall("sql_table_ddl", { connectionId, schema, table: originalName });
        definition = viewDefinitionFromDdl(ddl);
      } catch (error) {
        showOperationError(error, "视图定义读取失败");
        return;
      }
    }
    objectDdlEditor = { connectionId, protocol: connection.protocol, schema, edit, originalName, objectType };
    showModal(edit ? `编辑视图 ${originalName}` : "新建视图", edit ? "REPLACE VIEW" : "CREATE VIEW", `
      <div class="form-grid">
        <label class="full">视图名称<input id="view-name" value="${escapeHtml(draft?.name || originalName || "")}" ${edit ? "readonly" : ""} maxlength="128" spellcheck="false" /></label>
        <label class="full">SELECT 定义<textarea id="view-definition" class="modal-textarea view-definition" spellcheck="false" placeholder="SELECT ...">${escapeHtml(definition)}</textarea></label>
      </div>
      <div class="form-note"><svg><use href="#i-shield"/></svg>只接受一条 SELECT / WITH 查询。保存前会显示完整 DDL 并要求确认。</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="view-save">${edit ? "预览并替换" : "预览并创建"}</button></div>
    `, { wide: true });
  }

  async function saveView() {
    const state = objectDdlEditor;
    if (!state) return;
    const name = $("#view-name")?.value?.trim() || "";
    const definition = ($("#view-definition")?.value || "").trim().replace(/;\s*$/, "");
    if (!name) throw new Error("视图名称不能为空。");
    if (!/^(SELECT|WITH)\b/i.test(stripLeadingComments(definition)) || splitSqlStatements(definition).length !== 1) {
      throw new Error("视图定义必须是一条 SELECT 或 WITH 查询。");
    }
    const qualified = protocolQualified(state.protocol, state.schema, name);
    const statements = state.edit && state.protocol === "sqlite"
      ? [`DROP VIEW ${qualified}`, `CREATE VIEW ${qualified} AS\n${definition}`]
      : [`CREATE ${state.edit ? "OR REPLACE " : ""}VIEW ${qualified} AS\n${definition}`];
    const draft = { ...state, name, definition };
    const confirmed = await confirmChoice({
      title: `${state.edit ? "替换" : "创建"}视图 ${name}`,
      eyebrow: state.edit ? "REPLACE VIEW" : "CREATE VIEW",
      body: `<b>${escapeHtml(connectionLookup.get(state.connectionId)?.name || "")} / ${escapeHtml(state.schema)}</b><pre class="choice-sql">${escapeHtml(`${statements.join(";\n\n")};`)}</pre>`,
      confirmLabel: state.edit ? "替换视图" : "创建视图",
      danger: state.edit,
    });
    if (!confirmed) {
      await viewEditorModal(state.edit, draft);
      return;
    }
    const op = operationId();
    try {
      await backendCall("sql_apply_ddl", {
        connectionId: state.connectionId,
        operationId: op,
        statements: statements.map((sql, index) => ({ connectionId: state.connectionId, sql, params: [], operationId: `${op}-${index + 1}`, schema: state.schema })),
      });
      objectDdlEditor = null;
      closeModal();
      afterDdl(state.connectionId);
      setObjectsScope(state.connectionId, state.schema, { category: "view", select: name });
      await loadObjectStats(state.connectionId, state.schema, { refresh: true });
      await renderObjectsView();
      showObjectInfo({ connectionId: state.connectionId, schema: state.schema, table: name, objectType: "view" }, { refresh: true });
      toast(state.edit ? "视图已替换" : "视图已创建", `${state.schema}.${name} · 已写入审计日志。`);
      refreshAuditOnly();
    } catch (error) {
      objectDdlEditor = draft;
      showOperationError(error, state.edit ? "视图替换失败" : "视图创建失败");
    }
  }

  async function dropView() {
    const row = selectedObjectRow();
    const connectionId = objectsState.connectionId || activeTable?.connectionId;
    const schema = row?.dataset.tableSchema || objectsState.schema || activeTable?.schema;
    const name = row?.dataset.openTable || activeTable?.table;
    const connection = connectionLookup.get(connectionId);
    if (!connection || !schema || !name) {
      toast("请先选择视图", "在视图列表中选择要删除的视图。", "danger");
      return;
    }
    const objectType = row?.dataset.objectType || activeTable?.objectType || "view";
    const sql = `DROP ${/materialized/i.test(objectType) ? "MATERIALIZED VIEW" : "VIEW"} ${protocolQualified(connection.protocol, schema, name)}`;
    const confirmed = await confirmChoice({
      title: `删除视图 ${name}`,
      eyebrow: "DROP VIEW",
      body: `将永久删除 <b>${escapeHtml(schema)}.${escapeHtml(name)}</b> 的视图定义，不会删除基础表数据。<pre class="choice-sql">${escapeHtml(`${sql};`)}</pre>`,
      confirmLabel: "删除视图",
      danger: true,
    });
    if (!confirmed) return;
    const op = operationId();
    await backendCall("sql_apply_ddl", { connectionId, operationId: op, statements: [{ connectionId, sql, params: [], operationId: `${op}-1`, schema }] });
    for (const tab of sqlTabs.filter((item) => item.kind === "table" && item.connectionId === connectionId && item.schema === schema && item.table === name)) closeSqlTab(tab.id, { force: true });
    objectsState.selected = null;
    activeTable = null;
    afterDdl(connectionId);
    await loadObjectStats(connectionId, schema, { refresh: true });
    await renderObjectsView();
    toast("视图已删除", `${schema}.${name} · 基础表数据未改变。`, "danger");
    refreshAuditOnly();
  }

  function namespaceModal() {
    const connection = connectionLookup.get(objectsState.connectionId || activeConnectionId);
    if (!connection || !["mysql", "postgres"].includes(connection.protocol)) {
      toast("当前连接不支持", "MySQL 可管理数据库，PostgreSQL 可管理模式。", "danger");
      return;
    }
    namespaceEditor = { connectionId: connection.id, protocol: connection.protocol };
    const noun = connection.protocol === "postgres" ? "模式" : "数据库";
    showModal(`新建${noun}`, connection.protocol === "postgres" ? "CREATE SCHEMA" : "CREATE DATABASE", `
      <div class="form-grid"><label class="full">${noun}名称<input id="namespace-name" maxlength="128" spellcheck="false" /></label></div>
      <div class="form-note">名称会按 ${connection.protocol === "postgres" ? "PostgreSQL" : "MySQL"} 标识符规则引用；不会覆盖同名对象。</div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="namespace-create-save">创建${noun}</button></div>
    `);
    $("#namespace-name")?.focus();
  }

  async function createNamespace() {
    const state = namespaceEditor;
    const name = $("#namespace-name")?.value?.trim() || "";
    if (!state || !name) throw new Error("名称不能为空。");
    const sql = `CREATE ${state.protocol === "postgres" ? "SCHEMA" : "DATABASE"} ${quoteIdent(name, state.protocol)}`;
    const confirmed = await confirmChoice({
      title: `${state.protocol === "postgres" ? "创建模式" : "创建数据库"} ${name}`,
      eyebrow: state.protocol === "postgres" ? "CREATE SCHEMA" : "CREATE DATABASE",
      body: `<pre class="choice-sql">${escapeHtml(`${sql};`)}</pre>`,
      confirmLabel: "创建",
    });
    if (!confirmed) {
      namespaceEditor = state;
      namespaceModal();
      $("#namespace-name").value = name;
      return;
    }
    const op = operationId();
    await backendCall("sql_apply_ddl", { connectionId: state.connectionId, operationId: op, statements: [{ connectionId: state.connectionId, sql, params: [], operationId: `${op}-1`, schema: defaultSchemaFor(state.connectionId) }] });
    closeModal();
    metaCache.delete(state.connectionId);
    const group = $(`.connection-group[data-connection-id="${CSS.escape(state.connectionId)}"]`);
    if (group) {
      group.dataset.loaded = "false";
      const children = $(".tree-children", group);
      if (children) children.innerHTML = "";
      await loadConnectionTree(group);
    }
    setObjectsScope(state.connectionId, name);
    toast(state.protocol === "postgres" ? "模式已创建" : "数据库已创建", name);
    refreshAuditOnly();
  }

  async function dropNamespace() {
    const row = selectedObjectRow();
    const connectionId = objectsState.connectionId || activeConnectionId;
    const connection = connectionLookup.get(connectionId);
    const name = row?.dataset.sqlDatabase || activeSchema;
    if (!connection || !name || !["mysql", "postgres"].includes(connection.protocol)) {
      toast("请先选择对象", "在数据库或模式列表中选择要删除的对象。", "danger");
      return;
    }
    if (name === defaultSchemaFor(connectionId)) {
      toast("不能删除默认命名空间", "请先在连接设置中切换默认数据库；PostgreSQL 的默认 public 模式不在此处删除。", "danger");
      return;
    }
    const noun = connection.protocol === "postgres" ? "模式" : "数据库";
    const sql = `DROP ${connection.protocol === "postgres" ? "SCHEMA" : "DATABASE"} ${quoteIdent(name, connection.protocol)}`;
    const impact = connection.protocol === "postgres"
      ? "仅当模式为空时才会成功；不会自动 CASCADE 删除其中对象。"
      : "MySQL 会连同其中全部表、视图和数据一起永久删除。";
    const confirmed = await confirmChoice({
      title: `删除${noun} ${name}`,
      eyebrow: connection.protocol === "postgres" ? "DROP SCHEMA RESTRICT" : "DROP DATABASE",
      body: `<b>${escapeHtml(impact)}</b><pre class="choice-sql">${escapeHtml(`${sql};`)}</pre>`,
      confirmLabel: `删除${noun}`,
      danger: true,
    });
    if (!confirmed) return;
    const op = operationId();
    await backendCall("sql_apply_ddl", { connectionId, operationId: op, statements: [{ connectionId, sql, params: [], operationId: `${op}-1`, schema: defaultSchemaFor(connectionId) }] });
    for (const tab of sqlTabs.filter((item) => item.kind === "table" && item.connectionId === connectionId && item.schema === name)) closeSqlTab(tab.id, { force: true });
    for (const tab of sqlTabs.filter((item) => item.kind === "query" && item.connectionId === connectionId && item.schema === name)) tab.schema = null;
    renderSqlTabs();
    persistSqlTabs();
    objectsState.selected = null;
    activeSchema = null;
    metaCache.delete(connectionId);
    const group = $(`.connection-group[data-connection-id="${CSS.escape(connectionId)}"]`);
    if (group) {
      group.dataset.loaded = "false";
      const children = $(".tree-children", group);
      if (children) children.innerHTML = "";
      await loadConnectionTree(group);
    }
    setObjectsScope(connectionId, null);
    await renderObjectsView();
    toast(`${noun}已删除`, `${name} · 操作已写入审计日志。`, "danger");
    refreshAuditOnly();
  }

  /* ---------- new table (designer in create mode) ---------- */

  async function tableCreateModal(connectionId, schema, draft = null) {
    const connection = connectionLookup.get(connectionId);
    if (!connection || protocolFamily(connection.protocol) !== "sql") {
      toast("无法新建表", "请先在左侧选择一个 SQL 连接和数据库。", "danger");
      return;
    }
    const protocol = connection.protocol;
    const targetSchema = protocol === "sqlite" ? "main" : schema || defaultSchemaFor(connectionId);
    if (!targetSchema) {
      toast("请先选择数据库", "新表会建在当前选中的数据库中。", "danger");
      return;
    }
    const idType = { mysql: ["bigint", "auto_increment"], postgres: ["bigint", "identity"], sqlite: ["INTEGER", ""] }[protocol];
    designer = draft || {
      create: true,
      connectionId,
      protocol,
      schema: targetSchema,
      table: "",
      tableComment: "",
      original: { columns: [], indexes: [], foreignKeys: [] },
      columns: [{ key: "c-id", orig: null, isNew: true, dropped: false, name: "id", base: idType[0], length: "", scale: "", suffix: "", notNull: true, pk: true, defaultValue: "", comment: protocol === "sqlite" ? "" : "主键", extra: idType[1] }],
      droppedIndexes: new Set(),
      newIndexes: [],
      droppedForeignKeys: new Set(),
      plan: null,
    };
    showModal(`新建表 · ${protocol === "sqlite" ? connection.name : targetSchema}`, "NEW TABLE", `
      <div class="designer-create-head">
        <label>表名<input id="designer-table-name" value="${escapeHtml(designer.table)}" placeholder="例如 orders" spellcheck="false" autocomplete="off" /></label>
        ${protocol !== "sqlite" ? `<label class="wide">注释<input id="designer-table-comment" value="${escapeHtml(designer.tableComment)}" placeholder="可选" /></label>` : ""}
      </div>
      <div class="designer-tabs" role="tablist">
        <button class="active" data-designer-tab="columns">字段</button>
        <button data-designer-tab="indexes">索引</button>
        <button data-designer-tab="sql">SQL 预览</button>
        <span class="designer-summary" id="designer-summary">填写表名后即可创建</span>
      </div>
      <datalist id="designer-types">${(DESIGNER_TYPES[protocol] || []).map((type) => `<option value="${escapeHtml(type)}"></option>`).join("")}</datalist>
      <section data-designer-panel="columns">
        <div class="designer-grid${protocol === "sqlite" ? " no-comment" : ""}">
          <div class="designer-head"><span>#</span><span>名称</span><span>类型</span><span>长度</span><span>小数点</span><span>不是 NULL</span><span>主键</span><span>默认值</span>${protocol !== "sqlite" ? "<span>注释</span>" : ""}<span></span></div>
          <div id="designer-columns"></div>
        </div>
        <button class="add-field" data-action="designer-add-column"><svg><use href="#i-plus"/></svg>添加字段</button>
      </section>
      <section data-designer-panel="indexes" hidden>
        <div class="designer-list" id="designer-indexes"></div>
        <button class="add-field" data-action="designer-add-index"><svg><use href="#i-plus"/></svg>添加索引</button>
      </section>
      <section data-designer-panel="sql" hidden>
        <textarea id="designer-sql" class="modal-textarea designer-sql" readonly spellcheck="false"></textarea>
      </section>
      <div id="designer-plan-error" class="form-note danger-note" role="alert" hidden></div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="secondary-button" data-designer-tab="sql">预览 SQL</button><button class="primary-button" data-action="designer-save" disabled>创建表…</button></div>
    `, { wide: true });
    renderDesigner();
    setTimeout(() => $("#designer-table-name")?.focus(), 40);
  }

  function createTablePlan(state) {
    const name = String(state.table || "").trim();
    if (!name) throw new Error("请填写表名。");
    if (/[;\0]/.test(name)) throw new Error("表名不能包含分号。");
    validateDesigner(state);
    const protocol = state.protocol;
    const q = (value) => quoteIdent(value, protocol);
    const table = protocol === "sqlite" ? q(name) : `${q(state.schema)}.${q(name)}`;
    const live = state.columns.filter((column) => !column.dropped);
    const keys = live.filter((column) => column.pk).map((column) => column.name.trim());
    const inlineKey = protocol === "sqlite" && keys.length === 1;
    const definitions = live.map((column) => {
      if (protocol === "mysql") return mysqlColumnDefinition(column, protocol);
      let definition = genericColumnDefinition(column, protocol, { inlinePrimaryKey: inlineKey && column.pk });
      if (protocol === "postgres" && /identity/i.test(column.extra || "")) definition += " GENERATED BY DEFAULT AS IDENTITY";
      return definition;
    });
    if (keys.length && !inlineKey) definitions.push(`PRIMARY KEY (${keys.map(q).join(", ")})`);
    const comment = String(state.tableComment || "").trim();
    const statements = [`CREATE TABLE ${table} (\n  ${definitions.join(",\n  ")}\n)${protocol === "mysql" && comment ? ` COMMENT ${sqlLiteral(comment, protocol)}` : ""}`];
    if (protocol === "postgres") {
      if (comment) statements.push(`COMMENT ON TABLE ${table} IS ${sqlLiteral(comment, protocol)}`);
      for (const column of live) {
        if (String(column.comment || "").trim()) statements.push(`COMMENT ON COLUMN ${table}.${q(column.name.trim())} IS ${sqlLiteral(column.comment, protocol)}`);
      }
    }
    for (const index of state.newIndexes) {
      if (!index.name.trim() && !index.columns.trim()) continue;
      if (!index.name.trim() || !index.columns.trim()) throw new Error("新索引需要同时填写名称和字段。");
      const columns = index.columns.split(",").map((value) => value.trim()).filter(Boolean);
      statements.push(`CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${q(index.name.trim())} ON ${table} (${columns.map(q).join(", ")})`);
    }
    return { statements, mode: "ddl" };
  }

  /* ---------- layout: collapsible / resizable panels ---------- */

  function applyLayout() {
    const main = $(".main-layout");
    if (main) {
      main.style.setProperty("--sidebar-width", `${layout.sidebarCollapsed ? 0 : layout.sidebarWidth}px`);
      main.style.setProperty("--inspector-width", `${layout.inspectorCollapsed ? 0 : layout.inspectorWidth}px`);
      main.classList.toggle("sidebar-collapsed", layout.sidebarCollapsed);
      main.classList.toggle("inspector-collapsed", layout.inspectorCollapsed);
    }
    $("#view-sql")?.style.setProperty("--editor-height", `${layout.editorHeight}px`);
    $$('[data-action="toggle-sidebar"]').forEach((button) => button.classList.toggle("active", !layout.sidebarCollapsed));
    $$('[data-action="toggle-inspector"]').forEach((button) => button.classList.toggle("active", !layout.inspectorCollapsed));
    if (gridState.columns.length) requestAnimationFrame(renderGridBody);
  }

  function saveLayout() {
    try {
      localStorage.setItem(LAYOUT_KEY, JSON.stringify(layout));
    } catch {
      // Layout persistence is best-effort.
    }
  }

  function restoreLayout() {
    try {
      Object.assign(layout, JSON.parse(localStorage.getItem(LAYOUT_KEY) || "{}"));
    } catch {
      // Ignore a corrupt saved layout and keep defaults.
    }
    applyLayout();
  }

  const THEME_MODES = ["system", "light", "dark"];
  const darkThemeQuery = window.matchMedia("(prefers-color-scheme: dark)");

  function themePreference() {
    try {
      const stored = localStorage.getItem(THEME_KEY);
      return THEME_MODES.includes(stored) ? stored : "system";
    } catch {
      // A blocked storage area only costs persistence; the resolved appearance still applies.
      return "system";
    }
  }

  function themeIsDark(preference) {
    const mode = THEME_MODES.includes(preference) ? preference : themePreference();
    return mode === "dark" || (mode === "system" && darkThemeQuery.matches);
  }

  // One place decides everything the appearance touches: the root attribute the stylesheets read,
  // the Monaco theme, the native window chrome, and the controls that show the current choice.
  function applyThemePreference(preference) {
    const mode = THEME_MODES.includes(preference) ? preference : "system";
    try {
      localStorage.setItem(THEME_KEY, mode);
    } catch {
      // Appearance persistence is best-effort, like the layout.
    }
    const dark = themeIsDark(mode);
    document.documentElement.dataset.theme = dark ? "dark" : "light";
    document.documentElement.dataset.themePreference = mode;
    window.__luxbaseSetEditorTheme?.(dark ? "dark" : "light");
    // Scoped to the settings control: the root element carries data-theme-preference too, and a
    // bare attribute selector would press-fit aria state onto <html>.
    $$(".theme-choice[data-theme-preference]").forEach((control) => {
      const active = control.dataset.themePreference === mode;
      control.classList.toggle("active", active);
      control.setAttribute("aria-pressed", active ? "true" : "false");
    });
    // One button in the top toolbar is the primary appearance control: it always shows the mode
    // the user currently sees (icon + word), and clicking flips light ↔ dark. 跟随系统 stays a
    // deliberate choice in the native menu and the settings card, so a stray click never lands
    // the user in an OS-dependent mode they did not ask for.
    const toggle = $(".tb-theme-toggle");
    if (toggle) {
      const icon = $("#theme-toggle-icon");
      const label = $("#theme-toggle-label");
      if (icon) icon.setAttribute("href", mode === "system" ? "#t-auto" : dark ? "#t-moon" : "#t-sun");
      if (label) label.textContent = dark ? "深色" : "浅色";
      const hint = mode === "system" ? "（当前跟随系统，点击后固定为相反外观）" : "";
      toggle.title = `切换到${dark ? "浅色" : "深色"}外观${hint}`;
      toggle.setAttribute("aria-label", toggle.title);
      toggle.dataset.mode = dark ? "dark" : "light";
      toggle.dataset.follow = mode;
      toggle.classList.remove("theme-flip");
      void toggle.offsetWidth;
      toggle.classList.add("theme-flip");
    }
    // WKWebView keeps cached raster layers for button surfaces (connection cards, active tabs)
    // and does not invalidate them when only an ancestor attribute changes, leaving light islands
    // on dark chrome until something forces a full repaint. A root opacity nudge re-rasterizes
    // everything; it must stay up long enough for the whole-app raster to finish, otherwise
    // demoting the layer restores the stale tiles. Imperceptible at 0.999 and it creates no
    // containing block for fixed-position modals or toasts.
    const root = document.documentElement;
    root.style.opacity = "0.999";
    clearTimeout(themeRepaintTimer);
    themeRepaintTimer = setTimeout(() => { root.style.opacity = ""; }, 300);
    if (backendAvailable) backendCall("set_window_appearance", { mode }).catch(() => {});
  }

  darkThemeQuery.addEventListener("change", () => {
    if (themePreference() === "system") applyThemePreference("system");
  });

  // The toolbar button flips between the two explicit modes and pins the one the user now sees;
  // the whole-app repaint plus the icon/word flip is the feedback, so no toast is needed.
  function cycleTheme() {
    applyThemePreference(themeIsDark() ? "light" : "dark");
  }

  /* ---------- live editor options ---------- */

  const EDITOR_OPTS_KEY = "luxbase.editorOpts.v1";
  const EDITOR_OPTS_DEFAULT = { fontSize: 13, wordWrap: false, indentGuides: true, bracketColors: true, smoothCaret: true };
  let editorOptionsPanel = null;

  function editorOpts() {
    try {
      return { ...EDITOR_OPTS_DEFAULT, ...(JSON.parse(localStorage.getItem(EDITOR_OPTS_KEY) || "null") || {}) };
    } catch {
      return { ...EDITOR_OPTS_DEFAULT };
    }
  }

  function renderEditorOptions() {
    const opts = editorOpts();
    const row = (label, control) => `<div class="eo-row"><span>${label}</span>${control}</div>`;
    const sw = (key, label) => `<button class="eo-switch" role="switch" aria-checked="${opts[key] ? "true" : "false"}" aria-label="${label}" data-eo-switch="${key}"></button>`;
    editorOptionsPanel.innerHTML = `
      <p class="eo-title">编辑器选项</p>
      ${row("字号", `<span class="eo-stepper"><button data-eo-step="-1" aria-label="减小字号"${opts.fontSize <= 12 ? " disabled" : ""}>−</button><output>${opts.fontSize}px</output><button data-eo-step="1" aria-label="增大字号"${opts.fontSize >= 18 ? " disabled" : ""}>+</button></span>`)}
      ${row("自动换行", sw("wordWrap", "自动换行"))}
      ${row("缩进参考线", sw("indentGuides", "缩进参考线"))}
      ${row("括号对着色", sw("bracketColors", "括号对着色"))}
      ${row("平滑光标", sw("smoothCaret", "平滑光标"))}
      <p class="eo-note">改动立即生效并记住。超过 500,000 字符的脚本自动进入纯文本保护模式；补全始终按当前方言提供。</p>`;
  }

  function closeEditorOptions() {
    if (editorOptionsPanel) editorOptionsPanel.hidden = true;
  }

  function openEditorOptions(anchor) {
    if (!editorOptionsPanel) {
      editorOptionsPanel = document.createElement("div");
      editorOptionsPanel.className = "editor-options-panel";
      editorOptionsPanel.id = "editor-options-panel";
      editorOptionsPanel.setAttribute("role", "dialog");
      editorOptionsPanel.setAttribute("aria-label", "编辑器选项");
      document.body.appendChild(editorOptionsPanel);
      editorOptionsPanel.addEventListener("click", (event) => {
        const step = event.target.closest("[data-eo-step]");
        const switchButton = event.target.closest("[data-eo-switch]");
        const opts = editorOpts();
        if (step) opts.fontSize = Math.min(18, Math.max(12, opts.fontSize + Number(step.dataset.eoStep)));
        else if (switchButton) opts[switchButton.dataset.eoSwitch] = !opts[switchButton.dataset.eoSwitch];
        else return;
        try {
          localStorage.setItem(EDITOR_OPTS_KEY, JSON.stringify(opts));
        } catch {
          // Options persistence is best-effort; the live application still happens.
        }
        window.__luxbaseEditorSetOptions?.(opts);
        renderEditorOptions();
        // Re-rendering detaches the clicked node; put focus back on the same control so keyboard
        // users keep their place and steppers can be held down with repeated Enter/Space.
        const again = step
          ? editorOptionsPanel.querySelector(`[data-eo-step="${step.dataset.eoStep}"]`)
          : editorOptionsPanel.querySelector(`[data-eo-switch="${switchButton.dataset.eoSwitch}"]`);
        again?.focus({ preventScroll: true });
      });
      editorOptionsPanel.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          event.stopPropagation();
          closeEditorOptions();
        }
      });
    }
    renderEditorOptions();
    editorOptionsPanel.hidden = false;
    const rect = anchor.getBoundingClientRect();
    editorOptionsPanel.style.left = "0px";
    editorOptionsPanel.style.top = "0px";
    const { width, height } = editorOptionsPanel.getBoundingClientRect();
    // A hidden anchor (SQL view not active) reports a zero rect; fall back to the toolbar's right
    // edge instead of stacking the panel in the window corner over the sidebar.
    const anchorRight = rect.width || rect.height ? rect.right : window.innerWidth - 16;
    const anchorBottom = rect.width || rect.height ? rect.bottom : 96;
    editorOptionsPanel.style.left = `${Math.max(6, Math.min(anchorRight - width, window.innerWidth - width - 6))}px`;
    editorOptionsPanel.style.top = `${Math.max(6, Math.min(anchorBottom + 6, window.innerHeight - height - 6))}px`;
    editorOptionsPanel.querySelector("button:not([disabled])")?.focus({ preventScroll: true });
  }

  function toggleSidebar(force) {
    layout.sidebarCollapsed = typeof force === "boolean" ? force : !layout.sidebarCollapsed;
    applyLayout();
    saveLayout();
  }

  function toggleInspector(force) {
    layout.inspectorCollapsed = typeof force === "boolean" ? force : !layout.inspectorCollapsed;
    applyLayout();
    saveLayout();
  }

  function onPanelResize(event) {
    const delta = event.clientX - panelResize.startX;
    if (panelResize.kind === "sidebar") {
      layout.sidebarWidth = Math.min(520, Math.max(190, panelResize.start + delta));
      layout.sidebarCollapsed = false;
    } else if (panelResize.kind === "inspector") {
      layout.inspectorWidth = Math.min(640, Math.max(240, panelResize.start - delta));
      layout.inspectorCollapsed = false;
    } else if (panelResize.kind === "editor") {
      const view = $("#view-sql");
      const max = (view?.clientHeight || 700) - 120;
      layout.editorHeight = Math.min(max, Math.max(90, panelResize.start + (event.clientY - panelResize.startY)));
    }
    applyLayout();
  }

  function startPanelResize(event, kind) {
    panelResize = {
      kind,
      startX: event.clientX,
      startY: event.clientY,
      start: kind === "sidebar" ? layout.sidebarWidth : kind === "inspector" ? layout.inspectorWidth : layout.editorHeight,
    };
    document.body.classList.add("panel-resizing");
    if (kind === "editor") document.body.classList.add("row-resizing");
    event.preventDefault();
  }

  /* ---------- table designer ---------- */

  const DESIGNER_TYPES = {
    mysql: ["int", "bigint", "tinyint", "smallint", "mediumint", "decimal", "float", "double", "varchar", "char", "text", "mediumtext", "longtext", "json", "date", "datetime", "timestamp", "time", "year", "blob", "longblob", "varbinary", "binary", "enum", "set", "bit", "boolean"],
    postgres: ["integer", "bigint", "smallint", "numeric", "real", "double precision", "character varying", "character", "text", "boolean", "date", "timestamp without time zone", "timestamp with time zone", "time without time zone", "uuid", "json", "jsonb", "bytea", "interval", "inet"],
    sqlite: ["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", "BOOLEAN", "DATE", "DATETIME", "VARCHAR"],
  };

  function splitColumnType(dataType) {
    const text = String(dataType || "").trim();
    const match = /^(.+?)\s*\(([^()]*)\)\s*(.*)$/.exec(text);
    if (!match) return { base: text, length: "", scale: "", suffix: "" };
    const args = match[2].split(",").map((part) => part.trim());
    if (args.length <= 2 && args.every((part) => /^\d+$/.test(part))) {
      return { base: match[1].trim(), length: args[0] || "", scale: args[1] || "", suffix: match[3].trim() };
    }
    return { base: match[1].trim(), length: match[2].trim(), scale: "", suffix: match[3].trim() };
  }

  function joinColumnType(column) {
    let text = String(column.base || "").trim();
    const length = String(column.length ?? "").trim();
    const scale = String(column.scale ?? "").trim();
    if (length) text += `(${length}${scale ? `,${scale}` : ""})`;
    if (column.suffix) text += ` ${column.suffix}`;
    return text;
  }

  function defaultExpression(value, protocol, extra = "") {
    const text = String(value ?? "").trim();
    if (!text) return null;
    if (/^(NULL|TRUE|FALSE|CURRENT_TIMESTAMP(\(\d*\))?|CURRENT_DATE|CURRENT_TIME|LOCALTIMESTAMP|NOW\(\))$/i.test(text)) return text.toUpperCase();
    if (/^-?\d+(\.\d+)?$/.test(text)) return text;
    if (/^'(?:[^']|'')*'$/s.test(text)) return text;
    if (/^\(.*\)$/s.test(text)) return text;
    if (protocol === "postgres" && (/::/.test(text) || /^[a-z_][\w.]*\(.*\)$/is.test(text))) return text;
    if (protocol === "mysql" && /DEFAULT_GENERATED/i.test(extra)) return `(${text})`;
    return sqlLiteral(text, protocol);
  }

  function designerColumn(column) {
    const type = splitColumnType(column.dataType);
    return {
      key: `c-${Math.random().toString(36).slice(2, 9)}`,
      orig: column,
      isNew: false,
      dropped: false,
      name: column.name,
      ...type,
      notNull: !column.nullable,
      pk: Boolean(column.isPrimaryKey),
      defaultValue: column.defaultValue ?? "",
      comment: column.comment ?? "",
      extra: column.extra || "",
    };
  }

  function designerChanged(column) {
    if (column.isNew || column.dropped) return false;
    const original = column.orig;
    return column.name !== original.name
      || joinColumnType(column) !== joinColumnType(splitColumnType(original.dataType))
      || column.notNull !== !original.nullable
      || String(column.defaultValue ?? "") !== String(original.defaultValue ?? "")
      || String(column.comment ?? "") !== String(original.comment ?? "");
  }

  function designerTypeChanged(column) {
    return joinColumnType(column) !== joinColumnType(splitColumnType(column.orig.dataType));
  }

  function validateDesigner(state) {
    const live = state.columns.filter((column) => !column.dropped);
    if (!live.length) throw new Error("表至少需要保留一列。");
    const seen = new Set();
    for (const column of live) {
      const name = column.name.trim();
      if (!name) throw new Error("字段名不能为空。");
      if (/[;\0]/.test(name)) throw new Error(`字段名 ${name} 不能包含分号。`);
      if (seen.has(name.toLowerCase())) throw new Error(`字段名 ${name} 重复。`);
      seen.add(name.toLowerCase());
      if (!/^[A-Za-z][A-Za-z0-9_ ]*$/.test(String(column.base).trim())) throw new Error(`字段 ${name} 的类型“${column.base}”无效。`);
      if (!/^[0-9A-Za-z_,' ]*$/.test(String(column.length ?? ""))) throw new Error(`字段 ${name} 的长度“${column.length}”无效。`);
      if (String(column.scale ?? "") && !/^\d+$/.test(String(column.scale))) throw new Error(`字段 ${name} 的小数位必须是数字。`);
      if (state.create && /auto_increment|identity/i.test(column.extra || "") && !/^(tinyint|smallint|mediumint|int|integer|bigint|int2|int4|int8)$/i.test(String(column.base).trim())) {
        throw new Error(`字段 ${name} 的类型不支持自增，请取消“自增”或选择整数类型。`);
      }
    }
  }

  function mysqlColumnDefinition(column, protocol) {
    let definition = `${quoteIdent(column.name.trim(), protocol)} ${joinColumnType(column)}${column.notNull ? " NOT NULL" : " NULL"}`;
    const extra = String(column.extra || "");
    const expression = defaultExpression(column.defaultValue, protocol, extra);
    if (expression && !/auto_increment/i.test(extra)) definition += ` DEFAULT ${expression}`;
    if (/auto_increment/i.test(extra)) definition += " AUTO_INCREMENT";
    const onUpdate = /on update (\S+(?:\(\d*\))?)/i.exec(extra);
    if (onUpdate) definition += ` ON UPDATE ${onUpdate[1].toUpperCase()}`;
    if (String(column.comment || "").trim()) definition += ` COMMENT ${sqlLiteral(column.comment, protocol)}`;
    return definition;
  }

  function genericColumnDefinition(column, protocol, { inlinePrimaryKey = false } = {}) {
    let definition = `${quoteIdent(column.name.trim(), protocol)} ${joinColumnType(column)}`;
    if (inlinePrimaryKey) definition += " PRIMARY KEY";
    if (column.notNull) definition += " NOT NULL";
    const expression = defaultExpression(column.defaultValue, protocol, column.extra);
    if (expression) definition += ` DEFAULT ${expression}`;
    return definition;
  }

  function designerPlan(state) {
    if (state.create) return createTablePlan(state);
    validateDesigner(state);
    const protocol = state.protocol;
    const q = (name) => quoteIdent(name, protocol);
    const table = protocol === "sqlite" ? q(state.table) : `${q(state.schema)}.${q(state.table)}`;
    const live = state.columns.filter((column) => !column.dropped);
    const originalPk = state.original.columns.filter((column) => column.isPrimaryKey).map((column) => column.name);
    const nextPk = live.filter((column) => column.pk).map((column) => (column.isNew ? `new:${column.name}` : column.orig.name));
    const pkChanged = originalPk.join("\u0000") !== nextPk.join("\u0000");
    const newPkNames = live.filter((column) => column.pk).map((column) => column.name.trim());
    const dropped = state.columns.filter((column) => column.dropped && !column.isNew);
    const changed = state.columns.filter(designerChanged);
    const added = state.columns.filter((column) => column.isNew && !column.dropped);
    const statements = [];
    let mode = "ddl";

    if (protocol === "mysql") {
      const clauses = [];
      dropped.forEach((column) => clauses.push(`DROP COLUMN ${q(column.orig.name)}`));
      if (pkChanged && originalPk.length) clauses.push("DROP PRIMARY KEY");
      changed.forEach((column) => clauses.push(`CHANGE COLUMN ${q(column.orig.name)} ${mysqlColumnDefinition(column, protocol)}`));
      added.forEach((column) => {
        const index = live.indexOf(column);
        const previous = live[index - 1];
        clauses.push(`ADD COLUMN ${mysqlColumnDefinition(column, protocol)}${previous ? ` AFTER ${q(previous.name.trim())}` : " FIRST"}`);
      });
      if (pkChanged && newPkNames.length) clauses.push(`ADD PRIMARY KEY (${newPkNames.map(q).join(", ")})`);
      if (clauses.length) statements.push(`ALTER TABLE ${table}\n  ${clauses.join(",\n  ")}`);
    } else if (protocol === "postgres") {
      dropped.forEach((column) => statements.push(`ALTER TABLE ${table} DROP COLUMN ${q(column.orig.name)}`));
      if (pkChanged && originalPk.length) {
        const pkIndex = (state.original.indexes || []).find((index) => index.unique && /_pkey$/.test(index.name));
        statements.push(`ALTER TABLE ${table} DROP CONSTRAINT ${q(pkIndex?.name || `${state.table}_pkey`)}`);
      }
      for (const column of changed) {
        const original = column.orig;
        const name = column.name.trim();
        if (name !== original.name) statements.push(`ALTER TABLE ${table} RENAME COLUMN ${q(original.name)} TO ${q(name)}`);
        if (designerTypeChanged(column)) {
          const type = joinColumnType(column);
          statements.push(`ALTER TABLE ${table} ALTER COLUMN ${q(name)} TYPE ${type} USING ${q(name)}::${type}`);
        }
        if (column.notNull !== !original.nullable) statements.push(`ALTER TABLE ${table} ALTER COLUMN ${q(name)} ${column.notNull ? "SET" : "DROP"} NOT NULL`);
        if (String(column.defaultValue ?? "") !== String(original.defaultValue ?? "")) {
          const expression = defaultExpression(column.defaultValue, protocol);
          statements.push(`ALTER TABLE ${table} ALTER COLUMN ${q(name)} ${expression ? `SET DEFAULT ${expression}` : "DROP DEFAULT"}`);
        }
        if (String(column.comment ?? "") !== String(original.comment ?? "")) {
          statements.push(`COMMENT ON COLUMN ${table}.${q(name)} IS ${String(column.comment || "").trim() ? sqlLiteral(column.comment, protocol) : "NULL"}`);
        }
      }
      for (const column of added) {
        statements.push(`ALTER TABLE ${table} ADD COLUMN ${genericColumnDefinition(column, protocol)}`);
        if (String(column.comment || "").trim()) statements.push(`COMMENT ON COLUMN ${table}.${q(column.name.trim())} IS ${sqlLiteral(column.comment, protocol)}`);
      }
      if (pkChanged && newPkNames.length) statements.push(`ALTER TABLE ${table} ADD PRIMARY KEY (${newPkNames.map(q).join(", ")})`);
    } else {
      const needsRebuild = pkChanged
        || changed.some((column) => designerTypeChanged(column) || column.notNull !== !column.orig.nullable || String(column.defaultValue ?? "") !== String(column.orig.defaultValue ?? ""))
        || added.some((column) => column.pk || column.notNull && !String(column.defaultValue || "").trim());
      if (needsRebuild) {
        mode = "batch";
        const temporary = q(`_luxbase_rebuild_${state.table}`);
        const inline = newPkNames.length === 1;
        const definitions = live.map((column) => genericColumnDefinition(column, protocol, { inlinePrimaryKey: inline && column.pk }));
        if (newPkNames.length > 1) definitions.push(`PRIMARY KEY (${newPkNames.map(q).join(", ")})`);
        for (const key of state.original.foreignKeys || []) {
          const columns = key.columns.map((name) => live.find((column) => !column.isNew && column.orig.name === name)?.name.trim());
          if (columns.every(Boolean)) definitions.push(`FOREIGN KEY (${columns.map(q).join(", ")}) REFERENCES ${q(key.referencedTable)} (${key.referencedColumns.map(q).join(", ")})`);
        }
        statements.push(`CREATE TABLE ${temporary} (\n  ${definitions.join(",\n  ")}\n)`);
        const copied = live.filter((column) => !column.isNew);
        statements.push(`INSERT INTO ${temporary} (${copied.map((column) => q(column.name.trim())).join(", ")}) SELECT ${copied.map((column) => q(column.orig.name)).join(", ")} FROM ${q(state.table)}`);
        statements.push(`DROP TABLE ${q(state.table)}`);
        statements.push(`ALTER TABLE ${temporary} RENAME TO ${q(state.table)}`);
        for (const index of state.original.indexes || []) {
          if (/^sqlite_autoindex/.test(index.name) || state.droppedIndexes.has(index.name)) continue;
          const columns = index.columns.map((name) => live.find((column) => !column.isNew && column.orig.name === name)?.name.trim());
          if (columns.every(Boolean)) statements.push(`CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${q(index.name)} ON ${q(state.table)} (${columns.map(q).join(", ")})`);
        }
      } else {
        dropped.forEach((column) => statements.push(`ALTER TABLE ${table} DROP COLUMN ${q(column.orig.name)}`));
        changed.forEach((column) => {
          if (column.name.trim() !== column.orig.name) statements.push(`ALTER TABLE ${table} RENAME COLUMN ${q(column.orig.name)} TO ${q(column.name.trim())}`);
        });
        added.forEach((column) => statements.push(`ALTER TABLE ${table} ADD COLUMN ${genericColumnDefinition(column, protocol)}`));
      }
    }

    for (const name of state.droppedIndexes) {
      if (protocol === "sqlite" && mode === "batch") continue;
      if (protocol === "mysql") statements.push(`DROP INDEX ${q(name)} ON ${table}`);
      else if (protocol === "postgres") {
        statements.push(`ALTER TABLE ${table} DROP CONSTRAINT IF EXISTS ${q(name)}`);
        statements.push(`DROP INDEX IF EXISTS ${q(state.schema)}.${q(name)}`);
      } else statements.push(`DROP INDEX ${q(name)}`);
    }
    for (const index of state.newIndexes) {
      if (!index.name.trim() && !index.columns.trim()) continue;
      if (!index.name.trim() || !index.columns.trim()) throw new Error("新索引需要同时填写名称和字段。");
      const columns = index.columns.split(",").map((name) => name.trim()).filter(Boolean);
      statements.push(`CREATE ${index.unique ? "UNIQUE " : ""}INDEX ${q(index.name.trim())} ON ${table} (${columns.map(q).join(", ")})`);
    }
    for (const name of state.droppedForeignKeys) {
      if (protocol === "sqlite") throw new Error("SQLite 不支持单独删除外键，需要重建表。");
      statements.push(protocol === "mysql" ? `ALTER TABLE ${table} DROP FOREIGN KEY ${q(name)}` : `ALTER TABLE ${table} DROP CONSTRAINT ${q(name)}`);
    }
    return { statements, mode };
  }

  function designerRowHtml(column, index, protocol) {
    const classes = column.dropped ? "dropped" : column.isNew ? "added" : designerChanged(column) ? "modified" : "";
    const generated = /GENERATED/i.test(column.extra || "") && !/DEFAULT_GENERATED/i.test(column.extra || "");
    const disabled = column.dropped || generated ? "disabled" : "";
    const comment = protocol !== "sqlite" ? `<input data-designer-field="comment" value="${escapeHtml(column.comment || "")}" ${disabled} placeholder="注释" />` : "";
    return `<div class="designer-row ${classes}" data-designer-row="${index}">
      <span class="designer-index">${index + 1}</span>
      <input data-designer-field="name" value="${escapeHtml(column.name)}" ${disabled} aria-label="字段名" />
      <input data-designer-field="base" value="${escapeHtml(column.base)}" list="designer-types" ${disabled} aria-label="类型" />
      <input data-designer-field="length" value="${escapeHtml(column.length)}" ${disabled} aria-label="长度" />
      <input data-designer-field="scale" value="${escapeHtml(column.scale)}" ${disabled} aria-label="小数位" />
      <label class="designer-check"><input type="checkbox" data-designer-field="notNull" ${column.notNull ? "checked" : ""} ${disabled} /></label>
      <label class="designer-check"><input type="checkbox" data-designer-field="pk" ${column.pk ? "checked" : ""} ${disabled} /></label>
      <div class="designer-default-cell"><input data-designer-field="defaultValue" value="${escapeHtml(column.defaultValue ?? "")}" ${disabled} placeholder="${/auto_increment|identity/i.test(column.extra || "") ? "自增" : "无默认值"}" aria-label="默认值" />${designer?.create && protocol !== "sqlite" ? `<label><input type="checkbox" data-designer-field="autoIncrement" ${/auto_increment|identity/i.test(column.extra || "") ? "checked" : ""} ${disabled} />自增</label>` : ""}</div>
      ${comment}
      <button class="designer-row-action" data-designer-toggle-drop="${index}" title="${column.dropped ? "恢复字段" : "删除字段"}">${column.dropped ? "恢复" : "删除"}</button>
    </div>`;
  }

  function renderDesigner() {
    if (!designer) return;
    const protocol = designer.protocol;
    const body = $("#designer-columns");
    if (body) body.innerHTML = designer.columns.map((column, index) => designerRowHtml(column, index, protocol)).join("");
    const indexes = $("#designer-indexes");
    if (indexes) {
      indexes.innerHTML = `${(designer.original.indexes || []).map((index) => {
        const isPrimary = /^(PRIMARY|.*_pkey|sqlite_autoindex.*)$/.test(index.name);
        const dropped = designer.droppedIndexes.has(index.name);
        return `<div class="designer-index-row${dropped ? " dropped" : ""}"><span class="field">${escapeHtml(index.name)}</span><span>${escapeHtml(index.columns.join(", "))}</span><span>${isPrimary ? "主键" : index.unique ? "UNIQUE" : "普通"}</span>${isPrimary ? '<span class="muted">在字段页修改主键</span>' : `<button class="designer-row-action" data-designer-drop-index="${escapeHtml(index.name)}">${dropped ? "恢复" : "删除"}</button>`}</div>`;
      }).join("") || '<div class="empty-table">没有索引</div>'}
      ${designer.newIndexes.map((index, position) => `<div class="designer-index-row added"><input data-designer-index="${position}" data-index-field="name" value="${escapeHtml(index.name)}" placeholder="索引名" /><input data-designer-index="${position}" data-index-field="columns" value="${escapeHtml(index.columns)}" placeholder="字段，多个用逗号分隔" /><label class="designer-check"><input type="checkbox" data-designer-index="${position}" data-index-field="unique" ${index.unique ? "checked" : ""} />唯一</label><button class="designer-row-action" data-designer-remove-index="${position}">移除</button></div>`).join("")}`;
    }
    const foreignKeys = $("#designer-fks");
    if (foreignKeys) {
      foreignKeys.innerHTML = (designer.original.foreignKeys || []).map((key) => {
        const dropped = designer.droppedForeignKeys.has(key.name);
        return `<div class="designer-index-row${dropped ? " dropped" : ""}"><span class="field">${escapeHtml(key.name)}</span><span>${escapeHtml(key.columns.join(", "))}</span><span>→ ${escapeHtml(key.referencedTable)} (${escapeHtml(key.referencedColumns.join(", "))})</span><button class="designer-row-action" data-designer-drop-fk="${escapeHtml(key.name)}">${dropped ? "恢复" : "删除"}</button></div>`;
      }).join("") || '<div class="empty-table">没有外键</div>';
    }
    updateDesignerPreview();
  }

  function updateDesignerPreview() {
    if (!designer) return;
    const preview = $("#designer-sql");
    const error = $("#designer-plan-error");
    const save = $('[data-action="designer-save"]');
    try {
      const plan = designerPlan(designer);
      designer.plan = plan;
      if (preview) preview.value = plan.statements.length ? `${plan.statements.join(";\n\n")};` : "-- 没有待保存的修改";
      if (error) error.hidden = true;
      if (save) save.disabled = !plan.statements.length;
      const summary = $("#designer-summary");
      if (summary && designer.create) {
        summary.textContent = designer.table.trim() ? `${designer.columns.filter((column) => !column.dropped).length} 个字段 · ${plan.statements.length} 条语句` : "填写表名后即可创建";
      } else if (summary) {
        const changes = designer.columns.filter((column) => column.isNew || column.dropped || designerChanged(column)).length + designer.droppedIndexes.size + designer.newIndexes.filter((index) => index.name.trim()).length + designer.droppedForeignKeys.size;
        summary.textContent = changes ? `${changes} 处修改待确认${plan.mode === "batch" ? " · SQLite 将在事务中重建表" : ""}` : "尚未修改";
      }
    } catch (planError) {
      designer.plan = null;
      if (preview) preview.value = "";
      if (designer.create && !String(designer.table || "").trim()) {
        if (error) error.hidden = true;
        if (save) save.disabled = true;
        return;
      }
      if (error) {
        error.hidden = false;
        error.textContent = errorMessage(planError);
      }
      if (save) save.disabled = true;
    }
  }

  function setDesignerTab(name) {
    $$("[data-designer-tab]").forEach((button) => button.classList.toggle("active", button.dataset.designerTab === name));
    $$("[data-designer-panel]").forEach((panel) => { panel.hidden = panel.dataset.designerPanel !== name; });
    if (name === "sql") updateDesignerPreview();
  }

  async function tableDesignerModal() {
    try {
      const target = activeTable;
      if (!target?.table) throw new Error("请先在左侧选择一张表，或打开表数据标签页，再进入表设计器。");
      const connection = connectionLookup.get(target.connectionId || activeConnectionId);
      if (!connection || protocolFamily(connection.protocol) !== "sql") throw new Error("表设计器仅适用于 SQL 连接。");
      invalidateTableMeta(connection.id, target.schema, target.table);
      const meta = await loadTableMeta(connection.id, target.schema, target.table);
      if (/view/i.test(meta.tableType || "")) throw new Error("视图没有可直接修改的字段，请修改视图定义。");
      designer = {
        connectionId: connection.id,
        protocol: connection.protocol,
        schema: target.schema,
        table: target.table,
        original: meta,
        columns: (meta.columns || []).map(designerColumn),
        droppedIndexes: new Set(),
        newIndexes: [],
        droppedForeignKeys: new Set(),
        plan: null,
      };
      const protocol = connection.protocol;
      showModal(`设计表 · ${target.table}`, "TABLE DESIGNER", `
        <div class="designer-tabs" role="tablist">
          <button class="active" data-designer-tab="columns">字段</button>
          <button data-designer-tab="indexes">索引</button>
          <button data-designer-tab="fks">外键</button>
          <button data-designer-tab="sql">SQL 预览</button>
          <span class="designer-summary" id="designer-summary">尚未修改</span>
        </div>
        <datalist id="designer-types">${(DESIGNER_TYPES[protocol] || []).map((type) => `<option value="${escapeHtml(type)}"></option>`).join("")}</datalist>
        <section data-designer-panel="columns">
          <div class="designer-grid${protocol === "sqlite" ? " no-comment" : ""}">
            <div class="designer-head"><span>#</span><span>名称</span><span>类型</span><span>长度</span><span>小数点</span><span>不是 NULL</span><span>主键</span><span>默认值</span>${protocol !== "sqlite" ? "<span>注释</span>" : ""}<span></span></div>
            <div id="designer-columns"></div>
          </div>
          <button class="add-field" data-action="designer-add-column"><svg><use href="#i-plus"/></svg>添加字段</button>
        </section>
        <section data-designer-panel="indexes" hidden>
          <div class="designer-list" id="designer-indexes"></div>
          <button class="add-field" data-action="designer-add-index"><svg><use href="#i-plus"/></svg>添加索引</button>
        </section>
        <section data-designer-panel="fks" hidden>
          <div class="designer-list" id="designer-fks"></div>
        </section>
        <section data-designer-panel="sql" hidden>
          <textarea id="designer-sql" class="modal-textarea designer-sql" readonly spellcheck="false"></textarea>
        </section>
        <div id="designer-plan-error" class="form-note danger-note" role="alert" hidden></div>
        <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="secondary-button" data-designer-tab="sql">预览 SQL</button><button class="primary-button" data-action="designer-save" disabled>保存修改…</button></div>
      `, { wide: true });
      renderDesigner();
    } catch (error) {
      showOperationError(error, "表设计器打开失败");
    }
  }

  function onDesignerInput(event) {
    if (!designer) return;
    const row = event.target.closest("[data-designer-row]");
    const field = event.target.dataset.designerField;
    if (row && field) {
      const column = designer.columns[Number(row.dataset.designerRow)];
      if (!column) return;
      if (field === "autoIncrement") column.extra = event.target.checked ? (designer.protocol === "postgres" ? "identity" : "auto_increment") : "";
      else column[field] = event.target.type === "checkbox" ? event.target.checked : event.target.value;
      if (field === "pk" && column.pk) {
        column.notNull = true;
        const notNull = $('[data-designer-field="notNull"]', row);
        if (notNull) notNull.checked = true;
      }
      row.classList.toggle("modified", designerChanged(column));
      updateDesignerPreview();
      return;
    }
    const indexField = event.target.dataset.indexField;
    if (indexField) {
      const index = designer.newIndexes[Number(event.target.dataset.designerIndex)];
      if (index) index[indexField] = event.target.type === "checkbox" ? event.target.checked : event.target.value;
      updateDesignerPreview();
    }
  }

  function saveDesigner() {
    if (!designer) return;
    updateDesignerPreview();
    const plan = designer.plan;
    if (!plan?.statements.length) {
      toast("没有修改", "表结构没有需要保存的变更。", "danger");
      return;
    }
    const state = designer;
    const script = `${plan.statements.join(";\n\n")};`;
    if (state.create) {
      confirmChoice({
        title: `创建表 ${state.table.trim()}`,
        eyebrow: "CREATE TABLE",
        body: `将在 <b>${escapeHtml(connectionLookup.get(state.connectionId)?.name || "")}${state.protocol === "sqlite" ? "" : ` / ${escapeHtml(state.schema)}`}</b> 中执行：<pre class="choice-sql">${escapeHtml(script)}</pre>`,
        confirmLabel: "创建",
      }).then(async (confirmed) => {
        if (!confirmed) {
          await tableCreateModal(state.connectionId, state.schema, state);
          return;
        }
        const op = operationId();
        try {
          await backendCall("sql_apply_ddl", { connectionId: state.connectionId, operationId: op, statements: plan.statements.map((statement, index) => ({ connectionId: state.connectionId, sql: statement, params: [], operationId: `${op}-${index + 1}`, schema: state.schema || null })) });
          const name = state.table.trim();
          toast("表已创建", `${name} · ${plan.statements.length} 条语句已执行。`);
          setObjectsScope(state.connectionId, state.schema, { category: "table", select: name });
          showObjectInfo({ connectionId: state.connectionId, schema: state.schema, table: name });
        } catch (error) {
          showOperationError(error, "创建表失败");
        } finally {
          afterDdl(state.connectionId);
          refreshAuditOnly();
        }
      });
      return;
    }
    confirmDangerous(`ALTER TABLE ${state.table}`, `${connectionLookup.get(state.connectionId)?.name || "SQL"}\n\n${script}`, async () => {
      const op = operationId();
      const statements = plan.statements.map((statement, index) => ({ connectionId: state.connectionId, sql: statement, params: [], operationId: `${op}-${index + 1}`, schema: state.schema || null }));
      try {
        if (plan.mode === "batch") {
          await backendCall("sql_execute_batch", { connectionId: state.connectionId, request: { connectionId: state.connectionId, operationId: op, wrapInTransaction: true, statements } });
        } else {
          await backendCall("sql_apply_ddl", { connectionId: state.connectionId, operationId: op, statements });
        }
      } finally {
        afterDdl(state.connectionId);
        refreshAuditOnly();
      }
      invalidateTableMeta(state.connectionId, state.schema, state.table);
      designer = null;
      toast("表结构已更新", `${plan.statements.length} 条语句已执行并写入审计日志。`);
      for (const tab of sqlTabs.filter((item) => item.kind === "table" && item.connectionId === state.connectionId && item.table === state.table && item.schema === state.schema)) {
        tab.grid = emptyGrid("table");
        await loadTablePrimaryKeys(tab);
        if (tab.id === activeSqlTabId) {
          gridState = tab.grid;
          await loadTableData(tab, 0);
        }
      }
      showObjectInfo({ connectionId: state.connectionId, schema: state.schema, table: state.table }, { refresh: true });
      refreshAuditOnly();
    });
  }
  function bindGridEvents() {
    const wrap = $(".grid-wrap");
    if (!wrap || wrap.dataset.bound === "true") return;
    wrap.dataset.bound = "true";
    wrap.tabIndex = 0;
    wrap.addEventListener("scroll", () => {
      gridState.scrollTop = wrap.scrollTop;
      gridState.scrollLeft = wrap.scrollLeft;
      if (cellEditor) commitCellEditor(true);
      if (!gridScrollFrame) {
        gridScrollFrame = requestAnimationFrame(() => {
          gridScrollFrame = 0;
          renderGridBody();
        });
      }
    }, { passive: true });
    wrap.addEventListener("mousedown", onGridMouseDown);
    wrap.addEventListener("dblclick", onGridDoubleClick);
    wrap.addEventListener("contextmenu", onGridContextMenu);
    wrap.addEventListener("keydown", onGridKeyDown);
    document.addEventListener("mousemove", onDocumentMouseMove);
    document.addEventListener("mouseup", onDocumentMouseUp);
    if (typeof ResizeObserver === "function") new ResizeObserver(() => renderGridBody()).observe(wrap);
  }

  function bindTabStripEvents() {
    const strip = $("#sql-tab-strip");
    if (!strip) return;
    strip.addEventListener("scroll", updateTabStripNav, { passive: true });
    strip.addEventListener("wheel", (event) => {
      if (strip.scrollWidth <= strip.clientWidth + 2) return;
      const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
      if (!delta) return;
      event.preventDefault();
      strip.scrollLeft += delta;
    }, { passive: false });
    if (typeof ResizeObserver === "function") new ResizeObserver(updateTabStripNav).observe(strip);
    $$("[data-tab-scroll]").forEach((button) => button.addEventListener("click", () => {
      const distance = Number(button.dataset.tabScroll) * Math.max(180, strip.clientWidth * 0.7);
      strip.scrollBy({ left: distance, behavior: "smooth" });
    }));
    updateTabStripNav();
  }

  function bindPanelResizers() {
    document.addEventListener("mousedown", (event) => {
      const handle = event.target.closest?.("[data-panel-resize]");
      if (!handle || event.button !== 0) return;
      startPanelResize(event, handle.dataset.panelResize);
    });
    // The splitters are keyboard separators too: arrows nudge 16px with the same clamps as dragging.
    document.addEventListener("keydown", (event) => {
      const handle = event.target.closest?.("[data-panel-resize]");
      if (!handle) return;
      const kind = handle.dataset.panelResize;
      const horizontal = kind !== "editor";
      const forward = event.key === (horizontal ? "ArrowRight" : "ArrowDown");
      const backward = event.key === (horizontal ? "ArrowLeft" : "ArrowUp");
      if (!forward && !backward) return;
      event.preventDefault();
      const step = forward ? 16 : -16;
      if (kind === "sidebar") {
        layout.sidebarWidth = Math.min(520, Math.max(190, layout.sidebarWidth + step));
        layout.sidebarCollapsed = false;
      } else if (kind === "inspector") {
        layout.inspectorWidth = Math.min(640, Math.max(240, layout.inspectorWidth - step));
        layout.inspectorCollapsed = false;
      } else {
        const max = ($("#view-sql")?.clientHeight || 700) - 120;
        layout.editorHeight = Math.min(max, Math.max(90, layout.editorHeight + step));
      }
      applyLayout();
      saveLayout();
    });
  }

  function applyGridFilter(text) {
    const needle = String(text || "").trim().toLowerCase();
    if (!gridState.baseRows) gridState.baseRows = gridState.rows;
    gridState.rows = needle
      ? gridState.baseRows.filter((row) => row.some((value) => cellText(value).toLowerCase().includes(needle)))
      : gridState.baseRows;
    if (!needle) gridState.baseRows = null;
    gridState.selection = null;
    gridState.focus = null;
    gridState.filterText = needle;
    renderGridBody();
    renderGridFooter();
    const count = activeGridTabCount();
    if (count) count.textContent = String(gridState.rows.length);
  }

  function renameSqlTab(id) {
    const tab = sqlTabs.find((item) => item.id === id);
    if (!tab) return;
    showModal("重命名标签页", "RENAME TAB", `
      <div class="form-grid"><label class="full">名称<input id="tab-title-input" value="${escapeHtml(tab.title)}" maxlength="80" /></label></div>
      <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="rename-tab-confirm" data-tab="${escapeHtml(tab.id)}">确定</button></div>
    `);
  }
  async function runExplain(tab) {
    const panel = $('[data-result-content="plan"]');
    if (!panel) return;
    const sql = (window.__luxbaseGetSqlSelection?.() || window.__luxbaseGetSql?.() || tab.sql || "").trim();
    const statement = splitSqlStatements(sql)[0];
    if (!statement || !tab.connectionId) {
      panel.innerHTML = '<div class="empty-table">在编辑器中写一条查询并选择连接后，这里显示数据库返回的执行计划。</div>';
      return;
    }
    if (!isReadSql(statement)) {
      panel.innerHTML = '<div class="empty-table">执行计划只对 SELECT / WITH 等查询生成，不会执行写操作。</div>';
      return;
    }
    const protocol = connectionProtocol(tab.connectionId);
    const explainSql = protocol === "sqlite" ? `EXPLAIN QUERY PLAN ${statement}` : `EXPLAIN ${statement}`;
    panel.innerHTML = '<div class="empty-table">正在生成执行计划…</div>';
    try {
      const params = await parseBoundParameters(parameterCount(statement));
      const result = await backendCall("sql_fetch", { connectionId: tab.connectionId, request: sqlRequestFor(tab, explainSql, params, operationId(), { pageSize: 500, offset: 0 }) });
      if (result.columns.length === 1) {
        panel.innerHTML = `<pre class="plan-text">${escapeHtml(result.rows.map((row) => cellText(row[0])).join("\n"))}</pre>`;
      } else {
        panel.innerHTML = `<div class="plan-table-wrap"><table class="plan-table" data-resize-key="plan"><thead><tr>${result.columns.map((column, index) => `<th>${escapeHtml(column.name)}<span class="col-resizer" data-gcol="${index}"></span></th>`).join("")}</tr></thead><tbody>${result.rows.map((row) => `<tr>${row.map((value) => `<td>${formatCell(value)}</td>`).join("")}</tr>`).join("")}</tbody></table></div>`;
        applyGenericColWidths();
      }
    } catch (error) {
      panel.innerHTML = `<div class="empty-table danger-text">${escapeHtml(errorMessage(error))}</div>`;
    }
  }
  /* =================== end SQL workspace =================== */

  document.addEventListener("click", async (event) => {
    // composedPath, not target.closest: the panel re-renders on every control click, which detaches
    // the original target and would otherwise read as an outside click and close the panel.
    const clickPath = event.composedPath?.() || [];
    if (editorOptionsPanel && !editorOptionsPanel.hidden
      && !clickPath.includes(editorOptionsPanel)
      && !event.target.closest?.('[data-action="editor-options"]')) closeEditorOptions();
    const filterDropdown = $("#table-filter-dropdown");
    const filterPanel = $("#table-filter-panel");
    if (filterPanel && !filterPanel.hidden && filterDropdown && !clickPath.includes(filterDropdown)) toggleFilterPanel(false);
    const target = event.target.closest("button, [data-open-sql], [data-open-redis], [data-workspace], [data-view]");
    if (!target) {
      if (!event.target.closest("#context-menu")) contextMenu.hidden = true;
      if (!event.target.closest("#app-menu-popover")) closeAppMenu();
      return;
    }

    const closeTabTarget = event.target.closest("[data-close-sql-tab]");
    if (closeTabTarget) {
      event.stopPropagation();
      closeSqlTab(closeTabTarget.dataset.closeSqlTab);
      return;
    }
    if (target.dataset.sqlTab) {
      activateSqlTab(target.dataset.sqlTab, { focusEditor: true });
      return;
    }
    if (target.dataset.gridCopy) {
      contextMenu.hidden = true;
      await copyGridSelection(target.dataset.gridCopy);
      return;
    }
    if (target.hasAttribute("data-grid-edit")) {
      contextMenu.hidden = true;
      if (gridState.focus) beginCellEdit(gridState.focus.r, gridState.focus.c);
      return;
    }
    if (target.hasAttribute("data-grid-set-null")) {
      contextMenu.hidden = true;
      if (gridState.focus) await updateGridCell(gridState.focus.r, gridState.focus.c, null);
      return;
    }
    if (target.hasAttribute("data-grid-select-all")) {
      contextMenu.hidden = true;
      gridState.selection = { mode: "all" };
      refreshSelection();
      $(".grid-wrap")?.focus({ preventScroll: true });
      return;
    }
    if (target.dataset.inspectorTab) {
      setInspectorMode(target.dataset.inspectorTab);
      if (target.dataset.inspectorTab === "health") refreshPoolMetrics();
      return;
    }
    if (target.dataset.designerTab) {
      setDesignerTab(target.dataset.designerTab);
      return;
    }
    if (target.dataset.designerToggleDrop != null && designer) {
      const index = Number(target.dataset.designerToggleDrop);
      const column = designer.columns[index];
      if (column?.isNew) designer.columns.splice(index, 1);
      else if (column) column.dropped = !column.dropped;
      renderDesigner();
      return;
    }
    if (target.dataset.designerDropIndex && designer) {
      const name = target.dataset.designerDropIndex;
      if (designer.droppedIndexes.has(name)) designer.droppedIndexes.delete(name);
      else designer.droppedIndexes.add(name);
      renderDesigner();
      return;
    }
    if (target.dataset.designerRemoveIndex != null && designer) {
      designer.newIndexes.splice(Number(target.dataset.designerRemoveIndex), 1);
      renderDesigner();
      return;
    }
    if (target.dataset.designerDropFk && designer) {
      const name = target.dataset.designerDropFk;
      if (designer.droppedForeignKeys.has(name)) designer.droppedForeignKeys.delete(name);
      else designer.droppedForeignKeys.add(name);
      renderDesigner();
      return;
    }

    if (target.dataset.diffToggle != null) {
      target.closest(".diff-table")?.classList.toggle("open");
      return;
    }
    if (target.dataset.menu) {
      const wasOpen = target.getAttribute("aria-expanded") === "true";
      closeAppMenu();
      if (!wasOpen) showAppMenu(target.dataset.menu, target);
      return;
    }
    if (target.dataset.protocolCard) {
      setProtocolInForm(target.dataset.protocolCard);
      return;
    }
    const closeTool = event.target.closest("[data-close-tool]");
    if (closeTool) {
      event.stopPropagation();
      closeToolView(closeTool.dataset.closeTool);
      return;
    }
    if (target.dataset.workspace) {
      const name = target.dataset.workspace;
      if (name === "redis") {
        const conn = activeFamily === "kv" ? activeConnectionId : lastRedisConn;
        if (conn && conn !== activeConnectionId) setActiveConnectionState(connectionLookup.get(conn));
        const session = redisSessions.get(conn);
        showWorkspace("redis");
        if (session && (currentRedisConn !== conn || currentRedisValue?.key !== session.key || currentRedisDb !== session.db)) {
          await openRedisKey(session.key, "string", session.db, session.b64);
        }
        return;
      }
      if (name === "es") {
        const conn = activeFamily === "search" ? activeConnectionId : lastEsConn;
        if (conn && conn !== activeConnectionId) setActiveConnectionState(connectionLookup.get(conn));
        const snap = esSessions.get(conn);
        if (snap && esState.connectionId !== conn) Object.assign(esState, snap, { connectionId: conn, docCols: { ...snap.docCols } });
        showWorkspace("es");
        return;
      }
      showWorkspace(name);
      return;
    }
    if (target.dataset.objectCategory) {
      objectsState.category = target.dataset.objectCategory;
      objectsState.selected = null;
      renderObjectsView();
      showScopeInfo();
      return;
    }
    if (target.dataset.objectsScope) {
      if (target.dataset.objectsScope === "connection") setObjectsScope(objectsState.connectionId, null);
      return;
    }
    if (target.dataset.connectionSelect) {
      const group = target.closest(".connection-group");
      const children = group ? $(".tree-children", group) : null;
      if (children && !children.hidden && group.dataset.loaded === "true" && target.dataset.connectionSelect === activeConnectionId) {
        children.hidden = true;
        $(".tree-chevron", target)?.classList.remove("open");
        return;
      }
      try {
        await selectConnection(target.dataset.connectionSelect);
      } catch (error) {
        showOperationError(error, "连接打开失败");
      }
      return;
    }
    if (target.dataset.sqlDatabase || target.hasAttribute("data-toggle-drafts")) {
      const node = target.closest(".database-node");
      const children = node ? $(".tree-grandchildren", node) : null;
      let toggle = true;
      if (target.dataset.sqlDatabase) {
        const schema = target.dataset.sqlDatabase;
        const connectionId = target.closest(".connection-group")?.dataset.connectionId || activeConnectionId;
        if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
        activeSchema = schema;
        $$(".tree-row.selected").forEach((row) => row.classList.remove("selected"));
        target.classList.add("selected");
        // The first click on a database shows it; only a click on the shown database (or its arrow) collapses it.
        const alreadyShown = objectsState.connectionId === connectionId && objectsState.schema === schema;
        toggle = alreadyShown || Boolean(event.target.closest(".tree-chevron")) || children?.hidden;
        setObjectsScope(connectionId, schema);
      }
      if (children && toggle) {
        children.hidden = !children.hidden;
        $(".tree-chevron", target)?.classList.toggle("open", !children.hidden);
      }
      if (target.dataset.sqlDatabase && !children?.hidden) await loadDatabaseObjects(target);
      return;
    }
    if (target.dataset.historyOpen) {
      openHistoryEntry(target.dataset.historyOpen);
      return;
    }
    if (target.dataset.openDraft) {
      const draft = queryDrafts.get(target.dataset.openDraft);
      if (!draft) {
        toast("草稿不存在", "请刷新连接树后重试。", "danger");
        return;
      }
      const existing = sqlTabs.find((tab) => tab.draftId === String(draft.id));
      if (existing) {
        activateSqlTab(existing.id, { focusEditor: true });
        return;
      }
      const group = target.closest(".connection-group");
      newQueryTab({ connectionId: group?.dataset.connectionId || draft.connectionId, sql: draft.sqlText, title: draft.name, draftId: String(draft.id) });
      return;
    }
    if (target.hasAttribute("data-redis-db")) {
      const children = target.nextElementSibling;
      if (children) {
        children.hidden = !children.hidden;
        $(".tree-chevron", target)?.classList.toggle("open", !children.hidden);
        if (!children.hidden && !children.dataset.loaded) {
          const db = Number(target.dataset.redisDb || 0);
          redisScanState.db = db;
          redisScanState.pattern = "*";
          redisDbCursors.delete(db);
          children.innerHTML = '<div class="tree-loading">正在 SCAN…</div>';
          const ok = await loadRedisKeys(db, false);
          // Only latch "loaded" on success so a failed/empty first attempt can be retried.
          if (ok) children.dataset.loaded = "true";
        }
      }
      return;
    }
    if (target.dataset.esIndex) {
      const group = target.closest(".connection-group");
      const connectionId = group?.dataset.connectionId || activeConnectionId;
      if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
      $$(".tree-row.selected").forEach((row) => row.classList.remove("selected"));
      target.classList.add("selected");
      await openEsIndex(target.dataset.esIndex);
      return;
    }
    if (target.dataset.openRedis) {
      const db = Number(target.closest("[data-redis-key-list]")?.dataset.redisKeyList ?? currentRedisDb ?? 0);
      await openRedisKey(target.dataset.openRedis, target.dataset.keyType, db, target.dataset.keyB64 || null);
      return;
    }
    if (target.dataset.openTable) {
      const group = target.closest(".connection-group");
      const connectionId = group?.dataset.connectionId || activeConnectionId;
      $$(".tree-row.selected").forEach((row) => row.classList.remove("selected"));
      target.classList.add("selected");
      if (connectionId && connectionId !== activeConnectionId) setActiveConnectionState(connectionLookup.get(connectionId));
      activeSchema = target.dataset.tableSchema || activeSchema;
      activeTable = { schema: target.dataset.tableSchema || "", table: target.dataset.openTable, connectionId, objectType: target.dataset.objectType || "table" };
      selectObjectFromTree(activeTable);
      return;
    }
    if (target.hasAttribute("data-open-sql")) {
      showWorkspace("sql");
      return;
    }
    if (target.dataset.resultTab) {
      const tab = activeSqlTab();
      if (tab && target.dataset.resultIndex != null) {
        selectResult(tab, Number(target.dataset.resultIndex));
        return;
      }
      if (tab) tab.resultTab = target.dataset.resultTab;
      setResultTab(target.dataset.resultTab);
      if (target.dataset.resultTab === "plan" && tab?.kind === "query") await runExplain(tab);
      return;
    }
    if (target.dataset.gridPage) {
      await loadGridPage(target.dataset.gridPage);
      return;
    }
    if (target.dataset.redisTab) {
      setRedisTab(target.dataset.redisTab);
      return;
    }
    if (target.dataset.esTab) {
      setEsTab(target.dataset.esTab);
      return;
    }
    if (target.hasAttribute("data-open-console")) {
      openConsole(activeConnectionId);
      return;
    }
    if (target.hasAttribute("data-close-modal")) {
      if (pendingParameterEditor) {
        closeParameterEditor();
        return;
      }
      confirmAction = null;
      cancelPendingDraftClose();
      closeModal();
      return;
    }
    if (target.dataset.migrationTab) {
      $$("[data-migration-tab]").forEach((button) => button.classList.toggle("active", button === target));
      return;
    }

    const action = target.dataset.action;
    if (!action) return;
    contextMenu.hidden = true;
    closeAppMenu();
    const busyActions = new Set([
      "run", "stop", "test-connection", "save-connection", "save-draft-confirm", "save-draft-copy", "draft-rename-confirm", "draft-delete", "query-history",
      "reconnect", "disconnect", "test-saved-connection", "edit-connection",
      "delete-connection", "scan-more", "redis-refresh", "redis-scan", "redis-create-save",
      "start-transfer", "generate-alter", "review-alter",
      "execute-schema-sync", "grant-identity", "revoke-identity",
      "identity-create", "identity-reset-password", "identity-drop",
      "acl-apply", "acl-reset-password", "acl-enable", "acl-disable", "acl-delete",
      "verify-audit", "diagnostics-export", "window-close",
      "insert-row-save", "cell-detail-save", "query-export-save", "query-export-cancel",
      "view-save", "view-drop", "namespace-create-save", "namespace-drop",
    ]);
    const shouldShowBusy = busyActions.has(action);
    if (shouldShowBusy) {
      target.disabled = true;
      target.setAttribute("aria-busy", "true");
    }

    switch (action) {
      case "insert-row":
        await insertRowModal(false);
        break;
      case "duplicate-row":
        await insertRowModal(true);
        break;
      case "insert-row-save":
        await saveInsertedRow();
        break;
      case "cell-details":
        showCellDetails();
        break;
      case "cell-detail-save":
        await saveCellDetails();
        break;
      case "blob-save":
        if (blobTarget) await saveBlobCell(blobTarget.r, blobTarget.c);
        break;
      case "blob-copy-hex": {
        const text = $("#blob-hex-editor")?.value || "";
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制 HEX" : "复制失败", ok ? `${text.length} 字符写入剪贴板。` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "blob-copy-b64": {
        const text = $("#blob-hex-editor")?.value || "";
        const ok = await copyToClipboard(textCopyPayload(btoa(hexToBytes(text))));
        toast(ok ? "已复制 Base64" : "复制失败", ok ? "Base64 已写入剪贴板。" : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "cell-detail-copy":
        await copyToClipboard(textCopyPayload($('#cell-detail-null').checked ? 'NULL' : $('#cell-detail-value').value));
        break;
      case "cell-detail-json":
        try { $('#cell-detail-value').value = JSON.stringify(JSON.parse($('#cell-detail-value').value), null, 2); }
        catch { toast("无法格式化", "当前内容不是有效 JSON，原内容未改动。", "danger"); }
        break;
      case "export-query-result":
        queryExportModal();
        break;
      case "query-export-browse":
        await chooseQueryExportPath();
        break;
      case "query-export-save":
        await exportQueryResult();
        break;
      case "query-export-cancel":
        await stopQueryExport();
        break;
      case "view-explorer":
      case "view-objects":
        showWorkspace("objects");
        break;
      case "tab-menu": {
        const tab = activeSqlTab();
        if (!tab) break;
        tabMenuTargetId = tab.id;
        const rect = target.getBoundingClientRect();
        const switchItems = sqlTabs.map((item) => `<button class="tab-switch-item${item.id === tab.id ? " is-active" : ""}" data-action="tab-switch" data-tab-id="${escapeHtml(item.id)}" title="${escapeHtml(item.title)}"><span class="tab-switch-title">${escapeHtml(item.title)}</span>${item.id === tab.id ? "<kbd>当前</kbd>" : ""}</button>`).join("");
        placeContextMenu({ clientX: rect.left, clientY: rect.bottom + 4, preventDefault() {} },
          `<span class="context-label">全部标签页（${sqlTabs.length}）</span>${switchItems}<span class="context-sep"></span>${tabMenuHtml(tab)}`);
        break;
      }
      case "tab-switch": {
        contextMenu.hidden = true;
        const id = target.dataset.tabId;
        if (sqlTabs.some((item) => item.id === id)) activateSqlTab(id);
        break;
      }
      case "tab-save": {
        contextMenu.hidden = true;
        const tab = sqlTabs.find((item) => item.id === tabMenuTargetId);
        if (tab) {
          activateSqlTab(tab.id);
          saveDraftModal();
        }
        break;
      }
      case "tab-close":
        contextMenu.hidden = true;
        closeSqlTab(tabMenuTargetId);
        break;
      case "tab-close-others":
        contextMenu.hidden = true;
        closeTabBatch(sqlTabs.filter((item) => item.id !== tabMenuTargetId).map((item) => item.id), "关闭其他");
        break;
      case "tab-close-left": {
        contextMenu.hidden = true;
        const index = sqlTabs.findIndex((item) => item.id === tabMenuTargetId);
        closeTabBatch(sqlTabs.slice(0, Math.max(0, index)).map((item) => item.id), "关闭左侧");
        break;
      }
      case "tab-close-right": {
        contextMenu.hidden = true;
        const index = sqlTabs.findIndex((item) => item.id === tabMenuTargetId);
        closeTabBatch(index < 0 ? [] : sqlTabs.slice(index + 1).map((item) => item.id), "关闭右侧");
        break;
      }
      case "tab-close-all":
        contextMenu.hidden = true;
        closeTabBatch(sqlTabs.map((item) => item.id), "关闭全部");
        break;
      case "batch-close-discard":
        closeModal();
        if (pendingBatchClose) performBatchClose(pendingBatchClose.ids, pendingBatchClose.label);
        pendingBatchClose = null;
        break;
      case "batch-close-save":
        closeModal();
        if (pendingBatchClose) await saveTabsThenClose(pendingBatchClose.ids, pendingBatchClose.label);
        pendingBatchClose = null;
        break;
      case "cycle-theme":
        cycleTheme();
        break;
      case "set-theme":
        applyThemePreference(target.dataset.themePreference);
        break;
      case "more-menu": {
        const open = !appMenu.hidden;
        closeAppMenu();
        if (!open) showAppMenu("more", target);
        break;
      }
      case "show-objects":
        showObjectsCategory(target.dataset.category || "table");
        break;
      case "explain": {
        const tab = activeSqlTab();
        if (!tab || tab.kind !== "query") {
          toast("没有可解释的查询", "请在查询标签页中编写一条 SELECT 语句。", "danger");
          break;
        }
        tab.resultTab = "plan";
        setResultTab("plan");
        await runExplain(tab);
        break;
      }
      case "close-tab":
        closeCurrentTab();
        break;
      case "object-open":
        openSelectedObject();
        break;
      case "object-design":
        if (objectsState.category === "query") renameDraftModal();
        else if (objectsState.category === "view") await viewEditorModal(true);
        else if (selectObjectAsActiveTable()) await tableDesignerModal();
        break;
      case "object-new":
        if (!objectsState.schema) namespaceModal();
        else if (objectsState.category === "query") newQueryTab({ connectionId: objectsState.connectionId, schema: objectsState.schema });
        else if (objectsState.category === "view") await viewEditorModal(false);
        else await tableCreateModal(objectsState.connectionId, objectsState.schema);
        break;
      case "object-drop":
        try {
          if (!objectsState.schema) await dropNamespace();
          else if (objectsState.category === "query") await deleteDraft();
          else if (objectsState.category === "view") await dropView();
          else if (selectObjectAsActiveTable()) runAction("drop-table");
        } catch (error) { showOperationError(error, "对象删除失败"); }
        break;
      case "view-create":
        await viewEditorModal(false);
        break;
      case "view-edit":
        await viewEditorModal(true);
        break;
      case "view-save":
        try { await saveView(); }
        catch (error) { showOperationError(error, "视图定义无效"); }
        break;
      case "view-drop":
        try { await dropView(); }
        catch (error) { showOperationError(error, "视图删除失败"); }
        break;
      case "namespace-create":
        namespaceModal();
        break;
      case "namespace-create-save":
        try { await createNamespace(); }
        catch (error) { showOperationError(error, "创建失败"); }
        break;
      case "namespace-drop":
        try { await dropNamespace(); }
        catch (error) { showOperationError(error, "删除失败"); }
        break;
      case "object-refresh":
        await refreshObjects();
        break;
      case "view-console":
      case "view-transfer":
      case "view-sync":
      case "view-compare":
      case "view-audit":
      case "view-settings":
        if (action === "view-console") openConsole(activeConnectionId);
        else showWorkspace(action.replace("view-", ""));
        break;
      case "focus-search":
        focusSearch();
        break;
      case "window-minimize":
      case "window-maximize":
      case "window-close":
        if (nativeWindow) {
          const nativeAction = action === "window-minimize"
            ? nativeWindow.minimize
            : action === "window-maximize"
              ? nativeWindow.toggleMaximize
              : nativeWindow.close;
          if (typeof nativeAction === "function") nativeAction();
          break;
        }
        if (!backendAvailable) {
          toast("桌面后端未连接", "窗口控制仅在打包后的桌面应用中可用。", "danger");
          break;
        }
        try {
          await backendCall(action === "window-minimize"
            ? "minimize_window"
            : action === "window-maximize"
              ? "toggle_maximize_window"
              : "close_window");
        } catch (error) {
          showOperationError(error, "窗口操作失败");
        }
        break;
      case "editor-undo":
      case "editor-redo":
      case "editor-cut":
      case "editor-copy":
      case "editor-paste":
      case "editor-select-all":
        runEditorAction(action === "editor-select-all" ? "selectAll" : action.replace("editor-", ""));
        break;
      case "security-center":
        showSecurityCenter();
        break;
      case "legacy-secret-import": {
        target.disabled = true;
        try {
          lastLegacySecretImport = await backendCall("import_legacy_keychain_secrets");
          toast(
            lastLegacySecretImport.imported ? "旧密码已导入本地保险库" : "钥匙串里没有待导入的密码",
            legacyImportSummary(lastLegacySecretImport),
            lastLegacySecretImport.accessDenied ? "danger" : "success",
          );
        } catch (error) {
          showOperationError(error, "钥匙串导入失败");
        }
        showSecurityCenter();
        break;
      }
      case "app-info":
        infoModal("关于 LuxBase", "ABOUT", [
          ["版本", "2.0.0"],
          ["桌面壳", "Tauri 2"],
          ["协议", "MySQL · PostgreSQL · SQLite · Redis"],
          ["驱动边界", "SQL Driver / KV Driver 分离"],
          ["安全", "本地保险库密码、加密配置、只追加审计"],
          ["数据存储", "仅保存在本机，不上传连接配置和查询记录"],
        ]);
        break;
      case "workspace-info":
        infoModal("本地工作区", "WORKSPACE", [
          ["存储", "本机 SQLite"],
          ["账号", "无云端账号"],
          ["连接数", String(savedConnections.length)],
          ["说明", "连接配置与审计记录只保存在这台设备。"],
        ]);
        break;
      case "shortcuts":
        invokeAppCommand("shortcuts");
        break;
      case "new-connection":
        newConnectionModal();
        if (target.dataset.protocol) setProtocolInForm(target.dataset.protocol);
        break;
      case "test-saved-connection":
        await testSavedConnection();
        break;
      case "edit-connection":
        await editConnection();
        break;
      case "delete-connection":
        deleteConnection();
        break;
      case "reconnect":
        if (!activeConnectionId) {
          toast("没有活动连接", "请先选择连接。", "danger");
          break;
        }
        try {
          const group = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"]`);
          await backendCall("disconnect_connection", { connectionId: activeConnectionId });
          metaCache.delete(activeConnectionId);
          if (group) {
            group.dataset.loaded = "false";
            const children = $(".tree-children", group);
            if (children) {
              children.hidden = true;
              children.innerHTML = "";
            }
          }
          await selectConnection(activeConnectionId);
          toast("重新连接成功", `${activeConnection?.name || "连接"} 已完成健康检查。`);
        } catch (error) {
          showOperationError(error, "重新连接失败");
        }
        break;
      case "disconnect":
        if (!activeConnectionId) {
          toast("没有活动连接", "请先选择连接。", "danger");
          break;
        }
        try {
          await backendCall("disconnect_connection", { connectionId: activeConnectionId });
          metaCache.delete(activeConnectionId);
          const group = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"]`);
          if (group) {
            group.dataset.loaded = "false";
            const children = $(".tree-children", group);
            if (children) {
              children.hidden = true;
              children.innerHTML = "";
            }
          }
          toast("连接已断开", "连接池或 Redis 连接已释放。");
          refreshAuditOnly();
        } catch (error) {
          showOperationError(error, "断开连接失败");
        }
        break;
      case "new-query":
        newQueryTab();
        break;
      case "query-table": {
        if (!activeTable?.table) break;
        const protocol = connectionProtocol(activeTable.connectionId);
        const name = protocol === "sqlite" || !activeTable.schema ? quoteIdent(activeTable.table, protocol) : `${quoteIdent(activeTable.schema, protocol)}.${quoteIdent(activeTable.table, protocol)}`;
        newQueryTab({ connectionId: activeTable.connectionId, schema: activeTable.schema || null, sql: `SELECT * FROM ${name} LIMIT 100;`, title: `${activeTable.table} 查询` });
        break;
      }
      case "show-object-info":
        if (activeTable?.table) {
          if (layout.inspectorCollapsed) toggleInspector(false);
          showObjectInfo({ connectionId: activeTable.connectionId, schema: activeTable.schema, table: activeTable.table, objectType: activeTable.objectType });
        }
        break;
      case "copy-table-name":
        if (activeTable?.table && await copyToClipboard(textCopyPayload(activeTable.table))) toast("已复制表名", activeTable.table);
        break;
      case "copy-info-name": {
        const panel = $("#inspector-object");
        const table = panel?.dataset.table;
        if (table && await copyToClipboard(textCopyPayload(table))) toast("已复制表名", table);
        break;
      }
      case "copy-table-qualified":
        if (activeTable?.table && await copyToClipboard(textCopyPayload(qualifiedTableName(activeTable)))) toast("已复制限定名", qualifiedTableName(activeTable));
        break;
      case "compare-from-schema":
        compareState.source = { connectionId: activeConnectionId, schema: activeSchema || defaultSchemaFor(activeConnectionId) };
        compareState.target = {
          connectionId: activeConnectionId,
          schema: (metaFor(activeConnectionId).schemas || []).find((name) => name !== compareState.source.schema) || null,
        };
        compareState.result = null;
        showWorkspace("compare");
        break;
      case "open-table":
        if (activeTable?.table) await openTableTab(activeTable.connectionId || activeConnectionId, activeTable.schema || "", activeTable.table, activeTable.objectType || "table");
        break;
      case "toggle-sidebar":
        toggleSidebar();
        break;
      case "new-query-here":
        newQueryTab({ connectionId: activeConnectionId, schema: activeSchema || null });
        break;
      case "refresh-database": {
        const group = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId || "")}"]`);
        const button = group ? $$("[data-sql-database]", group).find((item) => item.dataset.sqlDatabase === activeSchema) : null;
        const node = button?.closest(".database-node");
        if (node) {
          delete node.dataset.objectsLoaded;
          metaFor(activeConnectionId).tables.delete(activeSchema);
          await loadDatabaseObjects(button);
        }
        break;
      }
      case "toggle-inspector":
        toggleInspector();
        break;
      case "table-refresh": {
        const tab = activeSqlTab();
        if (tab?.kind === "table") await loadTableData(tab, gridState.offset || 0);
        else await runQuery();
        break;
      }
      case "table-filter-toggle":
        toggleFilterPanel();
        break;
      case "filter-add-condition": {
        const tab = activeSqlTab();
        const box = $("#filter-conditions");
        if (box) {
          $(".filter-empty", box)?.remove();
          const isFirst = !$$("[data-filter-condition]", box).length;
          box.insertAdjacentHTML("beforeend", filterConditionRowHtml({ column: "", op: "=", value: "", connector: "AND" }, filterColumnNames(tab), isFirst));
          $(".filter-condition:last-child .filter-column", box)?.focus({ preventScroll: true });
        }
        break;
      }
      case "filter-remove-condition": {
        const row = target.closest("[data-filter-condition]");
        row?.remove();
        const box = $("#filter-conditions");
        if (box && !$$("[data-filter-condition]", box).length) renderFilterConditions(activeSqlTab());
        break;
      }
      case "table-apply-filter":
        await applyTableFilter();
        break;
      case "table-clear-filter": {
        const tab = activeSqlTab();
        if (tab?.kind === "table") {
          tab.filter = "";
          tab.orderBy = "";
          tab.filterConditions = [];
          tab.filterLogic = "AND";
          tab.filterRaw = "";
          renderTableToolbar(tab);
          toggleFilterPanel(false);
          await loadTableData(tab, 0);
        }
        break;
      }
      case "copy-ddl": {
        const ddl = $("#inspector-object")?.dataset.ddl;
        if (!ddl) {
          toast("没有可复制的 DDL", "请先选择一张表。", "danger");
          break;
        }
        const ok = await copyToClipboard(textCopyPayload(ddl));
        toast(ok ? "已复制建表语句" : "复制失败", ok ? `${ddl.split("\n").length} 行 DDL` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "ddl-to-query": {
        const panel = $("#inspector-object");
        if (panel?.dataset.ddl) newQueryTab({ connectionId: panel.dataset.connectionId, schema: panel.dataset.schema || null, sql: panel.dataset.ddl, title: panel.dataset.table ? `${panel.dataset.table} DDL` : "查询 SQL" });
        break;
      }
      case "refresh-ddl": {
        const panel = $("#inspector-object");
        if (panel?.dataset.table) showObjectInfo({ connectionId: panel.dataset.connectionId, schema: panel.dataset.schema || "", table: panel.dataset.table }, { refresh: true });
        break;
      }
      case "close-tab-discard":
        closeModal();
        closeSqlTab(target.dataset.tab, { force: true });
        break;
      case "close-tab-save": {
        const tab = sqlTabs.find((item) => item.id === target.dataset.tab);
        closeModal();
        if (tab) {
          activateSqlTab(tab.id);
          tab.closeAfterSave = true;
          saveDraftModal();
        }
        break;
      }
      case "rename-tab-confirm": {
        const tab = sqlTabs.find((item) => item.id === target.dataset.tab);
        const title = $("#tab-title-input")?.value.trim();
        if (tab && title) {
          tab.title = title;
          renderSqlTabs();
          if (tab.id === activeSqlTabId) {
            if (tab.kind === "query") renderQueryContext(tab);
            renderBreadcrumbForTab(tab);
          }
          persistSqlTabs();
        }
        closeModal();
        break;
      }
      case "designer-add-column":
        if (designer) {
          const defaults = { mysql: ["varchar", "255"], postgres: ["character varying", "255"], sqlite: ["TEXT", ""] }[designer.protocol] || ["varchar", "255"];
          designer.columns.push({ key: `c-${Math.random().toString(36).slice(2, 9)}`, orig: null, isNew: true, dropped: false, name: "", base: defaults[0], length: defaults[1], scale: "", suffix: "", notNull: false, pk: false, defaultValue: "", comment: "", extra: "" });
          renderDesigner();
          const rows = $$("[data-designer-row]");
          $('[data-designer-field="name"]', rows[rows.length - 1])?.focus();
        }
        break;
      case "designer-add-index":
        if (designer) {
          designer.newIndexes.push({ name: "", columns: "", unique: false });
          renderDesigner();
          setDesignerTab("indexes");
        }
        break;
      case "designer-save":
        saveDesigner();
        break;
      case "save":
        saveDraftModal();
        break;
      case "save-draft-confirm":
        await saveDraft(false);
        break;
      case "save-draft-copy":
        await saveDraft(true);
        break;
      case "draft-open": {
        const draft = queryDrafts.get(String(target.dataset.draftId));
        const existing = draft && sqlTabs.find((tab) => tab.draftId === String(draft.id));
        if (existing) activateSqlTab(existing.id, { focusEditor: true });
        else if (draft) newQueryTab({ connectionId: draft.connectionId, schema: defaultSchemaFor(draft.connectionId), sql: draft.sqlText, title: draft.name, draftId: String(draft.id) });
        break;
      }
      case "draft-rename":
        renameDraftModal(target.dataset.draftId);
        break;
      case "draft-rename-confirm":
        try { await renameDraft(target.dataset.draftId); }
        catch (error) { showOperationError(error, "查询重命名失败"); }
        break;
      case "draft-delete":
        try { await deleteDraft(target.dataset.draftId); }
        catch (error) { showOperationError(error, "查询删除失败"); }
        break;
      case "query-history":
        await queryHistoryModal();
        break;
      case "run":
        await runQuery();
        break;
      case "stop":
        await stopExecution();
        break;
      case "transaction":
        await runTransactionBlock();
        break;
      case "import":
        migrationModal("import");
        break;
      case "export":
        if (currentWorkspace() === "sql" && activeSqlTab()?.kind === "query" && gridState.columns.length) queryExportModal();
        else migrationModal("export");
        break;
      case "compare":
      case "compare-run":
        showWorkspace("compare");
        break;
      case "compare-run-now":
        await runSchemaCompare();
        break;
      case "compare-swap": {
        const { source, target: destination } = compareState;
        compareState.source = destination;
        compareState.target = source;
        compareState.result = null;
        renderCompareView();
        break;
      }
      case "compare-check": {
        const mode = target.dataset.mode;
        for (const table of compareState.result?.tables || []) {
          for (const item of table.items) {
            if (!item.ops.length) continue;
            item.checked = mode === "all" || (mode === "safe" && !DESTRUCTIVE_DIFFS.has(item.kind));
          }
        }
        $$("[data-diff-item]").forEach((input) => {
          const found = (compareState.result?.tables || []).flatMap((table) => table.items).find((item) => item.id === input.dataset.diffItem);
          if (found) input.checked = found.checked;
        });
        refreshCompareScript();
        break;
      }
      case "compare-open-query": {
        const result = compareState.result;
        const statements = compareSelectedStatements();
        if (!result || !statements.length) break;
        newQueryTab({ connectionId: result.targetId, schema: result.targetProtocol === "sqlite" ? "main" : result.targetSchema, sql: `-- 结构同步脚本（目标：${connectionLookup.get(result.targetId)?.name || ""}）\n${statements.join(";\n\n")};\n`, title: "结构同步脚本" });
        break;
      }
      case "diagnostics":
        await diagnosticModal();
        break;
      case "refresh-inspector":
        await refreshPoolMetrics();
        toast("诊断已刷新", "连接池指标已从驱动层读取。");
        break;
      case "refresh-connections":
        metaCache.clear();
        await refreshBootstrap();
        toast("连接已刷新", "已从本地安全存储重新读取连接。");
        break;
      case "scan-more":
        await scanMore(Number(target.closest("[data-redis-key-list]")?.dataset.redisKeyList ?? redisScanState.db));
        break;
      case "redis-retry": {
        const db = Number(target.dataset.db ?? redisScanState.db);
        const list = target.closest("[data-redis-key-list]");
        if (list) delete list.dataset.loaded;
        redisDbCursors.delete(db);
        const ok = await loadRedisKeys(db, false);
        if (ok && list) list.dataset.loaded = "true";
        break;
      }
      case "redis-create":
        redisCreateModal();
        break;
      case "redis-create-save":
        try { await createRedisKey(); }
        catch (error) { showOperationError(error, "Redis 键创建失败"); }
        break;
      case "redis-refresh":
        if (currentRedisValue?.key) await openRedisKey(currentRedisValue.key, currentRedisValue.keyType, currentRedisDb, currentRedisKeyB64);
        else toast("没有已打开的键", "请先选择 Redis 键。", "danger");
        break;
      case "redis-delete": {
        const keyRow = $(".redis-key.selected-key");
        const key = contextRedisKey
          || keyRow?.dataset.openRedis
          || currentRedisValue?.key;
        if (!key) {
          toast("没有选中的键", "请先选择一个 Redis 键。", "danger");
          break;
        }
        const db = Number(keyRow?.closest("[data-redis-key-list]")?.dataset.redisKeyList ?? currentRedisDb ?? 0);
        const keyB64 = keyRow?.dataset.keyB64 || (currentRedisValue?.key === key ? currentRedisKeyB64 : null);
        confirmDangerous(`DEL ${key}`, `${activeConnection?.name || "Redis"} · DB ${db}\nDEL ${key}\n影响：该键及其全部值`, async () => {
          const op = operationId();
          activeRedisOperationId = op;
          try {
            await backendCall("redis_delete", {
              connectionId: activeConnectionId,
              keys: [key],
              keysBase64: keyB64 ? [keyB64] : null,
              db,
              confirmedDangerous: true,
              operationId: op,
            });
          } finally {
            if (activeRedisOperationId === op) activeRedisOperationId = null;
          }
          toast("Redis 键已删除", `${key} · 操作已写入只追加审计。`, "danger");
          if (currentRedisValue?.key === key) currentRedisValue = null;
          contextRedisKey = null;
          $$(".redis-key").find((row) => row.dataset.openRedis === key)?.remove();
          await refreshAuditOnly();
        });
        break;
      }
      case "redis-add-field":
      case "redis-add-row": {
        const editor = $(".hash-editor");
        if (!editor) break;
        const addButton = $(".add-field", editor);
        const isZset = Boolean($("[data-zset-row]"));
        const isList = Boolean($("[data-list-index]"));
        const html = isZset
          ? '<div class="hash-row" data-zset-row style="grid-template-columns:1fr 120px 70px"><input class="field redis-zset-member" value="" placeholder="成员" /><input class="field-value redis-zset-score" type="number" step="any" value="0" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>'
          : isList
            ? '<div class="hash-row" data-list-index="new"><span class="field">新</span><input class="field-value redis-list-value" value="" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>'
            : '<div class="hash-row" data-hash-row="new"><input class="field redis-field-name" value="" placeholder="字段" /><input class="field-value redis-field-value" value="" /><button class="row-action danger-text" data-action="redis-remove-row">删除</button></div>';
        addButton?.insertAdjacentHTML("beforebegin", html);
        break;
      }
      case "redis-remove-row":
        target.closest(".hash-row")?.remove();
        break;
      case "redis-discard":
        if (currentRedisValue) renderRedisEditor(currentRedisValue);
        toast("已放弃未提交修改", "键值没有写入 Redis。");
        break;
      case "redis-save":
        await saveRedisValue();
        break;
      case "redis-copy-binary": {
        const text = $("#redis-binary-pre")?.textContent || "";
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制值" : "复制失败", ok ? `${text.length} 字符写入剪贴板。` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "redis-copy-key": {
        if (!currentRedisValue?.key) { toast("没有已打开的键", "请先选择 Redis 键。", "danger"); break; }
        const text = redisKeyLabel(currentRedisValue.key, currentRedisKeyB64);
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制键名" : "复制失败", ok ? text : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "redis-copy-value": {
        const v = currentRedisValue;
        if (!v) { toast("没有已打开的键", "请先选择 Redis 键。", "danger"); break; }
        let text;
        if (v.value.type === "string") text = $('[data-redis-editor="string"]')?.value ?? (v.rawBase64 ? ($("#redis-binary-pre")?.textContent || "") : v.value.data);
        else text = JSON.stringify(v.value.data, null, 2);
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制值" : "复制失败", ok ? `${text.length} 字符写入剪贴板。` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "redis-view-preview":
        redisValueView = "preview";
        if (currentRedisValue) renderRedisEditor(currentRedisValue);
        break;
      case "redis-view-edit":
        redisValueView = "edit";
        if (currentRedisValue) renderRedisEditor(currentRedisValue);
        break;
      case "redis-console-clear":
        if ($("#redis-console-output")) $("#redis-console-output").innerHTML = "";
        $("#redis-console-input")?.focus();
        break;
      case "redis-console-run":
        await runRedisConsole();
        break;
      case "es-search":
        esState.from = 0;
        await esSearch();
        break;
      case "es-open-browser":
        showWorkspace("es");
        break;
      case "es-context-mapping":
        await openEsIndex(esState.index);
        setEsTab("mapping");
        break;
      case "es-context-aliases":
        await openEsIndex(esState.index);
        setEsTab("aliases");
        break;
      case "es-match-all":
        esState.dsl = '{\n  "query": { "match_all": {} }\n}';
        esState.from = 0;
        if ($("#es-dsl")) $("#es-dsl").value = esState.dsl;
        await esSearch();
        break;
      case "es-prev":
        esState.from = Math.max(0, esState.from - esState.size);
        await esSearch();
        break;
      case "es-next":
        esState.from += esState.size;
        await esSearch();
        break;
      case "es-refresh":
        if (esState.tab === "mapping") await esLoadMapping();
        else if (esState.tab === "aliases") await esLoadAliases();
        else if (esState.index) await esSearch();
        else await refreshEsIndices().then(() => renderEsView()).catch((error) => showOperationError(error, "刷新失败"));
        break;
      case "es-refresh-mapping":
        await esLoadMapping();
        break;
      case "es-new-doc":
        esState.currentDoc = null;
        esDocModal(null);
        break;
      case "es-save-doc":
        try { await esSaveDoc(); } catch (error) { showOperationError(error, "文档保存失败"); }
        break;
      case "es-delete-doc":
        esDeleteDoc();
        break;
      case "es-create-index":
        esCreateIndexModal();
        break;
      case "es-create-index-save":
        try { await esCreateIndex(); } catch (error) { showOperationError(error, "索引创建失败"); }
        break;
      case "es-delete-index":
        esDeleteIndex();
        break;
      case "es-open-index": {
        const name = selectedEsIndex();
        if (name) await openEsIndex(name);
        else toast("没有选中的索引", "请先在列表中选择一个索引。", "danger");
        break;
      }
      case "es-objects-refresh":
        try {
          await refreshEsIndices();
          const group = $(`.connection-group[data-connection-id="${CSS.escape(esState.connectionId)}"]`);
          if (group) { group.dataset.loaded = "false"; await loadConnectionTree(group); }
          renderObjectsView();
        } catch (error) { showOperationError(error, "索引刷新失败"); }
        break;
      case "es-add-alias":
        esAddAliasModal();
        break;
      case "es-add-alias-save":
        try { await esAddAlias(); } catch (error) { showOperationError(error, "别名添加失败"); }
        break;
      case "es-remove-alias":
        esRemoveAlias(target.dataset.alias);
        break;
      case "es-send-rest":
        await esSendRest();
        break;
      case "es-copy-rest": {
        const text = $("#es-rest-response")?.textContent || "";
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制响应" : "复制失败", ok ? `${text.length} 字符写入剪贴板。` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "es-copy-doc": {
        const text = $("#es-doc-source")?.value ?? JSON.stringify(esDocModalDoc?.source ?? {}, null, 2);
        const ok = await copyToClipboard(textCopyPayload(text));
        toast(ok ? "已复制 _source" : "复制失败", ok ? `${text.length} 字符写入剪贴板。` : "系统剪贴板不可用。", ok ? "success" : "danger");
        break;
      }
      case "es-doc-view-preview":
        esDocView = "preview";
        esDocModal(esDocModalDoc);
        break;
      case "es-doc-view-edit":
        esDocView = "edit";
        esDocModal(esDocModalDoc);
        break;
      case "table-designer":
        if (!activeSupports("metadata")) toast("当前协议不支持", "当前连接的协议描述未声明表结构能力。", "danger");
        else await tableDesignerModal();
        break;
      case "copy":
      case "copy-alter": {
        const sql = $(".modal-textarea")?.value;
        if (!sql) break;
        try {
          await navigator.clipboard.writeText(sql);
          toast("已复制", "SQL 已复制到系统剪贴板。");
        } catch (error) {
          showOperationError(error, "复制失败");
        }
        break;
      }
      case "editor-options":
        openEditorOptions(target);
        break;
      case "result-filter": {
        const tab = activeSqlTab();
        if (tab?.kind === "table") {
          toggleFilterPanel(true);
          $("#table-filter")?.focus();
          break;
        }
        const bar = $("#grid-filter-bar");
        if (bar) {
          bar.hidden = !bar.hidden;
          if (!bar.hidden) $("#grid-filter-input")?.focus();
          else applyGridFilter("");
        }
        break;
      }
      case "format": {
        if (activeFamily !== "sql") {
          toast("当前协议不支持", "SQL 格式化仅适用于 MySQL、PostgreSQL 与 SQLite。", "danger");
          break;
        }
        const selected = window.__luxbaseGetSqlSelection?.() || "";
        const source = selected || window.__luxbaseGetSql?.() || "";
        if (!source.trim()) {
          toast("无法格式化", "当前 SQL 为空。", "danger");
          break;
        }
        const formatted = formatSqlText(source);
        if (selected && window.__luxbaseReplaceSqlSelection) {
          window.__luxbaseReplaceSqlSelection(formatted);
          toast("选区已格式化", "已保留字符串和注释内容，请执行前审阅结果。");
        } else if (window.__luxbaseSetSql) {
          window.__luxbaseSetSql(formatted);
          toast("SQL 已格式化", "已保留字符串和注释内容，请执行前审阅结果。");
        } else {
          toast("SQL 编辑器不可用", "请等待桌面编辑器加载后再试。", "danger");
        }
        break;
      }
      case "transfer-start":
        await runDataTransfer();
        break;
      case "transfer-stop": {
        if (!transferState.operationId) break;
        try {
          transferState.job = await backendCall("transfer_cancel", { operationId: transferState.operationId });
          renderTransferProgress();
          toast("正在停止传输", "当前批次完成后中止；已写入的行保持提交状态。", "danger");
        } catch (error) { showOperationError(error, "停止传输失败"); }
        break;
      }
      case "transfer-clear":
        transferState.job = null;
        renderTransferProgress();
        break;
      case "transfer-tables-all": {
        const needle = transferState.filter.trim().toLowerCase();
        const visible = transferState.available.filter((table) => !needle || table.name.toLowerCase().includes(needle));
        const merged = [...new Set([...transferState.tables, ...visible.map((table) => table.name)])];
        if (merged.length > 200) {
          toast("最多 200 张表", "一次传输不能超过 200 张表，已按名称截断。", "danger");
          transferState.tables = merged.slice(0, 200);
        } else {
          transferState.tables = merged;
        }
        renderTransferPicker();
        break;
      }
      case "transfer-tables-none":
        transferState.tables = [];
        renderTransferPicker();
        break;
      case "sync-plan-start":
        await startSyncPlan();
        break;
      case "sync-plan-stop": {
        if (!syncState.operationId) break;
        try {
          syncState.job = await backendCall("sync_plan_cancel", { operationId: syncState.operationId });
          renderSyncResults();
          toast("正在停止比对", "未完成的比对不会生成计划，也不会写入任何数据。", "danger");
        } catch (error) { showOperationError(error, "停止比对失败"); }
        break;
      }
      case "sync-apply":
        executeSyncPlan();
        break;
      case "sync-swap": {
        const destination = syncState.target;
        syncState.target = syncState.source;
        syncState.source = destination;
        syncState.job = null;
        syncState.plan = null;
        syncState.selected = new Set();
        renderSyncView();
        break;
      }
      case "sync-check": {
        const mode = target.dataset.mode;
        const operations = syncState.plan?.operations || [];
        if (mode === "none") syncState.selected = new Set();
        else if (mode === "all") syncState.selected = new Set(operations.map((op) => op.id));
        else syncState.selected = new Set(operations.filter((op) => op.kind !== "delete").map((op) => op.id));
        refreshSyncSelection();
        break;
      }
      case "copy-sync-sql":
        await copySyncSql();
        break;
      case "copy-schema-sync": {
        const statements = compareSelectedStatements();
        if (!statements.length) break;
        const copied = await copyToClipboard(textCopyPayload(`${statements.join(";\n\n")};\n`));
        if (copied) toast("同步脚本已复制", `${statements.length} 条语句，请审阅后再执行。`);
        else toast("复制失败", "系统剪贴板不可用。", "danger");
        break;
      }
      case "execute-schema-sync":
        executeSchemaSync();
        break;
      case "test-connection":
        await testConnection();
        break;
      case "save-connection":
        await saveConnection();
        break;
      case "choice-confirm":
        settleChoice(true);
        closeModal();
        break;
      case "danger-confirm": {
        const checkbox = $("#danger-confirm");
        if (!checkbox?.checked) {
          toast("需要确认", "请勾选确认项后再执行危险操作。", "danger");
          break;
        }
        const actionToRun = confirmAction;
        confirmAction = null;
        closeModal();
        if (!actionToRun) {
          toast("操作未执行", "没有可执行的后端操作。", "danger");
          break;
        }
        try {
          await actionToRun();
          refreshAuditOnly();
        } catch (error) {
          showOperationError(error, "危险操作未执行");
        }
        break;
      }
      case "start-transfer": {
        const path = $("#transfer-path")?.value?.trim();
        const format = $("#transfer-format")?.value || "jsonl";
        const exportScope = $("#transfer-scope")?.value || "table";
        if (!path) {
          toast("需要文件路径", "请输入绝对文件路径或数据库 CSV 输出文件夹路径。", "danger");
          break;
        }
        if (activeFamily === "sql" && migrationDirection === "import" && !activeTable) {
          toast("需要目标表", "请先从导航树打开一张表。", "danger");
          break;
        }
        if (activeFamily === "sql" && migrationDirection === "export" && exportScope === "table" && !activeTable) {
          toast("需要导出表", "请先打开一张表，或把导出范围改为整个数据库。", "danger");
          break;
        }
        if (activeFamily === "sql" && migrationDirection === "export" && exportScope === "database" && !(activeSchema || activeTable?.schema)) {
          toast("需要数据库", "请先从导航树选择一个数据库，再执行库级导出。", "danger");
          break;
        }
        if (migrationDirection === "import") {
          confirmDangerous(`IMPORT ${format.toUpperCase()}`, `${activeConnection?.name || "连接"}\n文件：${path}\n${activeFamily === "sql" ? "SQL 导入将作为单一事务执行" : "Redis 键将被逐键原子替换"}`, async () => {
            const count = await backendCall("import_data", {
              connectionId: activeConnectionId,
              format,
              path,
              schema: activeTable?.schema || null,
              table: activeTable?.table || null,
              batchSize: Math.max(1, Math.min(10000, Math.trunc(Number($("#transfer-batch")?.value) || 2000))),
            });
            toast("导入完成", `${count} 条记录已写入 · 已记录审计日志。`);
            refreshAuditOnly();
          });
        } else {
          try {
            const count = await backendCall("export_data", {
              connectionId: activeConnectionId,
              format,
              path,
              schema: activeSchema || activeTable?.schema || null,
              table: activeFamily === "sql" && exportScope === "database" ? null : activeTable?.table || null,
              pattern: activeFamily === "kv" ? ($("#transfer-pattern")?.value?.trim() || "*") : null,
            });
            closeModal();
            const outputDescription = activeFamily === "sql" && exportScope === "database" && format === "csv"
              ? `CSV 文件夹 ${path}`
              : `${path}`;
            toast("导出完成", `${count} 条记录已从一致快照流式写入 ${outputDescription}。`);
            refreshAuditOnly();
          } catch (error) {
            showOperationError(error, "导出失败");
          }
        }
        break;
      }
      case "users":
        await usersModal();
        break;
      case "preview-identity":
        updateIdentityPreview();
        break;
      case "grant-identity":
        executeIdentityChange("GRANT");
        break;
      case "revoke-identity":
        executeIdentityChange("REVOKE");
        break;
      case "identity-create":
        createIdentityAccount();
        break;
      case "identity-reset-password":
        resetIdentityPassword();
        break;
      case "identity-drop":
        dropIdentityAccount();
        break;
      case "acl-apply":
        applyRedisAclUser();
        break;
      case "acl-reset-password":
        resetRedisAclPassword();
        break;
      case "acl-enable":
        toggleRedisAclUser(true);
        break;
      case "acl-disable":
        toggleRedisAclUser(false);
        break;
      case "acl-delete":
        deleteRedisAclUser();
        break;
      case "edit-permissions":
        await usersModal();
        break;
      case "console-open":
        openConsole(activeConnectionId);
        if (activeSchema && protocolFamily(activeProtocol) === "sql" && consoleState.connectionId === activeConnectionId) {
          consoleState.schema = activeSchema;
          renderConsoleView();
        }
        break;
      case "console-clear": {
        const output = $("#console-output");
        if (output) output.innerHTML = "";
        $("#console-input")?.focus();
        break;
      }
      case "redis-scan":
        showModal("SCAN 键空间", "REDIS SCAN", `
          <div class="form-grid"><label class="full">键模式（使用游标 SCAN，不会执行 KEYS）<input id="scan-pattern" value="${escapeHtml(redisScanState.pattern || "*")}" spellcheck="false" /></label></div>
          <div class="modal-footer"><button class="secondary-button" data-close-modal>取消</button><button class="primary-button" data-action="redis-scan-confirm">开始扫描</button></div>
        `);
        break;
      case "redis-scan-confirm": {
        const pattern = $("#scan-pattern")?.value ?? "*";
        closeModal();
        redisScanState.pattern = pattern.trim() || "*";
        redisScanState.cursor = 0;
        redisDbCursors.delete(redisScanState.db);
        const group = $(`.connection-group[data-connection-id="${CSS.escape(activeConnectionId)}"]`);
        if (group && group.dataset.loaded !== "true") await loadConnectionTree(group);
        const keyList = group ? $(`[data-redis-key-list="${redisScanState.db}"]`, group) : null;
        if (keyList) {
          keyList.hidden = false;
          const ok = await loadRedisKeys(redisScanState.db, false);
          if (ok) keyList.dataset.loaded = "true";
        }
        break;
      }
      case "connections-export":
        connectionsExportModal();
        break;
      case "connections-export-browse":
        await chooseConnectionsExportPath();
        break;
      case "connections-export-save":
        try { await doExportConnections(); } catch (error) { showOperationError(error, "连接导出失败"); }
        break;
      case "connections-import":
        connectionsImportModal();
        break;
      case "connections-import-browse":
        await chooseConnectionsImportPath();
        break;
      case "connections-import-save":
        try { await doImportConnections(); } catch (error) { showOperationError(error, "连接导入失败"); }
        break;
      case "grid-apply-edits":
        await applyPendingEdits();
        break;
      case "grid-discard-edits":
        discardPendingEdits();
        break;
      case "batch-delete": {
        const tab = activeSqlTab();
        if (!tab || tab.kind !== "table" || !tab.primaryKeys.length) {
          toast("无法删除行", "请打开带主键的表；单主键和复合主键均可按行删除。", "danger");
          break;
        }
        const rowIndexes = selectedRowIndexes();
        if (!rowIndexes.length || rowIndexes.length > 500) {
          toast("请选择 1–500 行", "点击行号选择整行，按住 Shift 可连续选择。", "danger");
          break;
        }
        let keys;
        try { keys = rowIndexes.map((index) => gridRowKey(tab, tab.grid, index)); }
        catch (error) { showOperationError(error, "无法删除行"); break; }
        const keyColumns = [...tab.primaryKeys];
        const keyValues = structuredClone(keys.map((key) => key.values));
        confirmDangerous(`DELETE ${keys.length} 行`, `${connectionLookup.get(tab.connectionId)?.name || "SQL"}\n${tab.schema ? `${tab.schema}.` : ""}${tab.table}\n主键：${keyColumns.join(", ")}\n${keyValues.slice(0, 10).map((row) => row.map(cellText).join(" / ")).join("\n")}\n影响：${keys.length} 行`, async () => {
          let result;
          try {
            result = await backendCall("sql_delete_rows", { connectionId: tab.connectionId, schema: tab.schema || "main", table: tab.table,
              keyColumns, keyValues, operationId: operationId() });
          } finally {
            await loadTableData(tab, tab.grid.offset || 0);
          }
          toast("已删除", `删除 ${result.rowsAffected} 行 · 已刷新当前页并写入审计日志。`, "danger");
          refreshAuditOnly();
        });
        break;
      }
      case "drop-table": {
        if (
          activeFamily !== "sql"
          || !activeTable
          || activeTable.connectionId !== activeConnectionId
          || !activeTable.table
        ) {
          toast("无法删除表", "请先右键当前连接中的目标表，再执行删除。", "danger");
          break;
        }
        const target = { ...activeTable };
        confirmDangerous(
          `DROP TABLE ${target.schema}.${target.table}`,
          `${activeConnection?.name || "SQL"}\nDROP TABLE ${target.schema}.${target.table}\n影响：表结构与全部数据`,
          async () => {
            await backendCall("sql_drop_table", {
              connectionId: activeConnectionId,
              schema: target.schema,
              table: target.table,
              operationId: operationId(),
            });
            toast("表已删除", `${target.schema}.${target.table} · 操作已写入审计日志。`, "danger");
            activeTable = null;
            activePrimaryKey = null;
            if (objectsState.selected === target.table) objectsState.selected = null;
            for (const tab of sqlTabs.filter((item) => item.kind === "table" && item.connectionId === target.connectionId && item.schema === target.schema && item.table === target.table)) closeSqlTab(tab.id, { force: true });
            afterDdl(target.connectionId);
            await refreshAuditOnly();
          },
        );
        break;
      }
      case "verify-audit":
        try {
          const ok = await backendCall("verify_audit_chain");
          toast(ok ? "审计链验证通过" : "审计链验证失败", ok ? "SHA-256 链完整，未发现被修改或删除的记录。" : "账本内容与哈希链不一致，请停止写入并保留现场。", ok ? "success" : "danger");
        } catch (error) {
          showOperationError(error, "审计链验证失败");
        }
        break;
      case "diagnostics-export":
        try {
          await exportDiagnosticReport();
        } catch (error) {
          showOperationError(error, "诊断报告导出失败");
        }
        break;
      default:
        break;
    }
    if (shouldShowBusy) {
      target.disabled = false;
      target.removeAttribute("aria-busy");
    }
  });

  document.addEventListener("contextmenu", (event) => {
    const tableRow = event.target.closest?.("[data-open-table]");
    if (tableRow) {
      showTableContext(event, tableRow);
      return;
    }
    const databaseRow = event.target.closest?.("[data-sql-database]");
    if (databaseRow) {
      showDatabaseContext(event, databaseRow);
      return;
    }
    const draftRow = event.target.closest?.("[data-open-draft], [data-draft-row]");
    if (draftRow) {
      showDraftContext(event, draftRow);
      return;
    }
    const redisRow = event.target.closest?.("[data-open-redis]");
    if (redisRow) {
      const group = redisRow.closest(".connection-group");
      contextRedisKey = redisRow.dataset.openRedis || null;
      $$(".redis-key").forEach((row) => row.classList.toggle("selected-key", row === redisRow));
      showConnectionContext(event, group?.dataset.protocol || "redis", group?.dataset.connectionId);
      return;
    }
    const esRow = event.target.closest?.("[data-es-index]");
    if (esRow) {
      showEsIndexContext(event, esRow);
    }
  });

  document.addEventListener("change", (event) => {
    const target = event.target;
    if (target?.classList?.contains("filter-op")) {
      const op = FILTER_OPERATORS.find((item) => item.value === target.value);
      const valueInput = $(".filter-value", target.closest("[data-filter-condition]"));
      if (valueInput) {
        valueInput.disabled = !op?.needsValue;
        if (valueInput.disabled) valueInput.value = "";
      }
      return;
    }
    if (target?.hasAttribute?.('data-insert-mode')) {
      $('[data-insert-value]', target.closest('[data-insert-field]')).disabled = target.value !== 'value';
      return;
    }
    if (target?.id === 'cell-detail-null') {
      $('#cell-detail-value').disabled = target.checked;
      return;
    }
    if (target?.id === "redis-create-type") {
      updateRedisCreateEditor();
      return;
    }
    if (target?.dataset?.compareConnection || target?.dataset?.compareSchema) {
      const key = target.dataset.compareConnection || target.dataset.compareSchema;
      const side = compareState[key];
      if (target.dataset.compareConnection) {
        side.connectionId = target.value;
        side.schema = defaultSchemaFor(target.value);
      } else {
        side.schema = target.value || null;
      }
      compareState.result = null;
      renderCompareView();
      return;
    }
    if (target?.dataset?.transferConnection || target?.dataset?.transferSchema) {
      const key = target.dataset.transferConnection || target.dataset.transferSchema;
      const side = transferState[key];
      if (target.dataset.transferConnection) {
        side.connectionId = target.value;
        side.schema = defaultSchemaFor(target.value);
      } else {
        side.schema = target.value || null;
      }
      transferState.available = [];
      transferState.targetTables = [];
      transferState.tables = [];
      renderTransferView();
      return;
    }
    if (target?.hasAttribute?.("data-transfer-table")) {
      const name = target.dataset.transferTable;
      if (!target.checked) transferState.tables = transferState.tables.filter((table) => table !== name);
      else if (!transferState.tables.includes(name)) {
        if (transferState.tables.length >= 200) {
          target.checked = false;
          toast("最多 200 张表", "一次传输不能超过 200 张表，请分批执行。", "danger");
          return;
        }
        transferState.tables = [...transferState.tables, name];
      }
      renderTransferPicker();
      return;
    }
    if (target?.dataset?.transferMode) {
      transferState.mode = target.dataset.transferMode;
      renderTransferPicker();
      return;
    }
    if (target?.dataset?.transferOption) {
      transferState[target.dataset.transferOption] = target.checked;
      renderTransferPicker();
      return;
    }
    if (target?.hasAttribute?.("data-transfer-batch")) {
      const value = Math.min(5000, Math.max(50, Math.round(Number(target.value) || 500)));
      transferState.batchSize = value;
      if (target.value !== String(value)) target.value = String(value);
      return;
    }
    if (target?.dataset?.syncConnection || target?.dataset?.syncSchema) {
      const key = target.dataset.syncConnection || target.dataset.syncSchema;
      const side = syncState[key];
      if (target.dataset.syncConnection) {
        side.connectionId = target.value;
        side.schema = defaultSchemaFor(target.value);
      } else {
        side.schema = target.value || null;
      }
      side.table = null;
      syncState.available[key] = [];
      syncState.job = null;
      syncState.plan = null;
      syncState.selected = new Set();
      renderSyncView();
      return;
    }
    if (target?.dataset?.syncTable) {
      syncState[target.dataset.syncTable].table = target.value || null;
      syncState.job = null;
      syncState.plan = null;
      syncState.selected = new Set();
      renderSyncResults();
      return;
    }
    if (target?.hasAttribute?.("data-sync-max-rows")) {
      const value = Math.min(5_000_000, Math.max(1, Math.round(Number(target.value) || 200000)));
      syncState.maxRows = value;
      if (target.value !== String(value)) target.value = String(value);
      return;
    }
    if (target?.hasAttribute?.("data-sync-operation")) {
      const id = Number(target.dataset.syncOperation);
      if (target.checked) syncState.selected.add(id);
      else syncState.selected.delete(id);
      refreshSyncSelection();
      return;
    }
    if (target?.dataset?.syncGroup) {
      const ids = syncPlanOperations(target.dataset.syncGroup).map((op) => op.id);
      if (target.checked) for (const id of ids) syncState.selected.add(id);
      else for (const id of ids) syncState.selected.delete(id);
      refreshSyncSelection();
      return;
    }
    if (target?.id === "console-connection") {
      openConsole(target.value || null);
      renderConsoleView();
      $("#console-input")?.focus();
      return;
    }
    if (target?.id === "console-schema") {
      consoleState.schema = target.value || null;
      renderConsoleView();
      $("#console-input")?.focus();
      return;
    }
    if (target?.dataset?.diffItem) {
      const found = (compareState.result?.tables || []).flatMap((table) => table.items).find((item) => item.id === target.dataset.diffItem);
      if (found) found.checked = target.checked;
      refreshCompareScript();
      return;
    }
    if (target?.hasAttribute?.("data-compare-show-same")) {
      compareState.showSame = target.checked;
      const results = $("#compare-results");
      if (results) results.innerHTML = compareResultsHtml();
      colorizeCompareScript();
      return;
    }
    if (target?.id === "query-connection") {
      const tab = activeSqlTab();
      if (tab?.kind === "query") {
        tab.connectionId = target.value || null;
        tab.schema = defaultSchemaFor(tab.connectionId);
        const connection = connectionLookup.get(tab.connectionId);
        if (connection) setActiveConnectionState(connection);
        window.__luxbaseEditorOpen?.(tab.id, tab.sql, tabDialect(tab), false);
        window.__luxbaseSetSqlDialect?.(tabDialect(tab));
        renderQueryContext(tab);
        renderSqlTabs();
        renderBreadcrumbForTab(tab);
        persistSqlTabs();
      }
    }
    if (target?.id === "query-schema") {
      const tab = activeSqlTab();
      if (tab) {
        tab.schema = target.value || null;
        if (tab.schema) activeSchema = tab.schema;
        renderSqlTabs();
        renderBreadcrumbForTab(tab);
        persistSqlTabs();
      }
    }
    if (target?.hasAttribute?.("data-page-size")) {
      const tab = activeSqlTab();
      if (tab?.kind === "table") {
        tab.pageSize = Number(target.value) || 100;
        loadTableData(tab, 0);
      }
    }
    if (target?.id === "ssh-enabled") {
      const fields = $("#ssh-fields");
      if (fields) fields.hidden = target.value !== "true";
    }
    if (target?.id === "tls-mode") {
      const fields = $("#tls-fields");
      if (fields) fields.hidden = !target.value || target.value === "disabled";
    }
    if (target?.id === "redis-mode") {
      const mode = target.value;
      const sentinel = $("#sentinel-master")?.closest("label");
      const sentinelNodes = $("#sentinel-nodes")?.closest("label");
      const clusterNodes = $("#cluster-nodes")?.closest("label");
      if (sentinel) sentinel.hidden = mode !== "sentinel";
      if (sentinelNodes) sentinelNodes.hidden = mode !== "sentinel";
      if (clusterNodes) clusterNodes.hidden = mode !== "cluster";
    }
    if (target?.id === "es-auth-mode") {
      updateEsAuthFields();
    }
    if (target?.id === "redis-binary-encoding") {
      redisBinaryEncoding = target.value;
      const pre = $("#redis-binary-pre");
      if (pre && currentRedisValue?.rawBase64) pre.textContent = decodeRedisRaw(currentRedisValue.rawBase64, target.value);
    }
    if (target?.id === "export-secrets") {
      $("#export-passphrase-row").hidden = !target.checked;
    }
    if (target?.id === "import-passphrase") {
      previewConnectionsImport();
    }
  });

  $$(".connection-row").forEach((row) => {
    row.addEventListener("contextmenu", (event) => {
      const group = row.closest(".connection-group");
      showConnectionContext(event, group?.dataset.protocol || "mysql", group?.dataset.connectionId);
    });
  });

  document.addEventListener("auxclick", (event) => {
    const tab = event.target.closest(".workspace-tab");
    if (!tab || event.button !== 1) return;
    event.preventDefault();
    if (tab.classList.contains("pinned-tab")) return;
    if (tab.dataset.sqlTab) closeSqlTab(tab.dataset.sqlTab);
    else if (TOOL_VIEWS.includes(tab.dataset.workspace)) closeToolView(tab.dataset.workspace);
  });

  document.addEventListener("dblclick", async (event) => {
    const tableRow = event.target.closest?.("[data-open-table]");
    if (tableRow) {
      const group = tableRow.closest(".connection-group");
      await openTableTab(tableRow.dataset.connectionId || group?.dataset.connectionId || activeConnectionId, tableRow.dataset.tableSchema || "", tableRow.dataset.openTable, tableRow.dataset.objectType || "table");
      return;
    }
    const sqlTab = event.target.closest?.("[data-sql-tab]");
    if (sqlTab && !event.target.closest("[data-close-sql-tab]")) {
      renameSqlTab(sqlTab.dataset.sqlTab);
      return;
    }
    if (event.target.closest?.("#sql-tab-strip") || event.target.classList?.contains("tabbar-spacer")) newQueryTab();
  });

  // Navicat-style: with a table selected in the tree or object list, ⌘C copies its name.
  // Text selections, inputs, Monaco and the result grid keep their own copy behavior.
  document.addEventListener("keydown", async (event) => {
    if (event.defaultPrevented || event.altKey || event.shiftKey || !(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "c") return;
    if (!activeTable?.table) return;
    const active = document.activeElement;
    if (active?.closest?.("input, textarea, [contenteditable='true'], .monaco-editor, .grid-wrap, .context-menu, .modal-content")) return;
    if (window.getSelection()?.toString()) return;
    const objectsActive = $("#view-objects")?.classList.contains("active");
    const inSidebar = active?.closest?.(".sidebar");
    if (!objectsActive && !inSidebar) return;
    event.preventDefault();
    if (await copyToClipboard(textCopyPayload(activeTable.table))) toast("已复制表名", activeTable.table);
  });

  document.addEventListener("contextmenu", (event) => {
    const tabElement = event.target.closest?.(".sql-tab");
    if (!tabElement || event.target.closest("[data-close-sql-tab]")) return;
    openTabMenu(event, tabElement.dataset.sqlTab);
  });

  // Outside editable surfaces the webview's own context menu (reload / inspect / copy link) has no
  // place in a database client; custom menus above already preventDefault, this catches the rest.
  document.addEventListener("contextmenu", (event) => {
    if (event.defaultPrevented) return;
    if (event.target.closest?.("input, textarea, [contenteditable='true'], .monaco-editor")) return;
    event.preventDefault();
  });

  document.addEventListener("keydown", (event) => {
    if (event.defaultPrevented) return;
    if (event.key === "Tab" && !modalBackdrop.hidden) {
      const focusable = $$("button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [contenteditable='true']", modalContent)
        .filter((element) => element.getClientRects().length > 0);
      if (focusable.length) {
        const first = focusable[0];
        const last = focusable[focusable.length - 1];
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first.focus();
        }
      }
    }
    if (event.key === "Escape") {
      if (pendingParameterEditor) closeParameterEditor();
      confirmAction = null;
      closeModal();
      contextMenu.hidden = true;
      closeAppMenu();
      closeEditorOptions();
    }
    const primary = (event.metaKey || event.ctrlKey) && !event.altKey && !event.shiftKey;
    const key = event.key.toLowerCase();
    if (primary && key === "w") {
      event.preventDefault();
      closeCurrentTab();
      return;
    }
    if (primary && key === "b") {
      event.preventDefault();
      toggleSidebar();
      return;
    }
    if (primary && key === "r") {
      event.preventDefault();
      invokeAppCommand("run");
      return;
    }
    if (event.key === "Enter" && event.target?.id === "tab-title-input") {
      event.preventDefault();
      $('[data-action="rename-tab-confirm"]')?.click();
      return;
    }
    if (event.key === "Enter" && event.target?.id === "redis-console-input") {
      event.preventDefault();
      runRedisConsole();
      return;
    }
    if (event.key === "Enter" && (event.target?.id === "table-filter" || event.target?.id === "table-order" || event.target?.classList?.contains("filter-value"))) {
      event.preventDefault();
      $('[data-action="table-apply-filter"]')?.click();
      return;
    }
    handleAppShortcut(event);
  });

  const auditHeading = $("#view-audit .view-heading");
  if (auditHeading && !$("[data-action='verify-audit']", auditHeading)) {
    auditHeading.insertAdjacentHTML("beforeend", '<button class="secondary-button" data-action="verify-audit">验证哈希链</button>');
  }
  restoreLayout();
  applyThemePreference(themePreference());
  bindObjectsList();
  bindGridEvents();
  bindTabStripEvents();
  bindPanelResizers();
  document.addEventListener("input", (event) => {
    if (event.target.closest?.(".designer-grid, .designer-list")) onDesignerInput(event);
    if (designer && (event.target.id === "designer-table-name" || event.target.id === "designer-table-comment")) {
      if (event.target.id === "designer-table-name") designer.table = event.target.value;
      else designer.tableComment = event.target.value;
      updateDesignerPreview();
    }
    if (event.target.id === "grid-filter-input") applyGridFilter(event.target.value);
    if (event.target?.hasAttribute?.("data-transfer-filter")) {
      transferState.filter = event.target.value;
      const list = $("#transfer-picker-list");
      if (list) list.innerHTML = transferPickerRows();
    }
  });
  document.addEventListener("copy", (event) => {
    if (pendingClipboard) {
      event.clipboardData?.setData("text/plain", pendingClipboard.text);
      if (pendingClipboard.html) event.clipboardData?.setData("text/html", pendingClipboard.html);
      pendingClipboard.done = true;
      event.preventDefault();
      return;
    }
    const active = document.activeElement;
    if (active?.closest?.(".grid-wrap") && !active.closest(".cell-editor") && gridState.columns.length) {
      const payload = formatSelection("tsv");
      if (!payload) return;
      event.clipboardData?.setData("text/plain", payload.text);
      if (payload.html) event.clipboardData?.setData("text/html", payload.html);
      event.preventDefault();
    }
  });
  restoreSqlTabs();
  showWorkspace("objects");
  $("#connection-search")?.addEventListener("input", (event) => filterConnections(event.target.value));
  refreshBootstrap();

  // The screenshot harness sets this class so animations settle predictably.
  document.body.classList.add("ready");
}

if (!window.__LUXBASE_DEFER_MOUNT__) mountUi();
