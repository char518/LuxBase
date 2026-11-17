import { useEffect, useMemo, useRef, useState } from "react";
import html2canvas from "html2canvas";
import { invoke } from "@tauri-apps/api/core";
import shellHtml from "../ui/index.html?raw";
import { mountUi } from "../ui/app.js";
import { lintSql } from "../ui/sql-lint.js";

declare global {
  interface Window {
    __LUXBASE_DEFER_MOUNT__?: boolean;
    __luxbaseMounted?: boolean;
    __luxbaseGetSql?: () => string;
    __luxbaseGetSqlSelection?: () => string;
    __luxbaseSetSql?: (sql: string) => void;
    __luxbaseReplaceSqlSelection?: (sql: string) => void;
    __luxbaseSetSqlDialect?: (dialect: string) => void;
    __luxbaseEditorAction?: (action: string) => void;
    __luxbaseEditorInsertText?: (text: string) => void;
    __luxbaseEditorDeleteSelection?: () => void;
    __luxbaseEditorReady?: boolean;
    __luxbaseCommand?: (action: string) => boolean;
    __luxbaseNativeShortcutMenu?: boolean;
    __luxbaseEditorOpen?: (id: string, sql: string, dialect: string, focus?: boolean) => void;
    __luxbaseEditorClose?: (id: string) => void;
    __luxbaseEditorChanged?: (id: string, value: string) => void;
    __luxbaseColorizeSql?: (text: string, dialect: string) => Promise<string>;
    __luxbaseSqlCompletions?: (context: { textBefore: string; textAfter: string }) => Promise<CompletionEntry[]>;
    __luxbaseSetEditorTheme?: (mode: "light" | "dark") => void;
    __luxbaseEditorSetOptions?: (opts: EditorOptions) => void;
  }
}

type EditorOptions = {
  fontSize: number;
  wordWrap: boolean;
  indentGuides: boolean;
  bracketColors: boolean;
  smoothCaret: boolean;
};

const EDITOR_OPTIONS_DEFAULT: EditorOptions = {
  fontSize: 13,
  wordWrap: false,
  indentGuides: true,
  bracketColors: true,
  smoothCaret: true,
};

type CompletionEntry = {
  label: string;
  kind: "schema" | "table" | "view" | "column";
  detail?: string;
  insertText?: string;
  sortText?: string;
};

function extractBody(source: string): string {
  const parsed = new DOMParser().parseFromString(source, "text/html");
  parsed.querySelectorAll("script").forEach((script) => script.remove());
  return parsed.body.innerHTML;
}

function isTauri(): boolean {
  return "__TAURI_INTERNALS__" in window;
}

async function runScreenshotMode(): Promise<boolean> {
  if (!isTauri()) return false;
  const target = await invoke<string | null>("screenshot_target");
  if (!target) return false;

  await new Promise<void>((resolve) => {
    let frame = 0;
    const draw = () => {
      frame += 1;
      if (frame >= 120) resolve();
      else requestAnimationFrame(draw);
    };
    requestAnimationFrame(draw);
  });

  const canvas = await html2canvas(document.body, {
    backgroundColor: "#f4f6fa",
    logging: false,
    scale: Math.min(2, window.devicePixelRatio || 1),
    useCORS: true,
    width: window.innerWidth,
    height: window.innerHeight,
    windowWidth: window.innerWidth,
    windowHeight: window.innerHeight,
  });
  const dataUrl = canvas.toDataURL("image/png");
  await invoke("save_screenshot", { path: target, pngBase64: dataUrl.split(",")[1] });
  await invoke("exit_after_screenshot");
  return true;
}

async function mountMonaco(): Promise<void> {
  const host = document.getElementById("monaco-editor-host");
  if (!host || host.dataset.mounted === "true") return;
  // Claimed before the first await: React StrictMode runs the mount effect twice in development,
  // and two editors on one host leave the visible editor detached from the window hooks.
  host.dataset.mounted = "true";
  const editorShell = document.querySelector("#view-sql .editor-shell");
  if (editorShell && host.parentElement !== editorShell) editorShell.appendChild(host);

  const [
    monaco,
    editorWorker,
    jsonWorker,
    cssWorker,
    htmlWorker,
    tsWorker,
  ] = await Promise.all([
    import("monaco-editor"),
    import("monaco-editor/esm/vs/editor/editor.worker?worker"),
    import("monaco-editor/esm/vs/language/json/json.worker?worker"),
    import("monaco-editor/esm/vs/language/css/css.worker?worker"),
    import("monaco-editor/esm/vs/language/html/html.worker?worker"),
    import("monaco-editor/esm/vs/language/typescript/ts.worker?worker"),
  ]);

  self.MonacoEnvironment = {
    getWorker(_: string, label: string) {
      if (label === "json") return new jsonWorker.default();
      if (label === "css" || label === "scss" || label === "less") return new cssWorker.default();
      if (label === "html" || label === "handlebars" || label === "razor") return new htmlWorker.default();
      if (label === "typescript" || label === "javascript") return new tsWorker.default();
      return new editorWorker.default();
    },
  };

  const wordRange = (
    model: import("monaco-editor").editor.ITextModel,
    position: import("monaco-editor").Position,
  ) => {
    const word = model.getWordUntilPosition(position);
    return {
      startLineNumber: position.lineNumber,
      endLineNumber: position.lineNumber,
      startColumn: word.startColumn,
      endColumn: word.endColumn,
    };
  };

  const dialectKeywords: Record<string, string[]> = {
    mysql: [
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "ON",
      "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES",
      "UPDATE", "SET", "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX",
      "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "AUTO_INCREMENT", "ENGINE", "CHARSET",
      "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "EXPLAIN", "SHOW",
      "DESCRIBE", "USE", "REPLACE", "DUPLICATE", "IF", "EXISTS", "AS", "AND", "OR",
      "NOT", "NULL", "IS", "IN", "LIKE", "BETWEEN", "DISTINCT", "UNION", "ALL",
    ],
    pgsql: [
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "ON",
      "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "RETURNING", "INSERT", "INTO",
      "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "VIEW",
      "INDEX", "SCHEMA", "SEQUENCE", "SERIAL", "BIGSERIAL", "UUID", "JSONB", "CONFLICT",
      "DO", "NOTHING", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "EXPLAIN",
      "ANALYZE", "VACUUM", "WITH", "RECURSIVE", "AS", "AND", "OR", "NOT", "NULL", "IS",
      "IN", "LIKE", "ILIKE", "BETWEEN", "DISTINCT", "UNION", "ALL", "LATERAL",
    ],
    sqlite: [
      "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "ON",
      "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "INSERT", "INTO", "VALUES",
      "UPDATE", "SET", "DELETE", "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX",
      "TRIGGER", "AUTOINCREMENT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "PRAGMA",
      "VACUUM", "REINDEX", "WITH", "RECURSIVE", "RETURNING", "CONFLICT", "BEGIN",
      "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "AS", "AND", "OR", "NOT", "NULL",
      "IS", "IN", "LIKE", "GLOB", "BETWEEN", "DISTINCT", "UNION", "ALL",
    ],
  };
  for (const [dialect, keywords] of Object.entries(dialectKeywords)) {
    monaco.languages.register({ id: dialect });
    monaco.languages.setMonarchTokensProvider(dialect, {
      ignoreCase: true,
      keywords,
      operators: ["=", ">", "<", ">=", "<=", "<>", "!=", "+", "-", "*", "/", "%"],
      tokenizer: {
        root: [
          [/[ \t\r\n]+/, ""],
          [/--.*$/, "comment"],
          [/\/\*/, "comment", "@comment"],
          [/\$[A-Za-z_0-9]*\$/, "string", "@dollarString"],
          [/'([^']|'')*'/, "string"],
          [/"([^"]|"")*"/, "string"],
          [/`([^`]|``)*`/, "string"],
          [/\d+(?:\.\d+)?(?:e[+-]?\d+)?/, "number"],
          [/[A-Za-z_][\w$]*/, { cases: { "@keywords": "keyword", "@default": "identifier" } }],
          [/[;,.]/, "delimiter"],
          [/[{}\[\]()]/, "@brackets"],
          [/[<>=!+\-*/%]+/, "operator"],
        ],
        comment: [
          [/[^/*]+/, "comment"],
          [/\*\//, "comment", "@pop"],
          [/[/*]/, "comment"],
        ],
        dollarString: [
          [/[^$]+/, "string"],
          [/\$[A-Za-z_0-9]*\$/, "string", "@pop"],
        ],
      },
    });
    monaco.languages.registerCompletionItemProvider(dialect, {
      triggerCharacters: ["."],
      provideCompletionItems: async (model, position) => {
        const range = wordRange(model, position);
        const textBefore = model.getValueInRange({
          startLineNumber: Math.max(1, position.lineNumber - 300),
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const lastLine = Math.min(model.getLineCount(), position.lineNumber + 300);
        const textAfter = model.getValueInRange({
          startLineNumber: position.lineNumber,
          startColumn: position.column,
          endLineNumber: lastLine,
          endColumn: model.getLineMaxColumn(lastLine),
        });
        let dynamic: CompletionEntry[] = [];
        try {
          dynamic = (await window.__luxbaseSqlCompletions?.({ textBefore, textAfter })) || [];
        } catch {
          dynamic = [];
        }
        const qualified = /[\w$`"]\.[\w$]*$/.test(textBefore);
        const kinds = monaco.languages.CompletionItemKind;
        const kindFor: Record<string, number> = {
          schema: kinds.Module,
          table: kinds.Struct,
          view: kinds.Interface,
          column: kinds.Field,
        };
        const suggestions = dynamic.map((item) => ({
          label: item.label,
          kind: kindFor[item.kind] ?? kinds.Text,
          detail: item.detail,
          insertText: item.insertText ?? item.label,
          sortText: item.sortText,
          range,
        }));
        if (!qualified) {
          suggestions.push(...keywords.map((label) => ({
            label,
            kind: kinds.Keyword,
            detail: "关键字",
            insertText: label,
            sortText: `9${label}`,
            range,
          })));
        }
        return { suggestions };
      },
    });
  }

  const monacoThemeFor = (mode: string) => (mode === "dark" ? "luxbase-dark" : "luxbase-light");

  monaco.editor.defineTheme("luxbase-light", {
    base: "vs",
    inherit: true,
    rules: [
      { token: "keyword", foreground: "1566C9", fontStyle: "bold" },
      { token: "string", foreground: "8B4DBB" },
      { token: "number", foreground: "0B9188" },
      { token: "comment", foreground: "8494A9", fontStyle: "italic" },
    ],
    colors: {
      "editor.background": "#FFFFFF",
      "editor.foreground": "#26384F",
      "editorLineNumber.foreground": "#A5B0BF",
      "editorLineNumber.activeForeground": "#637A96",
      "editor.selectionBackground": "#DCEBFF",
      "editor.lineHighlightBackground": "#F8FAFD",
      "editorCursor.foreground": "#176DF5",
      "editorIndentGuide.background1": "#F0F2F6",
    },
  });

  monaco.editor.defineTheme("luxbase-dark", {
    base: "vs-dark",
    inherit: true,
    rules: [
      { token: "keyword", foreground: "7FB2FF", fontStyle: "bold" },
      { token: "string", foreground: "C39BF5" },
      { token: "number", foreground: "4BCFC0" },
      { token: "comment", foreground: "8795A8", fontStyle: "italic" },
    ],
    colors: {
      "editor.background": "#10151D",
      "editor.foreground": "#DCE5F0",
      "editorLineNumber.foreground": "#4C5A6D",
      "editorLineNumber.activeForeground": "#93A5BC",
      "editor.selectionBackground": "#26456D",
      "editor.lineHighlightBackground": "#161D27",
      "editorCursor.foreground": "#4D8DFF",
      "editorIndentGuide.background1": "#1F2833",
    },
  });

  const editor = monaco.editor.create(host, {
    model: null,
    theme: monacoThemeFor(document.documentElement.dataset.theme === "dark" ? "dark" : "light"),
    automaticLayout: true,
    minimap: { enabled: false },
    lineNumbersMinChars: 3,
    glyphMargin: true,
    folding: true,
    foldingStrategy: "indentation",
    fontFamily: '"SFMono-Regular", "Cascadia Code", "Roboto Mono", Menlo, monospace',
    fontSize: 13,
    lineHeight: 21,
    padding: { top: 8, bottom: 8 },
    renderLineHighlight: "all",
    scrollBeyondLastLine: false,
    wordWrap: "off",
    suggest: { showKeywords: true },
    cursorBlinking: "smooth",
    cursorSmoothCaretAnimation: "on",
    cursorWidth: 2,
    smoothScrolling: true,
    mouseWheelZoom: true,
    matchBrackets: "always",
    bracketPairColorization: { enabled: true, independentColorPoolPerBracketType: true },
    guides: { bracketPairs: true, indentation: true, highlightActiveIndentation: true },
    occurrencesHighlight: "singleFile",
    selectionHighlight: true,
    roundedSelection: false,
    renderWhitespace: "none",
    unicodeHighlight: { ambiguousCharacters: false, invisibleCharacters: false, nonBasicASCII: false },
    copyWithSyntaxHighlighting: true,
    quickSuggestions: { other: true, comments: false, strings: false },
    acceptSuggestionOnEnter: "smart",
    tabSize: 2,
    insertSpaces: true,
    detectIndentation: false,
    stickyScroll: { enabled: false },
    ariaLabel: "SQL 编辑器",
  });


  // The renderer's 编辑器选项 popover owns these values; applying them here keeps a single source
  // of truth for Monaco and lets the choices survive relaunches.
  const applyEditorOptions = (opts: EditorOptions) => {
    editor.updateOptions({
      fontSize: opts.fontSize,
      lineHeight: Math.round(opts.fontSize * 1.6),
      wordWrap: opts.wordWrap ? "on" : "off",
      guides: {
        indentation: opts.indentGuides,
        bracketPairs: opts.bracketColors,
        highlightActiveIndentation: opts.indentGuides,
      },
      bracketPairColorization: { enabled: opts.bracketColors, independentColorPoolPerBracketType: true },
      cursorSmoothCaretAnimation: opts.smoothCaret ? "on" : "off",
      cursorBlinking: opts.smoothCaret ? "smooth" : "blink",
    });
  };
  try {
    const stored = JSON.parse(localStorage.getItem("luxbase.editorOpts.v1") || "null");
    if (stored) applyEditorOptions({ ...EDITOR_OPTIONS_DEFAULT, ...stored });
  } catch {
    // Corrupt stored options fall back to the defaults above.
  }
  window.__luxbaseEditorSetOptions = applyEditorOptions;

  // Monaco's own placeholder contribution never attaches when the editor starts without a model.
  const emptyHint = document.createElement("div");
  emptyHint.className = "editor-empty-hint";
  emptyHint.innerHTML = "在这里编写 SQL，<b>⌘↵</b> 执行；选中部分文本时只执行选中部分。<br>输入表名、库名或「别名.」会提示补全；多条语句用分号分隔，每条的结果分别显示在下方。";
  host.appendChild(emptyHint);
  const syncEmptyHint = () => {
    const model = editor.getModel();
    emptyHint.hidden = !model || model.getValueLength() > 0;
    if (!emptyHint.hidden) {
      // Anchor the hint to the exact caret origin (after the line-number gutter and
      // editor padding) so it never sits left of / beyond the insertion point.
      const layout = editor.getLayoutInfo();
      emptyHint.style.left = `${layout.contentLeft}px`;
      emptyHint.style.top = `${editor.getTopForLineNumber(1)}px`;
      emptyHint.style.fontSize = `${editor.getOption(monaco.editor.EditorOption.fontSize)}px`;
      emptyHint.style.lineHeight = `${editor.getOption(monaco.editor.EditorOption.lineHeight)}px`;
    }
  };
  editor.onDidChangeModel(syncEmptyHint);
  editor.onDidChangeModelContent(syncEmptyHint);
  editor.onDidLayoutChange(syncEmptyHint);
  syncEmptyHint();

  window.__luxbaseGetSql = () => editor.getValue();
  window.__luxbaseGetSqlSelection = () => {
    const selection = editor.getSelection();
    const model = editor.getModel();
    if (!selection || !model || selection.isEmpty()) return "";
    return model.getValueInRange(selection);
  };
  window.__luxbaseSetSql = (sql: string) => {
    if (editor.getValue() !== sql) editor.setValue(sql);
  };
  window.__luxbaseReplaceSqlSelection = (sql: string) => {
    const selection = editor.getSelection();
    if (!selection || selection.isEmpty()) {
      window.__luxbaseSetSql?.(sql);
      return;
    }
    editor.executeEdits("menu", [{ range: selection, text: sql, forceMoveMarkers: true }]);
    editor.focus();
  };
  window.__luxbaseEditorInsertText = (text: string) => {
    editor.focus();
    const selection = editor.getSelection();
    if (!selection) return;
    editor.executeEdits("native-menu", [{ range: selection, text, forceMoveMarkers: true }]);
  };
  window.__luxbaseEditorDeleteSelection = () => {
    editor.focus();
    const selection = editor.getSelection();
    if (!selection || selection.isEmpty()) return;
    editor.executeEdits("native-menu", [{ range: selection, text: "", forceMoveMarkers: true }]);
  };
  window.__luxbaseEditorAction = (action: string) => {
    editor.focus();
    const commands: Record<string, string> = {
      undo: "undo",
      redo: "redo",
      cut: "editor.action.clipboardCutAction",
      copy: "editor.action.clipboardCopyAction",
      paste: "editor.action.clipboardPasteAction",
      selectAll: "editor.action.selectAll",
    };
    const command = commands[action];
    if (command) editor.trigger("menu", command, null);
  };
  const models = new Map<string, import("monaco-editor").editor.ITextModel>();
  const viewStates = new Map<string, import("monaco-editor").editor.ICodeEditorViewState | null>();
  let currentModelId: string | null = null;
  const languageFor = (dialect: string) => (dialect === "postgres" ? "pgsql" : dialect === "sqlite" ? "sqlite" : "mysql");
  window.__luxbaseEditorOpen = (id: string, sql: string, dialect: string, focus = false) => {
    if (currentModelId && currentModelId !== id && editor.getModel()) viewStates.set(currentModelId, editor.saveViewState());
    let model = models.get(id);
    if (!model || model.isDisposed()) {
      model = monaco.editor.createModel(sql ?? "", languageFor(dialect));
      models.set(id, model);
    } else if (model.getLanguageId() !== languageFor(dialect) && model.getLanguageId() !== "plaintext") {
      monaco.editor.setModelLanguage(model, languageFor(dialect));
    }
    currentModelId = id;
    if (editor.getModel() !== model) {
      editor.setModel(model);
      const state = viewStates.get(id);
      if (state) editor.restoreViewState(state);
    }
    applyDialectLabel(dialect);
    if (focus) editor.focus();
  };
  window.__luxbaseEditorClose = (id: string) => {
    const model = models.get(id);
    if (model) {
      if (editor.getModel() === model) editor.setModel(null);
      model.dispose();
    }
    models.delete(id);
    viewStates.delete(id);
    if (currentModelId === id) currentModelId = null;
  };
  window.__luxbaseColorizeSql = (text: string, dialect: string) => monaco.editor.colorize(text, languageFor(dialect), { tabSize: 4 });
  editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => window.__luxbaseCommand?.("run"));
  editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyR, () => window.__luxbaseCommand?.("run"));
  editor.onDidChangeCursorPosition((event) => {
    const text = `Ln ${event.position.lineNumber}, Col ${event.position.column}`;
    const footer = document.querySelector(".editor-footer > span");
    if (footer) footer.textContent = text;
    const status = document.querySelector(".status-right span:first-child");
    if (status) status.textContent = text;
  });
  let largeScriptMode = false;
  let activeDialect = "mysql";
  const applyDialectLabel = (dialect: string) => {
    activeDialect = dialect === "postgres" ? "pgsql" : dialect === "sqlite" ? "sqlite" : "mysql";
    const dialectLabel = document.querySelector(".editor-footer .status-text");
    if (dialectLabel) {
      dialectLabel.textContent = `${dialect === "postgres" ? "PostgreSQL" : dialect === "mysql" ? "MySQL" : dialect === "sqlite" ? "SQLite" : "SQL"} 方言 · UTF-8`;
    }
    const model = editor.getModel();
    if (model && !largeScriptMode) monaco.editor.setModelLanguage(model, activeDialect);
    scheduleLint();
  };
  window.__luxbaseSetSqlDialect = (dialect: string) => {
    applyDialectLabel(dialect);
  };
  editor.onDidChangeModelContent(() => {
    if (currentModelId) window.__luxbaseEditorChanged?.(currentModelId, editor.getValue());
    const isLarge = editor.getValue().length > 500_000;
    if (isLarge === largeScriptMode) return;
    largeScriptMode = isLarge;
    monaco.editor.setModelLanguage(editor.getModel()!, isLarge ? "plaintext" : activeDialect);
    editor.updateOptions({
      occurrencesHighlight: isLarge ? "off" : "singleFile",
      renderWhitespace: "none",
      stickyScroll: { enabled: false },
    });
  });
  const problemsButton = document.getElementById("editor-problems");
  let lintTimer = 0;
  function runLint() {
    const model = editor.getModel();
    if (!model) return;
    const value = model.getValue();
    const problems = largeScriptMode || value.length > 200_000
      ? []
      : lintSql(value, activeDialect === "pgsql" ? "postgres" : activeDialect);
    monaco.editor.setModelMarkers(model, "luxbase-sql", problems.map((problem) => ({
      severity: problem.severity === "error" ? monaco.MarkerSeverity.Error : monaco.MarkerSeverity.Warning,
      message: problem.message,
      startLineNumber: problem.line,
      startColumn: problem.col,
      endLineNumber: problem.endLine,
      endColumn: Math.max(problem.endCol, problem.col + 1),
    })));
    if (!problemsButton) return;
    const errors = problems.filter((problem) => problem.severity === "error").length;
    const warnings = problems.length - errors;
    problemsButton.hidden = problems.length === 0;
    problemsButton.classList.toggle("warn", errors === 0);
    problemsButton.textContent = errors ? `${errors} 个错误${warnings ? ` · ${warnings} 个警告` : ""}` : `${warnings} 个警告`;
    problemsButton.title = problems.slice(0, 6).map((problem) => `第 ${problem.line} 行：${problem.message}`).join("\n");
  }
  function scheduleLint() {
    window.clearTimeout(lintTimer);
    lintTimer = window.setTimeout(runLint, 250);
  }
  editor.onDidChangeModelContent(scheduleLint);
  editor.onDidChangeModel(scheduleLint);
  problemsButton?.addEventListener("click", () => {
    const model = editor.getModel();
    if (!model) return;
    const markers = monaco.editor.getModelMarkers({ resource: model.uri });
    const first = markers.find((marker) => marker.severity === monaco.MarkerSeverity.Error) || markers[0];
    if (!first) return;
    editor.setPosition({ lineNumber: first.startLineNumber, column: first.startColumn });
    editor.revealLineInCenter(first.startLineNumber);
    editor.focus();
  });
  scheduleLint();

  window.__luxbaseEditorReady = true;
  // The renderer owns the appearance preference and calls this whenever it changes; the editor is
  // created with the current theme, so a dark launch never shows a light SQL pane.
  window.__luxbaseSetEditorTheme = (mode) => monaco.editor.setTheme(monacoThemeFor(mode));
  window.dispatchEvent(new Event("luxbase:editor-ready"));

  document.querySelectorAll(".editor-gutter, .code-editor, .autocomplete-hint").forEach((node) => {
    if (node instanceof HTMLElement) node.style.display = "none";
  });
}

export default function App() {
  const rootRef = useRef<HTMLDivElement>(null);
  const [mounted, setMounted] = useState(false);
  const body = useMemo(() => extractBody(shellHtml), []);

  useEffect(() => {
    if (!rootRef.current) return;
    window.__LUXBASE_DEFER_MOUNT__ = true;
    mountUi();
    setMounted(true);

    const boot = async () => {
      try {
        await mountMonaco();
      } finally {
        void runScreenshotMode();
      }
    };
    void boot();
  }, []);

  return (
    <div ref={rootRef} className={mounted ? "app-react-root mounted" : "app-react-root"}>
      <div dangerouslySetInnerHTML={{ __html: body }} />
      <div id="monaco-editor-host" aria-hidden="false" />
    </div>
  );
}
