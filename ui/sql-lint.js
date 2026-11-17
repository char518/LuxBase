// Immediate, dialect-aware SQL feedback for the editor. A scanner, not a parser: it reports only
// defects a writer can see and fix on the spot (unterminated literals, unbalanced brackets, stray
// commas, truncated statements). Semantic errors still come from the server when a statement runs.

export const STATEMENT_HEADS = new Set([
  "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME",
  "EXPLAIN", "SHOW", "DESCRIBE", "DESC", "USE", "SET", "BEGIN", "START", "COMMIT", "ROLLBACK",
  "SAVEPOINT", "RELEASE", "GRANT", "REVOKE", "PRAGMA", "VACUUM", "ANALYZE", "REINDEX", "ATTACH",
  "DETACH", "REPLACE", "MERGE", "CALL", "DO", "LOCK", "UNLOCK", "COPY", "COMMENT", "REFRESH",
  "VALUES", "TABLE", "HANDLER", "PREPARE", "EXECUTE", "DEALLOCATE", "DISCARD", "LOAD", "OPTIMIZE",
  "CHECK", "REPAIR", "FLUSH", "RESET", "NOTIFY", "LISTEN", "UNLISTEN", "CLUSTER", "REASSIGN",
]);

const COMMA_STOPS = new Set([
  "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT",
  "RETURNING", "WINDOW", "FETCH", "FOR", ")", ";",
]);

const INCOMPLETE_TAILS = new Set([
  "WHERE", "AND", "OR", "NOT", "SET", "VALUES", "BY", "ON", "JOIN", "INNER", "LEFT", "RIGHT",
  "FULL", "CROSS", "LIKE", "ILIKE", "IN", "BETWEEN", "AS", "FROM", "INTO", "UPDATE", "DELETE",
  "INSERT", "SELECT", "HAVING", "WHEN", "THEN", "ELSE", "CASE", "DISTINCT", "ALL", "EXISTS",
]);

export function tokenizeSql(text, dialect) {
  const tokens = [];
  let index = 0;
  let line = 1;
  let col = 1;
  const advance = (count) => {
    for (let i = 0; i < count; i += 1) {
      if (text[index] === "\n") { line += 1; col = 1; } else col += 1;
      index += 1;
    }
  };
  const push = (kind, startLine, startCol, closed, value) => {
    tokens.push({ kind, text: value, line: startLine, col: startCol, endLine: line, endCol: col, closed });
  };
  while (index < text.length) {
    const ch = text[index];
    if (/\s/.test(ch)) { advance(1); continue; }
    const startLine = line;
    const startCol = col;
    const startIndex = index;
    if ((ch === "-" && text[index + 1] === "-") || (ch === "#" && dialect === "mysql")) {
      while (index < text.length && text[index] !== "\n") advance(1);
      push("comment", startLine, startCol, true, text.slice(startIndex, index));
      continue;
    }
    if (ch === "/" && text[index + 1] === "*") {
      advance(2);
      let closed = false;
      while (index < text.length) {
        if (text[index] === "*" && text[index + 1] === "/") { advance(2); closed = true; break; }
        advance(1);
      }
      push("comment", startLine, startCol, closed, "");
      continue;
    }
    if (ch === "'" || ch === '"' || ch === "`") {
      const quote = ch;
      const escapeBackslash = quote === "'" && dialect !== "postgres";
      advance(1);
      let closed = false;
      while (index < text.length) {
        const cur = text[index];
        if (escapeBackslash && cur === "\\") { advance(2); continue; }
        if (cur === quote) {
          if (text[index + 1] === quote) { advance(2); continue; }
          advance(1);
          closed = true;
          break;
        }
        if (cur === "\n" && quote !== "'") break;
        advance(1);
      }
      push(quote === "'" ? "string" : "ident", startLine, startCol, closed, "");
      continue;
    }
    if (ch === "$" && dialect === "postgres") {
      const tag = /^\$[A-Za-z_0-9]*\$/.exec(text.slice(index));
      if (tag) {
        advance(tag[0].length);
        const end = text.indexOf(tag[0], index);
        if (end === -1) {
          while (index < text.length) advance(1);
          push("string", startLine, startCol, false, "");
        } else {
          advance(end - index + tag[0].length);
          push("string", startLine, startCol, true, "");
        }
        continue;
      }
    }
    if (/[0-9]/.test(ch)) {
      advance(1);
      while (index < text.length && /[0-9._]/.test(text[index])) advance(1);
      if (index < text.length && /[eE]/.test(text[index])) {
        advance(1);
        if (index < text.length && /[+-]/.test(text[index])) advance(1);
        while (index < text.length && /[0-9]/.test(text[index])) advance(1);
      }
      push("number", startLine, startCol, true, text.slice(startIndex, index));
      continue;
    }
    if (/[A-Za-z_]/.test(ch)) {
      while (index < text.length && /[A-Za-z0-9_$]/.test(text[index])) advance(1);
      push("word", startLine, startCol, true, text.slice(startIndex, index));
      continue;
    }
    const pair = text.slice(index, index + 2);
    const size = ["<=", ">=", "<>", "!=", "||", "::", "->"].includes(pair) ? 2 : 1;
    advance(size);
    push("punct", startLine, startCol, true, size === 2 ? pair : ch);
  }
  return tokens;
}

export function lintSql(text, dialect) {
  const tokens = tokenizeSql(text, dialect);
  const problems = [];
  const add = (token, message, severity = "error") => {
    problems.push({ line: token.line, col: token.col, endLine: token.endLine, endCol: token.endCol, message, severity });
  };
  const brackets = [];
  const statements = [[]];
  const tainted = new Set();
  let depth = 0;
  for (const token of tokens) {
    if (!token.closed) {
      add(token, token.kind === "comment" ? "块注释没有闭合的 */" : token.kind === "string" ? "字符串没有闭合的引号" : "引号标识符没有闭合");
      // An unterminated literal swallows the rest of the statement; reporting a second
      // "incomplete statement" on top of it would only add noise.
      tainted.add(statements.length - 1);
      continue;
    }
    if (token.kind === "comment") continue;
    if (token.kind === "punct" && token.text === "(") { brackets.push(token); depth += 1; }
    if (token.kind === "punct" && token.text === ")") {
      if (brackets.length) brackets.pop();
      else add(token, "多余的右括号，没有与之配对的左括号");
      depth = Math.max(0, depth - 1);
    }
    if (token.kind === "punct" && token.text === ";" && depth === 0) statements.push([]);
    else statements[statements.length - 1].push(token);
  }
  for (const token of brackets) add(token, "左括号没有闭合");
  for (const [statementIndex, statement] of statements.entries()) {
    if (!statement.length || tainted.has(statementIndex)) continue;
    const first = statement[0];
    const head = first.kind === "word" ? first.text.toUpperCase() : first.text;
    if (first.kind === "word" && !STATEMENT_HEADS.has(head)) {
      add(first, `无法识别的语句开头「${first.text}」`, "warning");
    }
    for (let i = 0; i < statement.length; i += 1) {
      const token = statement[i];
      if (token.kind === "punct" && token.text === ",") {
        const next = statement[i + 1];
        const nextText = next ? (next.kind === "word" ? next.text.toUpperCase() : next.text) : ";";
        if (!next || COMMA_STOPS.has(nextText)) add(token, "多余的逗号：后面没有跟随表达式");
      }
    }
    const last = statement[statement.length - 1];
    const tail = last.kind === "word" ? last.text.toUpperCase() : last.text;
    if (last.kind === "word" && INCOMPLETE_TAILS.has(tail)) {
      add(last, `语句不完整：「${last.text}」后面缺少内容`);
    }
    if (last.kind === "punct" && last.text === ",") add(last, "语句以逗号结尾，缺少后续表达式");
  }
  return problems.slice(0, 60);
}