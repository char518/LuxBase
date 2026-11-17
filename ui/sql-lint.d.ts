export type SqlToken = {
  kind: "word" | "number" | "string" | "ident" | "comment" | "punct";
  text: string;
  line: number;
  col: number;
  endLine: number;
  endCol: number;
  closed: boolean;
};

export type SqlProblem = {
  line: number;
  col: number;
  endLine: number;
  endCol: number;
  message: string;
  severity: "error" | "warning";
};

export const STATEMENT_HEADS: Set<string>;
export function tokenizeSql(text: string, dialect: string): SqlToken[];
export function lintSql(text: string, dialect: string): SqlProblem[];