import { readFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const ledger = readFileSync(resolve(root, "checks/issue-ledger.md"), "utf8");
const rows = ledger
  .split("\n")
  .filter((line) => /^\| P[0-3]-\d+ /.test(line))
  .map((line) => line.split("|").map((cell) => cell.trim()));
const failures = [];

if (!rows.length) failures.push("the issue ledger has no rows; record at least one tracked issue");

const allowed = (status) => status === "已关闭" || status === "待验证" || status.startsWith("当前版本不支持");
for (const row of rows) {
  const id = row[1];
  const severity = row[row.length - 3];
  const status = row[row.length - 2];
  if (!/^P[0-3]$/.test(severity)) failures.push(`${id}: invalid severity ${severity}`);
  if (!allowed(status)) failures.push(`${id}: invalid or unfinished status ${status}`);
}

const fixReport = readFileSync(resolve(root, "checks/fix-report.md"), "utf8");
for (const row of rows) {
  const id = row[1];
  if (!fixReport.includes(`### ${id} —`) && !fixReport.includes(`- **${id}**`)) {
    failures.push(`${id}: fix report entry is missing`);
  }
}

const counts = rows.reduce((result, row) => {
  const status = row[row.length - 2];
  result[status] = (result[status] || 0) + 1;
  return result;
}, {});

console.log(JSON.stringify({ ok: failures.length === 0, rows: rows.length, counts, failures }, null, 2));
if (failures.length) process.exit(1);
