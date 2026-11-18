import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Tauri signs the bundle without --deep and its cargo invocation relinks luxbase-mcp
// unsigned, so every copy source must be ad-hoc signed after cargo, before bundling.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
for (const dir of ["release", "x86_64-apple-darwin/release", "universal-apple-darwin/release"]) {
  const binary = resolve(root, "src-tauri/target", dir, "luxbase-mcp");
  if (!existsSync(binary)) continue;
  const sign = spawnSync("/usr/bin/codesign", ["--force", "--sign", "-", binary], { stdio: "inherit" });
  if (sign.status !== 0) process.exit(sign.status ?? 1);
}