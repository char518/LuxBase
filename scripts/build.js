import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const root = resolve(import.meta.dirname, "..");
const output = resolve(root, "dist");
const viteBinary = resolve(root, "node_modules/.bin/vite");
const useVite = existsSync(viteBinary) && existsSync(resolve(root, "node_modules/react-dom"));
const productionBuild = process.argv.includes("--production");

if (!useVite && productionBuild) {
  console.error(
    "Production build refused: React, Vite and Monaco dependencies are unavailable. " +
    "Install dependencies and rerun npm run build:production. The static preview can still be built with npm run build.",
  );
  process.exit(1);
}

// A build must not retain a previous macOS application inside dist/.
rmSync(output, { recursive: true, force: true });
mkdirSync(output, { recursive: true });

if (useVite) {
  const result = spawnSync(viteBinary, ["build", "--outDir", "dist"], {
    cwd: root,
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
} else {
  const source = resolve(root, "ui");
  mkdirSync(output, { recursive: true });
  cpSync(source, output, { recursive: true });

  // WKWebView can refuse ES-module imports when the standalone application is
  // opened from file://, even though the HTML itself renders. Ship the fallback
  // renderer as a classic script so its click handlers always attach. The
  // source remains an ES module for the React/Vite production renderer.
  const appScriptPath = resolve(output, "app.js");
  const appScript = readFileSync(appScriptPath, "utf8");
  const transformedAppScript = appScript.replace(
    /^export\s+function\s+mountUi\s*\(/m,
    "function mountUi(",
  );
  if (transformedAppScript === appScript || /^\s*(?:import|export)\s/m.test(transformedAppScript)) {
    throw new Error("Build verification failed: standalone renderer is not a classic script");
  }
  writeFileSync(appScriptPath, transformedAppScript);
  console.warn("Vite dependencies are unavailable; emitted the dependency-free renderer fallback.");
}

const required = ["index.html"];
for (const file of required) {
  const path = resolve(output, file);
  if (!existsSync(path) || statSync(path).size < 100) {
    throw new Error(`Build verification failed: ${file}`);
  }
}

const manifest = {
  name: "LuxBase",
  version: "2.0.0",
  builtAt: new Date().toISOString(),
  renderer: useVite ? "react-ts-monaco" : "static-fallback",
  protocols: ["mysql", "postgresql", "sqlite", "redis"]
};
writeFileSync(resolve(output, "build-manifest.json"), JSON.stringify(manifest, null, 2));

const html = readFileSync(resolve(output, "index.html"), "utf8");
if (!useVite && (!html.includes("LuxBase") || !html.includes("view-redis") || !html.includes("audit-rows"))) {
  throw new Error("Build verification failed: required product views are missing");
}
if (!useVite && (!html.includes('src="./app.js"') || html.includes('type="module"'))) {
  throw new Error("Build verification failed: standalone interaction script is not locally loadable");
}

console.log(`LuxBase built to ${output} (${manifest.renderer})`);
