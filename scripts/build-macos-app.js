import { cpSync, existsSync, mkdirSync, readdirSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

// Packages the Tauri desktop application (Rust drivers + React/Monaco renderer) and places it at
// the single canonical location app/LuxBase.app.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const appName = "LuxBase.app";
const tauri = resolve(root, "node_modules/.bin/tauri");
const destination = resolve(root, appName);

if (process.platform !== "darwin") {
  console.error("This script packages the macOS bundle. Use `npm run tauri:build` on other platforms.");
  process.exit(1);
}
if (!existsSync(tauri)) {
  console.error("The Tauri CLI is missing. Run `npm install` first.");
  process.exit(1);
}

const withDmg = process.argv.includes("--dmg");
const universal = process.argv.includes("--universal");
const intel = process.argv.includes("--intel");
const targetTriple = universal ? "universal-apple-darwin" : intel ? "x86_64-apple-darwin" : null;
const targetDir = targetTriple ? `${targetTriple}/release` : "release";
const bundle = resolve(root, "src-tauri/target", targetDir, "bundle/macos", appName);
const cargo = process.env.CARGO || resolve(process.env.HOME || "", ".cargo/bin/cargo");
let mcpBinary;
if (universal) {
  // Tauri's universal bundle copies every cargo bin from the universal target dir but
  // only lipos the main binary itself, so merge luxbase-mcp before bundling.
  const mcpBuild = spawnSync(
    cargo,
    ["build", "--release", "--bin", "luxbase-mcp", "--features", "tauri/custom-protocol", "--target", "aarch64-apple-darwin", "--target", "x86_64-apple-darwin"],
    { cwd: resolve(root, "src-tauri"), stdio: "inherit", env: process.env },
  );
  if (mcpBuild.status !== 0) process.exit(mcpBuild.status ?? 1);
  const universalDir = resolve(root, "src-tauri/target/universal-apple-darwin/release");
  mkdirSync(universalDir, { recursive: true });
  mcpBinary = resolve(universalDir, "luxbase-mcp");
  const lipo = spawnSync(
    "/usr/bin/lipo",
    [
      "-create",
      resolve(root, "src-tauri/target/aarch64-apple-darwin/release/luxbase-mcp"),
      resolve(root, "src-tauri/target/x86_64-apple-darwin/release/luxbase-mcp"),
      "-output",
      mcpBinary,
    ],
    { stdio: "inherit" },
  );
  if (lipo.status !== 0) process.exit(lipo.status ?? 1);
  // Tauri signs the bundle without --deep, so nested binaries must arrive pre-signed.
  const signMcp = spawnSync("/usr/bin/codesign", ["--force", "--sign", "-", mcpBinary], { stdio: "inherit" });
  if (signMcp.status !== 0) process.exit(signMcp.status ?? 1);
} else if (intel) {
  mcpBinary = resolve(root, "src-tauri/target/x86_64-apple-darwin/release/luxbase-mcp");
}
const buildArgs = ["build", "--bundles", withDmg ? "app,dmg" : "app"];
if (targetTriple) buildArgs.push("--target", targetTriple);
const build = spawnSync(tauri, buildArgs, {
  cwd: root,
  stdio: "inherit",
  env: process.env,
});
if (build.status !== 0) process.exit(build.status ?? 1);
if (!existsSync(resolve(bundle, "Contents/MacOS"))) {
  throw new Error(`Tauri did not produce ${bundle}`);
}

const verify = spawnSync("/usr/bin/codesign", ["--verify", "--deep", "--strict", bundle], {
  stdio: "inherit",
});
if (verify.status !== 0) throw new Error("The Tauri bundle failed code-signature verification.");

// Ship the MCP stdio bridge inside the bundle so agents can launch it directly.
if (!universal && !intel) {
  const mcpBuild = spawnSync(cargo, ["build", "--release", "--bin", "luxbase-mcp"], {
    cwd: resolve(root, "src-tauri"),
    stdio: "inherit",
    env: process.env,
  });
  if (mcpBuild.status !== 0) process.exit(mcpBuild.status ?? 1);
  mcpBinary = resolve(root, "src-tauri/target/release/luxbase-mcp");
}
const installMcp = (appPath) => {
  cpSync(mcpBinary, resolve(appPath, "Contents/MacOS/luxbase-mcp"));
};

for (const stale of [resolve(root, "build", appName), resolve(root, "dist", appName)]) {
  rmSync(stale, { recursive: true, force: true });
}
mkdirSync(dirname(destination), { recursive: true });
rmSync(destination, { recursive: true, force: true });
cpSync(bundle, destination, { recursive: true, verbatimSymlinks: true });

const register = spawnSync(
  "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister",
  ["-f", destination],
  { stdio: "ignore" },
);
if (register.status !== 0) {
  console.warn("The macOS application was built, but Launch Services could not refresh its registration.");
}

// Stamp developer credit into the bundle and re-sign (ad-hoc) so About / GetInfo show it.
const creditApp = (appPath) => {
  const plist = resolve(appPath, "Contents", "Info.plist");
  const credit = "© 2026 charl · 开发并构建：charl";
  for (const key of ["CFBundleCopyright", "CFBundleGetInfoString"]) {
    const set = spawnSync("/usr/libexec/PlistBuddy", ["-c", `Set :${key} ${credit}`, plist], { encoding: "utf8" });
    if (set.status !== 0) spawnSync("/usr/libexec/PlistBuddy", ["-c", `Add :${key} string ${credit}`, plist], { encoding: "utf8" });
  }
  const sign = spawnSync("/usr/bin/codesign", ["--force", "--deep", "--sign", "-", appPath], { encoding: "utf8" });
  if (sign.status !== 0) throw new Error(`codesign failed for ${appPath}: ${sign.stderr || sign.stdout}`);
};
installMcp(destination);
creditApp(destination);
console.log(`Built macOS application: ${destination}`);

if (withDmg) {
  const dmgDir = resolve(root, "src-tauri/target", targetDir, "bundle/dmg");
  const dmgName = existsSync(dmgDir)
    ? readdirSync(dmgDir).find((entry) => entry.endsWith(".dmg"))
    : undefined;
  if (!dmgName) throw new Error("Tauri did not produce a DMG in src-tauri/target/release/bundle/dmg.");
  const dmgDestination = resolve(root, dmgName);
  rmSync(dmgDestination, { force: true });
  cpSync(resolve(dmgDir, dmgName), dmgDestination);

  // Inject the install guide (and keep Tauri's background / icon layout) by rewriting the
  // DMG read-write, copying the guide in, then re-compressing read-only.
  const readme = resolve(root, "installer", "安装说明.txt");
  if (existsSync(readme)) {
    const hdiutil = "/usr/bin/hdiutil";
    const run = (args) => {
      const result = spawnSync(hdiutil, args, { encoding: "utf8" });
      if (result.status !== 0) throw new Error(`hdiutil ${args[0]} failed: ${result.stderr || result.stdout}`);
      return result.stdout || "";
    };
    const rw = resolve(root, "build", "installer-rw.dmg");
    const mnt = resolve(root, "build", "dmg-mnt");
    mkdirSync(dirname(rw), { recursive: true });
    mkdirSync(mnt, { recursive: true });
    rmSync(rw, { force: true });
    run(["convert", "-format", "UDRW", "-o", rw, dmgDestination]);
    run(["attach", "-nobrowse", "-mountpoint", mnt, rw]);
    try {
      cpSync(readme, resolve(mnt, "安装说明.txt"));
      // codesign hits internal errors on universal binaries inside the mounted HFS+
      // image, so ship the already-signed staged app instead of re-signing on the mount.
      rmSync(resolve(mnt, "LuxBase.app"), { recursive: true, force: true });
      cpSync(destination, resolve(mnt, "LuxBase.app"), { recursive: true, verbatimSymlinks: true });
    } finally {
      spawnSync(hdiutil, ["detach", mnt], { encoding: "utf8" });
    }
    rmSync(dmgDestination, { force: true });
    run(["convert", "-format", "UDZO", "-o", dmgDestination, rw]);
    rmSync(rw, { force: true });
  }
  console.log(`Built macOS installer: ${dmgDestination}`);
}
