import {
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
} from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const appName = "LuxBase.app";
const bundleIdentifier = "com.luxbase.app";
const legacyBundleIdentifier = "com.luxbase.studio";
const sourceBundle = resolve(root, appName);
const defaultInstallRoot = resolve(homedir(), "Applications");
const installRoot = process.env.LUXBASE_INSTALL_DIR || process.env.LUXBASE_INSTALL_DIR
  ? resolve(process.env.LUXBASE_INSTALL_DIR || process.env.LUXBASE_INSTALL_DIR)
  : defaultInstallRoot;
const installedBundle = resolve(installRoot, appName);

function plistPath(bundle) {
  return join(bundle, "Contents", "Info.plist");
}

function assertKnownBundle(bundle) {
  const plist = plistPath(bundle);
  if (!existsSync(plist)) {
    throw new Error(`Refusing to remove ${bundle}: its bundle metadata is missing.`);
  }
  const contents = readFileSync(plist, "utf8");
  const known = contents.includes(`<string>${bundleIdentifier}</string>`)
    || contents.includes(`<string>${legacyBundleIdentifier}</string>`);
  if (!known) {
    throw new Error(`Refusing to remove ${bundle}: it is not ${bundleIdentifier} (or its legacy identifier).`);
  }
}

function removeBundles(paths) {
  for (const bundle of paths) {
    if (!existsSync(bundle)) continue;
    assertKnownBundle(bundle);
    rmSync(bundle, { recursive: true, force: true });
    console.log(`Removed stale LuxBase bundle: ${bundle}`);
  }
}

// Build first so a failed install can never leave the owner without the fresh
// application bundle in app/.
const build = spawnSync(process.execPath, [resolve(root, "scripts/build-macos-app.js")], {
  cwd: root,
  stdio: "inherit",
  env: process.env,
});
if (build.status !== 0) {
  process.exit(build.status ?? 1);
}

if (!existsSync(plistPath(sourceBundle))) {
  throw new Error("The build did not produce LuxBase.app.");
}
assertKnownBundle(sourceBundle);

// Remove only known installed/duplicate LuxBase bundles. The freshly built
// source bundle is not on this list. User database connections, credentials,
// caches and preferences are deliberately left untouched.
removeBundles([
  installedBundle,
  resolve(defaultInstallRoot, appName),
  resolve("/Applications", appName),
  resolve(root, "build", appName),
  resolve(root, "dist", appName),
  resolve(root, "src-tauri/target/release/bundle/macos", appName),
  // Pre-rename installs carry the legacy product name and identifier; upgrading replaces them.
  resolve(installRoot, "LuxBase Studio.app"),
  resolve(defaultInstallRoot, "LuxBase Studio.app"),
  resolve("/Applications", "LuxBase Studio.app"),
  resolve(root, "LuxBase Studio.app"),
]);

mkdirSync(dirname(installedBundle), { recursive: true });
cpSync(sourceBundle, installedBundle, {
  recursive: true,
  verbatimSymlinks: true,
});

const verify = spawnSync("/usr/bin/codesign", ["--verify", "--deep", "--strict", installedBundle], {
  stdio: "ignore",
});
if (verify.status !== 0) {
  rmSync(installedBundle, { recursive: true, force: true });
  throw new Error("The installed LuxBase bundle failed code-signature verification.");
}

const register = spawnSync(
  "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister",
  ["-f", installedBundle],
  { stdio: "ignore" },
);
if (register.status !== 0) {
  console.warn("LuxBase was installed, but Launch Services could not refresh its registration.");
}

console.log(`Installed latest LuxBase: ${installedBundle}`);
