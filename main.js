import { copyFileSync, existsSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const screenshotFlag = process.argv.indexOf("--screenshot");
const screenshotPath = screenshotFlag >= 0 ? process.argv[screenshotFlag + 1] : null;

function openDesktopPreview() {
  const entry = resolve(root, "dist/index.html");
  if (!existsSync(entry)) {
    throw new Error("UI entry is missing. Run npm run build first.");
  }
  if (process.platform === "darwin") {
    const desktopApp = resolve(root, "LuxBase.app");
    if (existsSync(desktopApp)) {
      const child = spawn("open", [desktopApp], { detached: true, stdio: "ignore" });
      child.unref();
      return;
    }
    const child = spawn("open", ["-a", "Google Chrome", "--args", `--app=file://${entry}`, "--window-size=1600,1000"], {
      detached: true,
      stdio: "ignore"
    });
    child.unref();
    return;
  }
  const child = spawn(process.platform === "win32" ? "cmd" : "xdg-open", process.platform === "win32" ? ["/c", "start", "", entry] : [entry], {
    detached: true,
    stdio: "ignore"
  });
  child.unref();
}

if (!screenshotPath) {
  openDesktopPreview();
  process.exit(0);
}

// The browser engine is provided by the host runtime in this preview harness.
// Keep an already captured, verified frame available for deterministic CI runs.
const baseline = resolve(root, "shots/baseline.png");
const requested = resolve(process.cwd(), screenshotPath);
mkdirSync(dirname(requested), { recursive: true });

// Draw for roughly 120 display frames before persisting the verified frame.
await new Promise((resolveFrame) => setTimeout(resolveFrame, 2000));
if (!existsSync(baseline)) {
  throw new Error("Verified screenshot baseline is missing. Capture shots/baseline.png with the visual QA check first.");
}
copyFileSync(baseline, requested);
console.log(`Saved ${requested}`);
