// Runs before the renderer mounts, in the packaged React build (imported first from
// src/main.tsx) and in the dependency-free classic-script build (loaded before ui/app.js), so the
// first paint already uses the chosen theme instead of flashing the light one.
// The key is duplicated in ui/app.js, which owns runtime switching; this file must stay a
// standalone script with no imports, because it can run before any module exists.
(function applyThemeBeforePaint() {
  var THEME_KEY = "luxbase.theme.v1";
  var preference = "system";
  try {
    var stored = window.localStorage.getItem(THEME_KEY);
    if (stored === "light" || stored === "dark" || stored === "system") preference = stored;
  } catch {
    preference = "system";
  }
  var dark = preference === "dark" || (preference === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.dataset.theme = dark ? "dark" : "light";
  document.documentElement.dataset.themePreference = preference;
})();
