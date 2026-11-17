// ui/app.js mounts itself when imported unless this flag is already set; the React shell must
// render the markup first and then call mountUi(), so this module is imported before App.
window.__LUXBASE_DEFER_MOUNT__ = true;

export {};
