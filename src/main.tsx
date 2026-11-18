import "../ui/theme-preload.js";
import "../ui/i18n-dict-1.js";
import "../ui/i18n-dict-2.js";
import "../ui/i18n-dict-3.js";
import "../ui/i18n-dict-4.js";
import "../ui/i18n.js";
import "./defer-ui-mount";
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import "./styles.css";
import "../ui/styles.css";
import "../ui/theme-dark-auto.css";
import "../ui/theme-dark.css";

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
