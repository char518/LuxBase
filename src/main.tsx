import "../ui/theme-preload.js";
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
