#!/bin/sh
# Build the Tauri desktop application on a host with Rust and network access.
set -eu
cd "$(dirname "$0")"
npm install
npm run validate
npm run tauri:build
