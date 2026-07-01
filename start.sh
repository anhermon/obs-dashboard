#!/bin/bash
# Start observability dashboard + Codex session watcher
# Dashboard: http://localhost:3000

DIR="$(cd "$(dirname "$0")" && pwd)"

cleanup() {
  echo ""
  echo "Stopping..."
  kill "$CODEX_PID" 2>/dev/null
  exit 0
}
trap cleanup INT TERM

# Start Codex watcher in background
node "$DIR/watchers/codex-watcher.js" &
CODEX_PID=$!
echo "Codex watcher started (PID $CODEX_PID)"

# Start dashboard server (foreground)
node "$DIR/server.js"
