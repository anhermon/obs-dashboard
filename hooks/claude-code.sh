#!/usr/bin/env bash
# Claude Code / CoWork observability hook — exits immediately, never delays Claude

PAYLOAD=$(cat)

AGENT_FOLDER="${AGENT_FOLDER:-}"
if [ -n "$AGENT_FOLDER" ]; then
  HOOK_AGENT="cowork:$(basename "$AGENT_FOLDER")"
else
  HOOK_AGENT="claude-code"
fi

# Pass via env vars to avoid quoting complexity; run async
HOOK_PAYLOAD="$PAYLOAD" HOOK_AGENT="$HOOK_AGENT" \
  node "$HOME/dev/obs-dashboard/hooks/claude-code.js" &

exit 0
