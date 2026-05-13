#!/usr/bin/env bash
set -euo pipefail

export NO_PROXY="${NO_PROXY:-localhost,127.0.0.1,::1}"
export no_proxy="${no_proxy:-localhost,127.0.0.1,::1}"

CODEX_BIN="${CODEX_BIN:-}"
if [[ -z "$CODEX_BIN" ]]; then
  if [[ -x "/Applications/Codex.app/Contents/Resources/codex" ]]; then
    CODEX_BIN="/Applications/Codex.app/Contents/Resources/codex"
  else
    CODEX_BIN="$(command -v codex || true)"
  fi
fi

if [[ -z "$CODEX_BIN" ]]; then
  echo "codex CLI not found. Install Codex.app or put codex on PATH." >&2
  exit 1
fi

echo "Using codex: $CODEX_BIN"
"$CODEX_BIN" exec --skip-git-repo-check --dangerously-bypass-approvals-and-sandbox "只回复 pong"
