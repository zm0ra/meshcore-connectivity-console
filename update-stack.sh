#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

force_rebuild=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --force)
      force_rebuild=1
      shift
      ;;
    -h|--help)
      cat <<'EOF'
Usage: ./update-stack.sh [--force]

Runs git pull --ff-only and rebuilds the Compose stack only when HEAD changed.

Options:
  --force   rebuild even when git pull does not fetch anything new
  -h, --help  show this help
EOF
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Run ./update-stack.sh --help for usage." >&2
      exit 1
      ;;
  esac
done

before_head="$(git rev-parse HEAD)"

echo "Fetching updates..."
git pull --ff-only

after_head="$(git rev-parse HEAD)"

if [[ "$before_head" == "$after_head" && "$force_rebuild" -eq 0 ]]; then
  echo "Already up to date. Skipping docker compose rebuild."
  exit 0
fi

if [[ "$before_head" != "$after_head" ]]; then
  echo "New commits pulled:"
  git --no-pager log --oneline --no-decorate "${before_head}..${after_head}"
else
  echo "Forcing rebuild on current HEAD ${after_head}."
fi

echo "Rebuilding and restarting Compose stack..."
docker compose up -d --build

