#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")" || exit 1

commit_message="Minor Updates"
if [[ -n "${1:-}" ]]; then
  commit_message="$1"
fi

git add .

git diff --cached --quiet || git commit -m "$commit_message"

git push
