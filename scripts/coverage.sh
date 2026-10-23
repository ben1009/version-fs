#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd -- "$repo_dir"

inventory="$(mktemp)"
trap 'rm -f -- "$inventory"' EXIT
cargo nextest list --locked --workspace --all-features --lib --bins --tests --message-format json > "$inventory"

# Empty crate skeletons contain no instrumentable code. Do not fabricate a
# coverage percentage or hide a real report-generation failure with `|| true`.
has_tests="$(python3 - "$inventory" <<'PY'
import json
import sys

with open(sys.argv[1]) as inventory:
    suites = json.load(inventory)["rust-suites"]
has_tests = any(
    not case["ignored"]
    for suite in suites.values()
    for case in suite["testcases"].values()
)
print("true" if has_tests else "false")
PY
)"

if [[ "$has_tests" == "false" ]]; then
    echo "Coverage skipped: the workspace has no runnable tests yet."
    exit 0
fi

cargo llvm-cov nextest --locked --workspace --all-features --lib --bins --tests --profile coverage "$@"
