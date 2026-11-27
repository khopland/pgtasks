#!/usr/bin/env bash
# Prints the "## [<version>]" section of CHANGELOG.md without its heading, its link definitions or the blank
# lines around it: the release notes.
set -euo pipefail
cd "$(dirname "$0")/../.."

awk -v header="## [$1]" '
  /^## / { if (found) exit; if (index($0, header) == 1) { found = 1; next } }
  found && !/^\[[^]]*\]: / { print }
' CHANGELOG.md | sed -e '/./,$!d' | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'
