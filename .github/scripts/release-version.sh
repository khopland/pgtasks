#!/usr/bin/env bash
# Prints the version of the newest "## [x.y.z]" section in CHANGELOG.md, which is the release that section
# describes, after checking that it is a release version and that the section says something. With a tag
# argument, also checks that the tag is v<version>.
set -euo pipefail
cd "$(dirname "$0")/../.."

version=$(sed -n -E 's/^## \[([0-9][^]]*)\].*/\1/p' CHANGELOG.md | head -n 1)
if [[ -z "$version" ]]; then
  echo "::error file=CHANGELOG.md::CHANGELOG.md has no '## [x.y.z]' section." >&2
  exit 1
fi
if [[ ! "$version" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.]+)?$ || "$version" == *SNAPSHOT* ]]; then
  echo "::error file=CHANGELOG.md::'## [$version]' is not a release version such as 1.2.0 or 1.2.0-rc.1." >&2
  exit 1
fi
if [[ -z "$(bash .github/scripts/release-notes.sh "$version")" ]]; then
  echo "::error file=CHANGELOG.md::The '## [$version]' section in CHANGELOG.md is empty." >&2
  exit 1
fi
if [[ $# -gt 0 && "$1" != "v$version" ]]; then
  echo "::error::Tag $1 doesn't match the newest version in CHANGELOG.md, $version (expected v$version)." >&2
  exit 1
fi
echo "$version"
