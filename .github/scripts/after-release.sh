#!/usr/bin/env bash
# The bookkeeping after releasing <version>, on a checkout of main. It moves main to the next minor
# snapshot unless main is already past the release, and points the README's BOM snippet and the
# quickstart at the release. It also makes the release the API baseline and clears the accepted
# breaks, and gives CHANGELOG.md an [Unreleased] section, the release's date and its links.
# Prints the version main is on afterwards. The release workflow adds the schema snapshot itself.
set -euo pipefail
cd "$(dirname "$0")/../.."

version=$1
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "::error::after-release.sh takes a release version such as 1.2.0, not '$version'." >&2
  exit 1
fi

# The aggregator has no parent, so its first <version> is the project's.
current=$(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' pom.xml | head -n 1)
if [[ "$(printf '%s\n' "${current%-SNAPSHOT}" "$version" | sort -V | tail -n 1)" == "$version" ]]; then
  IFS=. read -r major minor _ <<< "$version"
  next="$major.$((minor + 1)).0-SNAPSHOT"
  mvn -B -ntp -q org.codehaus.mojo:versions-maven-plugin:2.19.1:set -DnewVersion="$next" -DprocessAllModules -DgenerateBackupPoms=false >&2
else
  next=$current
fi

VERSION=$version NEXT=$next REPOSITORY=${GITHUB_REPOSITORY:-khopland/pgtasks} RELEASE_DATE=$(date -u +%F) python3 - <<'EOF'
import os, re

version, next_version = os.environ["VERSION"], os.environ["NEXT"]
url = "https://github.com/" + os.environ["REPOSITORY"]

def edit(path, pattern, replacement, flags=0):
    with open(path) as f:
        text = f.read()
    text, count = re.subn(pattern, replacement, text, count=1, flags=flags)
    if count != 1:
        raise SystemExit(f"{path}: found nothing matching {pattern!r}")
    with open(path, "w") as f:
        f.write(text)

edit("README.md", r"(<artifactId>pgtasks-bom</artifactId>\s*<version>)[^<]*(</version>)", rf"\g<1>{version}\g<2>")
edit("examples/quickstart/pom.xml", r"<pgtasks.version>[^<]*</pgtasks.version>", f"<pgtasks.version>{version}</pgtasks.version>")
edit("examples/order-service/pom.xml", r"<pgtasks.version>[^<]*</pgtasks.version>", f"<pgtasks.version>{next_version}</pgtasks.version>")
edit("pgtasks-parent/pom.xml", r"<pgtasks.api.baseline>[^<]*</pgtasks.api.baseline>", f"<pgtasks.api.baseline>{version}</pgtasks.api.baseline>")
edit("pgtasks-parent/pom.xml", r"<japicmp.skip>[^<]*</japicmp.skip>", "<japicmp.skip>false</japicmp.skip>")
edit("pgtasks-parent/pom.xml", r"([ \t]*)<excludes>.*?</excludes>", r"\g<1><excludes>\n\g<1></excludes>", re.S)

with open("CHANGELOG.md") as f:
    changelog = f.read()
heading = re.search(rf"^## \[{re.escape(version)}\][^\n]*$", changelog, re.M)
if not heading:
    raise SystemExit(f"CHANGELOG.md has no '## [{version}]' section")
if heading.group(0) == f"## [{version}]":
    changelog = changelog[:heading.start()] + f"## [{version}] - {os.environ['RELEASE_DATE']}" + changelog[heading.end():]
if not re.search(r"^## \[Unreleased\]", changelog, re.M):
    changelog = re.sub(rf"^## \[{re.escape(version)}\]", f"## [Unreleased]\n\n## [{version}]", changelog, count=1, flags=re.M)
previous = re.search(r"^\[Unreleased\]: \S*/compare/(\S+)\.\.\.HEAD$", changelog, re.M)
release_link = f"[{version}]: {url}/compare/{previous.group(1)}...v{version}" if previous else f"[{version}]: {url}/releases/tag/v{version}"
unreleased_link = f"[Unreleased]: {url}/compare/v{version}...HEAD"
if previous:
    changelog = changelog[:previous.start()] + unreleased_link + changelog[previous.end():]
else:
    changelog = changelog.rstrip("\n") + "\n\n" + unreleased_link + "\n"
if not re.search(rf"^\[{re.escape(version)}\]: ", changelog, re.M):
    changelog = changelog.replace(unreleased_link, unreleased_link + "\n" + release_link, 1)
with open("CHANGELOG.md", "w") as f:
    f.write(changelog)
EOF

echo "$next"
