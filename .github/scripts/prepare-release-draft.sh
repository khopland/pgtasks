#!/usr/bin/env bash
# Creates or refreshes the draft GitHub release for the newest version in CHANGELOG.md, on the tested commit
# $GITHUB_SHA, with that version's section as its notes. Publishing the draft creates the v<version> tag and
# starts the release workflow. Does nothing once that version is released.
set -euo pipefail
cd "$(dirname "$0")/../.."

version=$(bash .github/scripts/release-version.sh)
tag="v$version"

# Listing releases tells an absent release apart from authentication or API failures.
releases=$(gh api "repos/$GITHUB_REPOSITORY/releases" --paginate | jq -s 'add // []')

if jq -e --arg tag "$tag" 'any(.[]; (.draft | not) and .tag_name == $tag)' <<< "$releases" > /dev/null; then
  echo "pgtasks $version is released. For the next release, move what is under [Unreleased] in CHANGELOG.md to a new '## [x.y.z]' section." | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
  exit 0
fi

# Never move a published tag: a tag on another commit is a stray one, or a release made by hand.
remote_tag=$(git ls-remote origin "refs/tags/$tag" "refs/tags/$tag^{}")
if [[ -n "$remote_tag" ]]; then
  tag_sha=$(awk '/\^\{\}$/ { peeled = $1 } { direct = $1 } END { print peeled ? peeled : direct }' <<< "$remote_tag")
  if [[ "$tag_sha" != "$GITHUB_SHA" ]]; then
    echo "::error::Tag $tag already points to $tag_sha, not $GITHUB_SHA. Delete the stray tag, or add a section for a new version to CHANGELOG.md."
    exit 1
  fi
fi

mkdir -p target/release-notes
notes="target/release-notes/$version.md"
bash .github/scripts/release-notes.sh "$version" > "$notes"

# Only one version is drafted at a time, so a draft for any other version is stale.
draft_id=$(jq -r --arg tag "$tag" '[.[] | select(.draft and .tag_name == $tag)] | first | .id // empty' <<< "$releases")
jq -r --arg keep "$draft_id" '
  .[] | select(.draft and (.id | tostring) != $keep and (.tag_name | test("^v[0-9]+\\.[0-9]+\\.[0-9]+")))
  | "\(.id) \(.tag_name)"
' <<< "$releases" |
  while read -r id stale_tag; do
    gh api --method DELETE "repos/$GITHUB_REPOSITORY/releases/$id"
    echo "Deleted the stale draft release for $stale_tag."
  done

# Before 1.0 every release is a GitHub pre-release, as is a version such as 1.2.0-rc.1.
prerelease=false
if [[ "$version" == 0.* || "$version" == *-* ]]; then prerelease=true; fi
if [[ -n "$draft_id" ]]; then
  gh release edit "$tag" --draft --target "$GITHUB_SHA" --title "pgtasks $version" --notes-file "$notes" --prerelease="$prerelease"
else
  gh release create "$tag" --draft --target "$GITHUB_SHA" --title "pgtasks $version" --notes-file "$notes" --prerelease="$prerelease"
fi
echo "Drafted $tag on tested commit $GITHUB_SHA. Publish the draft to release pgtasks $version to Maven Central." | tee -a "${GITHUB_STEP_SUMMARY:-/dev/null}"
