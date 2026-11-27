# Releasing

Releases go to Maven Central through the [Central Portal](https://central.sonatype.com). A release
starts when a new version appears in `CHANGELOG.md`: the build workflow drafts a GitHub release for
it, and publishing the draft starts `.github/workflows/release.yml`. The poms on `main` always carry
the next snapshot; the release workflow takes the version from the tag the draft creates.

## One-time setup

1. **Namespace.** Sign in to the Central Portal with the `khopland` GitHub account. The
   `io.github.khopland` namespace is verified with it, and covers the `io.github.khopland.pgtasks`
   group.
2. **Token.** In the portal, under *View Account*, generate a user token.
3. **Signing key.** Create a GPG key for releases and publish its public half, which Central checks
   signatures against:

   ```sh
   gpg --quick-gen-key 'pgtasks releases' ed25519 sign 3y
   gpg --keyserver keys.openpgp.org --send-keys <key id>
   gpg --armor --export-secret-keys <key id>     # the value of GPG_PRIVATE_KEY
   ```

4. **Secrets.** Add these as repository secrets (*Settings → Secrets and variables → Actions*):

   | Secret | Value |
   |---|---|
   | `CENTRAL_USERNAME` | The token's username |
   | `CENTRAL_TOKEN` | The token's password |
   | `GPG_PRIVATE_KEY` | The armored secret key |
   | `GPG_PASSPHRASE` | Its passphrase |

5. **Pull requests from Actions.** Under *Settings → Actions → General*, allow GitHub Actions to
   create pull requests, for the one the release workflow opens after each release.

## A release

1. **Bump the version in the changelog.** In a pull request, move what is under `[Unreleased]` in
   `CHANGELOG.md` to a new section for the version, above the last release, and leave
   `[Unreleased]` empty:

   ```markdown
   ## [Unreleased]

   ## [0.2.0]

   ### Added
   ...
   ```

   The newest `## [x.y.z]` heading is the version to release, and its section becomes the release
   notes. Before 1.0, a break or a new feature raises the minor version and a fix the patch
   version. `release-check` fails the build on a heading that isn't a release version, such as
   `0.2.0-SNAPSHOT`, or on an empty section. The date and the links at the bottom can be left out;
   the pull request after the release adds them.
2. **Merge it.** Once the build passes on `main`, its `release-draft` job keeps a draft release
   "pgtasks 0.2.0" on that commit, with the section as its notes, under *Releases*. Every green
   push to `main` refreshes it and moves it to the new commit until the version is released, so
   anything merged in the meantime is part of the release.
3. **Publish the draft.** That creates the tag `v0.2.0` and starts the release workflow. It checks
   the tag against the changelog and sets the version. It runs every test, signs every artifact
   and uploads one bundle, which the Central Portal publishes once it validates. No second step in
   the portal is needed. A bundle that fails validation is never published: delete the release
   and its tag, fix the problem, and the next green build drafts it again. Before 1.0, every
   release is a GitHub pre-release.
4. **Merge the pull request after the release.** Once Maven Central serves the release, usually
   within half an hour, the workflow opens "Move main past the 0.2.0 release" and starts its build.
   It:
   - snapshots the schemas the release shipped into each module's
     `src/test/resources/.../released/0.2.0/`, with the rows the release leaves in every state.
     `SchemaFreezeTest` then keeps those versioned files from changing, and `SchemaUpgradeTest` and
     `RateLimitSchemaUpgradeTest` upgrade from them;
   - moves `main` to the next minor snapshot, unless `main` is already past the release. It points
     the README's BOM snippet and the quickstart at the release;
   - sets `pgtasks.api.baseline` to the release and empties the japicmp plugin's `<excludes>`, so
     every build compares each jar with it; see [API compatibility](#api-compatibility);
   - adds the release's date and links to `CHANGELOG.md`.

A version such as `0.2.0-rc.1` is released the same way, as a GitHub pre-release, and leaves `main`
as it is.

**If the release workflow fails** after the draft is published, fix the cause. Then run *Actions →
release → Run workflow* with the tag. If the version already reached Maven Central, turn
*publish* off: the workflow then skips the upload and only opens the pull request after the release.

## Checking the release build locally

The `release` profile builds the sources and Javadoc jars, signs everything and bundles it. To
try it without uploading, set a release version in a copy of the repository and run:

```sh
mvn -Prelease verify -DskipTests -Dgpg.skip
```

The enforcer in the profile fails on a snapshot version or a snapshot dependency.

## API compatibility

Once a release is the baseline, `mvn verify` runs japicmp on every published jar and fails on any
binary or source incompatible change to a public or protected class or member, with a report in
each module's `target/japicmp/`. Before 1.0 a minor release may still break the API, but only on
purpose: add the changed class or member to the plugin's `<excludes>` in `pgtasks-parent/pom.xml`
(the build failure names it) and say what changed under `[Unreleased]` in `CHANGELOG.md`. To try
it against a version in your local repository:

```sh
mvn verify -DskipTests -Dpgtasks.api.baseline=0.1.0 -Djapicmp.skip=false
```

## Test coverage

`mvn verify -Pcoverage` runs every module's tests with the JaCoCo agent and writes one report for
all modules to `pgtasks-coverage/target/site/jacoco-aggregate/index.html`. CI's coverage job does the
same, puts the totals in the job summary and keeps the report as an artifact. It never fails
the run; the build job, without the agent, is the one that gates.

## What is published

Every module in the default build except the aggregator (`pgtasks-build`): `pgtasks-bom`,
`pgtasks-parent` and the library modules. `pgtasks-benchmark` (profile `benchmark`),
`pgtasks-coverage` (profile `coverage`) and `examples/` are never published.
