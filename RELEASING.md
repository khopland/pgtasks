# Releasing

Releases go to Maven Central through the [Central Portal](https://central.sonatype.com), from
`.github/workflows/release.yml`, when a version tag is pushed. The poms on `main` always carry the
next snapshot; the workflow takes the release version from the tag.

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

## A release

1. In `CHANGELOG.md`, move what is under `[Unreleased]` to a new `## [x.y.z]` section and update
   the links at the bottom. The workflow refuses a tag without a section, and uses that section
   as the GitHub release notes.
   In the same commit, snapshot the schemas the release ships, so every later build tests an
   upgrade from them (`SchemaUpgradeTest`, `RateLimitSchemaUpgradeTest`):

   ```sh
   mvn test -pl pgtasks-postgres,pgtasks-rate-limit-postgres -am -Dpgtasks.schema.snapshot=0.1.0 \
     -Dtest='SchemaSnapshot,RateLimitSchemaSnapshot' -Dsurefire.failIfNoSpecifiedTests=false
   ```

   It copies the schema files and the rows the release leaves in every state into each
   module's `src/test/resources/.../released/0.1.0/`; commit them as they are. Pin each new
   versioned file's checksum in `SchemaFreezeTest.RELEASED` too.
2. Commit, merge to `main`, then tag that commit and push the tag:

   ```sh
   git tag -a v0.1.0 -m 'pgtasks 0.1.0'
   git push origin v0.1.0
   ```

3. The workflow sets the version, runs every test, signs the artifacts and uploads one bundle.
   Central validates it, then it waits under *Deployments* in the portal. Check it and press
   *Publish*, or *Drop* it and tag again after a fix. Artifacts appear on Maven Central within
   about half an hour of publishing. The workflow also creates the GitHub release (a pre-release
   for 0.x).
4. On `main`, raise the snapshot (`0.1.0-SNAPSHOT` → `0.2.0-SNAPSHOT`) and set the README's BOM
   snippet to the version just released. After the first release, also drop the notes that say
   pgtasks isn't on Maven Central yet, in the README (*Status* and *Modules*) and in
   `examples/quickstart/README.md`:

   ```sh
   mvn org.codehaus.mojo:versions-maven-plugin:2.19.1:set -DnewVersion=0.2.0-SNAPSHOT -DprocessAllModules -DgenerateBackupPoms=false
   ```

5. In `pgtasks-parent/pom.xml`, set `pgtasks.api.baseline` to the version just published, set
   `japicmp.skip` to `false` (only after the first release), and empty the japicmp plugin's
   `<excludes>`. From then on, every build compares each jar with that release; see
   [API compatibility](#api-compatibility).

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
