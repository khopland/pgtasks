# Contributing

Thanks for helping. Bug reports and questions go in the
[issues](https://github.com/khopland/pgtasks/issues), and changes come as pull requests. For
anything bigger than a fix, open an issue first, so we can agree on the shape before you write it. Security
problems are reported privately: see [SECURITY.md](SECURITY.md).

## Building

You need Java 25 and Maven 3.9 or later; the build checks both. Nothing else: the Postgres tests
run on an embedded server (zonky) that the build downloads for Linux (x64 and Arm), macOS (Intel
and Apple silicon) and Windows (x64), so there is no Docker and no database to set up.

```sh
mvn verify                                  # every module and every test, a few minutes
mvn verify -pl pgtasks-engine -am           # one module and what it depends on
mvn install -DskipTests                     # snapshots for examples/ to build against
```

The tests run on Postgres 17 unless you name another supported major, or a server of your own:

```sh
mvn verify -Dpgtasks.test.postgres.version=19
mvn verify -Dpgtasks.test.postgres.url=jdbc:postgresql://localhost:5432/postgres -Dpgtasks.test.postgres.user=postgres
```

Other runs, each also a CI job:

| What | Command |
|---|---|
| Thread interleavings of the engine's coordination code, on Fray's instrumented JDK | `mvn verify -pl pgtasks-engine -am -Pfray -Dtest='*FrayTest' -Dsurefire.failIfNoSpecifiedTests=false` |
| Coverage, one report for all modules in `pgtasks-coverage/target/site/jacoco-aggregate/` | `mvn verify -Pcoverage` |
| The release build without signing or uploading | see [RELEASING.md](RELEASING.md#checking-the-release-build-locally) |

The benchmarks in `pgtasks-benchmark` are never part of a normal build. Run one by name, with
the settings its Javadoc describes; [docs/benchmarks](docs/benchmarks) has earlier results:

```sh
mvn -Pbenchmark -pl pgtasks-benchmark -am test -Dtest=NotifyBenchmark -Dsurefire.failIfNoSpecifiedTests=false
```

CI runs the build, Postgres 18 and 19, Fray, coverage and the release check on every pull
request.

## What a change needs

- **A test that fails without it.** Behaviour every store must share goes in the store TCK in
  `pgtasks-test` (`*Tck`), which the Postgres and in-memory stores both run, rather than in one
  store's tests.
- **Code that passes the compiler's checks.** Error Prone and NullAway run when the main code
  compiles and fail the build. Every package is `@NullMarked`; mark what may be null with
  JSpecify's `@Nullable`.
  Formatting follows [.editorconfig](.editorconfig): four spaces, and imports grouped as `java`
  and `javax`, then pgtasks, then the rest, then static imports.
- **A line in [CHANGELOG.md](CHANGELOG.md)** under `[Unreleased]` for anything a user would notice.
- **Docs.** If behaviour or configuration changes, update the README or the page in `docs/` that
  describes it.

Schema and API changes have rules of their own:

- **Schema.** A released versioned migration never changes; `SchemaFreezeTest` checks this.
  Change the schema with the next versioned file, and when a repeatable file changes, raise its
  `-- revision`. See [Changing the schema](docs/operations.md#changing-the-schema).
- **Public API.** After the first release, every build compares each jar with that release
  (japicmp). Before 1.0, a break is allowed when it is on purpose: list it as the build failure
  says and describe it in the changelog. See [API compatibility](RELEASING.md#api-compatibility).

## Pull requests

Keep a pull request to one topic, with `mvn verify` passing. Write commit messages with a short
subject in the imperative ("Call the dead-letter state dead_letter, not failed") and a body that says what changes
and why. Contributions are licensed under the [Apache License 2.0](LICENSE), like the rest of the
project.
