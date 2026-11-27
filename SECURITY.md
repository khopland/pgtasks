# Security

## Reporting a vulnerability

Please don't open a public issue or pull request for a security problem. Report it privately
through GitHub instead: [Report a vulnerability](https://github.com/khopland/pgtasks/security/advisories/new),
under the repository's *Security* tab. Say which version or commit is affected, how to reproduce
it, and what someone could do with it. The fix and the advisory are worked out in that report,
and you are credited in the advisory unless you'd rather not be.

## Supported versions

pgtasks is before 1.0: fixes go into the latest minor release only, so upgrading to it is how to
get them.

| Version | Security fixes |
|---|---|
| Latest 0.x | Yes |
| Older 0.x | No: upgrade to the latest |

## What counts

Anything that lets someone do or see more than the application allows, for instance:

- SQL injection through topic names, identifiers, the schema name or any other value pgtasks puts
  into SQL.
- The Actuator endpoint or the dashboard showing payloads, headers or errors that
  `management.endpoint.tasks.show-values` should hide, or taking an operator action that the
  application's security rules should refuse.
- Script injection in the dashboard, or a way around its Content-Security-Policy or its CSRF
  protection.
- A payload codec that decodes stored text into more than the payload type allows.

Not a vulnerability in pgtasks: an endpoint or dashboard that the application exposes without
securing it (see [the endpoint](docs/operations.md#actuator-the-tasks-endpoint-and-health) and
[the dashboard](docs/operations.md#dashboard)), and anything that already needs
write access to the pgtasks schema.
