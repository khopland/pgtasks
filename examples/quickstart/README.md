# Quickstart: pgtasks in five minutes

The smallest useful pgtasks application. A signup is saved with its welcome email task in one
transaction; the email goes out once that commits, a follow-up goes out 30 seconds later, and a
flaky mail server is retried until it works. It is a standalone Spring Boot project, not a
module of the root build.

You need Java 25, Maven and Docker.

## 1. Start Postgres

```bash
docker compose up -d --wait          # Postgres 17 on localhost:55433
```

## 2. Run the application

```bash
mvn spring-boot:run
```

On startup pgtasks creates its own schema, `pgtasks`, next to the application's `signup` table,
and starts polling the two topics in [`EmailHandlers`](src/main/java/com/example/quickstart/EmailHandlers.java).

## 3. Sign someone up

```bash
curl -i -H 'Content-Type: application/json' -d '{"email":"ada@example.com"}' localhost:8080/signups
```

`202 Accepted`. The log shows the welcome email straight away, and the follow-up 30 seconds later:

```
Sent 'Welcome!' to ada@example.com (attempt 1)
Sent 'How is it going?' to ada@example.com (attempt 1)
```

What happened, in [`Signups`](src/main/java/com/example/quickstart/Signups.java):

```java
@Transactional
public void signUp(String email) {
    jdbc.sql("INSERT INTO signup (email) VALUES (?)").param(email).update();
    tasks.push(Topics.WELCOME, email, new Topics.Welcome(email));
}
```

The push joins the transaction. There is no outbox table and no message broker: the task is a row
in the same database, committed with the signup.

## 4. See what happens when something fails

Sign the same address up again:

```bash
curl -i -H 'Content-Type: application/json' -d '{"email":"ada@example.com"}' localhost:8080/signups
```

`409 Conflict`: the insert fails, the transaction rolls back, and the push rolls back with it. No
second welcome email.

Now an address the mail server chokes on (the fake [`Mailer`](src/main/java/com/example/quickstart/Mailer.java)
fails twice for anything starting with `flaky`):

```bash
curl -i -H 'Content-Type: application/json' -d '{"email":"flaky@example.com"}' localhost:8080/signups
```

The log shows two failures, then `Sent 'Welcome!' to flaky@example.com (attempt 3)`. A handler
fails by throwing; the task is retried with backoff (about 2 s, then 4 s here, set in
`application.yml`; 10 s doubling to 30 minutes by default, with 20 % jitter). The follow-up is pushed only by the
attempt that succeeded, in the same commit as its success, so it is never sent twice.

## 5. Look at the queue

- The dashboard: <http://localhost:8080/pgtasks/>. Topics, queue counts, task search, and the
  actions an operator needs: revive or discard dead letters, release suspended tasks.
- The same as JSON: `curl localhost:8080/actuator/tasks`
- Health: down when this instance's heartbeat is late or the database is unreachable, with stuck
  claims in the details: `curl localhost:8080/actuator/health`

## 6. Run more than one instance

Start a second instance on another port, and sign a few more people up:

```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
for n in 1 2 3 4 5 6; do curl -s -H 'Content-Type: application/json' -d "{\"email\":\"user$n@example.com\"}" localhost:8080/signups; done
```

Both instances claim tasks from the same queue (`SELECT ... FOR UPDATE SKIP LOCKED`), and each
task runs on one of them. Stop one with Ctrl-C mid-flight: it hands back what it holds, and the
other picks it up. Kill one with `kill -9` and its claims are requeued once it misses its
heartbeats: a minute here, 10 minutes by default (`pgtasks.node-expiry`).

## The test

[`QuickstartTest`](src/test/java/com/example/quickstart/QuickstartTest.java) runs all of the above
against a real Postgres that `pgtasks-test` embeds, so `mvn verify` needs no Docker.

## Next

- [Concepts](../../docs/concepts.md): ordering per identifier, insertion policies, decisions, retries.
- [Patterns](../../docs/patterns.md): effects that must happen exactly once, fan-out and fan-in.
- [Operations](../../docs/operations.md): the schema with Flyway or Liquibase, metrics, alerts, deploys.
- [The order service](../order-service/README.md): a larger example with Flyway, security,
  tracing, a load generator and a chaos script.

Clean up with `docker compose down -v`.
