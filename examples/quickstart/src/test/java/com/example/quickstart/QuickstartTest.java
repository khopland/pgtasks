package com.example.quickstart;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.function.BooleanSupplier;

import io.github.khopland.pgtasks.test.PostgresDatabase;
import io.github.khopland.pgtasks.test.TestPostgres;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The whole flow on a real Postgres, embedded by pgtasks-test, so the test needs no Docker. */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"quickstart.follow-up-delay=1s", "pgtasks.topics.welcome.retry.first-backoff=100ms"})
class QuickstartTest {

    static final PostgresDatabase DATABASE = TestPostgres.createDatabase();

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", DATABASE::jdbcUrl);
        registry.add("spring.datasource.username", DATABASE::user);
        registry.add("spring.datasource.password", DATABASE::password);
    }

    @Autowired
    Signups signups;

    @Autowired
    Mailer mailer;

    @Autowired
    JdbcClient jdbc;

    @Value("${local.server.port}")
    int port;

    /** What the README's curl commands do, and the pages it points to. */
    @Test
    void the_tutorials_requests_work() throws Exception {
        assertThat(post("/signups", "{\"email\":\"linus@example.com\"}")).isEqualTo(202);
        assertThat(post("/signups", "{\"email\":\"linus@example.com\"}")).isEqualTo(409);
        await(() -> mailer.sent().contains("Welcome! -> linus@example.com"));
        assertThat(get("/actuator/tasks").body()).contains("welcome", "follow-up");
        assertThat(get("/actuator/health").statusCode()).isEqualTo(200);
        assertThat(get("/pgtasks/").statusCode()).isEqualTo(200);
    }

    private int post(String path, String json) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
            .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json)).build();
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        }
    }

    private HttpResponse<String> get(String path) throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path)).build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    @Test
    void a_signup_gets_a_welcome_and_later_a_follow_up() throws Exception {
        signups.signUp("ada@example.com");

        await(() -> mailer.sent().contains("Welcome! -> ada@example.com"));
        await(() -> mailer.sent().contains("How is it going? -> ada@example.com"));
    }

    @Test
    void a_failed_signup_pushes_nothing() {
        signups.signUp("grace@example.com");
        assertThatThrownBy(() -> signups.signUp("grace@example.com")).isInstanceOf(DuplicateKeyException.class);

        // The second push rolled back with the second insert.
        assertThat(jdbc.sql("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'welcome' AND identifier = ?")
            .param("grace@example.com").query(Long.class).single()).isEqualTo(1);
    }

    @Test
    void a_flaky_mail_server_is_retried_until_it_works() throws Exception {
        signups.signUp("flaky@example.com");

        // The handler sends, then the task's success commits: wait for the commit, not the send.
        await(() -> jdbc.sql("SELECT count(*) FROM pgtasks.task_view WHERE topic = 'welcome' AND identifier = ? AND state = 'succeeded'")
            .param("flaky@example.com").query(Long.class).single() == 1);
        assertThat(mailer.sent()).contains("Welcome! -> flaky@example.com");
        assertThat(jdbc.sql("SELECT attempt FROM pgtasks.task_view WHERE topic = 'welcome' AND identifier = ?")
            .param("flaky@example.com").query(Integer.class).single()).isEqualTo(3);
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out").isLessThan(deadline);
            Thread.sleep(50);
        }
    }
}
