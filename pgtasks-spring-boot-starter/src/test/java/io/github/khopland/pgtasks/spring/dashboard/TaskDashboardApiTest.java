package io.github.khopland.pgtasks.spring.dashboard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.EngineFixture;
import io.github.khopland.pgtasks.engine.Rate;
import io.github.khopland.pgtasks.engine.Retry;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskGlobals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code pgtasks.dashboard.api=dashboard}, as in Kubernetes: Actuator on its own port with the
 * tasks endpoint not exposed, and the page with its API under one path on the application's port.
 */
@SpringBootTest(classes = TaskDashboardApiTest.App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "pgtasks.store=memory",
    "pgtasks.dashboard.enabled=true",
    "pgtasks.dashboard.path=/admin/pgtasks",
    "pgtasks.dashboard.api=dashboard",
    "management.server.port=0",
    "management.endpoints.web.exposure.include=health"
})
@DirtiesContext
class TaskDashboardApiTest {

    static final TopicRef<String> WORK = TopicRef.of("work", String.class);

    @SpringBootApplication(excludeName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
    static class App {
        /** Dead-letters a payload starting with "flaky", with a message holding personal data. */
        @Bean
        TaskConfigurer workTasks() {
            return tasks -> tasks.topic(WORK, batch -> batch.decide(task -> task.payload().startsWith("flaky")
                ? Decision.failed(new IllegalStateException("person 12345678901 is unknown")) : Decision.success()),
                topic -> topic.retry(Retry.none()).limit("register"));
        }

        @Bean
        TaskGlobals taskGlobals() {
            return globals -> globals.limit("register", limit -> limit.rate(Rate.perSecond(100)).burst(10));
        }
    }

    record Response(int status, String body) {
    }

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    @Autowired
    Environment environment;
    @Autowired
    Tasks tasks;
    @Autowired
    TaskEngine engine;

    private String server;

    @BeforeEach
    void server() {
        server = "http://localhost:" + environment.getProperty("local.server.port");
    }

    private Response send(String method, String path, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(server + path)).timeout(Duration.ofSeconds(10));
        if (body != null) {
            request.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body());
    }

    private Response get(String path) throws Exception {
        return send("GET", path, null);
    }

    @Test
    void the_page_and_its_api_share_one_path_on_the_application_port() throws Exception {
        assertThat(get("/admin/pgtasks/").status()).isEqualTo(200);
        assertThat(json.readTree(get("/admin/pgtasks/config.json").body()).path("endpoint").asString()).isEqualTo("/admin/pgtasks/api");
        assertThat(get("/actuator/tasks").status()).as("Actuator isn't on this port").isEqualTo(404);
        Response overview = get("/admin/pgtasks/api");
        assertThat(overview.status()).isEqualTo(200);
        assertThat(json.readTree(overview.body()).path("modules").path("workTasks").get(0).asString()).isEqualTo("work");
    }

    @Test
    void every_route_the_page_calls_exists() throws Exception {
        long taskId = tasks.independently().push(WORK, "a", "payload").id();
        engine.admin().createTopic("scratch");
        long version = engine.limits().find("register").orElseThrow().version();

        DashboardRoutes.callEvery((method, path, body) -> {
            Response response = send(method, path, body);
            return response.status() + " " + response.body();
        }, "/admin/pgtasks/api", taskId, version);
    }

    @Test
    void values_stay_hidden_as_show_values_says() throws Exception {
        long id = tasks.independently().push(WORK, "b", "flaky fnr 12345678901").id();
        EngineFixture.await("dead-lettered", Duration.ofSeconds(5),
            () -> engine.admin().deadLetters("work", 0, 100).stream().anyMatch(dead -> dead.id() == id));

        Response task = get("/admin/pgtasks/api/tasks/" + id);

        assertThat(task.status()).isEqualTo(200);
        assertThat(task.body()).doesNotContain("12345678901").contains("java.lang.IllegalStateException");
        assertThat(json.readTree(task.body()).path("payload").asString()).isEqualTo("******");
    }

    @Test
    void answers_like_the_endpoint() throws Exception {
        long version = engine.limits().find("register").orElseThrow().version();
        assertThat(send("POST", "/admin/pgtasks/api/limits/register", "{\"rate\": \"30/s\", \"version\": " + (version + 10) + "}").status())
            .as("a stale version").isEqualTo(409);
        assertThat(get("/admin/pgtasks/api/topics/missing").status()).isEqualTo(404);
        assertThat(get("/admin/pgtasks/api/nonsense/work/x/y").status()).isEqualTo(404);
        assertThat(send("POST", "/admin/pgtasks/api/topics/work/revive", "{}").status()).as("all only when asked for").isEqualTo(400);
        assertThat(send("POST", "/admin/pgtasks/api/topics/work/revive", "{not json").status()).isEqualTo(400);
        assertThat(send("POST", "/admin/pgtasks/api/limits/register", "{\"burst\": \"many\"}").status()).isEqualTo(400);
        assertThat(send("PUT", "/admin/pgtasks/api/topics/work", "{}").status()).isEqualTo(405);
        JsonNode topic = json.readTree(get("/admin/pgtasks/api/topics/work").body());
        assertThat(topic.path("settings").path("retry").path("source").asString()).isEqualTo("topic");
    }
}
