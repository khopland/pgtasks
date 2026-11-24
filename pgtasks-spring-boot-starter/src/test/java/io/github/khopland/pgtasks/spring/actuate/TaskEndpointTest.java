package io.github.khopland.pgtasks.spring.actuate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
import io.github.khopland.pgtasks.engine.EngineFixture;
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

/** The tasks endpoint and health indicator over HTTP, in a Boot app with Actuator and the in-memory store. */
@SpringBootTest(classes = TaskEndpointTest.App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "pgtasks.store=memory",
    "management.endpoints.web.exposure.include=health,tasks",
    "management.endpoint.health.show-details=always",
    "management.endpoint.tasks.show-values=always"
})
@DirtiesContext
class TaskEndpointTest {

    static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    /** An empty topic for the delete route. */
    static final TopicRef<String> OLD_WORK = TopicRef.of("old-work", String.class);

    /** No database: JDBC is on the test classpath for the other tests. */
    @SpringBootApplication(excludeName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
    static class App {
        /** Fails a payload starting with "flaky" once; no retries, so it is dead-lettered. */
        @Bean
        TaskConfigurer workTasks() {
            Set<String> failedOnce = ConcurrentHashMap.newKeySet();
            return tasks -> tasks.topic(WORK, batch -> batch.decide(task ->
                    task.payload().startsWith("flaky") && failedOnce.add(task.payload()) ? Decision.failed("not yet") : Decision.success()),
                topic -> topic.retry(Retry.none()));
        }

        @Bean
        TaskConfigurer oldTasks() {
            return tasks -> tasks.eachTask(OLD_WORK, task -> { });
        }

    }

    record Response(int status, JsonNode body) {
    }

    private final HttpClient http = HttpClient.newHttpClient();
    private final JsonMapper json = JsonMapper.builder().build();

    @Autowired
    Environment environment;
    @Autowired
    Tasks tasks;
    @Autowired
    TaskEngine engine;

    private String base;

    @BeforeEach
    void base() {
        base = "http://localhost:" + environment.getProperty("local.server.port") + "/actuator";
    }

    private Response send(String method, String path, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(10));
        if (body != null) {
            request.header("Content-Type", "application/json").method(method, HttpRequest.BodyPublishers.ofString(body));
        } else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        return new Response(response.statusCode(), response.body().isEmpty() ? null : json.readTree(response.body()));
    }

    private static boolean unchecked(Callable<Boolean> check) {
        try {
            return check.call();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Response get(String path) throws Exception {
        return send("GET", path, null);
    }

    @Test
    void the_overview_lists_topics_by_module() throws Exception {
        Response overview = get("/tasks");

        assertThat(overview.status()).isEqualTo(200);
        assertThat(overview.body().path("modules").path("workTasks").get(0).asString()).isEqualTo("work");
        assertThat(overview.body().path("topics").get(0).path("processing").asBoolean()).isTrue();
        assertThat(overview.body().path("node").path("running").asBoolean()).isTrue();
        long self = overview.body().path("node").path("id").asLong();
        JsonNode nodes = overview.body().path("nodes");
        assertThat(nodes.isArray()).isTrue();
        assertThat(nodes.valueStream().filter(node -> node.path("id").asLong() == self).findFirst()).as("this node is listed")
            .hasValueSatisfying(node -> assertThat(node.path("heartbeatAt").asString()).isNotEmpty());
    }

    @Test
    void a_topic_shows_its_settings_and_where_they_came_from() throws Exception {
        Response topic = get("/tasks/topics/work");

        assertThat(topic.status()).isEqualTo(200);
        assertThat(topic.body().path("settings").path("retry").path("source").asString()).isEqualTo("topic");
        assertThat(topic.body().path("settings").path("batch-size").path("source").asString()).isEqualTo("default");
        assertThat(get("/tasks/topics/missing").status()).isEqualTo(404);
        assertThat(get("/tasks/nonsense/work").status()).isEqualTo(404);
    }

    @Test
    void dead_letters_are_listed_and_revived() throws Exception {
        long id = tasks.independently().push(WORK, "a", "flaky-a").id();
        EngineFixture.await("the task is dead-lettered", Duration.ofSeconds(5), () -> unchecked(() -> !engine.admin().deadLetters("work", 0, 10).isEmpty()));

        Response dead = get("/tasks/topics/work/dead-letters?limit=10");
        assertThat(dead.status()).isEqualTo(200);
        assertThat(dead.body().path("deadLetters").get(0).path("id").asLong()).isEqualTo(id);
        assertThat(dead.body().path("deadLetters").get(0).path("error").asString()).contains("not yet");
        assertThat(get("/tasks/topics/work/failing").status()).as("no longer a route").isEqualTo(404);

        assertThat(send("POST", "/tasks/topics/work/discard", "{}").status()).as("all of them only when asked for").isEqualTo(400);
        assertThat(send("POST", "/tasks/topics/work/revive", "{}").status()).isEqualTo(400);
        assertThat(send("POST", "/tasks/topics/work/revive", "{\"error\": \"something else\"}").body().path("revived").asInt()).isZero();
        Response revived = send("POST", "/tasks/topics/work/revive", "{\"error\": \"not yet\"}");
        assertThat(revived.status()).isEqualTo(200);
        assertThat(revived.body().path("revived").asInt()).isEqualTo(1);
        EngineFixture.await("the revived task succeeded", Duration.ofSeconds(5),
            () -> unchecked(() -> "succeeded".equals(get("/tasks/tasks/" + id).body().path("state").asString())));
    }

    @Test
    void suspended_tasks_are_released() throws Exception {
        long held = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("release-a", "held").asSuspended()).id();
        long other = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("release-b", "other").asSuspended()).id();

        assertThat(send("POST", "/tasks/topics/work/release", "{}").status()).as("all of them only when asked for").isEqualTo(400);
        assertThat(send("POST", "/tasks/topics/work/release", "{\"ids\": \"x\"}").status()).isEqualTo(400);
        Response released = send("POST", "/tasks/topics/work/release", "{\"identifier\": \"release-a\"}");
        assertThat(released.status()).isEqualTo(200);
        assertThat(released.body().path("released").asInt()).isEqualTo(1);
        EngineFixture.await("the released task succeeded", Duration.ofSeconds(5),
            () -> unchecked(() -> "succeeded".equals(get("/tasks/tasks/" + held).body().path("state").asString())));
        assertThat(get("/tasks/tasks/" + other).body().path("state").asString()).isEqualTo("suspended");
        assertThat(send("POST", "/tasks/topics/work/release", "{\"ids\": \"" + other + "\"}").body().path("released").asInt()).isEqualTo(1);
    }

    @Test
    void unfinished_tasks_are_searched() throws Exception {
        long later = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("search-a", "later").withDelay(Duration.ofHours(1))
            .withHeader("tenant", "north")).id();
        long tomorrow = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("search-b", "tomorrow")
            .withRunAt(Instant.now().plus(Duration.ofDays(1)))).id();

        Response found = get("/tasks/topics/work/tasks?state=scheduled&header=tenant=north");
        assertThat(found.status()).isEqualTo(200);
        assertThat(found.body().path("tasks").size()).isOne();
        assertThat(found.body().path("tasks").get(0).path("id").asLong()).isEqualTo(later);
        assertThat(found.body().path("tasks").get(0).path("runAt").asString()).isNotEmpty();
        assertThat(get("/tasks/topics/work/tasks?identifier=search-b").body().path("tasks").get(0).path("id").asLong()).isEqualTo(tomorrow);
        assertThat(get("/tasks/topics/work/tasks?state=waiting").status()).isEqualTo(400);
        assertThat(get("/tasks/topics/work/tasks?header=tenant").status()).isEqualTo(400);
        assertThat(get("/tasks/topics/work/tasks?createdFrom=yesterday").status()).isEqualTo(400);
        assertThat(get("/tasks/topics/missing/tasks").status()).isEqualTo(404);

        assertThat(send("POST", "/tasks/topics/work/cancel", "{\"all\": \"true\"}").status()).as("no longer an action").isEqualTo(404);
    }

    @Test
    void a_topic_no_longer_used_is_deleted() throws Exception {
        Response deleted = send("DELETE", "/tasks/topics/old-work", null);
        assertThat(deleted.status()).isEqualTo(200);
        assertThat(deleted.body().path("deletedTasks").asLong()).isZero();
        assertThat(get("/tasks/topics/old-work").status()).isEqualTo(404);
        assertThat(send("DELETE", "/tasks/topics/old-work", null).status()).isEqualTo(404);
    }

    @Test
    void an_unknown_topic_action_is_not_found() throws Exception {
        assertThat(send("POST", "/tasks/topics/work/explode", "{}").status()).isEqualTo(404);
    }

    @Test
    void the_dashboard_is_off_unless_enabled() throws Exception {
        String server = base.substring(0, base.length() - "/actuator".length());
        HttpResponse<String> page = http.send(HttpRequest.newBuilder(URI.create(server + "/pgtasks/")).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(page.statusCode()).isEqualTo(404);
        assertThat(http.send(HttpRequest.newBuilder(URI.create(server + "/pgtasks/app.js")).build(), HttpResponse.BodyHandlers.ofString())
            .statusCode()).isEqualTo(404);
    }

    @Test
    void health_reports_the_engine() throws Exception {
        Response health = get("/health");

        assertThat(health.body().path("components").path("tasks").path("status").asString()).isEqualTo("UP");
        assertThat(health.body().path("components").path("tasks").path("details").path("stuckClaims").asInt()).isZero();
    }
}
