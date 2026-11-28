package io.github.khopland.pgtasks.spring.actuate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import io.github.khopland.pgtasks.engine.extension.Extension;
import io.github.khopland.pgtasks.engine.extension.OperatorView;
import io.github.khopland.pgtasks.spring.dashboard.ParkedTopics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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
    /** A topic for the parking extension to park. */
    static final TopicRef<String> PARKED = TopicRef.of("parked", String.class);

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

        @Bean
        TaskConfigurer parkedTasks() {
            return tasks -> tasks.eachTask(PARKED, task -> { });
        }

        /** An extension with an operator view, shaped like a pause. */
        @Bean
        ParkedTopics parking() {
            return new ParkedTopics();
        }

        /** An extension whose view fails, as one whose table is missing would. */
        @Bean
        Extension broken() {
            return Extension.of("broken", context -> context.operatorView(new OperatorView() {
                @Override
                public String name() {
                    return "broken";
                }

                @Override
                public String title() {
                    return "Broken";
                }

                @Override
                public List<Column> columns() {
                    return List.of(Column.text("name", "Name"));
                }

                @Override
                public List<Row> rows() {
                    throw new IllegalStateException("relation \"broken\" does not exist");
                }

                @Override
                public Map<String, TopicState> topics(Set<String> topics) {
                    throw new IllegalStateException("relation \"broken\" does not exist");
                }
            }));
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
    @DisplayName("An extension's view shows in the overview and on each topic, and its actions run only where it offers them")
    void an_extensions_view_and_actions() throws Exception {
        Response overview = get("/tasks");
        JsonNode view = overview.body().path("extensions").valueStream().filter(each -> each.path("name").asString().equals("parking"))
            .findFirst().orElseThrow();
        assertThat(view.has("rows")).as("rows come with the view's own request").isFalse();
        assertThat(view.has("error")).isFalse();
        assertThat(view.path("title").asString()).isEqualTo("Parked topics");
        assertThat(view.path("columns").valueStream().map(column -> column.path("kind").asString()).toList()).containsExactly("topic", "time", "flag");
        assertThat(view.path("actions").valueStream().map(action -> action.path("name").asString()).toList()).containsExactly("park", "unpark", "break");
        assertThat(overview.body().path("topics").valueStream().filter(topic -> topic.path("name").asString().equals("parked")).findFirst())
            .hasValueSatisfying(topic -> {
                JsonNode park = topic.path("extensions").path("parking").path("actions").get(0);
                assertThat(park.path("name").asString()).isEqualTo("park");
                assertThat(park.path("label").asString()).isEqualTo("Park");
                assertThat(park.path("confirm").asString()).isEqualTo("No node claims from it until it is unparked.");
            });

        assertThat(send("POST", "/tasks/extensions/parking/unpark", "{\"topic\": \"parked\"}").status())
            .as("not parked, so unpark isn't offered").isEqualTo(404);
        assertThat(send("POST", "/tasks/extensions/parking/park", "{}").status()).as("neither a row nor a topic").isEqualTo(400);
        assertThat(send("POST", "/tasks/extensions/parking/park", "{\"topic\": \"nowhere\"}").status()).isEqualTo(404);
        assertThat(send("POST", "/tasks/extensions/nothing/park", "{\"topic\": \"parked\"}").status()).isEqualTo(404);

        Response parked = send("POST", "/tasks/extensions/parking/park", "{\"topic\": \"parked\"}");
        assertThat(parked.status()).isEqualTo(200);
        assertThat(parked.body().path("topic").asString()).isEqualTo("parked");
        JsonNode topic = get("/tasks/topics/parked").body();
        assertThat(topic.path("extensions").path("parking").path("status").asString()).isEqualTo("parked");
        JsonNode rows = get("/tasks/extensions/parking").body().path("rows");
        assertThat(rows.get(0).path("key").asString()).isEqualTo("parked");
        assertThat(rows.get(0).path("values").path("here").asBoolean()).isTrue();
        assertThat(rows.get(0).path("values").path("since").asString()).as("an instant as text").isNotEmpty();
        // A push wakes the topic, which asks the gate, and the gate holds it back.
        tasks.independently().push(PARKED, "waiting", "x");
        EngineFixture.await("the health to show the held topic", Duration.ofSeconds(10), () -> unchecked(() ->
            get("/health").body().path("components").path("tasks").path("details").path("held").valueStream()
                .anyMatch(held -> held.path("topic").asString().equals("parked") && held.path("gate").asString().equals("parking")
                    && held.path("reason").asString().equals("parked"))));

        Response broken = send("POST", "/tasks/extensions/parking/break", "{\"topic\": \"parked\"}");
        assertThat(broken.status()).isEqualTo(409);
        assertThat(broken.body().path("error").asString()).isEqualTo("could not break parked");
        assertThat(send("POST", "/tasks/extensions/parking/unpark", "{\"key\": \"parked\"}").status()).as("on the row").isEqualTo(200);
        assertThat(get("/tasks/extensions/parking").body().path("rows").isEmpty()).isTrue();
    }

    @Test
    @DisplayName("A view that fails shows its error in its place, and the rest of each response as usual")
    void a_failing_view_leaves_the_rest_alone() throws Exception {
        Response overview = get("/tasks");
        assertThat(overview.status()).isEqualTo(200);
        assertThat(overview.body().path("extensions").valueStream().filter(view -> view.path("name").asString().equals("broken")).findFirst())
            .hasValueSatisfying(view -> assertThat(view.path("error").asString()).isEqualTo("IllegalStateException: relation \"broken\" does not exist"));
        assertThat(overview.body().path("topics").valueStream().filter(topic -> topic.path("name").asString().equals("parked")).findFirst())
            .hasValueSatisfying(topic -> assertThat(topic.path("extensions").has("parking")).as("the other view as usual").isTrue());

        Response rows = get("/tasks/extensions/broken");
        assertThat(rows.status()).isEqualTo(500);
        assertThat(rows.body().path("error").asString()).contains("relation \"broken\" does not exist");
        assertThat(rows.body().path("title").asString()).isEqualTo("Broken");

        Response topic = get("/tasks/topics/work");
        assertThat(topic.status()).isEqualTo(200);
        assertThat(topic.body().path("extensionErrors").path("broken").asString()).contains("does not exist");
        assertThat(topic.body().path("settings").isMissingNode()).isFalse();
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
        assertThat(dead.body().path("deadLetters").get(0).path("error").path("message").asString())
            .as("the error is an object, not JSON in a string").isEqualTo("not yet");
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
    void unfinished_tasks_are_cancelled() throws Exception {
        long first = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("cancel-a", "first").withDelay(Duration.ofHours(1))).id();
        long second = tasks.independently().push(WORK, Insertion.APPEND, Creation.of("cancel-b", "second").asSuspended()).id();

        assertThat(send("POST", "/tasks/topics/work/cancel", "{}").status()).as("all of them only when asked for").isEqualTo(400);
        Response cancelled = send("POST", "/tasks/topics/work/cancel", "{\"identifier\": \"cancel-a\"}");
        assertThat(cancelled.status()).isEqualTo(200);
        assertThat(cancelled.body().path("cancelled").asInt()).isEqualTo(1);
        assertThat(get("/tasks/tasks/" + first).body().path("state").asString()).isEqualTo("cancelled");
        assertThat(get("/tasks/tasks/" + second).body().path("state").asString()).isEqualTo("suspended");
        assertThat(send("POST", "/tasks/topics/work/cancel", "{\"ids\": \"" + second + "\"}").body().path("cancelled").asInt()).isEqualTo(1);
        assertThat(send("POST", "/tasks/topics/work/cancel", "{\"ids\": \"" + second + "\"}").body().path("cancelled").asInt())
            .as("already finished").isZero();
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

        assertThat(send("POST", "/tasks/topics/work/run-now", "{\"all\": \"true\"}").status()).as("not an action").isEqualTo(404);
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
