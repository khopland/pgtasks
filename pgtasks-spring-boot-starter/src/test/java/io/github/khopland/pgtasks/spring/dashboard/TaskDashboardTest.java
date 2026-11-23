package io.github.khopland.pgtasks.spring.dashboard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Pattern;

import io.github.khopland.pgtasks.Decision;
import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.TopicRef;
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
 * The dashboard at a configured path below a context path, and every endpoint route its script
 * calls, against the real endpoint.
 */
@SpringBootTest(classes = TaskDashboardTest.App.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "pgtasks.store=memory",
    "pgtasks.dashboard.enabled=true",
    "pgtasks.dashboard.path=/ops/tasks/",
    "server.servlet.context-path=/app",
    "management.endpoints.web.exposure.include=health,tasks"
})
@DirtiesContext
class TaskDashboardTest {

    static final TopicRef<String> WORK = TopicRef.of("work", String.class);
    /** An empty topic for the delete route. */
    static final TopicRef<String> SCRATCH = TopicRef.of("scratch", String.class);

    @SpringBootApplication(excludeName = "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration")
    static class App {
        @Bean
        TaskConfigurer workTasks() {
            return tasks -> tasks.topic(WORK, batch -> batch.decide(task -> Decision.success()), topic -> { });
        }

        @Bean
        TaskConfigurer scratchTasks() {
            return tasks -> tasks.eachTask(SCRATCH, task -> { });
        }

    }

    record Response(int status, HttpResponse<String> raw) {
        String body() {
            return raw.body();
        }

        String header(String name) {
            return raw.headers().firstValue(name).orElse(null);
        }
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
        return new Response(response.statusCode(), response);
    }

    private Response get(String path) throws Exception {
        return send("GET", path, null);
    }

    @Test
    void the_page_is_served_at_the_configured_path() throws Exception {
        Response bare = get("/app/ops/tasks");
        assertThat(bare.status()).as("redirected to the trailing slash, for relative links").isEqualTo(302);
        assertThat(bare.header("Location")).endsWith("/app/ops/tasks/");

        Response page = get("/app/ops/tasks/");
        assertThat(page.status()).isEqualTo(200);
        assertThat(page.header("Content-Type")).startsWith("text/html");
        assertThat(page.body()).contains("<script src=\"app.js\" defer></script>");
        assertThat(page.header("Content-Security-Policy")).contains("script-src 'self'").contains("frame-ancestors 'none'");
        assertThat(page.header("X-Content-Type-Options")).isEqualTo("nosniff");

        assertThat(get("/app/ops/tasks/app.js").header("Content-Type")).startsWith("text/javascript");
        assertThat(get("/app/ops/tasks/app.css").header("Content-Type")).startsWith("text/css");
        assertThat(get("/app/ops/tasks/favicon.svg").status()).isEqualTo(200);
        assertThat(get("/app/ops/tasks/application.yml").status()).as("only the dashboard's own files").isEqualTo(404);
        assertThat(get("/app/ops/tasks/../ops/tasks/app.js").status()).isNotEqualTo(500);
        assertThat(send("POST", "/app/ops/tasks/", "{}").status()).isEqualTo(405);
        assertThat(get("/app/pgtasks/").status()).as("not at the default path as well").isEqualTo(404);
    }

    @Test
    void the_config_points_the_page_at_the_endpoint() throws Exception {
        Response config = get("/app/ops/tasks/config.json");

        assertThat(config.status()).isEqualTo(200);
        assertThat(config.header("Cache-Control")).isEqualTo("no-store");
        JsonNode body = json.readTree(config.body());
        assertThat(body.path("endpoint").asString()).isEqualTo("/app/actuator/tasks");
        assertThat(body.path("csrf").isNull()).as("no Spring Security in front of it").isTrue();
    }

    @Test
    void the_script_never_writes_html_and_calls_the_endpoint_only_through_its_routes() throws Exception {
        String script = DashboardRoutes.script();

        assertThat(script).doesNotContain("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function",
            "setHTML", "srcdoc", ".style.");
        // config.json, and the one call that every route goes through.
        assertThat(Pattern.compile("\\bfetch\\(").matcher(script).results().count()).isEqualTo(2);
        assertThat(script).contains("fetch(\"config.json\"").contains("const response = await fetch(target,");
        assertThat(DashboardRoutes.routes()).hasSizeGreaterThanOrEqualTo(10);
    }

    /** Every route in the script's table, called against the endpoint the page is pointed at. */
    @Test
    void every_route_the_page_calls_exists() throws Exception {
        long taskId = tasks.independently().push(WORK, "a", "payload").id();
        String endpoint = json.readTree(get("/app/ops/tasks/config.json").body()).path("endpoint").asString();

        DashboardRoutes.callEvery((method, path, body) -> {
            Response response = send(method, path, body);
            return response.status() + " " + response.body();
        }, endpoint, taskId);
    }
}
