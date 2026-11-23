package io.github.khopland.pgtasks.spring.dashboard;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** The ROUTES table of the dashboard's script, and a call of every route the way the page makes it. */
final class DashboardRoutes {

    /** One route per line between the markers, as {@code name: ["METHOD", "/path/{param}"],}. */
    private static final Pattern ROUTE = Pattern.compile("^\\s*(\\w+): \\[\"(GET|POST|DELETE)\", \"([^\"]*)\"],$");

    @FunctionalInterface
    interface Sender {
        /** @return the status, and the body after a space */
        String send(String method, String url, String body) throws Exception;
    }

    private DashboardRoutes() {
    }

    static String script() throws IOException {
        try (InputStream in = DashboardRoutes.class.getClassLoader().getResourceAsStream("META-INF/pgtasks/dashboard/app.js")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /** Name to method and path template. */
    static Map<String, String[]> routes() throws IOException {
        String script = script();
        String block = script.substring(script.indexOf("// routes:begin"), script.indexOf("// routes:end"));
        Map<String, String[]> routes = new LinkedHashMap<>();
        for (String line : block.split("\n")) {
            Matcher matcher = ROUTE.matcher(line);
            if (matcher.matches()) {
                routes.put(matcher.group(1), new String[] {matcher.group(2), matcher.group(3)});
            }
        }
        return routes;
    }

    /**
     * Calls every route against an endpoint and expects success. The app has topic {@code work},
     * and an empty topic {@code scratch} to delete. A route added to the script without an example here fails.
     */
    static void callEvery(Sender http, String endpoint, long taskId) throws Exception {
        Map<String, String[]> examples = new LinkedHashMap<>();
        // name -> param, query, body
        examples.put("overview", new String[] {"", null, null});
        examples.put("topic", new String[] {"topic=work", null, null});
        examples.put("deadLetters", new String[] {"topic=work", "limit=50", null});
        examples.put("revive", new String[] {"topic=work", null, "{\"ids\": \"999999\"}"});
        examples.put("discard", new String[] {"topic=work", null, "{\"error\": \"java.lang.IllegalStateException\"}"});
        examples.put("tasks", new String[] {"topic=work", "state=scheduled,retrying&identifier=nobody", null});
        examples.put("purge", new String[] {"topic=work", null, "{\"outcome\": \"succeeded\", \"before\": \"2000-01-01T00:00:00Z\"}"});
        examples.put("deleteTopic", new String[] {"topic=scratch", null, null});
        examples.put("task", new String[] {"id=" + taskId, null, null});

        Map<String, String[]> routes = routes();
        assertThat(routes.keySet()).as("every route has an example").containsExactlyInAnyOrderElementsOf(examples.keySet());
        for (Map.Entry<String, String[]> route : routes.entrySet()) {
            String[] example = examples.get(route.getKey());
            String path = route.getValue()[1];
            if (!example[0].isEmpty()) {
                String[] param = example[0].split("=", 2);
                path = path.replace("{" + param[0] + "}", param[1]);
            }
            assertThat(path).as(route.getKey()).doesNotContain("{");
            String answer = http.send(route.getValue()[0], endpoint + path + (example[1] == null ? "" : "?" + example[1]), example[2]);
            int status = Integer.parseInt(answer.substring(0, 3));
            assertThat(status).as(route.getKey() + " " + route.getValue()[0] + " " + path + ": " + answer).isBetween(200, 299);
        }
    }
}
