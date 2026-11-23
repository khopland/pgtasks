package io.github.khopland.pgtasks.spring;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.SecurityContext;
import org.springframework.boot.actuate.endpoint.web.WebEndpointResponse;
import org.springframework.web.HttpRequestHandler;
import org.springframework.web.util.UriUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * The tasks endpoint's operations at {@code <dashboard path>/api}, on the application's own port,
 * for {@code pgtasks.dashboard.api=dashboard}: when Actuator runs on a port the browser can't
 * reach. The same paths, bodies and answers as {@code /actuator/tasks}, because it calls
 * {@link TaskEndpoint}: values are hidden as {@code management.endpoint.tasks.show-values} says,
 * with the roles of the request's user, and limit changes are versioned. Its security is the
 * application's, like any other path.
 */
final class TaskDashboardApi implements HttpRequestHandler {

    private final String path;
    private final TaskEndpoint endpoint;
    private final JsonMapper json;

    /** @param path the API's path, without the context path, such as {@code /pgtasks/api} */
    TaskDashboardApi(String path, TaskEndpoint endpoint, JsonMapper json) {
        this.path = path;
        this.endpoint = endpoint;
        this.json = json;
    }

    @Override
    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String base = request.getContextPath() + path;
        String uri = request.getRequestURI();
        String rest = uri.startsWith(base) ? uri.substring(base.length()) : null;
        if (rest == null || !(rest.isEmpty() || rest.startsWith("/"))) {
            write(response, new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND));
            return;
        }
        List<String> parts = new ArrayList<>();
        for (String part : rest.split("/", -1)) {
            if (!part.isEmpty()) {
                parts.add(UriUtils.decode(part, StandardCharsets.UTF_8));
            }
        }
        response.setHeader("Cache-Control", "no-store");
        response.setHeader("X-Content-Type-Options", "nosniff");
        try {
            write(response, dispatch(request, parts));
        } catch (BadRequest e) {
            String message = Objects.requireNonNullElse(e.getMessage(), "invalid request");
            Map<String, Object> error = Map.of("error", message);
            write(response, new WebEndpointResponse<Object>(error, WebEndpointResponse.STATUS_BAD_REQUEST));
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private WebEndpointResponse<?> dispatch(HttpServletRequest request, List<String> parts) throws Exception {
        SecurityContext security = security(request);
        String method = request.getMethod();
        return switch (method + " " + parts.size()) {
            case "GET 0" -> new WebEndpointResponse<>(endpoint.overview());
            case "GET 2" -> endpoint.read(parts.get(0), parts.get(1), security);
            case "GET 3" -> endpoint.readPart(parts.get(0), parts.get(1), parts.get(2),
                longValue(request.getParameter("after"), "after"), intValue(request.getParameter("limit"), "limit"),
                request.getParameter("identifier"), request.getParameter("reference"), request.getParameter("state"),
                request.getParameter("header"), request.getParameter("createdFrom"), request.getParameter("createdUntil"), security);
            case "POST 2" -> {
                Map<String, Object> body = body(request);
                yield endpoint.override(parts.get(0), parts.get(1), text(body, "rate"), intValue(body.get("burst"), "burst"),
                    text(body, "until"), longValue(body.get("version"), "version"), security);
            }
            case "POST 3" -> {
                Map<String, Object> body = body(request);
                yield endpoint.act(parts.get(0), parts.get(1), parts.get(2), text(body, "ids"), text(body, "outcome"), text(body, "before"),
                    text(body, "error"), text(body, "failedFrom"), text(body, "failedUntil"), text(body, "all"),
                    text(body, "identifier"), text(body, "reference"), text(body, "state"), text(body, "header"),
                    text(body, "createdFrom"), text(body, "createdUntil"));
            }
            case "DELETE 2" -> endpoint.delete(parts.get(0), parts.get(1), longValue(request.getParameter("version"), "version"), security);
            default -> method.equals("GET") || method.equals("POST") || method.equals("DELETE")
                ? new WebEndpointResponse<>(WebEndpointResponse.STATUS_NOT_FOUND)
                : new WebEndpointResponse<>(405);
        };
    }

    /** The user and roles of the request, as Actuator would see them. */
    private static SecurityContext security(HttpServletRequest request) {
        return new SecurityContext() {
            @Override
            public @Nullable Principal getPrincipal() {
                return request.getUserPrincipal();
            }

            @Override
            public boolean isUserInRole(String role) {
                return request.isUserInRole(role);
            }
        };
    }

    /** A flat JSON object, as Actuator reads write operations; an empty body is an empty object. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> body(HttpServletRequest request) throws IOException {
        byte[] bytes = request.getInputStream().readAllBytes();
        if (bytes.length == 0) {
            return Map.of();
        }
        try {
            Object body = json.readValue(bytes, Object.class);
            if (body instanceof Map<?, ?> map) {
                return (Map<String, Object>) map;
            }
        } catch (JacksonException e) {
            throw new BadRequest("The body is not JSON: " + e.getOriginalMessage());
        }
        throw new BadRequest("The body is a JSON object, such as {\"ids\": \"1,2\"}");
    }

    private static @Nullable String text(Map<String, Object> body, String key) {
        Object value = body.get(key);
        return value == null ? null : String.valueOf(value);
    }

    private static @Nullable Long longValue(@Nullable Object value, String name) {
        if (value == null || value instanceof Number) {
            return value == null ? null : ((Number) value).longValue();
        }
        try {
            return Long.valueOf(String.valueOf(value).strip());
        } catch (NumberFormatException e) {
            throw new BadRequest(name + " is a whole number");
        }
    }

    private static @Nullable Integer intValue(@Nullable Object value, String name) {
        Long number = longValue(value, name);
        if (number != null && (number > Integer.MAX_VALUE || number < Integer.MIN_VALUE)) {
            throw new BadRequest(name + " is too large");
        }
        return number == null ? null : number.intValue();
    }

    private void write(HttpServletResponse response, WebEndpointResponse<?> answer) throws IOException {
        response.setStatus(answer.getStatus());
        Object body = answer.getBody();
        if (body != null) {
            byte[] bytes = json.writeValueAsBytes(body);
            response.setContentType("application/json");
            response.setContentLength(bytes.length);
            response.getOutputStream().write(bytes);
        }
    }

    private static final class BadRequest extends RuntimeException {
        BadRequest(String message) {
            super(message);
        }
    }
}
