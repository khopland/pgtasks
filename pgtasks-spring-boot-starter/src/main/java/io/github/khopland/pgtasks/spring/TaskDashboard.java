package io.github.khopland.pgtasks.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.jspecify.annotations.Nullable;
import org.springframework.web.HttpRequestHandler;

/**
 * Serves the dashboard: a fixed set of static files, and {@code config.json}, which tells the page
 * where the tasks endpoint is and, with Spring Security, the CSRF token its changes send. Nothing
 * else is served, and every response forbids framing, inline scripts and requests to other origins.
 */
final class TaskDashboard implements HttpRequestHandler {

    static final String LOCATION = "META-INF/pgtasks/dashboard/";
    private static final String CSRF_TOKEN = "org.springframework.security.web.csrf.CsrfToken";
    private static final Map<String, String> FILES = Map.of(
        "index.html", "text/html;charset=UTF-8",
        "app.js", "text/javascript;charset=UTF-8",
        "app.css", "text/css;charset=UTF-8",
        "favicon.svg", "image/svg+xml");

    private final String path;
    private final @Nullable String endpoint;
    private final String contentSecurityPolicy;
    private final Map<String, byte[]> content = new LinkedHashMap<>();
    private final @Nullable Class<?> csrfToken;

    /**
     * @param path     the page's path, without the context path or a trailing slash
     * @param endpoint the tasks endpoint: a path below the context path, or an absolute URL;
     *                 null when it isn't exposed
     */
    TaskDashboard(String path, @Nullable String endpoint, ClassLoader classLoader) {
        this.path = path;
        this.endpoint = endpoint;
        String connect = "'self'";
        if (endpoint != null && endpoint.contains("://")) {
            URI uri = URI.create(endpoint);
            connect += " " + uri.getScheme() + "://" + uri.getRawAuthority();
        }
        this.contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src " + connect
            + "; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
        FILES.keySet().forEach(name -> content.put(name, read(classLoader, name)));
        Class<?> token;
        try {
            token = Class.forName(CSRF_TOKEN, false, classLoader);
        } catch (ClassNotFoundException | LinkageError e) {
            token = null;
        }
        this.csrfToken = token;
    }

    private static byte[] read(ClassLoader classLoader, String name) {
        try (InputStream in = classLoader.getResourceAsStream(LOCATION + name)) {
            if (in == null) {
                throw new IllegalStateException("The dashboard's " + name + " is missing from the pgtasks starter");
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void handleRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        String base = request.getContextPath() + path;
        String uri = request.getRequestURI();
        String rest = uri.startsWith(base) ? uri.substring(base.length()) : null;
        if (rest == null || !(rest.isEmpty() || rest.startsWith("/"))) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        if (!request.getMethod().equals("GET") && !request.getMethod().equals("HEAD")) {
            response.setHeader("Allow", "GET, HEAD");
            response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }
        if (rest.isEmpty()) {
            // Relative links in the page resolve against the trailing slash.
            response.sendRedirect(base + "/");
            return;
        }
        String name = rest.equals("/") ? "index.html" : rest.substring(1);
        secure(response);
        if (name.equals("config.json")) {
            response.setHeader("Cache-Control", "no-store");
            write(request, response, "application/json", config(request).getBytes(StandardCharsets.UTF_8));
            return;
        }
        String type = FILES.get(name);
        byte[] body = content.get(name);
        if (type == null || body == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        response.setHeader("Cache-Control", "no-cache");
        write(request, response, type, body);
    }

    private void secure(HttpServletResponse response) {
        response.setHeader("Content-Security-Policy", contentSecurityPolicy);
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("X-Frame-Options", "DENY");
        response.setHeader("Referrer-Policy", "no-referrer");
    }

    private static void write(HttpServletRequest request, HttpServletResponse response, String type, byte[] body) throws IOException {
        response.setContentType(type);
        response.setContentLength(body.length);
        if (!request.getMethod().equals("HEAD")) {
            response.getOutputStream().write(body);
        }
    }

    /** {"endpoint": "...", "csrf": {"header": "...", "token": "..."}}, by hand to need no JSON library. */
    String config(HttpServletRequest request) {
        StringBuilder json = new StringBuilder("{\"endpoint\":");
        String url = endpoint == null ? null : endpoint.contains("://") ? endpoint : request.getContextPath() + endpoint;
        json.append(string(url));
        String[] csrf = csrf(request);
        json.append(",\"csrf\":");
        if (csrf == null) {
            json.append("null");
        } else {
            json.append("{\"header\":").append(string(csrf[0])).append(",\"token\":").append(string(csrf[1])).append('}');
        }
        return json.append('}').toString();
    }

    /** Spring Security's token for this request, if it protects the page; without a compile-time dependency on it. */
    private String @Nullable [] csrf(HttpServletRequest request) {
        if (csrfToken == null) {
            return null;
        }
        Object token = request.getAttribute(CSRF_TOKEN);
        if (!csrfToken.isInstance(token)) {
            return null;
        }
        try {
            Method header = csrfToken.getMethod("getHeaderName");
            Method value = csrfToken.getMethod("getToken");
            return new String[] {(String) header.invoke(token), (String) value.invoke(token)};
        } catch (ReflectiveOperationException | RuntimeException e) {
            return null;
        }
    }

    static String string(@Nullable String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder json = new StringBuilder("\"");
        value.chars().forEach(c -> {
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                default -> {
                    if (c < 0x20 || c == '<' || c == '>' || c == '&' || c == 0x2028 || c == 0x2029) {
                        json.append(String.format("\\u%04x", c));
                    } else {
                        json.append((char) c);
                    }
                }
            }
        });
        return json.append('"').toString();
    }
}
