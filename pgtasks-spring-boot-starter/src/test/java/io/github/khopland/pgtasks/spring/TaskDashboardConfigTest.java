package io.github.khopland.pgtasks.spring;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.DefaultCsrfToken;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/** config.json, which with Spring Security in front of the page hands it the request's CSRF token; and the path setting. */
class TaskDashboardConfigTest {

    private final TaskDashboard dashboard = new TaskDashboard("/pgtasks", "/actuator/tasks", TaskDashboardConfigTest.class.getClassLoader());

    @Test
    void the_token_of_the_request_goes_to_the_page() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/app/pgtasks/config.json");
        request.setContextPath("/app");
        request.setAttribute(CsrfToken.class.getName(), new DefaultCsrfToken("X-CSRF-TOKEN", "_csrf", "abc\"<123>"));

        assertThat(dashboard.config(request)).isEqualTo(
            "{\"endpoint\":\"/app/actuator/tasks\",\"csrf\":{\"header\":\"X-CSRF-TOKEN\",\"token\":\"abc\\\"\\u003c123\\u003e\"}}");
    }

    @Test
    void without_spring_security_there_is_no_token() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/pgtasks/config.json");

        assertThat(dashboard.config(request)).isEqualTo("{\"endpoint\":\"/actuator/tasks\",\"csrf\":null}");
    }

    @Test
    void an_absolute_endpoint_is_used_as_it_is() {
        TaskDashboard elsewhere = new TaskDashboard("/pgtasks", "https://ops.example.com/actuator/tasks", TaskDashboardConfigTest.class.getClassLoader());

        assertThat(elsewhere.config(new MockHttpServletRequest("GET", "/pgtasks/config.json")))
            .isEqualTo("{\"endpoint\":\"https://ops.example.com/actuator/tasks\",\"csrf\":null}");
    }

    @Test
    void the_path_is_normalized_and_must_be_a_path() {
        assertThat(new TaskDashboardProperties(true, " /ops/tasks// ", " ").path()).isEqualTo("/ops/tasks");
        assertThat(new TaskDashboardProperties(true, "/ops", " ").endpoint()).isNull();
        assertThatIllegalArgumentException().isThrownBy(() -> new TaskDashboardProperties(true, "/", null));
        assertThatIllegalArgumentException().isThrownBy(() -> new TaskDashboardProperties(true, "pgtasks", null));
        assertThatIllegalArgumentException().isThrownBy(() -> new TaskDashboardProperties(true, "/pgtasks/**", null));
    }
}
