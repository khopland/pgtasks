package com.example.orders;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.filter.OncePerRequestFilter;

/** Security for the API, Actuator and the pgtasks dashboard, and a request id in the MDC. */
@Configuration
class WebConfig {

    @Bean
    SecurityFilterChain security(HttpSecurity http) throws Exception {
        return http
            .authorizeHttpRequests(auth -> auth
                .requestMatchers("/actuator/health/**", "/actuator/prometheus", "/error").permitAll()
                .requestMatchers("/pgtasks/**", "/actuator/**").hasRole("OPS")
                .requestMatchers("/orders/**").hasRole("API")
                .anyRequest().denyAll())
            .httpBasic(Customizer.withDefaults())
            .formLogin(Customizer.withDefaults())
            .csrf(csrf -> csrf.ignoringRequestMatchers("/orders/**"))
            .build();
    }

    /**
     * Demo users. Not an InMemoryUserDetailsManager: that one upgrades {noop} passwords to bcrypt
     * after the first login, so every basic-auth request from the load generator would hash.
     */
    @Bean
    UserDetailsService users() {
        Map<String, UserDetails> users = Map.of(
            "api", User.withUsername("api").password("{noop}api").roles("API").build(),
            "ops", User.withUsername("ops").password("{noop}ops").roles("OPS").build());
        return name -> {
            UserDetails user = users.get(name);
            if (user == null) {
                throw new UsernameNotFoundException(name);
            }
            return User.withUserDetails(user).build();
        };
    }

    /** The request id the client sent, or a new one; pgtasks carries it to every task (pgtasks.mdc-headers). */
    @Bean
    OncePerRequestFilter requestId() {
        return new OncePerRequestFilter() {
            @Override
            protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
                throws ServletException, IOException {
                String id = request.getHeader("X-Request-Id");
                MDC.put("requestId", id == null || id.isBlank() ? UUID.randomUUID().toString() : id);
                try {
                    chain.doFilter(request, response);
                } finally {
                    MDC.remove("requestId");
                }
            }
        };
    }
}
