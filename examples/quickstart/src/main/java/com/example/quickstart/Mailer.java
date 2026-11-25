package com.example.quickstart;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stands in for an email service: logs what it sends. An address that starts with "flaky" fails
 * twice before it gets through, to show retries.
 */
@Component
public class Mailer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mailer.class);

    private final List<String> sent = new CopyOnWriteArrayList<>();

    public void send(String email, String subject, int attempt) {
        if (email.startsWith("flaky") && attempt < 3) {
            throw new IllegalStateException("The mail server is having a bad day (attempt " + attempt + ")");
        }
        LOGGER.info("Sent '{}' to {} (attempt {})", subject, email, attempt);
        sent.add(subject + " -> " + email);
    }

    /** What was sent, for the test. */
    public List<String> sent() {
        return List.copyOf(sent);
    }
}
