package com.example.quickstart;

import io.github.khopland.pgtasks.TopicRef;

/** The topics and their payloads: a name and a type, shared by the code that pushes and the code that handles. */
public final class Topics {

    public record Welcome(String email) {
    }

    public record FollowUp(String email) {
    }

    public static final TopicRef<Welcome> WELCOME = TopicRef.of("welcome", Welcome.class);
    public static final TopicRef<FollowUp> FOLLOW_UP = TopicRef.of("follow-up", FollowUp.class);

    private Topics() {
    }
}
