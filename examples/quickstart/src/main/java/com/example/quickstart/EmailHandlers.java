package com.example.quickstart;

import java.time.Duration;

import io.github.khopland.pgtasks.Creation;
import io.github.khopland.pgtasks.Insertion;
import io.github.khopland.pgtasks.engine.TaskConfigurer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** The handlers. Each runs after the push committed, on whichever instance claims the task. */
@Configuration
public class EmailHandlers {

    @Bean
    TaskConfigurer emailTasks(Mailer mailer, @Value("${quickstart.follow-up-delay}") Duration followUpDelay) {
        return tasks -> tasks
            // Returning succeeds; throwing retries with backoff.
            .eachTask(Topics.WELCOME, task -> {
                mailer.send(task.payload().email(), "Welcome!", task.attempt());
                // Committed together with this task's success, so the follow-up exists exactly when the welcome went out.
                task.push(Topics.FOLLOW_UP, Insertion.APPEND,
                    Creation.of(task.identifier(), new Topics.FollowUp(task.payload().email())).withDelay(followUpDelay));
            })
            .eachTask(Topics.FOLLOW_UP, task -> mailer.send(task.payload().email(), "How is it going?", task.attempt()));
    }
}
