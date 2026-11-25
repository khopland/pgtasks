package com.example.quickstart;

import io.github.khopland.pgtasks.Tasks;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Saves a signup and pushes its welcome email in one transaction: both commit, or neither does. */
@Service
public class Signups {

    private final JdbcClient jdbc;
    private final Tasks tasks;

    public Signups(JdbcClient jdbc, Tasks tasks) {
        this.jdbc = jdbc;
        this.tasks = tasks;
    }

    @Transactional
    public void signUp(String email) {
        jdbc.sql("INSERT INTO signup (email) VALUES (?)").param(email).update();
        // Joins this transaction. If the insert above had failed, there would be no task either.
        tasks.push(Topics.WELCOME, email, new Topics.Welcome(email));
    }
}
