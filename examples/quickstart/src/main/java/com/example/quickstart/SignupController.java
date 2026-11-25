package com.example.quickstart;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SignupController {

    public record SignupRequest(String email) {
    }

    private final Signups signups;

    public SignupController(Signups signups) {
        this.signups = signups;
    }

    @PostMapping("/signups")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void signUp(@RequestBody SignupRequest request) {
        signups.signUp(request.email());
    }

    @ExceptionHandler(DuplicateKeyException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public String duplicate() {
        return "Already signed up";
    }
}
