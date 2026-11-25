CREATE TABLE IF NOT EXISTS signup (
    email      text PRIMARY KEY,
    signed_up  timestamptz NOT NULL DEFAULT now()
);
