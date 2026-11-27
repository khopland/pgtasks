-- One sliding-window counter per topic, shared by every node. Each update takes the row's lock
-- in a short transaction, so nodes spend the same allowance in turn. A row lasts until its topic
-- does; an idle counter fades as its windows pass.
CREATE TABLE ${pgtasks_schema}.rate_limit_window (
    topic           text PRIMARY KEY,
    window_start    bigint NOT NULL,
    previous        bigint NOT NULL DEFAULT 0,
    current         bigint NOT NULL DEFAULT 0
);
