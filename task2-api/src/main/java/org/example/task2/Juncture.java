package org.example.task2;

/**
 * Fan-in: which tasks count as present when a junction checks its group of identifiers in the
 * target topic. When every identifier in the group has one, the group's suspended tasks in that
 * topic become ready.
 */
public enum Juncture {

    /** Only a suspended task counts: the classic "all parts have arrived and are waiting". */
    SINGULAR,

    /** Any task counts, finished or not, except one that was made redundant. */
    ETERNAL
}
