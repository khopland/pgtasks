/** What application code sees: topics, pushing, handlers, decisions and the operator views. */
module io.github.khopland.pgtasks {
    requires transitive java.sql;
    requires transitive org.jspecify;

    exports io.github.khopland.pgtasks;
    exports io.github.khopland.pgtasks.admin;
}
