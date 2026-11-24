/** JSON payloads with Jackson 3. Jackson reads and writes the application's payload types, so their packages must be open to it. */
module io.github.khopland.pgtasks.jackson {
    requires transitive io.github.khopland.pgtasks.engine;
    requires transitive tools.jackson.databind;

    exports io.github.khopland.pgtasks.jackson;
}
