package io.github.khopland.pgtasks.engine;

/**
 * Registers the topics one module owns. Any number of configurers can exist; they are merged and
 * validated together before anything starts, and {@link TaskRegistry#defaults} inside one only
 * applies to the topics it registers.
 */
@FunctionalInterface
public interface TaskConfigurer {

    void configure(TaskRegistry tasks);
}
