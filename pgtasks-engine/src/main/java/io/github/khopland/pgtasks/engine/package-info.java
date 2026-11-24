/**
 * The engine, for application code: build a {@link io.github.khopland.pgtasks.engine.TaskEngine}
 * with its {@link io.github.khopland.pgtasks.engine.TaskEngineBuilder}, register topics in
 * {@link io.github.khopland.pgtasks.engine.TaskConfigurer}s (with
 * {@link io.github.khopland.pgtasks.engine.TopicOptions} and
 * {@link io.github.khopland.pgtasks.engine.Retry}), set node-wide options in a
 * {@link io.github.khopland.pgtasks.engine.TaskGlobals}, run transactions without a framework
 * ({@link io.github.khopland.pgtasks.engine.JdbcTransactions},
 * {@link io.github.khopland.pgtasks.engine.LocalTransactions}), and operate it through
 * {@link io.github.khopland.pgtasks.engine.TaskAdmin}. Tests drive it with
 * {@link io.github.khopland.pgtasks.engine.TaskTester}.
 *
 * <p>Adding a feature to the engine is in {@link io.github.khopland.pgtasks.engine.extension}.
 * What producers and handlers see is in {@code io.github.khopland.pgtasks}, and what a store
 * implements in {@code io.github.khopland.pgtasks.spi}. Nothing here is null unless marked
 * {@code @Nullable}.
 */
@NullMarked
package io.github.khopland.pgtasks.engine;

import org.jspecify.annotations.NullMarked;
