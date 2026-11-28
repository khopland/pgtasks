/**
 * {@link io.github.khopland.pgtasks.circuitbreaker.CircuitBreaker}, an extension that stops this
 * node claiming from a topic whose recent tasks mostly fail, and probes it with one task at a time
 * until one succeeds. Nothing here is null unless marked {@code @Nullable}.
 */
@NullMarked
package io.github.khopland.pgtasks.circuitbreaker;

import org.jspecify.annotations.NullMarked;
