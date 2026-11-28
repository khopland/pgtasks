/**
 * For adding a feature to the engine without changing it: an
 * {@link io.github.khopland.pgtasks.engine.extension.Extension} installs its hooks through an
 * {@link io.github.khopland.pgtasks.engine.extension.ExtensionContext}: claim gates, batch
 * listeners, handler decorators, observers, jobs, notification channels, typed settings, operator
 * views and store capabilities. Applications that only push and handle tasks never need this package. See
 * docs/extensions.md. Nothing here is null unless marked {@code @Nullable}.
 */
@NullMarked
package io.github.khopland.pgtasks.engine.extension;

import org.jspecify.annotations.NullMarked;

