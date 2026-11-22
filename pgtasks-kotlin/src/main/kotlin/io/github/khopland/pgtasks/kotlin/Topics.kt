@file:JvmName("Topics")

package io.github.khopland.pgtasks.kotlin

import io.github.khopland.pgtasks.TaskHandler
import io.github.khopland.pgtasks.TopicRef
import io.github.khopland.pgtasks.Work
import io.github.khopland.pgtasks.engine.TaskRegistry
import io.github.khopland.pgtasks.engine.TopicOptions

import java.util.function.Consumer

/**
 * A typed topic reference without a `Class<T>` token. The type is the one Kotlin can see at the
 * call site; [TopicRef.payloadType] still carries it to the engine.
 */
inline fun <reified T : Any> topic(name: String): TopicRef<T> = TopicRef.of(name, T::class.java)

/**
 * Register a topic by name with a reified payload type. It is the ordinary [TaskRegistry.eachTask]
 * underneath, so it takes part in the same defaults, options and validation. The handler is last so
 * the usual trailing lambda works; pass `options` by name when you need it.
 */
inline fun <reified T : Any> TaskRegistry.eachTopic(
    name: String,
    noinline options: (TopicOptions) -> Unit = {},
    noinline handle: (Work<T>) -> Unit,
): TaskRegistry = eachTask(topic<T>(name), TaskHandler<T> { work -> handle(work) }, Consumer<TopicOptions> { options(it) })

/**
 * The payload of this task, failing if it has none. The Java API's payload is nullable because a
 * task may be pushed without one; a topic that always has a payload can use this to say so.
 */
fun <T : Any> Work<T>.requirePayload(): T = payload() ?: error("Task ${task().id()} has no payload")
