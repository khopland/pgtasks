package io.github.khopland.pgtasks.kotlin

import io.github.khopland.pgtasks.Creation
import io.github.khopland.pgtasks.Insertion
import io.github.khopland.pgtasks.Junction
import io.github.khopland.pgtasks.Task
import io.github.khopland.pgtasks.TopicRef
import io.github.khopland.pgtasks.Work
import io.github.khopland.pgtasks.engine.TaskConfigurer
import io.github.khopland.pgtasks.engine.TaskEngine
import io.github.khopland.pgtasks.engine.TaskTester
import io.github.khopland.pgtasks.memory.InMemoryTaskStore

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

import java.time.Instant
import java.util.Collections.synchronizedList
import java.util.concurrent.atomic.AtomicBoolean

class KotlinApiTest {

    @Test
    fun `a topic keeps its name and payload type`() {
        val ref = topic<Request>("assessment")

        assertThat(ref.name()).isEqualTo("assessment")
        assertThat(ref.payloadType()).isEqualTo(Request::class.java)
        assertThat(ref.toString()).isEqualTo("assessment")
    }

    @Test
    fun `a reified topic registers a handler that gets a non-null payload`() {
        val handled = synchronizedList(mutableListOf<String>())
        val sawOptions = AtomicBoolean()
        val configurer = TaskConfigurer { tasks ->
            tasks.eachTopic<String>("notify", options = { sawOptions.set(true); it.batchSize(1) }) { work ->
                handled.add(work.requirePayload())
            }
        }
        val engine = TaskEngine.builder().store(InMemoryTaskStore()).configure(configurer).build().start()
        try {
            engine.tasks().independently().push(topic<String>("notify"), "anna", "hello")

            TaskTester.of(engine).drain(topic<String>("notify"))

            assertThat(handled).containsExactly("hello")
            assertThat(sawOptions).isTrue()
        } finally {
            engine.close()
        }
    }

    @Test
    fun `requirePayload fails clearly when the task has no payload`() {
        val ref = topic<String>("notify")
        val now = Instant.now()
        val task = Task(ref, 1L, "anna", null, 1, null, null, null, now, now, emptyMap())
        val work = object : Work<String> {
            override fun task(): Task<String> = task

            override fun <C : Any> push(topic: TopicRef<C>, insertion: Insertion, creation: Creation<C>) = Unit

            override fun junction(topic: TopicRef<*>, counts: Junction.Counts, identifiers: Set<String>) = Unit
        }

        assertThatThrownBy { work.requirePayload() }.hasMessage("Task 1 has no payload")
    }

    data class Request(val id: String)
}
