package io.github.khopland.pgtasks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

class DecisionsTest {

    private static final TopicRef<String> TOPIC = TopicRef.of("topic", String.class);
    private static final TopicRef<String> CHILD = TopicRef.of("child", String.class);

    private static Task<String> task(long id) {
        return new Task<>(TOPIC, id, "id-" + id, null, 1, null, null, null, null, null, null);
    }

    @Test
    void merge_prefers_the_more_severe_decision() {
        List<Decision> ascending = List.of(
            Decision.success(),
            Decision.filtered("irrelevant"),
            Decision.later(Duration.ofSeconds(1)),
            Decision.throttled(Duration.ofSeconds(1)),
            Decision.suspended(),
            Decision.failed("boom")
        );
        for (int lower = 0; lower < ascending.size(); lower++) {
            for (int higher = lower; higher < ascending.size(); higher++) {
                assertThat(Decision.merge(ascending.get(lower), ascending.get(higher))).isEqualTo(ascending.get(higher));
                assertThat(Decision.merge(ascending.get(higher), ascending.get(lower))).isEqualTo(ascending.get(higher));
            }
        }
    }

    @Test
    void merge_keeps_the_longer_delay() {
        assertThat(Decision.merge(Decision.later(Duration.ofSeconds(1)), Decision.later(Duration.ofSeconds(5))))
            .isEqualTo(Decision.later(Duration.ofSeconds(5)));
        assertThat(Decision.merge(Decision.throttled(Duration.ofSeconds(9)), Decision.throttled(Duration.ofSeconds(5))))
            .isEqualTo(Decision.throttled(Duration.ofSeconds(9)));
    }

    @Test
    void only_success_continues() {
        assertThat(Decision.success().continues()).isTrue();
        assertThat(Decision.filtered("x").continues()).isFalse();
        assertThat(Decision.later(Duration.ZERO).continues()).isFalse();
        assertThat(Decision.throttled(Duration.ZERO).continues()).isFalse();
        assertThat(Decision.suspended().continues()).isFalse();
        assertThat(Decision.failed("x").continues()).isFalse();
    }

    @Test
    void merged_decisions_keep_the_handlers_downstream_work() {
        Task<String> first = task(1), second = task(2);
        Push<String> child = new Push<>(CHILD, Insertion.APPEND, Creation.of("c", "payload"));
        Decisions handled = Decisions.builder()
            .decide(first, Decision.success(), List.of(child))
            .decide(second, Decision.success(), List.of(child))
            .build();
        Decisions reviewed = handled.revise(second, Decision.failed("vetoed"));

        Decisions merged = handled.merge(reviewed);

        assertThat(merged.continuing()).containsExactly(first);
        assertThat(merged.decision(second)).isInstanceOf(Decision.Failed.class);
        assertThat(merged.downstream(second)).containsExactly(child);
    }

    @Test
    void cannot_decide_a_task_twice_or_merge_different_batches() {
        assertThatThrownBy(() -> Decisions.builder().decide(task(1), Decision.success()).decide(task(1), Decision.success()))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Decisions.all(List.of(task(1)), Decision.success()).merge(Decisions.all(List.of(task(2)), Decision.success())))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topic_names_are_validated() {
        assertThatThrownBy(() -> TopicRef.of("Has Spaces", String.class)).isInstanceOf(IllegalArgumentException.class);
        assertThat(TopicRef.of("assessment-2026", String.class).name()).isEqualTo("assessment-2026");
    }
}
