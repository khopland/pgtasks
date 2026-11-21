package io.github.khopland.pgtasks.spring;

import java.util.Map;

import javax.sql.DataSource;

import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskEngineBuilder;
import io.github.khopland.pgtasks.engine.TaskGlobals;
import io.github.khopland.pgtasks.engine.TaskObserver;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.TaskStore;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * One engine from every {@link TaskConfigurer} bean and at most one {@link TaskGlobals} bean,
 * with the {@code pgtasks.*} overrides on top (see {@link TaskProperties}). All configurers are merged
 * and validated before anything starts, so bean order doesn't matter, and errors name the beans.
 * Producers inject {@link Tasks}; {@code tasks.push(...)} joins the current Spring transaction.
 */
@AutoConfiguration(afterName = {
    "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
    "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"
})
@EnableConfigurationProperties(TaskProperties.class)
public class TaskAutoConfiguration {

    static final String NO_DATA_SOURCE = "pgtasks keeps its tasks in Postgres, through the application's DataSource, "
        + "and there is no DataSource bean. Configure spring.datasource.*, or set pgtasks.store=memory (with pgtasks-memory "
        + "on the classpath) for a local run or a test. Memory is never chosen on its own, because it loses every task on restart.";

    static final String NO_MEMORY_STORE = "pgtasks.store=memory needs io.github.khopland.pgtasks:pgtasks-memory on the classpath";

    @Bean(destroyMethod = "") // the lifecycle drains it; close() would drain it a second time
    @ConditionalOnMissingBean
    public TaskEngine taskEngine(TaskProperties properties, ObjectProvider<DataSource> dataSources, ConfigurableListableBeanFactory beans,
                                 Environment environment, ObjectProvider<TaskObserver> observer,
                                 ObjectProvider<org.springframework.boot.info.BuildProperties> build,
                                 ObjectProvider<io.github.khopland.pgtasks.engine.PayloadCodecs> codecs) {
        TaskEngineBuilder builder = TaskEngine.builder()
            .node(properties.node() != null ? properties.node() : environment.getProperty("spring.application.name", "task"))
            .notifications(properties.notifications())
            .overrides(properties.overrides());
        if (properties.store() == TaskProperties.Store.MEMORY) {
            if (!ClassUtils.isPresent("io.github.khopland.pgtasks.memory.InMemoryTaskStore", TaskAutoConfiguration.class.getClassLoader())) {
                throw new IllegalStateException(NO_MEMORY_STORE);
            }
            builder.store(Memory.store()).transactions(new SpringTransactions(null));
        } else {
            DataSource dataSource = dataSources.getIfAvailable();
            if (dataSource == null) {
                throw new IllegalStateException(NO_DATA_SOURCE);
            }
            builder.dataSource(dataSource).schemaMode(properties.schemaMode()).transactions(new SpringTransactions(dataSource));
        }
        Map<String, TaskConfigurer> configurers = beansOfType(beans, TaskConfigurer.class);
        Map<String, String> modules = ModuleNames.of(beans, java.util.List.copyOf(configurers.keySet()));
        configurers.forEach((name, configurer) -> builder.configure(modules.get(name), configurer));
        beansOfType(beans, TaskGlobals.class).forEach(builder::globals);
        // Every observer bean, such as pgtasks-micrometer's and the application's own.
        java.util.List<TaskObserver> observers = observer.orderedStream().toList();
        if (!observers.isEmpty()) {
            builder.observer(TaskObserver.composite(observers));
        }
        codecs.ifUnique(builder::codecs);
        builder.version(properties.version() != null ? properties.version()
            : build.getIfAvailable() != null ? build.getIfAvailable().getVersion() : null);
        // A module test runs one module's topics; pushes to the others' are stored for its assertions.
        builder.topicsCreatedOnPush(ModuleNames.inModuleTest(beans));
        return builder.build();
    }

    /**
     * Every bean of a type, created one by one so a failure surfaces. {@code getBeansOfType} would
     * quietly leave out a bean that is still being created, which is what a configurer depending
     * on the engine through other beans is: the engine would start without its topics.
     */
    private static <T> Map<String, T> beansOfType(ConfigurableListableBeanFactory beans, Class<T> type) {
        Map<String, T> found = new java.util.LinkedHashMap<>();
        for (String name : beans.getBeanNamesForType(type)) {
            try {
                found.put(name, beans.getBean(name, type));
            } catch (org.springframework.beans.factory.BeanCreationException e) {
                if (e.contains(org.springframework.beans.factory.BeanCurrentlyInCreationException.class)) {
                    throw new IllegalStateException(String.format(CYCLE, type.getSimpleName(), name), e);
                }
                throw e;
            }
        }
        return found;
    }

    static final String CYCLE = "The %s bean '%s' depends on the task engine, which is built from it. Inject Tasks, which "
        + "finds the engine on first use, or ObjectProvider<TaskEngine>, instead of TaskEngine in the beans it uses.";

    /** Finds the engine on first use, so services that push can be used by handlers. */
    @Bean
    @ConditionalOnMissingBean
    public Tasks tasks(ObjectProvider<TaskEngine> engine) {
        return new LazyTasks(() -> engine.getObject().tasks());
    }

    /** JSON for payload types without a codec, with the application's Jackson mapper, or a plain one. */
    @Configuration(proxyBeanMethods = false)
    @org.springframework.boot.autoconfigure.condition.ConditionalOnClass(name = "tools.jackson.databind.json.JsonMapper")
    static class Json {
        @Bean
        @ConditionalOnMissingBean(io.github.khopland.pgtasks.engine.PayloadCodecs.class)
        io.github.khopland.pgtasks.jackson.JacksonPayloadCodecs taskPayloadCodecs(ObjectProvider<tools.jackson.databind.json.JsonMapper> mapper) {
            return new io.github.khopland.pgtasks.jackson.JacksonPayloadCodecs(mapper.getIfAvailable(() -> tools.jackson.databind.json.JsonMapper.builder().build()));
        }
    }

    /** Pushes the tasks the configurers' event routes make of application events. */
    @Bean
    @ConditionalOnMissingBean
    public TaskEventBridge taskEventBridge(TaskEngine engine) {
        return new TaskEventBridge(engine);
    }

    /**
     * For tests: in a Spring Modulith module test, or with {@code pgtasks.testing=true}. It records
     * every push, so it has no place in a running application.
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(Testing.class)
    public TaskTester taskTester(TaskEngine engine) {
        return TaskTester.of(engine);
    }

    static final class Testing implements org.springframework.context.annotation.Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return context.getEnvironment().getProperty("pgtasks.testing", Boolean.class, false)
                || context.getBeanFactory() != null && ModuleNames.inModuleTest(context.getBeanFactory());
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskEngineLifecycle taskEngineLifecycle(TaskEngine engine, TaskProperties properties) {
        if (!properties.enabled() && properties.store() == TaskProperties.Store.MEMORY) {
            System.getLogger(TaskAutoConfiguration.class.getName()).log(System.Logger.Level.WARNING, "pgtasks.enabled=false with pgtasks.store=memory: "
                + "this node keeps its tasks in its own memory and processes none of them, so what it pushes never runs");
        }
        return new TaskEngineLifecycle(engine, properties.shutdownTimeout(), properties.enabled());
    }

    /** Loaded only when pgtasks.store=memory, so pgtasks-memory stays optional. */
    private static final class Memory {
        static TaskStore store() {
            return new InMemoryTaskStore();
        }
    }
}
