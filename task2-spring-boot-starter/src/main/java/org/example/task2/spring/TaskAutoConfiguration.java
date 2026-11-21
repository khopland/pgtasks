package org.example.task2.spring;

import java.util.Map;

import javax.sql.DataSource;

import org.example.task2.Tasks;
import org.example.task2.engine.TaskConfigurer;
import org.example.task2.engine.TaskEngine;
import org.example.task2.engine.TaskEngineBuilder;
import org.example.task2.engine.TaskGlobals;
import org.example.task2.engine.TaskObserver;
import org.example.task2.engine.TaskTester;
import org.example.task2.memory.InMemoryTaskStore;
import org.example.task2.spi.TaskStore;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * One engine from every {@link TaskConfigurer} bean and at most one {@link TaskGlobals} bean,
 * with the {@code task.*} overrides on top (see {@link TaskProperties}). All configurers are merged
 * and validated before anything starts, so bean order doesn't matter, and errors name the beans.
 * Producers inject {@link Tasks}; {@code tasks.push(...)} joins the current Spring transaction.
 */
@AutoConfiguration(afterName = {
    "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
    "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"
})
@EnableConfigurationProperties(TaskProperties.class)
public class TaskAutoConfiguration {

    static final String NO_DATA_SOURCE = "Task 2.0 keeps its tasks in Postgres, through the application's DataSource, "
        + "and there is no DataSource bean. Configure spring.datasource.*, or set task.store=memory (with task2-memory "
        + "on the classpath) for a local run or a test. Memory is never chosen on its own, because it loses every task on restart.";

    static final String NO_MEMORY_STORE = "task.store=memory needs org.example.task2:task2-memory on the classpath";

    @Bean(destroyMethod = "") // the lifecycle drains it; close() would drain it a second time
    @ConditionalOnMissingBean
    public TaskEngine taskEngine(TaskProperties properties, ObjectProvider<DataSource> dataSources, ConfigurableListableBeanFactory beans,
                                 Environment environment, ObjectProvider<TaskObserver> observer,
                                 ObjectProvider<org.springframework.boot.info.BuildProperties> build) {
        TaskEngineBuilder builder = TaskEngine.builder()
            .node(properties.node() != null ? properties.node() : environment.getProperty("spring.application.name", "task"))
            .notifications(properties.notifications())
            .overrides(properties.overrides());
        if (properties.store() == TaskProperties.Store.MEMORY) {
            if (!ClassUtils.isPresent("org.example.task2.memory.InMemoryTaskStore", TaskAutoConfiguration.class.getClassLoader())) {
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
        Map<String, TaskConfigurer> configurers = beans.getBeansOfType(TaskConfigurer.class);
        Map<String, String> modules = ModuleNames.of(beans, java.util.List.copyOf(configurers.keySet()));
        configurers.forEach((name, configurer) -> builder.configure(modules.get(name), configurer));
        beans.getBeansOfType(TaskGlobals.class).forEach(builder::globals);
        observer.ifUnique(builder::observer);
        builder.version(properties.version() != null ? properties.version()
            : build.getIfAvailable() != null ? build.getIfAvailable().getVersion() : null);
        // A module test runs one module's topics; pushes to the others' are stored for its assertions.
        builder.topicsCreatedOnPush(ModuleNames.inModuleTest(beans));
        return builder.build();
    }

    @Bean
    @ConditionalOnMissingBean
    public Tasks tasks(TaskEngine engine) {
        return engine.tasks();
    }

    /** Pushes the tasks the configurers' event routes make of application events. */
    @Bean
    @ConditionalOnMissingBean
    public TaskEventBridge taskEventBridge(TaskEngine engine) {
        return new TaskEventBridge(engine);
    }

    /**
     * For tests: in a Spring Modulith module test, or with {@code task.testing=true}. It records
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
            return context.getEnvironment().getProperty("task.testing", Boolean.class, false)
                || context.getBeanFactory() != null && ModuleNames.inModuleTest(context.getBeanFactory());
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskEngineLifecycle taskEngineLifecycle(TaskEngine engine, TaskProperties properties) {
        return new TaskEngineLifecycle(engine, properties.shutdownTimeout());
    }

    /** Loaded only when task.store=memory, so task2-memory stays optional. */
    private static final class Memory {
        static TaskStore store() {
            return new InMemoryTaskStore();
        }
    }
}
