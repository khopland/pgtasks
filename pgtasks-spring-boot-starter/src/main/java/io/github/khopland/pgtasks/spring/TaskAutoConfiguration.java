package io.github.khopland.pgtasks.spring;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import io.github.khopland.pgtasks.Tasks;
import io.github.khopland.pgtasks.engine.PayloadCodecs;
import io.github.khopland.pgtasks.engine.Extension;
import io.github.khopland.pgtasks.engine.TaskConfigurer;
import io.github.khopland.pgtasks.engine.TaskEngine;
import io.github.khopland.pgtasks.engine.TaskEngineBuilder;
import io.github.khopland.pgtasks.engine.TaskGlobals;
import io.github.khopland.pgtasks.engine.TaskObserver;
import io.github.khopland.pgtasks.engine.TaskTester;
import io.github.khopland.pgtasks.jackson.JacksonPayloadCodecs;
import io.github.khopland.pgtasks.memory.InMemoryTaskStore;
import io.github.khopland.pgtasks.spi.TaskStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.BeanCurrentlyInCreationException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.util.ClassUtils;
import tools.jackson.databind.json.JsonMapper;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskAutoConfiguration.class);

    static final String NO_DATA_SOURCE = "pgtasks keeps its tasks in Postgres, through the application's DataSource, "
        + "and there is no DataSource bean. Configure spring.datasource.*, or set pgtasks.store=memory (with pgtasks-memory "
        + "on the classpath) for a local run or a test. Memory is never chosen on its own, because it loses every task on restart.";

    static final String NO_MEMORY_STORE = "pgtasks.store=memory needs io.github.khopland.pgtasks:pgtasks-memory on the classpath";

    @Bean(destroyMethod = "") // the lifecycle drains it; close() would drain it a second time
    @ConditionalOnMissingBean
    public TaskEngine taskEngine(TaskProperties properties, ObjectProvider<DataSource> dataSources, ConfigurableListableBeanFactory beans,
                                 Environment environment, ObjectProvider<TaskObserver> observer,
                                 ObjectProvider<BuildProperties> build,
                                 ObjectProvider<PayloadCodecs> codecs) {
        TaskEngineBuilder builder = TaskEngine.builder()
            .node(properties.node() != null ? properties.node() : defaultNode(environment))
            .heartbeat(properties.heartbeat(), properties.nodeExpiry())
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
            builder.dataSource(dataSource).schemaMode(properties.schemaMode()).schema(properties.schema()).transactions(new SpringTransactions(dataSource));
        }
        // Each configurer's module is its bean name.
        beansOfType(beans, TaskConfigurer.class).forEach(builder::configure);
        beansOfType(beans, TaskGlobals.class).forEach(builder::globals);
        // Every Extension bean, in the beans' order (@Order, Ordered).
        beans.getBeanProvider(Extension.class).orderedStream().forEach(builder::extension);
        // Every observer bean, such as pgtasks-micrometer's and the application's own, and the MDC keys to carry.
        List<TaskObserver> observers = new ArrayList<>(observer.orderedStream().toList());
        if (!properties.mdcHeaders().isEmpty()) {
            observers.add(TaskObserver.mdc(properties.mdcHeaders()));
        }
        if (!observers.isEmpty()) {
            builder.observer(TaskObserver.composite(observers));
        }
        codecs.ifUnique(builder::codecs);
        builder.version(properties.version() != null ? properties.version()
            : build.getIfAvailable() != null ? build.getIfAvailable().getVersion() : null);
        return builder.build();
    }

    /**
     * {@code <application>@<host>}: every instance of a deployment has the same application name,
     * so the host tells them apart on the dashboard. {@code HOSTNAME} is the pod's name in
     * Kubernetes; elsewhere, the machine's name.
     */
    static String defaultNode(Environment environment) {
        String application = environment.getProperty("spring.application.name", "pgtasks");
        String host = System.getenv("HOSTNAME");
        if (host == null || host.isBlank()) {
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (IOException | RuntimeException e) {
                host = "unknown-host";
            }
        }
        return application + "@" + host;
    }

    /**
     * Every bean of a type, created one by one so a failure surfaces. {@code getBeansOfType} would
     * quietly leave out a bean that is still being created, which is what a configurer depending
     * on the engine through other beans is: the engine would start without its topics.
     */
    private static <T> Map<String, T> beansOfType(ConfigurableListableBeanFactory beans, Class<T> type) {
        Map<String, T> found = new LinkedHashMap<>();
        for (String name : beans.getBeanNamesForType(type)) {
            try {
                found.put(name, beans.getBean(name, type));
            } catch (BeanCreationException e) {
                if (e.contains(BeanCurrentlyInCreationException.class)) {
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
        @ConditionalOnMissingBean(PayloadCodecs.class)
        JacksonPayloadCodecs taskPayloadCodecs(ObjectProvider<JsonMapper> mapper) {
            return new JacksonPayloadCodecs(mapper.getIfAvailable(() -> JsonMapper.builder().build()));
        }
    }

    /**
     * For tests, with {@code pgtasks.testing=true}. It records
     * every push, so it has no place in a running application.
     */
    @Bean
    @ConditionalOnMissingBean
    @Conditional(Testing.class)
    public TaskTester taskTester(TaskEngine engine) {
        return TaskTester.of(engine);
    }

    static final class Testing implements Condition {
        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            return context.getEnvironment().getProperty("pgtasks.testing", Boolean.class, false);
        }
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskEngineLifecycle taskEngineLifecycle(TaskEngine engine, TaskProperties properties) {
        if (!properties.enabled() && properties.store() == TaskProperties.Store.MEMORY) {
            LOGGER.warn("pgtasks.enabled=false with pgtasks.store=memory: this node keeps its tasks in its own memory and processes none of "
                + "them, so what it pushes never runs");
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
