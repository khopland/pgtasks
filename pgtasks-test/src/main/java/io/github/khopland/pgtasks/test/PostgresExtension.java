package io.github.khopland.pgtasks.test;

import java.util.Objects;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * Gives every test method its own fresh database on a real Postgres server.
 *
 * <pre>{@code
 * @RegisterExtension
 * static PostgresExtension postgres = PostgresExtension.migrated(database -> Schema.migrate(database.dataSource()));
 *
 * @Test
 * void something(PostgresDatabase database) { ... }
 * }</pre>
 */
public final class PostgresExtension implements BeforeEachCallback, AfterEachCallback, ParameterResolver {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(PostgresExtension.class);

    /** Prepares a freshly created database, typically by running a changelog. */
    @FunctionalInterface
    public interface Preparation {
        void prepare(PostgresDatabase database) throws Exception;
    }

    private final Preparation preparation;

    private PostgresExtension(Preparation preparation) {
        this.preparation = Objects.requireNonNull(preparation);
    }

    public static PostgresExtension empty() {
        return new PostgresExtension(database -> { });
    }

    public static PostgresExtension migrated(Preparation preparation) {
        return new PostgresExtension(preparation);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        PostgresDatabase database = TestPostgres.createDatabase();
        context.getStore(NAMESPACE).put(PostgresDatabase.class, database);
        try {
            preparation.prepare(database);
        } catch (Exception | Error e) {
            database.close();
            context.getStore(NAMESPACE).remove(PostgresDatabase.class);
            throw e;
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        PostgresDatabase database = context.getStore(NAMESPACE).remove(PostgresDatabase.class, PostgresDatabase.class);
        if (database != null) {
            database.close();
        }
    }

    /** The database of the test that is currently running. */
    public PostgresDatabase database(ExtensionContext context) {
        return context.getStore(NAMESPACE).get(PostgresDatabase.class, PostgresDatabase.class);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return parameterContext.getParameter().getType() == PostgresDatabase.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return database(extensionContext);
    }
}
