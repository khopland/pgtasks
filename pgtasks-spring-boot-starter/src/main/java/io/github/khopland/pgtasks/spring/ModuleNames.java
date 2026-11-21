package io.github.khopland.pgtasks.spring;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.util.ClassUtils;

/**
 * The module each configurer bean belongs to, shown in the effective settings, in errors and by
 * the tasks endpoint. With Spring Modulith, the application module that declares the bean (its
 * configuration class for a {@code @Bean} method); otherwise, or outside every module (the
 * application's own package is Modulith's root module), the bean name. Two configurers in one
 * module are told apart by their bean names. Outside module tests the modules come from Spring
 * Modulith's runtime bean, which computes them once, at startup.
 */
final class ModuleNames {

    static final String MODULE_TEST_EXECUTION = "org.springframework.modulith.test.ModuleTestExecution";
    private static final String MODULES_RUNTIME = "org.springframework.modulith.runtime.ApplicationModulesRuntime";
    private static final boolean MODULITH = ClassUtils.isPresent("org.springframework.modulith.core.ApplicationModules",
        ModuleNames.class.getClassLoader());

    private ModuleNames() {
    }

    /** Whether this context runs a Spring Modulith module test, {@code @ApplicationModuleTest}. */
    static boolean inModuleTest(ConfigurableListableBeanFactory beans) {
        return beans.containsSingleton(MODULE_TEST_EXECUTION);
    }

    /** Names for the given beans, by bean name. */
    static Map<String, String> of(ConfigurableListableBeanFactory beans, List<String> beanNames) {
        Map<String, String> names = new HashMap<>();
        Map<String, String> modules = new HashMap<>();
        Optional<Modulith> modulith = MODULITH ? Modulith.find(beans) : Optional.empty();
        for (String bean : beanNames) {
            modulith.flatMap(found -> found.module(declaringClass(beans, bean))).ifPresent(module -> modules.put(bean, module));
        }
        for (String bean : beanNames) {
            String module = modules.get(bean);
            boolean shared = module != null && modules.values().stream().filter(module::equals).count() > 1;
            names.put(bean, module == null ? bean : shared ? module + "." + bean : module);
        }
        return names;
    }

    /** The class whose package says where a bean belongs: its {@code @Bean} method's class, or its own. */
    private static Class<?> declaringClass(ConfigurableListableBeanFactory beans, String bean) {
        BeanDefinition definition = beans.containsBeanDefinition(bean) ? beans.getMergedBeanDefinition(bean) : null;
        if (definition != null && definition.getFactoryBeanName() != null) {
            Class<?> factory = beans.getType(definition.getFactoryBeanName());
            if (factory != null) {
                return ClassUtils.getUserClass(factory);
            }
        }
        Class<?> type = beans.getType(bean);
        return type == null ? Object.class : ClassUtils.getUserClass(type);
    }

    /** Loaded only with Spring Modulith on the classpath. */
    private record Modulith(org.springframework.modulith.core.ApplicationModules modules) {

        @SuppressWarnings("unchecked")
        static Optional<Modulith> find(ConfigurableListableBeanFactory beans) {
            try {
                if (inModuleTest(beans)) {
                    // spring-modulith-test is a test dependency, so no compile-time reference.
                    Object execution = beans.getSingleton(MODULE_TEST_EXECUTION);
                    return Optional.of(new Modulith((org.springframework.modulith.core.ApplicationModules)
                        execution.getClass().getMethod("getModules").invoke(execution)));
                }
                if (ClassUtils.isPresent(MODULES_RUNTIME, ModuleNames.class.getClassLoader())) {
                    String[] runtimes = beans.getBeanNamesForType(ClassUtils.forName(MODULES_RUNTIME, ModuleNames.class.getClassLoader()));
                    if (runtimes.length == 1) {
                        return Optional.of(new Modulith(((Supplier<org.springframework.modulith.core.ApplicationModules>)
                            beans.getBean(runtimes[0])).get()));
                    }
                }
            } catch (ReflectiveOperationException | LinkageError e) {
                return Optional.empty();
            }
            return Optional.empty();
        }

        Optional<String> module(Class<?> type) {
            return modules.getModuleByType(type).filter(module -> !module.isRootModule()).map(module -> module.getIdentifier().toString());
        }
    }
}
