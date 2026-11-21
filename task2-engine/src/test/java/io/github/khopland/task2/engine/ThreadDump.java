package io.github.khopland.task2.engine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * The live threads of this JVM, virtual ones included, from the JDK's thread dump.
 * {@link Thread#getAllStackTraces()} lists platform threads only, and the engine's threads are
 * nearly all virtual.
 */
final class ThreadDump {

    private static final Pattern THREAD = Pattern.compile("\"tid\": \"(\\d+)\"[^{}]*?\"name\": \"([^\"]*)\"");

    private ThreadDump() {
    }

    /** Thread id to name, for every live thread whose name starts with the prefix. */
    static Map<Long, String> named(String prefix) {
        try {
            Path directory = Files.createTempDirectory("task2-threads");
            Path file = directory.resolve("threads.json");
            try {
                ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
                    .dumpThreads(file.toString(), HotSpotDiagnosticMXBean.ThreadDumpFormat.JSON);
                Map<Long, String> threads = new TreeMap<>();
                Matcher matcher = THREAD.matcher(Files.readString(file));
                while (matcher.find()) {
                    if (matcher.group(2).startsWith(prefix)) {
                        threads.put(Long.parseLong(matcher.group(1)), matcher.group(2));
                    }
                }
                return threads;
            } finally {
                Files.deleteIfExists(file);
                Files.deleteIfExists(directory);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
