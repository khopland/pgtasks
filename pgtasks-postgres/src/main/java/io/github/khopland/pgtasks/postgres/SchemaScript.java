package io.github.khopland.pgtasks.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

/**
 * One schema file, named the Flyway way: {@code V<version>__<description>.sql} runs once,
 * {@code R__<description>.sql} runs again whenever its content changes. A file whose header
 * comment contains {@value #NO_TRANSACTION} runs outside a transaction, statement by statement.
 *
 * <p>A repeatable file's header comment also carries its revision, {@code -- revision: <n>}, which
 * grows with every change to the file. The runner never applies a file over a higher revision
 * that a newer node applied, so an older node can't roll back functions, views or triggers during
 * a rolling deploy. A file without one is revision 0, as are the rows an older runner recorded.
 */
record SchemaScript(String name, @Nullable Integer version, String description, String content, boolean transactional, String checksum,
                    int revision) {

    static final String NO_TRANSACTION = "-- task:no-transaction";

    private static final Pattern REVISION = Pattern.compile("--\\s*revision:\\s*(\\d+)\\s*");

    private static final Pattern VERSIONED = Pattern.compile("V(\\d+)__([A-Za-z0-9_]+)\\.sql");
    private static final Pattern REPEATABLE = Pattern.compile("R__([A-Za-z0-9_]+)\\.sql");

    static SchemaScript of(String name, String content) {
        String normalized = content.replace("\r\n", "\n");
        Matcher versioned = VERSIONED.matcher(name);
        Matcher repeatable = REPEATABLE.matcher(name);
        if (versioned.matches()) {
            return new SchemaScript(name, Integer.parseInt(versioned.group(1)), versioned.group(2).replace('_', ' '),
                normalized, !hasNoTransactionHeader(normalized), checksum(normalized), 0);
        }
        if (repeatable.matches()) {
            return new SchemaScript(name, null, repeatable.group(1).replace('_', ' '),
                normalized, !hasNoTransactionHeader(normalized), checksum(normalized), revision(normalized));
        }
        throw new IllegalArgumentException("Schema file " + name + " is named neither V<version>__<description>.sql nor R__<description>.sql");
    }

    boolean repeatable() {
        return version == null;
    }

    /** The version of a versioned script. */
    int number() {
        if (version == null) {
            throw new IllegalStateException(name + " is repeatable and has no version");
        }
        return version;
    }

    /** The file's content with the schema placeholder filled in; the checksum is the file's own, whatever the name. */
    String content(String schema) {
        return TaskSchema.render(content, schema);
    }

    List<String> statements(String schema) {
        return SqlStatements.split(content(schema));
    }

    /** The marker counts only in the leading comment lines, before the first statement. */
    private static boolean hasNoTransactionHeader(String content) {
        return header(content).contains(NO_TRANSACTION);
    }

    /** The {@code -- revision: <n>} line among the leading comment lines, or 0. */
    private static int revision(String content) {
        for (String line : header(content)) {
            Matcher matcher = REVISION.matcher(line);
            if (matcher.matches()) {
                return Integer.parseInt(matcher.group(1));
            }
        }
        return 0;
    }

    /** The leading comment lines, stripped, before the first statement. */
    private static List<String> header(String content) {
        List<String> header = new ArrayList<>();
        for (String line : content.split("\n", -1)) {
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (!trimmed.startsWith("--")) {
                break;
            }
            header.add(trimmed);
        }
        return header;
    }

    private static String checksum(String content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
