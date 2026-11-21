package io.github.khopland.task2.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One schema file, named the Flyway way: {@code V<version>__<description>.sql} runs once,
 * {@code R__<description>.sql} runs again whenever its content changes. A file whose header
 * comment contains {@value #NO_TRANSACTION} runs outside a transaction, statement by statement.
 */
record SchemaScript(String name, Integer version, String description, String content, boolean transactional, String checksum) {

    static final String NO_TRANSACTION = "-- task:no-transaction";

    private static final Pattern VERSIONED = Pattern.compile("V(\\d+)__([A-Za-z0-9_]+)\\.sql");
    private static final Pattern REPEATABLE = Pattern.compile("R__([A-Za-z0-9_]+)\\.sql");

    static SchemaScript of(String name, String content) {
        String normalized = content.replace("\r\n", "\n");
        Matcher versioned = VERSIONED.matcher(name);
        Matcher repeatable = REPEATABLE.matcher(name);
        if (versioned.matches()) {
            return new SchemaScript(name, Integer.parseInt(versioned.group(1)), versioned.group(2).replace('_', ' '),
                normalized, !hasNoTransactionHeader(normalized), checksum(normalized));
        }
        if (repeatable.matches()) {
            return new SchemaScript(name, null, repeatable.group(1).replace('_', ' '),
                normalized, !hasNoTransactionHeader(normalized), checksum(normalized));
        }
        throw new IllegalArgumentException("Schema file " + name + " is named neither V<version>__<description>.sql nor R__<description>.sql");
    }

    boolean repeatable() {
        return version == null;
    }

    List<String> statements() {
        return SqlStatements.split(content);
    }

    /** The marker counts only in the leading comment lines, before the first statement. */
    private static boolean hasNoTransactionHeader(String content) {
        for (String line : content.split("\n")) {
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                continue;
            }
            if (!trimmed.startsWith("--")) {
                return false;
            }
            if (trimmed.equals(NO_TRANSACTION)) {
                return true;
            }
        }
        return false;
    }

    private static String checksum(String content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
