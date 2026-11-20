package org.example.task2.postgres;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Splits a SQL script into statements at top-level semicolons. Understands quoted strings,
 * E'' strings, quoted identifiers, line and nested block comments, and dollar quoting, which is
 * all the schema scripts use. Statements that hold only comments are dropped.
 */
final class SqlStatements {

    private static final Pattern DOLLAR_TAG = Pattern.compile("\\$([A-Za-z_][A-Za-z0-9_]*)?\\$");

    private SqlStatements() {
    }

    static List<String> split(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean content = false;
        int length = sql.length();
        int index = 0;
        while (index < length) {
            char character = sql.charAt(index);
            int end;
            if (character == '-' && sql.startsWith("--", index)) {
                end = sql.indexOf('\n', index);
                end = end < 0 ? length : end;
                current.append(sql, index, end);
                index = end;
                continue;
            } else if (character == '/' && sql.startsWith("/*", index)) {
                end = blockCommentEnd(sql, index);
            } else if (character == '\'') {
                boolean escapes = index > 0 && (sql.charAt(index - 1) == 'E' || sql.charAt(index - 1) == 'e')
                    && (index < 2 || !identifierPart(sql.charAt(index - 2)));
                end = quoteEnd(sql, index, '\'', escapes);
            } else if (character == '"') {
                end = quoteEnd(sql, index, '"', false);
            } else if (character == '$' && (index == 0 || !identifierPart(sql.charAt(index - 1)))) {
                Matcher tag = DOLLAR_TAG.matcher(sql).region(index, length);
                if (tag.lookingAt()) {
                    int close = sql.indexOf(tag.group(), tag.end());
                    end = close < 0 ? length : close + tag.group().length();
                } else {
                    end = index + 1;
                }
            } else if (character == ';') {
                if (content) {
                    statements.add(current.toString().strip());
                }
                current.setLength(0);
                content = false;
                index++;
                continue;
            } else {
                end = index + 1;
            }
            if (!(character == '/' && sql.startsWith("/*", index)) && !Character.isWhitespace(character)) {
                content = true;
            }
            current.append(sql, index, end);
            index = end;
        }
        if (content) {
            statements.add(current.toString().strip());
        }
        return statements;
    }

    private static int blockCommentEnd(String sql, int start) {
        int depth = 0;
        int index = start;
        while (index < sql.length()) {
            if (sql.startsWith("/*", index)) {
                depth++;
                index += 2;
            } else if (sql.startsWith("*/", index)) {
                depth--;
                index += 2;
                if (depth == 0) {
                    return index;
                }
            } else {
                index++;
            }
        }
        return sql.length();
    }

    private static int quoteEnd(String sql, int start, char quote, boolean backslashEscapes) {
        int index = start + 1;
        while (index < sql.length()) {
            char character = sql.charAt(index);
            if (backslashEscapes && character == '\\') {
                index += 2;
            } else if (character == quote) {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                    index += 2;
                } else {
                    return index + 1;
                }
            } else {
                index++;
            }
        }
        return sql.length();
    }

    private static boolean identifierPart(char character) {
        return Character.isLetterOrDigit(character) || character == '_' || character == '$';
    }
}
