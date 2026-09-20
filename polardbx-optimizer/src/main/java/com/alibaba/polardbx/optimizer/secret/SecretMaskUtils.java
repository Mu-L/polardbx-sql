package com.alibaba.polardbx.optimizer.secret;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SecretMaskUtils {

    private static final String MASKED = "<masked>";

    /**
     * Matches only the head of a secret DDL: the verb, the SECRET keyword and the name.
     * Everything after the name is the credential payload. The name charset is explicit
     * instead of \S+ so that an adjacent quote is not swallowed.
     */
    private static final Pattern SECRET_DDL_HEAD = Pattern.compile(
        "(?:CREATE|ALTER)\\s+SECRET\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?(?:`[^`]*`|[A-Za-z0-9_$\\-]+)",
        Pattern.CASE_INSENSITIVE);

    private SecretMaskUtils() {
    }

    public static String mask(String sql) {
        if (sql == null) {
            return null;
        }

        if (!containsSecretDdlHead(sql)) {
            return sql;
        }

        Matcher m = SECRET_DDL_HEAD.matcher(sql);
        m.region(skipLeadingNoise(sql), sql.length());
        // lookingAt anchors at the region start, so a secret DDL that merely appears
        // inside a literal or a comment of some other statement is never matched.
        if (!m.lookingAt()) {
            return sql;
        }
        int headEnd = m.end();
        if (isBlankTail(sql, headEnd)) {
            return sql;
        }
        return sql.substring(0, headEnd) + " " + MASKED;
    }

    /**
     * Fast fail without building a Matcher: true only if SECRET appears after a
     * CREATE or ALTER. False positives (e.g. inside a literal) are fine, the
     * anchored regex makes the final decision.
     */
    private static boolean containsSecretDdlHead(String sql) {
        int create = StringUtils.indexOfIgnoreCase(sql, "create");
        int alter = StringUtils.indexOfIgnoreCase(sql, "alter");
        int verb = create < 0 ? alter : (alter < 0 ? create : Math.min(create, alter));
        if (verb < 0) {
            return false;
        }
        return StringUtils.indexOfIgnoreCase(sql, "secret", verb) >= 0;
    }

    /**
     * Returns the offset of the first character that can start a statement, skipping
     * whitespace, block comments and line comments. TDDL hints are block comments too,
     * so they are skipped here and therefore survive in the masked output.
     */
    private static int skipLeadingNoise(String sql) {
        int len = sql.length();
        int i = 0;
        while (i < len) {
            char c = sql.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < len && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                if (end < 0) {
                    return len;
                }
                i = end + 2;
            } else if (c == '#' || (c == '-' && i + 1 < len && sql.charAt(i + 1) == '-')) {
                int end = sql.indexOf('\n', i);
                if (end < 0) {
                    return len;
                }
                i = end + 1;
            } else {
                return i;
            }
        }
        return len;
    }

    private static boolean isBlankTail(String sql, int from) {
        for (int i = from; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (!Character.isWhitespace(c) && c != ';') {
                return false;
            }
        }
        return true;
    }
}
