package com.alibaba.polardbx.gms.metadb.table;

/**
 * Utility class for externalized column name/type transformations.
 * <p>
 * Physical DDL stores the address column as: {col}_addr_ VARCHAR(128) DEFAULT NULL COMMENT 'ext_type:{ORIGINAL_TYPE}'
 * SQL NULL in the addr column represents "no externalized data".
 * This class provides methods to convert between logical column names/types and their physical representations.
 */
public class ExternalizedColumnInfo {

    public static final String ADDR_SUFFIX = "_addr_";
    public static final int ADDR_VARCHAR_LENGTH = 128;
    public static final String COMMENT_PREFIX = "ext_type:";

    /**
     * Convert logical column name to physical address column name.
     * e.g. "content" -> "content_addr_"
     */
    public static String toAddrColumnName(String logicalColumnName) {
        return logicalColumnName + ADDR_SUFFIX;
    }

    /**
     * Check if a column name is an externalized address column.
     * e.g. "content_addr_" -> true, "content" -> false
     */
    public static boolean isAddrColumn(String columnName) {
        return columnName != null && columnName.endsWith(ADDR_SUFFIX);
    }

    /**
     * Extract the logical column name from the physical address column name.
     * e.g. "content_addr_" -> "content"
     */
    public static String toLogicalColumnName(String addrColumnName) {
        if (!isAddrColumn(addrColumnName)) {
            throw new IllegalArgumentException("Not an address column: " + addrColumnName);
        }
        return addrColumnName.substring(0, addrColumnName.length() - ADDR_SUFFIX.length());
    }

    /**
     * Build the COMMENT string for the physical address column.
     * e.g. "LONGTEXT" -> "ext_type:LONGTEXT"
     */
    public static String buildComment(String originalType) {
        return COMMENT_PREFIX + originalType;
    }

    /**
     * Build the COMMENT carrying the original type AND the user's original COMMENT
     * (if any) so the COMMENT survives the rewrite + binlog revert round-trip.
     *
     * <p>Format: {@code ext_type:LONGTEXT|COMMENT_B64=<base64-of-user-comment>}
     * <p>The user COMMENT is base64-encoded to avoid collisions with separator chars.
     *
     * <p>NOTE: CHARSET / COLLATE are deliberately NOT preserved. CN reads/writes
     * externalized columns as UTF-8 byte streams (hard-coded in FetchBlob /
     * ExternalizedColumnDmlHelper), so CHARSET on an EXTERNALIZE column is purely
     * cosmetic. We reject non-utf8 charsets at CREATE time instead (GAP-DDL-I).
     */
    public static String buildComment(String originalType, String userComment) {
        StringBuilder sb = new StringBuilder(COMMENT_PREFIX).append(originalType);
        if (userComment != null && !userComment.isEmpty()) {
            sb.append("|COMMENT_B64=").append(
                java.util.Base64.getEncoder().encodeToString(
                    userComment.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        }
        return sb.toString();
    }

    /**
     * Extract the original data type from the address column's COMMENT.
     * e.g. "ext_type:LONGTEXT" -> "LONGTEXT"
     * e.g. "ext_type:LONGTEXT|COMMENT_B64=..." -> "LONGTEXT"
     * Returns null if the comment does not contain externalized type info.
     */
    public static String extractOriginalType(String comment) {
        if (comment == null || !comment.startsWith(COMMENT_PREFIX)) {
            return null;
        }
        String body = comment.substring(COMMENT_PREFIX.length());
        int pipe = body.indexOf('|');
        return pipe < 0 ? body : body.substring(0, pipe);
    }

    /**
     * Extract the user-supplied original COMMENT (base64-decoded), or null.
     */
    public static String extractUserComment(String comment) {
        if (comment == null || !comment.startsWith(COMMENT_PREFIX)) {
            return null;
        }
        for (String token : comment.substring(COMMENT_PREFIX.length()).split("\\|")) {
            if (token.startsWith("COMMENT_B64=")) {
                try {
                    return new String(java.util.Base64.getDecoder().decode(token.substring("COMMENT_B64=".length())),
                        java.nio.charset.StandardCharsets.UTF_8);
                } catch (IllegalArgumentException e) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Whether the given character set is acceptable for an EXTERNALIZE column.
     * CN reads/writes blob data as UTF-8 byte streams, so only the utf8 family
     * is meaningful. Other charsets would silently encode wrong bytes.
     */
    public static boolean isAcceptableCharset(String charset) {
        if (charset == null) {
            return true;  // unspecified → inherits table default (typically utf8mb4)
        }
        String c = charset.toLowerCase();
        return c.equals("utf8") || c.equals("utf8mb3") || c.equals("utf8mb4");
    }

    /**
     * Check if a ColumnsRecord represents an externalized column (by flag).
     */
    public static boolean isExternalized(ColumnsRecord record) {
        return record != null && record.isExternalizedColumn();
    }

    /**
     * Check if the given data type is supported for externalization.
     * Currently supports: TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT, BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB.
     */
    public static boolean isSupportedType(String dataType) {
        if (dataType == null) {
            return false;
        }
        String upper = dataType.toUpperCase();
        switch (upper) {
        case "TEXT":
        case "TINYTEXT":
        case "MEDIUMTEXT":
        case "LONGTEXT":
        case "BLOB":
        case "TINYBLOB":
        case "MEDIUMBLOB":
        case "LONGBLOB":
            return true;
        default:
            return false;
        }
    }

    /**
     * Get the physical column name for backfill SQL generation.
     * For externalized columns, returns the physical address column name (mappingName);
     * for normal columns, returns the logical column name.
     *
     * @param cm ColumnMeta from TableMeta (uses optimizer ColumnMeta to avoid gms→optimizer dependency)
     */
    public static String getBackfillColumnName(
        String logicalName, String mappingName, boolean isExternalized) {
        if (isExternalized && mappingName != null) {
            return mappingName;
        }
        return logicalName;
    }

    /**
     * Map the original TEXT/BLOB type name to its JDBC type code.
     */
    public static int getJdbcType(String originalType) {
        if (originalType == null) {
            return java.sql.Types.LONGVARCHAR;
        }
        String upper = originalType.toUpperCase();
        switch (upper) {
        case "BLOB":
        case "TINYBLOB":
        case "MEDIUMBLOB":
        case "LONGBLOB":
            return java.sql.Types.LONGVARBINARY;
        case "TEXT":
        case "TINYTEXT":
        case "MEDIUMTEXT":
        case "LONGTEXT":
        default:
            return java.sql.Types.LONGVARCHAR;
        }
    }
}
