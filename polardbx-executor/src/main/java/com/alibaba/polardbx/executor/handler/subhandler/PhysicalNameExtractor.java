package com.alibaba.polardbx.executor.handler.subhandler;

/**
 * Utility class to extract logical schema/table names from physical names.
 * <p>
 * Physical schemas are suffixed with partition identifiers:
 * - Partition tables:     dxlauto_p00000 -> dxlauto
 * - Non-partition tables: slt_000003     -> slt
 * <p>
 * Physical tables carry both a random suffix and a partition-number suffix:
 * - orders_log_v3gg_00001        -> orders_log  (random suffix "v3gg", 5-digit partition)
 * - orderby_nosort_100_2_tab1_0vgk_15 -> orderby_nosort_100_2_tab1  (2-digit partition)
 */
public class PhysicalNameExtractor {

    /**
     * Extract the logical schema name from a physical schema name.
     * <p>
     * Patterns (checked in order):
     * 1. Ends with {@code _p#####} (partition table) — strip last 7 characters.
     * 2. Ends with {@code _######} (non-partition table) — strip last 7 characters.
     * 3. No match — return as-is.
     */
    public static String extractLogicalSchemaName(String physicalSchema) {
        if (physicalSchema == null) {
            return null;
        }
        // Pattern 1: _p##### (partition tables, e.g. dxlauto_p00000)
        if (physicalSchema.matches(".*_p\\d{5}$")) {
            return physicalSchema.substring(0, physicalSchema.length() - 7);
        }
        // Pattern 2: _###### (non-partition tables, e.g. slt_000003)
        if (physicalSchema.matches(".*_\\d{6}$")) {
            return physicalSchema.substring(0, physicalSchema.length() - 7);
        }
        return physicalSchema;
    }

    /**
     * Extract the logical table name from a physical table name.
     * <p>
     * Patterns are tried from the longest partition-number suffix to the shortest:
     * 5 digits → 4 digits → 3 digits → 2 digits → as-is.
     */
    public static String extractLogicalTableName(String physicalTableName) {
        if (physicalTableName == null) {
            return null;
        }
        if (physicalTableName.matches(".*_\\d{5}$")) {
            return extractLogicalTableNameWithDigits(physicalTableName, 5);
        }
        if (physicalTableName.matches(".*_\\d{4}$")) {
            return extractLogicalTableNameWithDigits(physicalTableName, 4);
        }
        if (physicalTableName.matches(".*_\\d{3}$")) {
            return extractLogicalTableNameWithDigits(physicalTableName, 3);
        }
        if (physicalTableName.matches(".*_\\d{2}$")) {
            return extractLogicalTableNameWithDigits(physicalTableName, 2);
        }
        return physicalTableName;
    }

    /**
     * Strip the trailing partition-number suffix ({@code _<digitCount digits>}) and,
     * if the remaining string ends with a short alphanumeric random suffix
     * (≤ 5 characters), strip that too.
     *
     * @param physicalTableName the physical table name
     * @param digitCount number of digits in the partition suffix
     * @return the logical table name
     */
    public static String extractLogicalTableNameWithDigits(String physicalTableName, int digitCount) {
        // Remove trailing _<digits>
        String withoutPartitionSuffix =
            physicalTableName.substring(0, physicalTableName.length() - (digitCount + 1));

        // Check for a short random suffix (e.g. _v3gg, _0vgk, _xean)
        int lastUnderscore = withoutPartitionSuffix.lastIndexOf('_');
        if (lastUnderscore > 0) {
            String potentialSuffix = withoutPartitionSuffix.substring(lastUnderscore + 1);
            if (potentialSuffix.length() <= 5 && potentialSuffix.matches("[a-zA-Z0-9]+")) {
                return withoutPartitionSuffix.substring(0, lastUnderscore);
            }
        }
        return withoutPartitionSuffix;
    }
}
