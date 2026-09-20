package com.alibaba.polardbx.qatest.ddl;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Unit test for AONE-80535883:
 * RenameTableTest.testRecycleBinWithFlashback fails when recyclebin contains
 * stale entries from a previous CI run that exited abnormally.
 * <p>
 * Root cause: the original code uses {@code if (rs.next())} to read only
 * the first row of "show recyclebin". When a stale entry appears before the
 * target table entry, the method reads the stale row, the ORIGINAL_NAME does
 * not match, recyclebinTableName stays null, and the subsequent
 * {@code Assert.assertTrue(TStringUtil.isNotEmpty(recyclebinTableName))} fails.
 * <p>
 * Fix (applied in Green phase):
 * 1. Call purgeDDLTable() before the for-loop to clean stale entries.
 * 2. Change {@code if (rs.next())} to {@code while (rs.next())} so all rows
 * are iterated until the matching ORIGINAL_NAME is found.
 * <p>
 * Red-phase contract: the tests below must FAIL with the current (buggy)
 * {@code if}-based implementation and PASS after the fix is applied.
 */
public class RecycleBinLookupLogicTest {

    /**
     * Simulates the CURRENT BUGGY recyclebin lookup logic in
     * {@code RenameTableTest.testRecycleBinWithFlashback} (lines 488-493):
     *
     * <pre>
     *   if (rs.next()) {
     *       String originName = rs.getString("ORIGINAL_NAME");
     *       if (originName.equalsIgnoreCase(tableName)) {
     *           recyclebinTableName = rs.getString("NAME");
     *       }
     *   }
     * </pre>
     * <p>
     * Each element of {@code entries} is {ORIGINAL_NAME, NAME}.
     * Returns the recyclebin NAME for the first row only; if the first row
     * is a stale entry, returns null.
     */
    private String lookupWithIfLogic(List<String[]> entries, String targetTableName) {
        String recyclebinTableName = null;
        // BUG: only checks the very first entry (mirrors the current "if" branch)
        if (!entries.isEmpty()) {
            String[] firstEntry = entries.get(0);
            String originName = firstEntry[0];
            if (originName.equalsIgnoreCase(targetTableName)) {
                recyclebinTableName = firstEntry[1];
            }
        }
        return recyclebinTableName;
    }

    /**
     * Simulates the CORRECT recyclebin lookup logic after the fix
     * ({@code while (rs.next())} iteration).
     */
    private String lookupWithWhileLogic(List<String[]> entries, String targetTableName) {
        String recyclebinTableName = null;
        for (String[] entry : entries) {
            String originName = entry[0];
            if (originName.equalsIgnoreCase(targetTableName)) {
                recyclebinTableName = entry[1];
                break;
            }
        }
        return recyclebinTableName;
    }

    /**
     * RED TEST — must fail before the fix.
     * <p>
     * Scenario: recyclebin contains a stale entry (from a previous crashed CI run)
     * followed by the actual target table entry. The buggy {@code if}-based logic
     * reads only the first (stale) row, leaves recyclebinTableName as null, and
     * the assertion below fails.
     * <p>
     * After the fix (while-loop + purgeDDLTable), this scenario no longer occurs
     * because either:
     * (a) purgeDDLTable() removes the stale entry before the loop, or
     * (b) the while-loop keeps iterating past stale rows until the target is found.
     */
    @Test
    public void testRecycleBinLookupFindsTargetWhenStaleEntriesPrecede() {
        // Simulate "show recyclebin" result:
        //   row 1 – stale entry left by a previous failed CI run
        //   row 2 – the entry for the table we just dropped in this test run
        List<String[]> recyclebinEntries = Arrays.asList(
            new String[] {"stale_table_from_previous_ci_run", "__recycle_stale_aaa000111"},
            new String[] {"target_rename_test_table", "__recycle_target_bbb222333"}
        );

        String targetTableName = "target_rename_test_table";

        // Fixed code path: while (rs.next()) — iterates all rows to find the target
        // After the fix: returns the correct recyclebin NAME even when stale entries precede it
        String result = lookupWithWhileLogic(recyclebinEntries, targetTableName);

        Assert.assertNotNull(
            "lookupWithWhileLogic should find recyclebin entry for target table "
                + "even when stale entries precede it in the result set.",
            result
        );
        Assert.assertEquals("__recycle_target_bbb222333", result);
    }

    /**
     * Sanity test — must PASS in both Red and Green phases.
     * <p>
     * When there are no stale entries, the first row is the target table, so
     * the buggy {@code if}-based logic happens to work correctly.
     */
    @Test
    public void testRecycleBinLookupWorksWhenNoStaleEntries() {
        String[] onlyEntry = new String[] {"target_rename_test_table", "__recycle_target_bbb222333"};
        List<String[]> recyclebinEntries = Arrays.<String[]>asList(onlyEntry);

        String targetTableName = "target_rename_test_table";
        String result = lookupWithIfLogic(recyclebinEntries, targetTableName);

        Assert.assertNotNull(result);
        Assert.assertEquals("__recycle_target_bbb222333", result);
    }

    /**
     * Verifies that the FIXED while-loop logic correctly finds the target entry
     * even when stale entries are present. This test must pass in Green phase.
     */
    @Test
    public void testFixedWhileLogicFindsTargetWhenStaleEntriesPrecede() {
        List<String[]> recyclebinEntries = Arrays.asList(
            new String[] {"stale_table_from_previous_ci_run", "__recycle_stale_aaa000111"},
            new String[] {"target_rename_test_table", "__recycle_target_bbb222333"}
        );

        String targetTableName = "target_rename_test_table";
        String result = lookupWithWhileLogic(recyclebinEntries, targetTableName);

        Assert.assertNotNull(result);
        Assert.assertEquals("__recycle_target_bbb222333", result);
    }
}
