package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Unit tests for {@link OmcUtils#validateKeepFilter(String, Set)}.
 * <p>
 * Covers:
 * - Column existence validation (the main fix for REBUILD_TABLE_KEEP_FILTER)
 * - Backward compatibility with the single-arg overload
 * - Syntax and forbidden-pattern checks remain intact
 */
public class OmcUtilsKeepFilterTest {

    private static Set<String> newColumns(String... cols) {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (String c : cols) {
            set.add(c);
        }
        return set;
    }

    // ==================== Column existence validation ====================

    /**
     * Filter referencing a non-existent column must be rejected with "does not exist".
     */
    @Test
    public void testRejectNonExistentColumn() {
        Set<String> columns = newColumns("id", "balance", "name");
        try {
            OmcUtils.validateKeepFilter("nonexistent_col > 0", columns);
            Assert.fail("expected TddlRuntimeException for non-existent column");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue("error message should contain 'does not exist', got: " + e.getMessage(),
                e.getMessage().contains("does not exist"));
            Assert.assertTrue("error message should contain the column name, got: " + e.getMessage(),
                e.getMessage().contains("nonexistent_col"));
        }
    }

    /**
     * Filter referencing only existing columns should pass.
     */
    @Test
    public void testAcceptExistingColumns() {
        Set<String> columns = newColumns("id", "balance", "name");
        OmcUtils.validateKeepFilter("balance != 0", columns);
        OmcUtils.validateKeepFilter("balance > 0 AND name = 'active'", columns);
        OmcUtils.validateKeepFilter("id > 10 OR balance < 100", columns);
    }

    /**
     * Column matching should be case-insensitive.
     */
    @Test
    public void testCaseInsensitiveColumnMatch() {
        Set<String> columns = newColumns("id", "Balance", "Name");
        OmcUtils.validateKeepFilter("balance != 0", columns);
        OmcUtils.validateKeepFilter("BALANCE > 0", columns);
        OmcUtils.validateKeepFilter("name = 'test'", columns);
    }

    /**
     * Quoted identifiers emitted by Calcite should be compared by their normalized column names.
     */
    @Test
    public void testQuotedColumnMatch() {
        Set<String> columns = newColumns("id", "balance");
        Set<String> referencedColumns = OmcUtils.validateKeepFilter("((`balance` > 0) IS NOT TRUE)", columns);
        Assert.assertEquals(newColumns("balance"), referencedColumns);
    }

    /**
     * Qualified quoted identifiers should validate only their column part.
     */
    @Test
    public void testQualifiedQuotedColumnMatch() {
        Set<String> columns = newColumns("id", "balance");
        Set<String> referencedColumns = OmcUtils.validateKeepFilter("`t`.`balance` > 0", columns);
        Assert.assertEquals(newColumns("balance"), referencedColumns);
    }

    /**
     * A filter that references both existing and non-existing columns should be rejected.
     */
    @Test
    public void testRejectMixedColumns() {
        Set<String> columns = newColumns("id", "balance");
        try {
            OmcUtils.validateKeepFilter("balance > 0 AND unknown_col < 10", columns);
            Assert.fail("expected TddlRuntimeException for non-existent column");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    // ==================== Backward compatibility (null / empty column set) ====================

    /**
     * Null column set should skip column existence check (backward compat).
     */
    @Test
    public void testNullColumnSetSkipsCheck() {
        // Would fail with non-null set, but should pass with null
        OmcUtils.validateKeepFilter("nonexistent_col > 0", null);
    }

    /**
     * Empty column set should skip column existence check.
     */
    @Test
    public void testEmptyColumnSetSkipsCheck() {
        OmcUtils.validateKeepFilter("nonexistent_col > 0", new HashSet<>());
    }

    /**
     * Single-arg overload should skip column existence check.
     */
    @Test
    public void testSingleArgOverloadSkipsCheck() {
        OmcUtils.validateKeepFilter("nonexistent_col > 0");
    }

    // ==================== Syntax and forbidden-pattern checks ====================

    /**
     * Syntax error should still be rejected regardless of column set.
     */
    @Test
    public void testRejectSyntaxError() {
        Set<String> columns = newColumns("id", "balance");
        try {
            OmcUtils.validateKeepFilter("balance !! 0", columns);
            Assert.fail("expected TddlRuntimeException for syntax error");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid REBUILD_TABLE_KEEP_FILTER"));
        }
    }

    /**
     * Subquery should still be rejected regardless of column set.
     */
    @Test
    public void testRejectSubquery() {
        Set<String> columns = newColumns("id", "balance");
        try {
            OmcUtils.validateKeepFilter("exists (select 1 from t where id = 1)", columns);
            Assert.fail("expected TddlRuntimeException for subquery");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("subqueries are not allowed"));
        }
    }

    /**
     * Non-deterministic function should still be rejected regardless of column set.
     */
    @Test
    public void testRejectNonDeterministicFunction() {
        Set<String> columns = newColumns("id", "balance");
        try {
            OmcUtils.validateKeepFilter("balance > NOW()", columns);
            Assert.fail("expected TddlRuntimeException for non-deterministic function");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("non-deterministic function"));
        }
    }

    /**
     * UNIX_TIMESTAMP with a constant arg is deterministic and should pass.
     */
    @Test
    public void testAllowUnixTimestampWithArg() {
        Set<String> columns = newColumns("id", "gmt_created");
        OmcUtils.validateKeepFilter("gmt_created > UNIX_TIMESTAMP('2024-01-01')", columns);
    }

    @Test
    public void testRejectRebuildCleanupTableWithSecondaryStructures() {
        TableMeta plainTable = Mockito.mock(TableMeta.class);
        OmcUtils.validateRebuildCleanupTable(plainTable, false);

        TableMeta primaryWithGsi = Mockito.mock(TableMeta.class);
        Mockito.when(primaryWithGsi.withGsiExcludingPureCci()).thenReturn(true);
        assertRebuildCleanupTableRejected(primaryWithGsi, "FORCE_REBUILD_CLEANUP_WITH_GSI");

        GsiMetaManager.GsiIndexMetaBean cci = new GsiMetaManager.GsiIndexMetaBean(
            null, "test_schema", "test_table", true, "test_schema", "test_cci",
            Collections.emptyList(), Collections.emptyList(), null, null, null, null, "test_cci",
            IndexStatus.PUBLIC, 1, true, true, IndexVisibility.VISIBLE, LackLocalIndexStatus.NO_LACKIING);
        Map<String, GsiMetaManager.GsiIndexMetaBean> cciMap = Collections.singletonMap("test_cci", cci);
        GsiMetaManager.GsiTableMetaBean tableBean = new GsiMetaManager.GsiTableMetaBean(
            null, "test_schema", "test_table", GsiMetaManager.TableType.SHARDING,
            null, null, null, null, null, null, cciMap, null, null);
        TableMeta primaryWithCci = Mockito.mock(TableMeta.class);
        Mockito.when(primaryWithCci.withCci()).thenReturn(true);
        Mockito.when(primaryWithCci.getGsiTableMetaBean()).thenReturn(tableBean);
        Mockito.when(primaryWithCci.getColumnarIndexPublished()).thenReturn(cciMap);
        assertRebuildCleanupTableRejected(primaryWithCci, "FORCE_REBUILD_CLEANUP_WITH_GSI");
        OmcUtils.validateRebuildCleanupTable(primaryWithCci, true);

        TableMeta primaryWithNonPublicCci = Mockito.mock(TableMeta.class);
        Mockito.when(primaryWithNonPublicCci.withCci()).thenReturn(true);
        Mockito.when(primaryWithNonPublicCci.getGsiTableMetaBean()).thenReturn(tableBean);
        Mockito.when(primaryWithNonPublicCci.getColumnarIndexPublished()).thenReturn(Collections.emptyMap());
        assertRebuildCleanupTableRejected(primaryWithNonPublicCci, true, "all CCIs to be PUBLIC");

        TableMeta gsiTable = Mockito.mock(TableMeta.class);
        Mockito.when(gsiTable.isGsi()).thenReturn(true);
        assertRebuildCleanupTableRejected(gsiTable, "primary table");

        TableMeta columnarTable = Mockito.mock(TableMeta.class);
        Mockito.when(columnarTable.isColumnar()).thenReturn(true);
        assertRebuildCleanupTableRejected(columnarTable, "primary table");
    }

    private static void assertRebuildCleanupTableRejected(TableMeta tableMeta, String message) {
        assertRebuildCleanupTableRejected(tableMeta, false, message);
    }

    private static void assertRebuildCleanupTableRejected(TableMeta tableMeta, boolean forceWithGsi, String message) {
        try {
            OmcUtils.validateRebuildCleanupTable(tableMeta, forceWithGsi);
            Assert.fail("expected table with secondary structures to be rejected");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains(message));
        }
    }
}
