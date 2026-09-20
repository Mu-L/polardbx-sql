package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.util.JsonBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Unit tests for LogicalView lockMode serialization / deserialization changes
 * introduced in:
 * fix #80205502 [for update] Fix the issue where FOR UPDATE was not correctly
 * materialized in LogicalView.
 * <p>
 * Covers:
 * 1. DRDSRelJson.toJson() can serialize SqlSelect.LockMode enum values.
 * 2. LogicalView.explainTerms() emits "lockMode" only when it is not UNDEF.
 * 3. DRDSRelJsonReader can round-trip a LogicalView that carries a non-UNDEF
 * lockMode (write → JSON string → read → same lockMode).
 * 4. A LogicalView with UNDEF lockMode round-trips without emitting "lockMode"
 * in the JSON (backward-compatible).
 *
 * @author fangwu
 */
public class LogicalViewLockModeExternalizeTest extends BaseRuleTest {

    // -----------------------------------------------------------------------
    // DRDSRelJson.toJson() – LockMode serialization
    // -----------------------------------------------------------------------

    @Test
    public void testDrdsRelJsonSerializesLockModeExclusiveAsString() {
        DRDSRelJson drdsRelJson = new DRDSRelJson(new JsonBuilder(), false);
        Object serialized = drdsRelJson.toJson(LockMode.EXCLUSIVE_LOCK);
        Assert.assertEquals("EXCLUSIVE_LOCK", serialized);
    }

    @Test
    public void testDrdsRelJsonSerializesLockModeSharedAsString() {
        DRDSRelJson drdsRelJson = new DRDSRelJson(new JsonBuilder(), false);
        Object serialized = drdsRelJson.toJson(LockMode.SHARED_LOCK);
        Assert.assertEquals("SHARED_LOCK", serialized);
    }

    @Test
    public void testDrdsRelJsonSerializesLockModeUndefAsString() {
        DRDSRelJson drdsRelJson = new DRDSRelJson(new JsonBuilder(), false);
        Object serialized = drdsRelJson.toJson(LockMode.UNDEF);
        Assert.assertEquals("UNDEF", serialized);
    }

    // -----------------------------------------------------------------------
    // LogicalView.explainTerms() – lockMode field presence in JSON
    // -----------------------------------------------------------------------

    /**
     * When lockMode is UNDEF the "lockMode" key must NOT appear in the JSON
     * (itemIf condition is false), preserving backward compatibility with
     * cached plans that were serialized before this fix.
     */
    @Test
    public void testLogicalViewWithUndefLockModeDoesNotEmitLockModeField() {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        // lockMode defaults to UNDEF – no explicit set needed

        DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        logicalView.explain(writer);
        String json = writer.asString();

        Assert.assertFalse(
            "JSON must not contain 'lockMode' when lockMode is UNDEF",
            json.contains("\"lockMode\""));
    }

    /**
     * When lockMode is EXCLUSIVE_LOCK the "lockMode" key must appear in the JSON.
     */
    @Test
    public void testLogicalViewWithExclusiveLockModeEmitsLockModeField() throws Exception {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        writeLockMode(logicalView, LockMode.EXCLUSIVE_LOCK);

        DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        logicalView.explain(writer);
        String json = writer.asString();

        Assert.assertTrue(
            "JSON must contain 'lockMode' when lockMode is EXCLUSIVE_LOCK",
            json.contains("\"lockMode\""));
        Assert.assertTrue(
            "JSON must contain 'EXCLUSIVE_LOCK' as the lockMode value",
            json.contains("EXCLUSIVE_LOCK"));
    }

    // -----------------------------------------------------------------------
    // Round-trip: write → JSON → read → same lockMode
    // -----------------------------------------------------------------------

    /**
     * A LogicalView with EXCLUSIVE_LOCK must survive a full write/read cycle
     * and come back with the same lockMode.
     */
    @Test
    public void testLogicalViewExclusiveLockModeRoundTrip() throws Exception {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView original = LogicalView.create(scan, scan.getTable());
        writeLockMode(original, LockMode.EXCLUSIVE_LOCK);

        // Serialize
        DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        original.explain(writer);
        String json = writer.asString();

        // Deserialize
        DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
        RelNode deserialized = reader.read(json);

        Assert.assertTrue("Deserialized node must be a LogicalView", deserialized instanceof LogicalView);
        LogicalView deserializedView = (LogicalView) deserialized;
        Assert.assertEquals(
            "lockMode must be preserved through serialization round-trip",
            LockMode.EXCLUSIVE_LOCK,
            readLockMode(deserializedView));
    }

    /**
     * A LogicalView with UNDEF lockMode must survive a full write/read cycle
     * and come back with UNDEF (the default), ensuring backward compatibility.
     */
    @Test
    public void testLogicalViewUndefLockModeRoundTrip() throws Exception {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView original = LogicalView.create(scan, scan.getTable());
        // lockMode is UNDEF by default

        // Serialize
        DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        original.explain(writer);
        String json = writer.asString();

        // Deserialize
        DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
        RelNode deserialized = reader.read(json);

        Assert.assertTrue("Deserialized node must be a LogicalView", deserialized instanceof LogicalView);
        LogicalView deserializedView = (LogicalView) deserialized;
        Assert.assertEquals(
            "lockMode must default to UNDEF when not present in JSON",
            LockMode.UNDEF,
            readLockMode(deserializedView));
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    /**
     * Read the protected {@code lockMode} field from a LogicalView via reflection,
     * since LogicalView does not expose a public getLockMode() accessor.
     */
    private LockMode readLockMode(LogicalView logicalView) throws Exception {
        Field field = LogicalView.class.getDeclaredField("lockMode");
        field.setAccessible(true);
        return (LockMode) field.get(logicalView);
    }

    /**
     * Write the protected {@code lockMode} field on a LogicalView via reflection,
     * since there is no public setter for lockMode.
     */
    private void writeLockMode(LogicalView logicalView, LockMode lockMode) throws Exception {
        Field field = LogicalView.class.getDeclaredField("lockMode");
        field.setAccessible(true);
        field.set(logicalView, lockMode);
    }
}
