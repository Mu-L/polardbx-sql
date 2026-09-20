package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link ScanWorkId} and {@link ScanWorkId.WorkId}.
 */
public class ScanWorkIdTest {

    // ========== WorkId Constructor ==========

    @Test
    public void testWorkIdConstructorAndGetters() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId(
            "query-123", "schema_db", "table_t1", "file_001.orc", 3, 7);

        Assert.assertEquals("query-123", workId.getQueryId());
        Assert.assertEquals("schema_db", workId.getLogicalSchema());
        Assert.assertEquals("table_t1", workId.getLogicalTable());
        Assert.assertEquals("file_001.orc", workId.getFilePath());
        Assert.assertEquals(3, workId.getStripeId());
        Assert.assertEquals(7, workId.getWorkNumber());
    }

    @Test
    public void testWorkIdSetters() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId(
            "q1", "s1", "t1", "f1", 0, 0);

        workId.setQueryId("q2");
        workId.setLogicalSchema("s2");
        workId.setLogicalTable("t2");
        workId.setFilePath("f2");
        workId.setStripeId(10);
        workId.setWorkNumber(20);

        Assert.assertEquals("q2", workId.getQueryId());
        Assert.assertEquals("s2", workId.getLogicalSchema());
        Assert.assertEquals("t2", workId.getLogicalTable());
        Assert.assertEquals("f2", workId.getFilePath());
        Assert.assertEquals(10, workId.getStripeId());
        Assert.assertEquals(20, workId.getWorkNumber());
    }

    // ========== WorkId.of (parsing) ==========

    @Test
    public void testWorkIdOfValidFormat() {
        // Format: $queryId$prefix_filePath$stripeId$workNumber
        String rawWorkId = "$myQuery$some_prefix_actualFile$5$12";
        ScanWorkId.WorkId workId = ScanWorkId.WorkId.of(rawWorkId, "mySchema", "myTable");

        Assert.assertEquals("myQuery", workId.getQueryId());
        Assert.assertEquals("mySchema", workId.getLogicalSchema());
        Assert.assertEquals("myTable", workId.getLogicalTable());
        // filePath is substring after last '_' in parts[2]
        Assert.assertEquals("actualFile", workId.getFilePath());
        Assert.assertEquals(5, workId.getStripeId());
        Assert.assertEquals(12, workId.getWorkNumber());
    }

    @Test
    public void testWorkIdOfFilePathWithMultipleUnderscores() {
        // parts[2] = "a_b_c_realFile", after last '_' => "realFile"
        String rawWorkId = "$q1$a_b_c_realFile$0$1";
        ScanWorkId.WorkId workId = ScanWorkId.WorkId.of(rawWorkId, "schema", "table");

        Assert.assertEquals("realFile", workId.getFilePath());
    }

    @Test
    public void testWorkIdOfFilePathWithNoUnderscore() {
        // parts[2] = "noUnderscore", lastIndexOf('_') = -1, substring(0) => "noUnderscore"
        String rawWorkId = "$q1$noUnderscore$2$3";
        ScanWorkId.WorkId workId = ScanWorkId.WorkId.of(rawWorkId, "s", "t");

        Assert.assertEquals("noUnderscore", workId.getFilePath());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWorkIdOfInvalidFormatTooFewParts() {
        ScanWorkId.WorkId.of("$only$three$parts", "s", "t");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWorkIdOfInvalidFormatTooManyParts() {
        ScanWorkId.WorkId.of("$a$b$c$d$e$f", "s", "t");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWorkIdOfEmptyString() {
        ScanWorkId.WorkId.of("", "s", "t");
    }

    // ========== WorkId equals / hashCode / toString (Lombok @Data) ==========

    @Test
    public void testWorkIdEqualsAndHashCode() {
        ScanWorkId.WorkId workId1 = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 2);
        ScanWorkId.WorkId workId2 = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 2);
        ScanWorkId.WorkId workId3 = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 99);

        Assert.assertEquals(workId1, workId2);
        Assert.assertEquals(workId1.hashCode(), workId2.hashCode());
        Assert.assertNotEquals(workId1, workId3);
    }

    @Test
    public void testWorkIdToString() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 2);
        String str = workId.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("q"));
        Assert.assertTrue(str.contains("s"));
        Assert.assertTrue(str.contains("f"));
    }

    // ========== ScanWorkId Constructor & of ==========

    @Test
    public void testScanWorkIdConstructorAndGetters() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId("q", "s", "t", "f", 0, 0);
        ScanWorkId scanWorkId = new ScanWorkId(workId, 42);

        Assert.assertSame(workId, scanWorkId.getWorkId());
        Assert.assertEquals(42, scanWorkId.getSequence());
    }

    @Test
    public void testScanWorkIdOf() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId("q", "s", "t", "f", 0, 0);
        ScanWorkId scanWorkId = ScanWorkId.of(workId, 99);

        Assert.assertSame(workId, scanWorkId.getWorkId());
        Assert.assertEquals(99, scanWorkId.getSequence());
    }

    @Test
    public void testScanWorkIdSetters() {
        ScanWorkId.WorkId workId1 = new ScanWorkId.WorkId("q1", "s1", "t1", "f1", 0, 0);
        ScanWorkId.WorkId workId2 = new ScanWorkId.WorkId("q2", "s2", "t2", "f2", 1, 1);
        ScanWorkId scanWorkId = new ScanWorkId(workId1, 0);

        scanWorkId.setWorkId(workId2);
        scanWorkId.setSequence(100);

        Assert.assertSame(workId2, scanWorkId.getWorkId());
        Assert.assertEquals(100, scanWorkId.getSequence());
    }

    // ========== ScanWorkId equals / hashCode / toString (Lombok @Data) ==========

    @Test
    public void testScanWorkIdEqualsAndHashCode() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 2);
        ScanWorkId scanWorkId1 = new ScanWorkId(workId, 10);
        ScanWorkId scanWorkId2 = new ScanWorkId(workId, 10);
        ScanWorkId scanWorkId3 = new ScanWorkId(workId, 20);

        Assert.assertEquals(scanWorkId1, scanWorkId2);
        Assert.assertEquals(scanWorkId1.hashCode(), scanWorkId2.hashCode());
        Assert.assertNotEquals(scanWorkId1, scanWorkId3);
    }

    @Test
    public void testScanWorkIdEqualsWithDifferentWorkId() {
        ScanWorkId.WorkId workId1 = new ScanWorkId.WorkId("q1", "s", "t", "f", 1, 2);
        ScanWorkId.WorkId workId2 = new ScanWorkId.WorkId("q2", "s", "t", "f", 1, 2);
        ScanWorkId scanWorkId1 = new ScanWorkId(workId1, 10);
        ScanWorkId scanWorkId2 = new ScanWorkId(workId2, 10);

        Assert.assertNotEquals(scanWorkId1, scanWorkId2);
    }

    @Test
    public void testScanWorkIdToString() {
        ScanWorkId.WorkId workId = new ScanWorkId.WorkId("q", "s", "t", "f", 1, 2);
        ScanWorkId scanWorkId = new ScanWorkId(workId, 10);
        String str = scanWorkId.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("10"));
    }

    // ========== Edge Cases ==========

    @Test
    public void testWorkIdOfWithZeroStripeAndWork() {
        String rawWorkId = "$q$prefix_file$0$0";
        ScanWorkId.WorkId workId = ScanWorkId.WorkId.of(rawWorkId, "s", "t");

        Assert.assertEquals(0, workId.getStripeId());
        Assert.assertEquals(0, workId.getWorkNumber());
    }

    @Test
    public void testScanWorkIdWithNullWorkId() {
        ScanWorkId scanWorkId = new ScanWorkId(null, 0);
        Assert.assertNull(scanWorkId.getWorkId());
    }
}
