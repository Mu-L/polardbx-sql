package com.alibaba.polardbx.planner.ddl;

import org.apache.calcite.sql.SqlAlterTableGroupSplitPartition;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for SqlAlterTableSplitPartition and SqlAlterTableGroupSplitPartition
 * with multi-partition split support.
 */
public class SqlAlterTableSplitPartitionMultiTest {

    private SqlNode id(String name) {
        return new SqlIdentifier(name, SqlParserPos.ZERO);
    }

    @Test
    public void test_multiPartitionNames_constructor() {
        SqlNode partName = id("p1");
        List<SqlNode> multiNames = Arrays.asList(id("p1"), id("p2"), id("p3"));
        SqlNode num = SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO);

        SqlAlterTableSplitPartition split = new SqlAlterTableSplitPartition(
            SqlParserPos.ZERO, partName, multiNames, null, null, null, num, false
        );

        Assert.assertEquals(3, split.getSplitPartitionNames().size());
        Assert.assertEquals("p1", ((SqlIdentifier) split.getSplitPartitionNames().get(0)).getSimple());
        Assert.assertEquals("p2", ((SqlIdentifier) split.getSplitPartitionNames().get(1)).getSimple());
        Assert.assertEquals("p3", ((SqlIdentifier) split.getSplitPartitionNames().get(2)).getSimple());
        Assert.assertFalse(split.isSubPartitionsSplit());
    }

    @Test
    public void test_singlePartitionName_backwardCompat() {
        SqlNode partName = id("p0");

        SqlAlterTableSplitPartition split = new SqlAlterTableSplitPartition(
            SqlParserPos.ZERO, partName, null, null, null, null, false
        );

        Assert.assertEquals(1, split.getSplitPartitionNames().size());
        Assert.assertEquals("p0", ((SqlIdentifier) split.getSplitPartitionNames().get(0)).getSimple());
        Assert.assertEquals("p0", ((SqlIdentifier) split.getSplitPartitionName()).getSimple());
    }

    @Test
    public void test_tableGroupSplitPartition_multiNames() {
        SqlNode partName = id("p1");
        List<SqlNode> multiNames = Arrays.asList(id("p1"), id("p2"));
        SqlNode num = SqlLiteral.createExactNumeric("6", SqlParserPos.ZERO);

        SqlAlterTableGroupSplitPartition split = new SqlAlterTableGroupSplitPartition(
            SqlParserPos.ZERO, partName, multiNames, null, null, null, num, false
        );

        Assert.assertEquals(2, split.getSplitPartitionNames().size());
        Assert.assertNotNull(split.getOperator());
        Assert.assertEquals(SqlKind.SPLIT_PARTITION, split.getOperator().kind);
    }

    @Test
    public void test_fieldsPreservation() {
        SqlNode partName = id("p0");
        SqlNode atValue = SqlLiteral.createExactNumeric("100", SqlParserPos.ZERO);
        SqlNode prefix = id("np");
        SqlNode num = SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO);
        List<SqlPartition> newParts = new ArrayList<>();

        SqlAlterTableSplitPartition split = new SqlAlterTableSplitPartition(
            SqlParserPos.ZERO, partName, atValue, newParts, prefix, num, true
        );

        Assert.assertEquals(atValue, split.getAtValue());
        Assert.assertEquals(prefix, split.getNewPartitionPrefix());
        Assert.assertEquals(num, split.getNewPartitionNum());
        Assert.assertTrue(split.isSubPartitionsSplit());
        Assert.assertSame(newParts, split.getNewPartitions());
    }

    @Test
    public void test_multiPartitionNames_emptyList() {
        SqlNode partName = id("p1");
        List<SqlNode> emptyNames = new ArrayList<>();
        SqlNode num = SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO);

        SqlAlterTableSplitPartition split = new SqlAlterTableSplitPartition(
            SqlParserPos.ZERO, partName, emptyNames, null, null, null, num, false
        );

        // Empty list triggers fallback: constructor adds splitPartitionName to the list
        Assert.assertNotNull(split.getSplitPartitionNames());
        Assert.assertEquals(1, split.getSplitPartitionNames().size());
        Assert.assertEquals("p1", ((SqlIdentifier) split.getSplitPartitionNames().get(0)).getSimple());
    }

    @Test
    public void test_multiPartitionNames_nullList() {
        SqlNode partName = id("p1");
        SqlNode num = SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO);

        // Use single-partition constructor which sets splitPartitionNames to null internally
        SqlAlterTableSplitPartition split = new SqlAlterTableSplitPartition(
            SqlParserPos.ZERO, partName, null, null, null, num, false
        );

        // getSplitPartitionNames() should still return a list with the single partition name
        Assert.assertNotNull(split.getSplitPartitionNames());
        Assert.assertEquals(1, split.getSplitPartitionNames().size());
        Assert.assertEquals("p1", ((SqlIdentifier) split.getSplitPartitionNames().get(0)).getSimple());
    }
}
