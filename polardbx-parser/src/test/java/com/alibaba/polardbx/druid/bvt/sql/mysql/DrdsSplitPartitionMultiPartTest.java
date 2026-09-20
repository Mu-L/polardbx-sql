package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsSplitPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableGroupStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import org.junit.Assert;

import java.util.List;

/**
 * Unit tests for multi-partition split parsing.
 * Covers: basic multi-partition split, output visitor round-trip, AT clause rejection,
 * explicit definitions rejection, backward compatibility, tablegroup syntax, many partitions,
 * backtick names, prefix syntax, and subpartition split.
 */
public class DrdsSplitPartitionMultiPartTest extends MysqlTest {

    private static String normalizeWs(String s) {
        return s.toUpperCase().replaceAll("\\s+", " ").trim();
    }

    /**
     * Basic multi-partition split: ALTER TABLE t SPLIT PARTITION p1, p2, p3 INTO PARTITIONS 4
     */
    public void test_multiSplit_basic() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2, p3 INTO PARTITIONS 4";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        Assert.assertEquals(1, stmtList.size());

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        Assert.assertEquals(1, stmt.getItems().size());

        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(3, item.getSplitPartitionNames().size());
        Assert.assertEquals("p1", item.getSplitPartitionNames().get(0).getSimpleName());
        Assert.assertEquals("p2", item.getSplitPartitionNames().get(1).getSimpleName());
        Assert.assertEquals("p3", item.getSplitPartitionNames().get(2).getSimpleName());
        Assert.assertEquals(4, item.getNewPartitionNum().getNumber().intValue());
        Assert.assertFalse(item.isSubPartitionsSplit());
    }

    /**
     * Output visitor round-trip: parse SQL and serialize back, should produce equivalent result.
     */
    public void test_multiSplit_roundTrip() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2, p3 INTO PARTITIONS 4";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals(normalizeWs(sql), normalizeWs(result.toString()));
    }

    /**
     * AT clause is rejected for multi-partition split.
     */
    public void test_multiSplit_atClauseRejected() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2 AT(100) INTO (PARTITION p10, PARTITION p11)";
        try {
            MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
            parser.parseStatementList();
            fail("Expected ParserException for AT clause with multi-partition split");
        } catch (ParserException e) {
            assertTrue(e.getMessage().contains("AT clause is not supported with multi-partition split"));
        }
    }

    /**
     * Explicit partition definitions are rejected for multi-partition split.
     */
    public void test_multiSplit_explicitDefsRejected() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2 INTO (PARTITION p10, PARTITION p11)";
        try {
            MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
            parser.parseStatementList();
            fail("Expected ParserException for explicit partition definitions with multi-partition split");
        } catch (ParserException e) {
            assertTrue(e.getMessage().contains("Explicit partition definitions are not supported"));
        }
    }

    /**
     * Single-partition split backward compatibility: existing syntax still works.
     */
    public void test_singleSplit_backwardCompat() {
        String sql = "ALTER TABLE t SPLIT PARTITION p0 AT(15) INTO (PARTITION p10, PARTITION p11)";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        Assert.assertEquals(1, stmtList.size());

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(1, item.getSplitPartitionNames().size());
        Assert.assertEquals("p0", item.getSplitPartitionName().getSimpleName());
        Assert.assertNotNull(item.getAtValue());
        Assert.assertEquals(2, item.getPartitions().size());
    }

    /**
     * Single-partition with INTO PARTITIONS N backward compatibility.
     */
    public void test_singleSplit_intoPartitions_backwardCompat() {
        String sql = "ALTER TABLE t SPLIT PARTITION p0 INTO PARTITIONS 3";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(1, item.getSplitPartitionNames().size());
        Assert.assertEquals(3, item.getNewPartitionNum().getNumber().intValue());
        Assert.assertNull(item.getAtValue());

        // round-trip (normalize whitespace since ALTER TABLE toString adds newline+tab)
        Assert.assertEquals(normalizeWs(sql), normalizeWs(stmtList.get(0).toString()));
    }

    /**
     * TABLEGROUP multi-partition split syntax.
     */
    public void test_multiSplit_tableGroup() {
        String sql = "ALTER TABLEGROUP tg1 SPLIT PARTITION p1, p2 INTO PARTITIONS 6";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        Assert.assertEquals(1, stmtList.size());

        SQLAlterTableGroupStatement stmt = (SQLAlterTableGroupStatement) stmtList.get(0);
        Assert.assertNotNull(stmt.getItem());

        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItem();
        Assert.assertEquals(2, item.getSplitPartitionNames().size());
        Assert.assertEquals("p1", item.getSplitPartitionNames().get(0).getSimpleName());
        Assert.assertEquals("p2", item.getSplitPartitionNames().get(1).getSimpleName());
        Assert.assertEquals(6, item.getNewPartitionNum().getNumber().intValue());

        // round-trip
        Assert.assertEquals(normalizeWs(sql), normalizeWs(stmtList.get(0).toString()));
    }

    /**
     * Many partitions (5 partitions) to verify comma parsing loop.
     */
    public void test_multiSplit_manyPartitions() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2, p3, p4, p5 INTO PARTITIONS 2";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(5, item.getSplitPartitionNames().size());
        Assert.assertEquals("p1", item.getSplitPartitionNames().get(0).getSimpleName());
        Assert.assertEquals("p5", item.getSplitPartitionNames().get(4).getSimpleName());
        Assert.assertEquals(2, item.getNewPartitionNum().getNumber().intValue());

        // round-trip
        Assert.assertEquals(normalizeWs(sql), normalizeWs(stmtList.get(0).toString()));
    }

    /**
     * Backtick-quoted partition names.
     */
    public void test_multiSplit_backtickNames() {
        String sql = "ALTER TABLE t SPLIT PARTITION `p-1`, `p-2` INTO PARTITIONS 3";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(2, item.getSplitPartitionNames().size());
        // getSimpleName() retains backticks for names with special characters
        assertTrue(item.getSplitPartitionNames().get(0).toString().contains("p-1"));
        assertTrue(item.getSplitPartitionNames().get(1).toString().contains("p-2"));
    }

    /**
     * Multi-partition split with partition name prefix.
     */
    public void test_multiSplit_withPrefix() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2, p3 INTO np PARTITIONS 4";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertEquals(3, item.getSplitPartitionNames().size());
        Assert.assertEquals("np", item.getNewPartitionNamePrefix().getSimpleName());
        Assert.assertEquals(4, item.getNewPartitionNum().getNumber().intValue());

        // round-trip
        Assert.assertEquals(normalizeWs(sql), normalizeWs(stmtList.get(0).toString()));
    }

    /**
     * Multi-partition SUBPARTITION split syntax.
     */
    public void test_multiSplit_subPartition() {
        String sql = "ALTER TABLE t SPLIT SUBPARTITION sp1, sp2 INTO SUBPARTITIONS 3";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        Assert.assertTrue(item.isSubPartitionsSplit());
        Assert.assertEquals(2, item.getSplitPartitionNames().size());
        Assert.assertEquals("sp1", item.getSplitPartitionNames().get(0).getSimpleName());
        Assert.assertEquals("sp2", item.getSplitPartitionNames().get(1).getSimpleName());
        Assert.assertEquals(3, item.getNewPartitionNum().getNumber().intValue());

        // round-trip
        Assert.assertEquals(normalizeWs(sql), normalizeWs(stmtList.get(0).toString()));
    }

    /**
     * Parser accepts INTO PARTITIONS 0 — validated later at executor level.
     */
    public void test_multiSplit_zeroPartitionCount_parsesSuccessfully() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2 INTO PARTITIONS 0";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        SQLAlterTableStatement stmt = (SQLAlterTableStatement) stmtList.get(0);
        DrdsSplitPartition item = (DrdsSplitPartition) stmt.getItems().get(0);
        // Parser stores the value; executor validates it
        Assert.assertEquals(0, item.getNewPartitionNum().getNumber().intValue());
    }

    /**
     * Parser accepts INTO PARTITIONS -1 — validated later at executor level.
     * The parser may interpret "-1" as a negative literal.
     */
    public void test_multiSplit_negativePartitionCount_parseBehavior() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2 INTO PARTITIONS -1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();
        // Parser accepts it (negative number); executor is responsible for validation
        Assert.assertNotNull(stmtList);
        Assert.assertFalse(stmtList.isEmpty());
    }

    /**
     * Negative: trailing comma in partition list should be rejected or handled.
     */
    public void test_multiSplit_trailingComma() {
        String sql = "ALTER TABLE t SPLIT PARTITION p1, p2, INTO PARTITIONS 4";
        try {
            MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql), SQLParserFeature.DrdsMisc);
            parser.parseStatementList();
            Assert.fail("Expected exception for trailing comma in partition list");
        } catch (Exception e) {
            // expected: parser should reject trailing comma
        }
    }
}
