package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for AONE#81368978: slow.log output contains newlines when SQL has -- comments.
 * <p>
 * Root cause: MySqlLexer.scanComment() does not increment the line counter
 * when encountering '\n' in '--' style comments. This causes SqlParameterizeUtils
 * to incorrectly set isMultiLine=false for SQL that actually spans multiple lines,
 * bypassing the formatLog() step in LogUtils, and resulting in raw newlines
 * being written to slow.log.
 */
public class SlowLogNewlineTest {

    /**
     * SQL with only a -- comment containing a newline should still be detected as multi-line.
     * Before fix: line counter stays 0, isMultiLine=false, slow.log gets raw newlines.
     * After fix: line counter should be >= 1, isMultiLine=true, formatLog() is called.
     */
    @Test
    public void testMultiLineSqlWithLineComment() {
        String sql = "-- this is a comment\nSELECT 1";

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, new java.util.HashMap<>(), false);

        Assert.assertNotNull("parameterize should not return null", result);
        Assert.assertTrue(
            "SQL with -- comment containing newline should be detected as multi-line, " +
                "but isMultiLine=" + result.getOriginSql().isMultiLine() +
                " (line counter was not incremented in scanComment)",
            result.getOriginSql().isMultiLine()
        );
    }

    /**
     * SQL with multiple -- comments containing newlines.
     */
    @Test
    public void testMultiLineSqlWithMultipleLineComments() {
        String sql = "-- comment1\n-- comment2\nSELECT 1";

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, new java.util.HashMap<>(), false);

        Assert.assertNotNull("parameterize should not return null", result);
        Assert.assertTrue(
            "SQL with multiple -- comments containing newlines should be detected as multi-line, " +
                "but isMultiLine=" + result.getOriginSql().isMultiLine(),
            result.getOriginSql().isMultiLine()
        );
    }

    /**
     * Pure single-line SQL without any newlines should be single-line.
     */
    @Test
    public void testSingleLineSqlWithoutNewlines() {
        String sql = "SELECT 1";

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, new java.util.HashMap<>(), false);

        Assert.assertNotNull("parameterize should not return null", result);
        Assert.assertFalse(
            "Pure single-line SQL without newlines should NOT be detected as multi-line, " +
                "but isMultiLine=" + result.getOriginSql().isMultiLine(),
            result.getOriginSql().isMultiLine()
        );
    }

    /**
     * SQL with \r\n (Windows-style line ending) in -- comment should also be multi-line.
     */
    @Test
    public void testMultiLineSqlWithLineCommentCrLf() {
        String sql = "-- this is a comment\r\nSELECT 1";

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, new java.util.HashMap<>(), false);

        Assert.assertNotNull("parameterize should not return null", result);
        Assert.assertTrue(
            "SQL with -- comment containing \\r\\n should be detected as multi-line, " +
                "but isMultiLine=" + result.getOriginSql().isMultiLine(),
            result.getOriginSql().isMultiLine()
        );
    }

    /**
     * SQL with a practical example similar to the reported issue:
     * User SQL with embedded newlines in -- comments should produce clean slow.log output.
     */
    @Test
    public void testUserSqlWithCommentNewlines() {
        // Simulates the reported issue: ({化学管理-化学品管理-化学品审定单 %!s(bool=true)})
        // DROP TABLE IF EXISTS `ch_chemistry_chemistry_approve`;
        // where the comment text contains actual newline characters
        String sql = "-- ({化学管理-化学品管理-化学品审定单 %!s(bool=true)})\nDROP TABLE IF EXISTS `ch_chemistry_chemistry_approve`";

        SqlParameterized result = SqlParameterizeUtils.parameterize(sql, new java.util.HashMap<>(), false);

        Assert.assertNotNull("parameterize should not return null", result);
        Assert.assertTrue(
            "User SQL with -- comment containing newlines should be detected as multi-line, " +
                "but isMultiLine=" + result.getOriginSql().isMultiLine(),
            result.getOriginSql().isMultiLine()
        );
    }
}
