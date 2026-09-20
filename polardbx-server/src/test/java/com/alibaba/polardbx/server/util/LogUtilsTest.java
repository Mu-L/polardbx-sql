package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class LogUtilsTest {

    @Test
    public void testMaskSecretPassword() {
        String sql =
            "CREATE SECRET IF NOT EXISTS iso_test_secret WITH ('type' = 'jdbc', 'user' = 'diamond', 'password' = 'diamond1qaz@2wsx')";
        Assert.assertEquals("CREATE SECRET IF NOT EXISTS iso_test_secret <masked>",
            LogUtils.maskSecretPassword(sql));
    }

    @Test
    public void testLongCreateSecretSqlIsMaskedAfterTruncation() {
        String rawPassword = "plain_password_must_not_be_logged";
        StringBuilder sql = new StringBuilder("CREATE SECRET long_secret WITH ('type'='jdbc','password'='")
            .append(rawPassword)
            .append("','padding'='");
        for (int i = 0; i < 5000; i++) {
            sql.append('x');
        }
        sql.append("')");

        String recordedSql = LogUtils.truncateAndMaskSql(ByteString.from(sql.toString()));

        Assert.assertEquals("CREATE SECRET long_secret <masked>", recordedSql);
        Assert.assertFalse(recordedSql.contains(rawPassword));
    }

    @Test
    public void testMaskSecretPasswordWithSecretKey() {
        String sql = "CREATE SECRET oss_prod WITH ('type'='oss', 'access_key'='AK123', 'secret_key'='SK456')";
        Assert.assertEquals("CREATE SECRET oss_prod <masked>", LogUtils.maskSecretPassword(sql));
    }

    @Test
    public void testMaskSecretPasswordWithUnquotedKey() {
        String sql = "CREATE SECRET s WITH (password = 'pwd')";
        Assert.assertEquals("CREATE SECRET s <masked>", LogUtils.maskSecretPassword(sql));
    }

    @Test
    public void testMaskSecretPasswordNoSecret() {
        String sql = "CREATE SECRET s WITH ('type' = 'jdbc')";
        Assert.assertEquals("CREATE SECRET s <masked>", LogUtils.maskSecretPassword(sql));
    }

    @Test
    public void testMaskSecretPasswordSkipNonCreateSecret() {
        String sql = "INSERT INTO t (password) VALUES ('should_not_be_masked')";
        Assert.assertEquals(sql, LogUtils.maskSecretPassword(sql));
    }

    @Test
    public void testMaskSecretPasswordWithNull() {
        Assert.assertNull(LogUtils.maskSecretPassword(null));
    }

    @Test
    public void testMaskSecretPasswordWithEmpty() {
        Assert.assertEquals("", LogUtils.maskSecretPassword(""));
    }

    // ---- audit integrity: an ordinary statement must reach the log verbatim ----

    @Test
    public void testSelectMentioningSecretDdlIsRecordedVerbatim() {
        String sql = "SELECT * FROM audit WHERE note='alter secret abc' AND card='1234'";
        Assert.assertEquals(sql, LogUtils.truncateAndMaskSql(ByteString.from(sql)));
    }

    @Test
    public void testShowCreateSecretIsRecordedVerbatim() {
        String sql = "SHOW CREATE SECRET iso_test_secret";
        Assert.assertEquals(sql, LogUtils.truncateAndMaskSql(ByteString.from(sql)));
    }

    @Test
    public void testCreateTableWithSecretDdlInCommentIsRecordedVerbatim() {
        String sql = "CREATE TABLE t (c VARCHAR(10) COMMENT 'alter secret rotation policy')";
        Assert.assertEquals(sql, LogUtils.truncateAndMaskSql(ByteString.from(sql)));
    }
}
