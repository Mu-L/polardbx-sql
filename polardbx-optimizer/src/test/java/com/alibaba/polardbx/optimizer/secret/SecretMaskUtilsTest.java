package com.alibaba.polardbx.optimizer.secret;

import org.junit.Assert;
import org.junit.Test;

public class SecretMaskUtilsTest {

    // ---- secret DDL: the credential payload is replaced, the head is kept ----

    @Test
    public void mask_createSecretWithWithClause_masksPayload() {
        String sql = "CREATE SECRET s WITH ('type'='jdbc', 'password'='secret123')";
        Assert.assertEquals("CREATE SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_alterSecretWithSetClause_masksPayload() {
        String sql = "ALTER SECRET s SET ('password'='newpwd')";
        Assert.assertEquals("ALTER SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_createSecretIfNotExists_masksPayload() {
        String sql = "CREATE SECRET IF NOT EXISTS s WITH ('password'='pwd')";
        Assert.assertEquals("CREATE SECRET IF NOT EXISTS s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_lowercaseStatement_masksPayload() {
        String sql = "create secret s with (password='pwd')";
        Assert.assertEquals("create secret s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_backtickQuotedSecretName_masksPayload() {
        String sql = "CREATE SECRET `my secret` WITH ('password'='p')";
        Assert.assertEquals("CREATE SECRET `my secret` <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_hyphenatedSecretName_masksPayload() {
        String sql = "CREATE SECRET my-secret WITH ('password'='p')";
        Assert.assertEquals("CREATE SECRET my-secret <masked>", SecretMaskUtils.mask(sql));
    }

    // ---- the original prefix must survive so that audit records stay faithful ----

    @Test
    public void mask_leadingWhitespace_keepsWhitespace() {
        String sql = "  \n CREATE SECRET s WITH (password='pwd')";
        Assert.assertEquals("  \n CREATE SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_blockCommentPrefix_keepsComment() {
        String sql = "/*trace*/ CREATE SECRET s WITH ('password'='pwd')";
        Assert.assertEquals("/*trace*/ CREATE SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_hintPrefix_keepsHint() {
        String sql = "/*+TDDL:cmd_extra(X=1)*/ CREATE SECRET s WITH ('password'='pwd')";
        Assert.assertEquals("/*+TDDL:cmd_extra(X=1)*/ CREATE SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_lineCommentPrefix_keepsComment() {
        String sql = "-- audit\nCREATE SECRET s WITH ('password'='pwd')";
        Assert.assertEquals("-- audit\nCREATE SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    // ---- nothing sensitive to hide ----

    @Test
    public void mask_createSecretNoWithClause_unchanged() {
        String sql = "CREATE SECRET s";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_createSecretTrailingSemicolon_unchanged() {
        String sql = "CREATE SECRET s;";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_null_returnsNull() {
        Assert.assertNull(SecretMaskUtils.mask(null));
    }

    @Test
    public void mask_empty_returnsEmpty() {
        Assert.assertEquals("", SecretMaskUtils.mask(""));
    }

    @Test
    public void mask_unterminatedBlockComment_unchanged() {
        String sql = "/* create secret s";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    // ---- H4: a secret DDL mentioned anywhere but the statement head is not a match ----

    @Test
    public void mask_showCreateSecret_unchanged() {
        String sql = "SHOW CREATE SECRET s1";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_selectWithSecretDdlInLiteral_unchanged() {
        String sql = "SELECT * FROM audit WHERE note='alter secret abc' AND card='1234'";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_selectWithSecretDdlInMemo_unchanged() {
        String sql = "SELECT id FROM t WHERE memo='please create secret rotation ticket' AND amt=100";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_createTableWithSecretDdlInColumnComment_unchanged() {
        String sql = "CREATE TABLE t (c VARCHAR(10) COMMENT 'alter secret rotation policy')";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_selectWithSecretDdlInBlockComment_unchanged() {
        String sql = "SELECT * FROM t /* create secret placeholder */ WHERE id=1";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_updateWithSecretDdlInLiteral_unchanged() {
        String sql = "UPDATE runbook SET step='alter secret prod_oss then reload' WHERE id=7";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_dropSecret_unchanged() {
        String sql = "DROP SECRET s";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_showSecrets_unchanged() {
        String sql = "SHOW SECRETS";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    // ---- SHOW PROCESSLIST shapes ----

    @Test
    public void mask_procedureInfoWrappingSecretText_keepsProcedureName() {
        // ShowProcesslistSyncAction.addProcedureInfo builds this shape and the Info column
        // is then masked again at ShowProcesslistSyncAction:265.
        String sql = "CALL myproc: create secret rotation";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_truncatedShowCreateSecret_unchanged() {
        // Non-FULL SHOW PROCESSLIST cuts Info to 30 characters before masking.
        String sql = "SHOW CREATE SECRET my_secret_n";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    // ---- fast-fail scan: CREATE/ALTER followed by SECRET ----

    @Test
    public void mask_createSecretWithExtraSpacesBetweenKeywords_masksPayload() {
        String sql = "CREATE   SECRET s WITH ('password'='pwd')";
        Assert.assertEquals("CREATE   SECRET s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_createSecretWithNewlineBetweenKeywords_masksPayload() {
        String sql = "create\nsecret s with ('password'='pwd')";
        Assert.assertEquals("create\nsecret s <masked>", SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_alterTableMentioningSecret_unchanged() {
        String sql = "ALTER TABLE t COMMENT 'create secret here'";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_createxPrefixWord_unchanged() {
        String sql = "CREATEX SECRET s WITH ('password'='pwd')";
        Assert.assertEquals(sql, SecretMaskUtils.mask(sql));
    }

    @Test
    public void mask_alterSecretWithCreateInsidePayload_masksPayload() {
        // "create" appears only inside the credential payload, after "secret";
        // the fast fail must still let this statement through to be masked.
        String sql = "ALTER SECRET s SET ('password'='xx create yy')";
        Assert.assertEquals("ALTER SECRET s <masked>", SecretMaskUtils.mask(sql));
    }
}
