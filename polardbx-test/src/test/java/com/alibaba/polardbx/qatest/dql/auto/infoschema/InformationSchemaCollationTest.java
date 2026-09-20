package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;

public class InformationSchemaCollationTest extends DDLBaseNewDBTestCase {
    private static final String COLLATIONS_QUERY =
        "SELECT * FROM information_schema.COLLATIONS where CHARACTER_SET_NAME='%s'";
    private static final String COLLATION_CHARACTER_SET_APPLICABILITY_QUERY =
        "SELECT * FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY where CHARACTER_SET_NAME='%s'";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testViewCap() {
        String utf8CharsetName = isMySQL80() ? "utf8mb3" : "utf8";
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(COLLATIONS_QUERY, utf8CharsetName), tddlConnection)) {
            Assert.assertFalse(JdbcUtil.getAllResult(rs).isEmpty());
        } catch (Exception e) {
            Assert.fail("Failed to query information_schema.COLLATIONS");
        }

        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format(COLLATION_CHARACTER_SET_APPLICABILITY_QUERY, utf8CharsetName), tddlConnection)) {
            Assert.assertFalse(JdbcUtil.getAllResult(rs).isEmpty());
        } catch (Exception e) {
            Assert.fail("Failed to query information_schema.COLLATION_CHARACTER_SET_APPLICABILITY");
        }
    }
}
