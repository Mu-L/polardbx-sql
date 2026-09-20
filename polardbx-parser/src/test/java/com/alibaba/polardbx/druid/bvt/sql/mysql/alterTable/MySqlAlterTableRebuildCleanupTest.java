package com.alibaba.polardbx.druid.bvt.sql.mysql.alterTable;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.DrdsAlterTableRebuildCleanup;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

public class MySqlAlterTableRebuildCleanupTest extends TestCase {

    public void testRebuildCleanup() {
        String sql = "ALTER TABLE t REBUILD CLEANUP WHERE status = 'deleted'";
        SQLStatement statement = SQLUtils.parseSingleStatement(sql, JdbcConstants.MYSQL);

        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
        DrdsAlterTableRebuildCleanup cleanup = (DrdsAlterTableRebuildCleanup) alterTable.getItems().get(0);

        assertFalse(cleanup.isDryRun());
        assertEquals("status = 'deleted'", cleanup.getCleanupPredicate().toString());
        assertEquals("ALTER TABLE t\n\tREBUILD CLEANUP WHERE status = 'deleted'",
            SQLUtils.toMySqlString(statement));
    }

    public void testRebuildCleanupDryRun() {
        String sql = "ALTER TABLE t REBUILD CLEANUP WHERE status = 'deleted' AND tenant_id = 1 DRY RUN";
        SQLStatement statement = SQLUtils.parseSingleStatement(sql, JdbcConstants.MYSQL);

        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) statement;
        DrdsAlterTableRebuildCleanup cleanup = (DrdsAlterTableRebuildCleanup) alterTable.getItems().get(0);

        assertTrue(cleanup.isDryRun());
        assertEquals("status = 'deleted'\nAND tenant_id = 1", cleanup.getCleanupPredicate().toString());
        assertEquals("ALTER TABLE t\n"
                + "\tREBUILD CLEANUP WHERE status = 'deleted'\n"
                + "\tAND tenant_id = 1 DRY RUN",
            SQLUtils.toMySqlString(statement));
    }
}
