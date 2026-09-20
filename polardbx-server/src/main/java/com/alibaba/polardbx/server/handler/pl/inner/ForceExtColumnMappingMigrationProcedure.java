package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

/**
 * LEGACY COMPATIBILITY PATH: synchronously re-run the idempotent legacy external-column mapping migration on this CN.
 * Normal online external-column users are not expected to require this recovery procedure.
 *
 * <p>Usage: {@code CALL polardbx.force_ext_column_mapping_migration()}
 */
public class ForceExtColumnMappingMigrationProcedure extends BaseInnerProcedure {

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("MESSAGE", DataTypes.StringType);

        if (!statement.getParameters().isEmpty()) {
            throw new IllegalArgumentException("force_ext_column_mapping_migration does not accept parameters");
        }

        try {
            ExtColumnMappingManager.getInstance().forceMigrateLegacyMappings();
            cursor.addRow(new Object[] {"OK", "External column mapping migration completed"});
        } catch (Throwable t) {
            throw new RuntimeException("force_ext_column_mapping_migration failed", t);
        }
    }
}
