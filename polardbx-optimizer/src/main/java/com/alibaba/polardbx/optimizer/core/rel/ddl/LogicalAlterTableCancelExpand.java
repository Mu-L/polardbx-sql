package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableCancelExpand;

/**
 * Logical plan node for: ALTER TABLE t CANCEL EXPAND
 */
public class LogicalAlterTableCancelExpand extends BaseDdlOperation {

    public LogicalAlterTableCancelExpand(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    @Override
    public boolean isSupportedByCci(ExecutionContext ec) {
        return false;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public static LogicalAlterTableCancelExpand create(DDL ddl) {
        return new LogicalAlterTableCancelExpand(ddl);
    }

    /**
     * Basic validation for CANCEL EXPAND.
     */
    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;
        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableCancelExpand;

        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "CANCEL EXPAND is only supported in auto-mode (new partition) database");
        }
    }
}
