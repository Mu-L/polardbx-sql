package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.rel.ddl.AlterTableRemoveAutoPartition;
import org.apache.calcite.sql.SqlAlterTableRemoveAutoPartition;

/**
 * @author wumu
 */
public class LogicalAlterTableRemoveAutoPartition extends LogicalTableOperation {

    private SqlAlterTableRemoveAutoPartition sqlAlterTableRemoveAutoPartition;

    public LogicalAlterTableRemoveAutoPartition(AlterTableRemoveAutoPartition alterTableRemoveAutoPartition) {
        super(alterTableRemoveAutoPartition);
        this.sqlAlterTableRemoveAutoPartition = (SqlAlterTableRemoveAutoPartition) relDdl.sqlNode;
    }

    public static LogicalAlterTableRemoveAutoPartition create(
        AlterTableRemoveAutoPartition alterTableRemoveAutoPartition) {
        return new LogicalAlterTableRemoveAutoPartition(alterTableRemoveAutoPartition);
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public SqlAlterTableRemoveAutoPartition getSqlAlterTableRemoveAutoPartition() {
        return sqlAlterTableRemoveAutoPartition;
    }
}
