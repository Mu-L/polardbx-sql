package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.core.DDL;

/**
 * @author wumu
 */
public class LogicalAlterTableGhost extends BaseDdlOperation {

    public LogicalAlterTableGhost(DDL ddl) {
        super(ddl.getCluster(), ddl.getTraitSet(), ddl);
        // use cdc db instead
        this.schemaName = SystemDbHelper.DEFAULT_DB_NAME;
        this.tableName = "ghost";
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "not support archive table for ghost ddl");
    }

    public static LogicalAlterTableGhost create(DDL ddl) {
        return new LogicalAlterTableGhost(ddl);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        return false;
    }
}
