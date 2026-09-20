package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

/**
 * Validates the effective primary index used by the MCE backfill contract.
 */
public final class McePrimaryKeyValidator {

    private McePrimaryKeyValidator() {
    }

    public static IndexMeta requirePrimaryKey(TableMeta tableMeta, String schemaName, String tableName) {
        final IndexMeta primaryIndex = tableMeta == null ? null : tableMeta.getPrimaryIndex();
        if (primaryIndex == null || primaryIndex.getKeyColumns() == null
            || primaryIndex.getKeyColumns().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "[MCE] requires an effective primary key on " + schemaName + "." + tableName);
        }
        return primaryIndex;
    }
}
