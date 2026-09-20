package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import lombok.Data;

import java.sql.Connection;

/**
 * @author wumu
 */
@Data
public class CheckTableMetaVersionSyncAction implements ISyncAction {
    private String tableName;
    private String schemaName;

    public CheckTableMetaVersionSyncAction(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public ResultCursor sync() {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(tableName);
        if (tableMeta == null) {
            return null;
        }
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            long version = TableInfoManager.checkTableVersion(schemaName, tableName, metaDbConn);
            long tableMetaVersion = tableMeta.getVersion();
            if (version != tableMetaVersion) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_TABLE_META_VERSION,
                    String.format("the table meta version[%s] is not equal to the version[%s] in tables for [%s.%s]",
                        tableMetaVersion, version, schemaName, tableName));
            }
        } catch (Exception e) {
            // Change context:
            // - Before: every exception (including the precise version-mismatch TddlRuntimeException
            //   thrown above) was re-wrapped into "failed to check tableMeta version", hiding the
            //   in-memory vs MetaDB version comparison from clients and DDL error logs.
            // - Path impact: only the error message surfaced on version conflicts changes; the error
            //   code (TDDL-4669) and the check semantics are unchanged. Non-TddlRuntimeException
            //   failures (MetaDB connect/query errors) keep the original wrapping behavior.
            // - Capability regression: None; no control flow, compatibility or performance change, the
            //   rethrown exception carries the same error code and a more specific message.
            if (e instanceof TddlRuntimeException) {
                throw (TddlRuntimeException) e;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_TABLE_META_VERSION, e,
                "failed to check tableMeta version");
        }

        return null;
    }
}
