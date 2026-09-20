package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.cdc.ICdcManager.REFRESH_CREATE_SQL_4_PHY_TABLE;
import static com.alibaba.polardbx.common.cdc.ICdcManager.MCE_CREATE_SQL_EXCLUDE_COLUMN;
import static com.alibaba.polardbx.common.cdc.ICdcManager.USE_OMC;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * CDC DDL Mark Task for MCE internalize (externalized column back to a plain column) — the reverse
 * of {@link CdcMceExternalizeDdlMarkTask}.
 * <p>
 * Published after the read cutover to plaintext and its TableSync, while CN still dual-writes the
 * content and addr columns. It must precede the content-only cutover: after CDC consumes this
 * marker, row rebuild switches from the addr carrier back to the plaintext content column. The DDL
 * SQL sent downstream is the physical form {@code CHANGE COLUMN body_addr_ body <ORIGINAL_TYPE>};
 * createSql4PhyTable is projected to the terminal content-only schema even though the physical table temporarily
 * contains both columns until addr cleanup completes.
 */
@TaskName(name = "CdcMceInternalizeDdlMarkTask")
@Getter
@Setter
public class CdcMceInternalizeDdlMarkTask extends BaseCdcTask {

    private final String logicalTableName;
    private final String columnName;
    private final String originalType;

    @JSONCreator
    public CdcMceInternalizeDdlMarkTask(String schemaName, String logicalTableName, String columnName,
                                        String originalType) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.columnName = columnName;
        this.originalType = originalType;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();

        CdcMarkUtil.useExternalColumnDdl(executionContext);
        Map<String, Object> param = buildExtendParameter(executionContext);
        // USE_OMC → ConsistencyChecker.isAlterWithOMC() returns true → skip check while the
        // physical table temporarily contains both content and addr columns.
        param.put(USE_OMC, true);
        param.put(REFRESH_CREATE_SQL_4_PHY_TABLE, "true");

        String addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        // Persist the terminal content-only schema. The addr carrier is dropped after this marker, and keeping it in
        // distinctPhyMeta would make a later DDL without createSql4PhyTable fail CDC consistency checking.
        param.put(MCE_CREATE_SQL_EXCLUDE_COLUMN, addrColumnName);
        String restoredComment = loadRestoredComment(metaDbConnection);
        boolean isTextFamily = !originalType.toUpperCase().endsWith("BLOB");
        StringBuilder ddlSql = new StringBuilder();
        ddlSql.append(String.format("ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` %s",
            logicalTableName, addrColumnName, columnName, originalType));
        if (isTextFamily) {
            ddlSql.append(" CHARACTER SET utf8mb4");
        }
        ddlSql.append(" NULL");
        if (TStringUtil.isNotEmpty(restoredComment)) {
            ddlSql.append(" COMMENT '").append(restoredComment.replace("\\", "\\\\").replace("'", "\\'"))
                .append('\'');
        }

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.ALTER_TABLE.name(),
                ddlSql.toString(), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public, param);
    }

    /**
     * The restored content record carries the user's original comment (pulled from the DN when the
     * column was registered); NULL/empty means the source column had none.
     */
    private String loadRestoredComment(Connection metaDbConnection) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        List<ColumnsRecord> columns = tableInfoManager.queryOneColumn(schemaName, logicalTableName, columnName);
        if (columns.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "Expected exactly one restored content column '%s' in MetaDB for table %s.%s, found %d",
                columnName, schemaName, logicalTableName, columns.size()));
        }
        return columns.get(0).columnComment;
    }
}
