package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseCdcTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
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
 * CDC DDL Mark Task for MCE (Modify Column Externalize).
 * <p>
 * Published after the READ_ADDR cutover and its TableSync, while CN still dual-writes the content
 * and addr columns. It must precede the ADDR_ONLY cutover: after CDC consumes this marker, row
 * rebuild switches from the plaintext content column to the addr carrier and staging hydration.
 * <p>
 * Purpose:
 * <ol>
 *   <li>Set USE_OMC=true so CDC's ConsistencyChecker accepts the temporary logical/physical
 *       difference while both content and addr columns exist.</li>
 *   <li>Set REFRESH_CREATE_SQL_4_PHY_TABLE=true and exclude the departing content column so CDC records the terminal
 *       address-only physical schema rather than the temporary dual-column layout.</li>
 * </ol>
 * <p>
 * The DDL SQL passed to CDC remains a physical {@code CHANGE COLUMN body body_addr_ varchar(128)} marker. The ext
 * payload marks this as an external-column DDL and carries the user DDL as canonical originalDdl, so CDC applies and
 * emits the logical EXTERNALIZE schema while retaining terminal createSql4PhyTable for physical metadata.
 */
@TaskName(name = "CdcMceExternalizeDdlMarkTask")
@Getter
@Setter
public class CdcMceExternalizeDdlMarkTask extends BaseCdcTask {

    private final String logicalTableName;
    private final String columnName;
    private final String originalType;

    @JSONCreator
    public CdcMceExternalizeDdlMarkTask(String schemaName, String logicalTableName, String columnName,
                                        String originalType) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.columnName = columnName;
        this.originalType = originalType != null ? originalType : "LONGTEXT";
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        DdlContext ddlContext = executionContext.getDdlContext();

        CdcMarkUtil.useExternalColumnDdl(executionContext);
        Map<String, Object> param = buildExtendParameter(executionContext);
        // USE_OMC → ConsistencyChecker.isAlterWithOMC() returns true → skip check
        param.put(USE_OMC, true);
        // The physical table is still dual-column here. Persist the terminal address-only schema so a later DDL that
        // omits createSql4PhyTable cannot resurrect the departing content column from distinctPhyMeta.
        param.put(REFRESH_CREATE_SQL_4_PHY_TABLE, "true");
        param.put(MCE_CREATE_SQL_EXCLUDE_COLUMN, columnName);

        String addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        String storedComment = loadStoredComment(metaDbConnection, addrColumnName);
        String ddlSql = String.format(
            "ALTER TABLE `%s` CHANGE COLUMN `%s` `%s` varchar(%d) COMMENT '%s'",
            logicalTableName,
            columnName,
            addrColumnName,
            ExternalizedColumnInfo.ADDR_VARCHAR_LENGTH,
            storedComment);

        CdcManagerHelper.getInstance()
            .notifyDdlNew(schemaName, logicalTableName, SqlKind.ALTER_TABLE.name(),
                ddlSql, ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                CdcDdlMarkVisibility.Public, param);
    }

    private String loadStoredComment(Connection metaDbConnection, String addrColumnName) {
        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        List<ColumnsRecord> columns = tableInfoManager.queryOneColumn(schemaName, logicalTableName, addrColumnName);
        if (columns.size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "Expected exactly one terminal MCE addr column '%s' in MetaDB for table %s.%s, found %d",
                addrColumnName, schemaName, logicalTableName, columns.size()));
        }
        String storedComment = columns.get(0).columnComment;
        String storedType = ExternalizedColumnInfo.extractOriginalType(storedComment);
        if (storedType == null || !storedType.equalsIgnoreCase(originalType)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "MCE addr column '%s' comment type mismatch for table %s.%s: expected %s, found %s",
                addrColumnName, schemaName, logicalTableName, originalType, storedType));
        }
        String userComment = ExternalizedColumnInfo.extractUserComment(storedComment);
        if (!storedComment.equals(ExternalizedColumnInfo.buildComment(storedType, userComment))) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "MCE addr column '%s' has malformed externalized comment for table %s.%s",
                addrColumnName, schemaName, logicalTableName));
        }
        LimitValidator.validateColumnComment(addrColumnName, storedComment);
        return storedComment;
    }
}
