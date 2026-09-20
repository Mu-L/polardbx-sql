package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.AddDnCclRuleSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.List;
import java.util.Map;

/**
 * Add DN CCL Rule Procedure
 * <p>
 * Syntax: call polardbx.add_dn_ccl_rule('storageInstId', 'sqlType', 'dbName', 'tableName', concurrency, 'keywords')
 *
 * @author liugaoji
 */
public class AddDnCclRuleProcedure extends BaseInnerProcedure {

    protected static final Logger logger = LoggerFactory.getLogger(AddDnCclRuleProcedure.class);

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {

        List<SQLExpr> parameters = statement.getParameters();
        if (parameters.size() != 6) {
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_PARAMS_NOT_MATCH,
                "AddDnCclRuleProcedure expects 6 parameters: storageInstId, sqlType, dbName, tableName, concurrency, keywords");
        }

        // Parse parameters
        String targetStorageInstId = getStringParameter(parameters.get(0), "storageInstId");
        String sqlType = getStringParameter(parameters.get(1), "sqlType");
        long concurrency = getIntegerParameter(parameters.get(4), "concurrency");
        String keywords = getStringParameter(parameters.get(5), "keywords");

        // Use sync action to add DN CCL rule across all CN nodes
        AddDnCclRuleSyncAction syncAction =
            new AddDnCclRuleSyncAction(targetStorageInstId, sqlType, concurrency, keywords);
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            syncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

        boolean overallSuccess = true;
        String generatedSql = "";
        StringBuilder messageBuilder = new StringBuilder();

        // Process results from all nodes
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String computeNode = (String) row.get("COMPUTE_NODE");
                String status = (String) row.get("STATUS");
                String storageInstId = (String) row.get("STORAGE_INST_ID");
                String genSql = (String) row.get("GENERATED_SQL");
                String message = (String) row.get("MESSAGE");

                if (!"SUCCESS".equals(status)) {
                    overallSuccess = false;
                }

                if (genSql != null && !genSql.trim().isEmpty()) {
                    generatedSql = genSql;
                }

                if (computeNode != null && message != null && !message.trim().isEmpty()) {
                    messageBuilder.append("Node ").append(computeNode).append(": ").append(message).append(" ");
                }
            }
        }

        cursor.addColumn("Status", DataTypes.VarcharType);
        cursor.addColumn("StorageInstId", DataTypes.VarcharType);
        cursor.addColumn("GeneratedSQL", DataTypes.VarcharType);

        if (overallSuccess) {
            cursor.addRow(new Object[] {
                "Success",
                targetStorageInstId,
                generatedSql
            });
        } else {
            cursor.addRow(new Object[] {
                "Fail, Please Check Log. Details: " + messageBuilder.toString(),
                targetStorageInstId,
                generatedSql
            });
        }
    }

    /**
     * Extract string parameter from SQLExpr
     */
    private String getStringParameter(SQLExpr expr, String paramName) {
        if (expr instanceof SQLCharExpr) {
            return ((SQLCharExpr) expr).getText();
        }
        throw new TddlRuntimeException(ErrorCode.ERR_CCL,
            "Parameter " + paramName + " must be a string");
    }

    /**
     * Extract integer parameter from SQLExpr
     */
    private long getIntegerParameter(SQLExpr expr, String paramName) {
        if (expr instanceof SQLIntegerExpr) {
            return ((SQLIntegerExpr) expr).getNumber().longValue();
        }
        throw new TddlRuntimeException(ErrorCode.ERR_CCL,
            "Parameter " + paramName + " must be an integer");
    }
}