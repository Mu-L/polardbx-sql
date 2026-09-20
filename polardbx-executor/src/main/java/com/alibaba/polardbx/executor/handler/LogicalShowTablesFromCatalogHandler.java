package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowJavaFunctionsHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShowTablesFromCatalog;

import java.util.List;
import java.util.regex.Pattern;

public class LogicalShowTablesFromCatalogHandler extends HandlerCommon {

    public LogicalShowTablesFromCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlShowTablesFromCatalog showNode = (SqlShowTablesFromCatalog) show.getNativeSqlNode();
        if (showNode.where != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "WHERE is not supported for SHOW TABLES FROM external catalog");
        }
        Pattern regex = null;
        if (showNode.like instanceof SqlCharStringLiteral) {
            regex = LogicalShowJavaFunctionsHandler.convertLikeToRegex(
                ((SqlCharStringLiteral) showNode.like).getNlsString().getValue());
        }
        String catalogName = showNode.getCatalogName();
        String dbName = showNode.getExternalDbName();

        // Privilege check
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc != null && pc.getPolarUserInfo() != null
            && !pc.getPolarUserInfo().getAccountType().isSuperUser()) {
            if (!PolarPrivManager.hasAnyExternalPrivilege(
                pc.getPolarUserInfo(), catalogName, dbName, null)) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_DB,
                    pc.getUser(), pc.getHost(), catalogName + "." + dbName);
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("TABLES");
        boolean isFull = showNode.isFull();
        result.addColumn(SqlShowTablesFromCatalog.tablesColumnName(catalogName, dbName), DataTypes.StringType);
        if (isFull) {
            result.addColumn(SqlShowTablesFromCatalog.TABLE_TYPE_COLUMN, DataTypes.StringType);
            result.addColumn(SqlShowTablesFromCatalog.AUTO_PARTITION_COLUMN, DataTypes.StringType);
            result.addColumn(SqlShowTablesFromCatalog.TABLE_GROUP_COLUMN, DataTypes.StringType);
        }
        result.initMeta();

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get(catalogName);
        if (info == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "External catalog '" + catalogName + "' does not exist");
        }

        SecretBundle secret = SecretBundle.EMPTY;
        if (info.getSecretName() != null && !info.getSecretName().isEmpty()) {
            secret = SecretManager.getInstance().resolve(info.getSecretName(), info.getProperties());
        }

        try (ConnectorMetadata metadata = ConnectorRegistry.getInstance()
            .get(info.getConnector()).createMetadata(info.getProperties(), secret)) {
            List<String> tables = metadata.listTables(dbName.toLowerCase());
            for (String table : tables) {
                if (regex != null && !regex.matcher(table).matches()) {
                    continue;
                }
                // listTables() carries no table kind, and every external object is
                // exposed as a table. Auto_partition and Table_group reuse the
                // placeholders LogicalShowTablesMyHandler writes for foreign rows.
                result.addRow(isFull
                    ? new Object[] {table, "BASE TABLE", "NO", "-"}
                    : new Object[] {table});
            }
        }

        return result;
    }
}
