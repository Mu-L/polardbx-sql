package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDescribeExternalTable;

import java.util.Optional;

public class LogicalDescribeExternalTableHandler extends HandlerCommon {

    public LogicalDescribeExternalTableHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlDescribeExternalTable descNode = (SqlDescribeExternalTable) show.getNativeSqlNode();
        String catalogName = descNode.getCatalogName();
        String dbName = descNode.getExternalDbName();
        String tableName = descNode.getExternalTableName();

        // Privilege check
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc != null && pc.getPolarUserInfo() != null
            && !pc.getPolarUserInfo().getAccountType().isSuperUser()) {
            if (!PolarPrivManager.hasAnyExternalPrivilege(
                pc.getPolarUserInfo(), catalogName, dbName, tableName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_TABLE,
                    "DESCRIBE", tableName, pc.getUser(), pc.getHost(),
                    catalogName + "." + dbName);
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("DESCRIBE");
        result.addColumn("Field", DataTypes.StringType);
        result.addColumn("Type", DataTypes.StringType);
        result.addColumn("Null", DataTypes.StringType);
        result.addColumn("Key", DataTypes.StringType);
        result.addColumn("Default", DataTypes.StringType);
        result.addColumn("Extra", DataTypes.StringType);
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
            Optional<ConnectorTable> tableOpt = metadata.getTable(dbName.toLowerCase(), tableName.toLowerCase());
            if (!tableOpt.isPresent()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "Table '" + catalogName + "." + dbName + "." + tableName + "' does not exist");
            }
            ConnectorTable table = tableOpt.get();
            for (ColumnMeta col : table.columns) {
                String defaultVal = col.getField().getDefault();
                result.addRow(new Object[] {
                    col.getName(),
                    col.getDataType().getStringSqlType(),
                    col.isNullable() ? "YES" : "NO",
                    "",
                    defaultVal != null ? defaultVal : "NULL",
                    ""
                });
            }
        }

        return result;
    }
}
