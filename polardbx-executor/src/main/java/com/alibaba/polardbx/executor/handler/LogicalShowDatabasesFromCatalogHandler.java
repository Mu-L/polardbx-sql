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
import org.apache.calcite.sql.SqlShowDatabasesFromCatalog;

import java.util.List;
import java.util.regex.Pattern;

public class LogicalShowDatabasesFromCatalogHandler extends HandlerCommon {

    public LogicalShowDatabasesFromCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlShowDatabasesFromCatalog showNode = (SqlShowDatabasesFromCatalog) show.getNativeSqlNode();
        if (showNode.where != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "WHERE is not supported for SHOW DATABASES FROM external catalog");
        }
        Pattern regex = null;
        if (showNode.like instanceof SqlCharStringLiteral) {
            regex = LogicalShowJavaFunctionsHandler.convertLikeToRegex(
                ((SqlCharStringLiteral) showNode.like).getNlsString().getValue());
        }
        String catalogName = showNode.getCatalogName();

        // Privilege check: superUser exempt, ordinary users need any privilege on this catalog
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc != null && pc.getPolarUserInfo() != null
            && !pc.getPolarUserInfo().getAccountType().isSuperUser()) {
            if (!PolarPrivManager.hasAnyPrivOnCatalog(pc.getPolarUserInfo(), catalogName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED,
                    "any on external catalog " + catalogName, pc.getUser(), pc.getHost());
            }
        }

        ArrayResultCursor result = new ArrayResultCursor("DATABASES");
        result.addColumn("Database_in_" + catalogName, DataTypes.StringType);
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
            List<String> databases = metadata.listDatabases();
            for (String db : databases) {
                if (regex != null && !regex.matcher(db).matches()) {
                    continue;
                }
                result.addRow(new Object[] {db});
            }
        }

        return result;
    }
}
