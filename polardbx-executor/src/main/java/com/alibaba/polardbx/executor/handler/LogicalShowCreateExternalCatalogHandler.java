package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateExternalCatalogStatement;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.common.secret.CredentialUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateExternalCatalog;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class LogicalShowCreateExternalCatalogHandler extends HandlerCommon {

    public LogicalShowCreateExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlShowCreateExternalCatalog showNode = (SqlShowCreateExternalCatalog) show.getNativeSqlNode();
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

        ArrayResultCursor result = new ArrayResultCursor("SHOW_CREATE_EXTERNAL_CATALOG");
        result.addColumn("CATALOG_NAME", DataTypes.StringType);
        result.addColumn("CREATE_STATEMENT", DataTypes.StringType);
        result.initMeta();

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get(catalogName);
        if (info == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "External catalog '" + catalogName + "' does not exist");
        }

        MySqlCreateExternalCatalogStatement createStmt = new MySqlCreateExternalCatalogStatement();
        createStmt.setName(new SQLIdentifierExpr(info.getName()));
        if (StringUtils.isNotEmpty(info.getComment())) {
            createStmt.setComment(info.getComment());
        }

        Map<String, String> properties = createStmt.getProperties();
        properties.put("connector", info.getConnector());
        Map<String, String> maskedProps = CredentialUtil.maskAll(info.getProperties());
        if (maskedProps != null) {
            for (Map.Entry<String, String> entry : maskedProps.entrySet()) {
                if (!"connector".equals(entry.getKey())) {
                    properties.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (StringUtils.isNotEmpty(info.getSecretName())) {
            properties.put("secret", info.getSecretName());
        }

        result.addRow(new Object[] {info.getName(), createStmt.toString()});
        return result;
    }
}
