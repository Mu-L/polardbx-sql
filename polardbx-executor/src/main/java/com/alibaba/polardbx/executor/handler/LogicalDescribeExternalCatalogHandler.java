package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
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
import org.apache.calcite.sql.SqlDescribeExternalCatalog;

import java.util.Map;

public class LogicalDescribeExternalCatalogHandler extends HandlerCommon {

    public LogicalDescribeExternalCatalogHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlDescribeExternalCatalog showNode = (SqlDescribeExternalCatalog) show.getNativeSqlNode();
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

        ArrayResultCursor result = new ArrayResultCursor("DESCRIBE_EXTERNAL_CATALOG");
        result.addColumn("PROPERTY", DataTypes.StringType);
        result.addColumn("VALUE", DataTypes.StringType);
        result.initMeta();

        ExternalCatalogInfo info = ExternalCatalogManager.getInstance().get(catalogName);
        if (info == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "External catalog '" + catalogName + "' does not exist");
        }

        result.addRow(new Object[] {"name", info.getName()});
        result.addRow(new Object[] {"connector", info.getConnector()});
        if (info.getSecretName() != null && !info.getSecretName().isEmpty()) {
            result.addRow(new Object[] {"secret", info.getSecretName()});
        }
        if (info.getComment() != null && !info.getComment().isEmpty()) {
            result.addRow(new Object[] {"comment", info.getComment()});
        }
        Map<String, String> props = CredentialUtil.maskAll(info.getProperties());
        if (props != null) {
            for (Map.Entry<String, String> entry : props.entrySet()) {
                result.addRow(new Object[] {entry.getKey(), entry.getValue()});
            }
        }

        return result;
    }
}
