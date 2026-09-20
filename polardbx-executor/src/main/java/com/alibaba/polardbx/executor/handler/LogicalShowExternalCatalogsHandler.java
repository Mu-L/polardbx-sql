package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogInfo;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogManager;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowJavaFunctionsHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShow;

import java.util.regex.Pattern;

public class LogicalShowExternalCatalogsHandler extends HandlerCommon {

    public LogicalShowExternalCatalogsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlShow showNode = (SqlShow) show.getNativeSqlNode();

        String likePattern = null;
        if (showNode.like instanceof SqlCharStringLiteral) {
            likePattern = ((SqlCharStringLiteral) showNode.like).getNlsString().getValue();
        }
        Pattern regex =
            likePattern != null ? LogicalShowJavaFunctionsHandler.convertLikeToRegex(likePattern) : null;

        ArrayResultCursor result = new ArrayResultCursor("EXTERNAL_CATALOGS");
        result.addColumn("CATALOG_NAME", DataTypes.StringType);
        result.addColumn("CONNECTOR", DataTypes.StringType);
        result.addColumn("SECRET", DataTypes.StringType);
        result.addColumn("COMMENT", DataTypes.StringType);
        result.initMeta();

        for (ExternalCatalogInfo info : ExternalCatalogManager.getInstance().listAll()) {
            if (regex != null && !regex.matcher(info.getName()).matches()) {
                continue;
            }
            result.addRow(new Object[] {
                info.getName(),
                info.getConnector(),
                info.getSecretName() != null ? info.getSecretName() : "",
                info.getComment() != null ? info.getComment() : ""
            });
        }

        return result;
    }
}
