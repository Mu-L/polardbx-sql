package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowJavaFunctionsHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShow;

import java.util.regex.Pattern;

public class LogicalShowSecretsHandler extends HandlerCommon {

    public LogicalShowSecretsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShow showNode = (SqlShow) ((LogicalShow) logicalPlan).getNativeSqlNode();
        String likePattern = null;
        if (showNode.like instanceof SqlCharStringLiteral) {
            likePattern = ((SqlCharStringLiteral) showNode.like).getNlsString().getValue();
        }
        Pattern regex =
            likePattern != null ? LogicalShowJavaFunctionsHandler.convertLikeToRegex(likePattern) : null;

        ArrayResultCursor result = new ArrayResultCursor("SECRETS");
        result.addColumn("SECRET_NAME", DataTypes.StringType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("PROPERTIES", DataTypes.StringType);
        result.initMeta();

        for (SecretManager.SecretInfo info : SecretManager.getInstance().list()) {
            if (regex != null && !regex.matcher(info.name).matches()) {
                continue;
            }
            String props = info.properties != null ? info.properties.toString() : "";
            result.addRow(new Object[] {
                info.name,
                info.type,
                props
            });
        }

        return result;
    }
}
