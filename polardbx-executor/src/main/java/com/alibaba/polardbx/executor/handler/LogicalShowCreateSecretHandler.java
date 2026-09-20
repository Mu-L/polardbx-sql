package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateSecretStatement;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.secret.SecretManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateSecret;

import java.util.Map;

public class LogicalShowCreateSecretHandler extends HandlerCommon {

    public LogicalShowCreateSecretHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        SqlShowCreateSecret showNode = (SqlShowCreateSecret) show.getNativeSqlNode();
        String secretName = showNode.getSecretName();

        ArrayResultCursor result = new ArrayResultCursor("SHOW_CREATE_SECRET");
        result.addColumn("SECRET_NAME", DataTypes.StringType);
        result.addColumn("CREATE_STATEMENT", DataTypes.StringType);
        result.initMeta();

        SecretManager.SecretInfo info = SecretManager.getInstance().getInfo(secretName);
        if (info == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Secret '" + secretName + "' does not exist");
        }

        MySqlCreateSecretStatement createStmt = new MySqlCreateSecretStatement();
        createStmt.setName(new SQLIdentifierExpr(info.name));
        Map<String, String> properties = createStmt.getProperties();
        properties.put("type", info.type);
        for (Map.Entry<String, String> entry : info.properties.entrySet()) {
            if (!"type".equalsIgnoreCase(entry.getKey())) {
                properties.put(entry.getKey(), entry.getValue());
            }
        }

        result.addRow(new Object[] {info.name, createStmt.toString()});
        return result;
    }
}
