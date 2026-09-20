package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorDescriptor;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowJavaFunctionsHandler;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowConnectors;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LogicalShowConnectorsHandler extends HandlerCommon {

    public LogicalShowConnectorsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        boolean full = isFull(logicalPlan);
        Pattern regex = likeRegex(logicalPlan);

        if (full) {
            return handleFull(regex);
        } else {
            return handleSimple(regex);
        }
    }

    private Cursor handleSimple(Pattern regex) {
        ArrayResultCursor result = new ArrayResultCursor("CONNECTORS");
        result.addColumn("CONNECTOR_NAME", DataTypes.StringType);
        result.initMeta();

        for (String type : ConnectorRegistry.getInstance().visibleTypes()) {
            if (regex != null && !regex.matcher(type).matches()) {
                continue;
            }
            result.addRow(new Object[] {type});
        }
        return result;
    }

    private Cursor handleFull(Pattern regex) {
        ArrayResultCursor result = new ArrayResultCursor("CONNECTORS");
        result.addColumn("CONNECTOR_NAME", DataTypes.StringType);
        result.addColumn("SECRET_TYPE", DataTypes.StringType);
        result.addColumn("REQUIRED_SECRET_KEYS", DataTypes.StringType);
        result.addColumn("OPTIONAL_SECRET_KEYS", DataTypes.StringType);
        result.addColumn("SENSITIVE_KEYS", DataTypes.StringType);
        result.addColumn("ALLOW_UNKNOWN_KEYS", DataTypes.StringType);
        result.initMeta();

        for (String type : ConnectorRegistry.getInstance().visibleTypes()) {
            if (regex != null && !regex.matcher(type).matches()) {
                continue;
            }
            ConnectorDescriptor factory = ConnectorRegistry.getInstance().getOrNull(type);
            if (factory == null) {
                continue;
            }
            List<PropertyDefinition> definitions = factory.secretDefinitions();
            if (definitions == null || definitions.isEmpty()) {
                result.addRow(new Object[] {type, "", "", "", "", ""});
            } else {
                for (PropertyDefinition def : definitions) {
                    result.addRow(new Object[] {
                        type,
                        def.getType(),
                        joinKeys(def.getRequiredKeys()),
                        joinKeys(def.getOptionalKeys()),
                        joinKeys(def.getSensitiveKeys()),
                        def.isAllowUnknownKeys() ? "YES" : "NO"
                    });
                }
            }
        }
        return result;
    }

    private boolean isFull(RelNode logicalPlan) {
        if (logicalPlan instanceof BaseDalOperation) {
            SqlNode sqlNode = ((BaseDalOperation) logicalPlan).getNativeSqlNode();
            if (sqlNode instanceof SqlShowConnectors) {
                return ((SqlShowConnectors) sqlNode).isFull();
            }
        }
        return false;
    }

    private Pattern likeRegex(RelNode logicalPlan) {
        if (logicalPlan instanceof BaseDalOperation) {
            SqlNode sqlNode = ((BaseDalOperation) logicalPlan).getNativeSqlNode();
            if (sqlNode instanceof SqlShow
                && ((SqlShow) sqlNode).like instanceof SqlCharStringLiteral) {
                String likePattern =
                    ((SqlCharStringLiteral) ((SqlShow) sqlNode).like).getNlsString().getValue();
                return LogicalShowJavaFunctionsHandler.convertLikeToRegex(likePattern);
            }
        }
        return null;
    }

    private String joinKeys(Set<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return "";
        }
        return keys.stream().sorted().collect(Collectors.joining(", "));
    }
}
