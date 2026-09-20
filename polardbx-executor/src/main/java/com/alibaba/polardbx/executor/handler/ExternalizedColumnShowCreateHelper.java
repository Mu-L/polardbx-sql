package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.sql.SqlIdentifier;

final class ExternalizedColumnShowCreateHelper {

    private ExternalizedColumnShowCreateHelper() {
    }

    static ColumnMeta restoreExternalizedColumn(SQLColumnDefinition columnDefinition, TableMeta tableMeta) {
        String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getColumnName());
        ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(columnName);
        if (columnMeta != null || !ExternalizedColumnInfo.isAddrColumn(columnName)) {
            return columnMeta;
        }

        String logicalName = ExternalizedColumnInfo.toLogicalColumnName(columnName);
        ColumnMeta externalizedColumnMeta = tableMeta.getColumnIgnoreCase(logicalName);
        if (externalizedColumnMeta == null || !externalizedColumnMeta.isExternalizedColumn()) {
            return null;
        }

        String originalType = null;
        SQLExpr commentExpr = columnDefinition.getComment();
        if (commentExpr instanceof SQLCharExpr) {
            originalType = ExternalizedColumnInfo.extractOriginalType(((SQLCharExpr) commentExpr).getText());
        }
        if (originalType == null) {
            originalType = "TEXT";
        }

        columnDefinition.setName(new SQLIdentifierExpr(SqlIdentifier.surroundWithBacktick(logicalName)));
        columnDefinition.setDataType(new SQLDataTypeImpl(originalType));
        columnDefinition.setExternalize(true);
        columnDefinition.setDefaultExpr(null);
        columnDefinition.setComment((SQLExpr) null);
        columnDefinition.getConstraints().clear();
        return externalizedColumnMeta;
    }

    static String toLogicalColumnName(String columnName, TableMeta tableMeta) {
        if (!ExternalizedColumnInfo.isAddrColumn(columnName)) {
            return columnName;
        }
        String logicalName = ExternalizedColumnInfo.toLogicalColumnName(columnName);
        ColumnMeta columnMeta = tableMeta.getColumnIgnoreCase(logicalName);
        return columnMeta != null && columnMeta.isExternalizedColumn() ? logicalName : columnName;
    }
}
