package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.columnar.ColumnarOption;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.ColumnarConfigHandler;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.columnar.ColumnarUtils.AddCDCMarkEventForColumnar;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.COLUMNAR_SET_CONFIG;
import static com.alibaba.polardbx.server.handler.pl.inner.InnerProcedureUtils.POLARDBX_INNER_PROCEDURE;

/**
 * @author lijiu
 */
public class ColumnarSetConfigProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<SQLExpr> params = statement.getParameters();
        //参数解析
        ColumnarOption.Param param = checkParameters(params, statement);
        fillTableId(param);

        // Validate and handle.
        ColumnarOption option = ColumnarConfig.get(param.key);
        if (null == option) {
            // For convenience, unknown options are just added into system table columnar_config,
            // and will be handled in columnar.
            ColumnarConfigHandler.setColumnarConfig(param);
        } else {
            option.handle(param);
        }

        if (param.key.equalsIgnoreCase("TYPE")) {
            long tableId = checkParameters4SetConfig(params, statement);
            String sql =
                "call " + POLARDBX_INNER_PROCEDURE + "." + COLUMNAR_SET_CONFIG + "(" + tableId + ", '" + param.key
                    + "', '"
                    + param.value + "')";
            Long tso = AddCDCMarkEventForColumnar(sql, SqlKind.PROCEDURE_CALL.name());

            if (tso == null || tso <= 0) {
                throw new RuntimeException(sql + " is failed, because tso: " + tso);
            }
        }

        buildResultCursor(cursor, param);
    }

    private static void buildResultCursor(ArrayResultCursor cursor, ColumnarOption.Param param) {
        if (param.tableId == 0) {
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {param.key, param.value});
        } else if (null == param.schemaName) {
            cursor.addColumn("index_id", DataTypes.LongType);
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {param.tableId, param.key, param.value});
        } else {
            cursor.addColumn("schema_name", DataTypes.StringType);
            cursor.addColumn("table_name", DataTypes.StringType);
            cursor.addColumn("index_name", DataTypes.StringType);
            cursor.addColumn("index_id", DataTypes.LongType);
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {
                param.schemaName, param.tableName, param.indexName,
                param.tableId, param.key, param.value});
        }
    }

    /**
     * 3个参数时，第一个是table id，代表设置表级别；后面两个是key，value
     * 2个参数时，代表实例级别；后面两个是key，value
     * 5个参数时，代表设置某个索引的参数，schema、table、index、key、value
     */
    private ColumnarOption.Param checkParameters(List<SQLExpr> params, SQLCallStatement statement) {
        //参数不是两个，或者3个，5个报错
        if (!(params.size() == 2 || params.size() == 3 || params.size() == 5)) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 2/3/5 parameters");
        }
        if (params.size() == 3 && !(params.get(0) instanceof SQLIntegerExpr)) {
            throw new IllegalArgumentException(
                statement.toString() + " first parameters need Long number when hava 3 parameters");
        }
        if (params.size() == 5
            && (!(params.get(0) instanceof SQLCharExpr)
            || !(params.get(1) instanceof SQLCharExpr)
            || !(params.get(2) instanceof SQLCharExpr))) {
            throw new IllegalArgumentException(
                statement.toString() + " first/second/third parameters need String when hava 5 parameters");
        }
        //默认0是实例级别
        SQLExpr key;
        SQLExpr value;
        if (params.size() == 3) {
            key = params.get(1);
            value = params.get(2);
        } else if (params.size() == 2) {
            key = params.get(0);
            value = params.get(1);
        } else {
            key = params.get(3);
            value = params.get(4);
        }

        if (!(key instanceof SQLCharExpr)) {
            throw new IllegalArgumentException(statement.toString() + " key parameters need String");
        }

        String setKey = ((SQLCharExpr) key).getText();
        String setValue;

        if (value instanceof SQLCharExpr) {
            setValue = ((SQLCharExpr) value).getText();
        } else if (value instanceof SQLNumericLiteralExpr) {
            setValue = String.valueOf(((SQLNumericLiteralExpr) value).getNumber());
        } else if (value instanceof SQLBooleanExpr) {
            setValue = String.valueOf(((SQLBooleanExpr) value).getBooleanValue());
        } else if (value instanceof SQLNullExpr) {
            setValue = value.toString();
        } else {
            throw new IllegalArgumentException(value + " value parameters need String");
        }

        ColumnarOption.Param param = new ColumnarOption.Param();
        param.key = setKey;
        param.value = setValue;

        if (params.size() == 3) {
            param.tableId = ((SQLIntegerExpr) params.get(0)).getNumber().longValue();
        } else if (params.size() == 5) {
            param.schemaName = ((SQLCharExpr) params.get(0)).getText();
            param.tableName = ((SQLCharExpr) params.get(1)).getText();
            param.indexName = ((SQLCharExpr) params.get(2)).getText();
        } else {
            // Global.
            param.tableId = 0L;
        }

        return param;
    }

    public static void fillTableId(ColumnarOption.Param param) {
        if (null != param.schemaName) {
            boolean accurateIndexName = false;
            OptimizerContext oc = OptimizerContext.getContext(param.schemaName);
            if (null != oc) {
                TableMeta tableMeta = oc.getLatestSchemaManager().getTable(param.tableName);
                GsiMetaManager.GsiIndexMetaBean indexMetaBean = tableMeta.findGlobalSecondaryIndexByNameOrFullName(
                    param.indexName);
                if (null != indexMetaBean) {
                    accurateIndexName = true;
                    param.indexName = indexMetaBean.indexName;
                }
            }
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                // Find table id.
                ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
                tableMappingAccessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = tableMappingAccessor.queryBySchemaTableIndexLike(
                    param.schemaName, param.tableName, param.indexName + (accurateIndexName ? "" : '%'),
                    ColumnarTableStatus.PUBLIC.name());
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index like %s not found columnar index", param.schemaName,
                            param.tableName, param.indexName));
                }
                if (records.size() >= 2) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index %s found more than one columnar index[%s]", param.schemaName,
                            param.tableName, param.indexName,
                            records.stream().map(r -> r.indexName).collect(Collectors.joining(","))));
                }
                param.tableId = records.get(0).tableId;
                param.indexName = records.get(0).indexName;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }
    }

    /**
     * 检查参数，返回正确的列存索引ID，未找到则报错
     *
     * @return 列存索引ID
     */
    private long checkParameters4SetConfig(List<SQLExpr> params, SQLCallStatement statement) {
        //2个参数不需要进该函数
        if (!(params.size() == 3 || params.size() == 5)) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 2/3/5 parameters");
        }

        if (params.size() == 3) {
            //3个参数，第一个代表列存索引ID
            if (!(params.get(0) instanceof SQLNumericLiteralExpr)) {
                throw new IllegalArgumentException(
                    statement.toString() + " first parameters need Long number when hava 1 parameters");
            }
            long indexId = ((SQLNumericLiteralExpr) params.get(0)).getNumber().longValue();

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                accessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = accessor.queryTableId(indexId);
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + indexId + " not found columnar index");
                }
                if (!ColumnarTableStatus.PUBLIC.name().equalsIgnoreCase(records.get(0).status)) {
                    throw new IllegalArgumentException(
                        statement.toString() + " indexId: " + indexId + " is not PUBLIC columnar index, status is "
                            + records.get(0).status);
                }
                return records.get(0).tableId;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            //前3个参数，schemaName, tableName, indexName，通过库名、表名、索引名匹配
            if (!(params.get(0) instanceof SQLCharExpr)
                || !(params.get(1) instanceof SQLCharExpr)
                || !(params.get(2) instanceof SQLCharExpr)) {
                throw new IllegalArgumentException(
                    statement.toString() + " first/second/third parameters need String when hava 3 parameters");
            }

            String schemaName = ((SQLCharExpr) params.get(0)).getText();
            String tableName = ((SQLCharExpr) params.get(1)).getText();
            String indexName = ((SQLCharExpr) params.get(2)).getText();

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                accessor.setConnection(metaDbConn);
                List<ColumnarTableMappingRecord> records = accessor.queryBySchemaTableIndexLike(schemaName,
                    tableName, indexName + '%', ColumnarTableStatus.PUBLIC.name());
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index like %s not found columnar index", schemaName, tableName,
                            indexName));
                }
                return records.get(0).tableId;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
