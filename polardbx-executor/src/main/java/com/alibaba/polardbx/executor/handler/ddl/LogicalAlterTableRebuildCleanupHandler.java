package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.factory.RebuildCleanupJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.PolarPrivilegeUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterTableRebuildCleanup;
import org.apache.calcite.sql.SqlIdentifier;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class LogicalAlterTableRebuildCleanupHandler extends LogicalCommonDdlHandler {

    private static final int MAX_SAMPLE_PARTITIONS = 2;
    private static final int MAX_SAMPLE_ROWS = 10;
    private static final int MAX_SAMPLE_ROWS_PER_TYPE = MAX_SAMPLE_ROWS / 2;
    private static final int SAMPLE_QUERY_TIMEOUT_SECONDS = 1;
    private static final int SAMPLE_QUERY_TIMEOUT_MILLISECONDS = 1000;

    public LogicalAlterTableRebuildCleanupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalAlterTable logicalAlterTable = (LogicalAlterTable) logicalPlan;
        SqlAlterTableRebuildCleanup cleanup = getCleanupSpecification(logicalAlterTable);
        if (cleanup.isDryRun()) {
            return handleDryRun(logicalAlterTable, cleanup, executionContext);
        }
        return super.handle(logicalPlan, executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTable logicalAlterTable = (LogicalAlterTable) logicalDdlPlan;
        String schemaName = logicalDdlPlan.getSchemaName();
        String tableName = logicalDdlPlan.getTableName();
        TableValidator.validateTableExistence(schemaName, tableName, executionContext);
        prepareAndValidateKeepFilter(
            schemaName, tableName, getCleanupSpecification(logicalAlterTable), executionContext);
        return false;
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTable logicalAlterTable = (LogicalAlterTable) logicalDdlPlan;
        SqlAlterTableRebuildCleanup cleanup = getCleanupSpecification(logicalAlterTable);
        String schemaName = logicalDdlPlan.getSchemaName();
        String tableName = logicalDdlPlan.getTableName();

        checkPrivileges(schemaName, tableName, executionContext, false);
        String keepFilter = buildKeepFilter(cleanup.getCleanupPredicate().toString());
        return new RebuildCleanupJobFactory(
            schemaName,
            tableName,
            keepFilter,
            executionContext.getOriginSql(),
            executionContext).create();
    }

    private Cursor handleDryRun(LogicalAlterTable logicalAlterTable, SqlAlterTableRebuildCleanup cleanup,
                                ExecutionContext executionContext) {
        String schemaName = logicalAlterTable.getSchemaName();
        String tableName = logicalAlterTable.getTableName();

        TableValidator.validateTableExistence(schemaName, tableName, executionContext);
        checkPrivileges(schemaName, tableName, executionContext, true);

        String cleanupPredicate = cleanup.getCleanupPredicate().toString();
        String keepFilter = prepareAndValidateKeepFilter(schemaName, tableName, cleanup, executionContext);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        Set<String> referencedColumns = OmcUtils.validateKeepFilterForTable(keepFilter, tableMeta);
        List<String> sampleColumns = buildSampleColumns(tableMeta, referencedColumns);
        List<SampleTarget> targets = buildSampleTargets(tableMeta);

        ArrayResultCursor result = buildDryRunResultCursor();
        result.addRow(new Object[] {"CLEANUP_PREDICATE", "", "", cleanupPredicate});
        result.addRow(new Object[] {"KEEP_PREDICATE", "", "", keepFilter});
        appendSamples(result, schemaName, targets, sampleColumns, cleanupPredicate,
            "SAMPLE_CLEANUP", MAX_SAMPLE_ROWS_PER_TYPE);
        appendSamples(result, schemaName, targets, sampleColumns, keepFilter,
            "SAMPLE_KEEP", MAX_SAMPLE_ROWS_PER_TYPE);
        return result;
    }

    private static SqlAlterTableRebuildCleanup getCleanupSpecification(LogicalAlterTable logicalAlterTable) {
        return (SqlAlterTableRebuildCleanup) logicalAlterTable.getSqlAlterTable().getAlters().get(0);
    }

    private static void checkPrivileges(String schemaName, String tableName, ExecutionContext executionContext,
                                        boolean dryRun) {
        PolarPrivilegeUtils.checkPrivilege(schemaName, tableName, PrivilegePoint.ALTER, executionContext);
        PolarPrivilegeUtils.checkPrivilege(schemaName, tableName, PrivilegePoint.DELETE, executionContext);
        if (dryRun) {
            PolarPrivilegeUtils.checkPrivilege(schemaName, tableName, PrivilegePoint.SELECT, executionContext);
        }
    }

    private static String prepareAndValidateKeepFilter(String schemaName, String tableName,
                                                       SqlAlterTableRebuildCleanup cleanup,
                                                       ExecutionContext executionContext) {
        String keepFilter = buildKeepFilter(cleanup.getCleanupPredicate().toString());
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        boolean forceWithGsi =
            executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_REBUILD_CLEANUP_WITH_GSI);
        OmcUtils.validateRebuildCleanupTable(tableMeta, forceWithGsi);
        validateOmc30Support(schemaName, tableName, executionContext);
        OmcUtils.validateKeepFilterForTable(keepFilter, tableMeta);
        if (forceWithGsi && tableMeta.withGsiExcludingPureCci()) {
            List<String> gsiNames = new ArrayList<>(tableMeta.getGsiPublished().keySet());
            gsiNames.sort(String.CASE_INSENSITIVE_ORDER);
            for (String gsiName : gsiNames) {
                TableMeta gsiTableMeta = executionContext.getSchemaManager(schemaName).getTable(gsiName);
                validateOmc30Support(schemaName, gsiName, executionContext);
                try {
                    OmcUtils.validateKeepFilterForTable(keepFilter, gsiTableMeta);
                } catch (TddlRuntimeException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Invalid cleanup predicate for GSI '" + gsiName + "': " + e.getMessage());
                }
            }
        }
        if (forceWithGsi && tableMeta.withCci()) {
            List<String> cciNames = new ArrayList<>(tableMeta.getColumnarIndexPublished().keySet());
            cciNames.sort(String.CASE_INSENSITIVE_ORDER);
            for (String cciName : cciNames) {
                TableMeta cciTableMeta = executionContext.getSchemaManager(schemaName).getTable(cciName);
                try {
                    OmcUtils.validateKeepFilterForTable(keepFilter, cciTableMeta);
                } catch (TddlRuntimeException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        "Invalid cleanup predicate for CCI '" + cciName + "': " + e.getMessage());
                }
            }
        }
        return keepFilter;
    }

    private static void validateOmc30Support(String schemaName, String tableName,
                                             ExecutionContext executionContext) {
        if (!OmcUtils.supportOmc30(executionContext, schemaName, tableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "REBUILD CLEANUP requires OMC 3.0 support for table '" + tableName + "'");
        }
    }

    static String buildKeepFilter(String cleanupPredicate) {
        return String.format("((%s) IS NOT TRUE)", cleanupPredicate);
    }

    static List<String> buildSampleColumns(TableMeta tableMeta, Collection<String> referencedColumns) {
        LinkedHashSet<String> columns = new LinkedHashSet<>();
        IndexMeta primaryIndex = tableMeta.getPrimaryIndex();
        if (primaryIndex == null || primaryIndex.getKeyColumns().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "REBUILD CLEANUP requires a primary key");
        }
        primaryIndex.getKeyColumns().stream().map(ColumnMeta::getName).forEach(columns::add);
        columns.addAll(referencedColumns);
        return new ArrayList<>(columns);
    }

    private static List<SampleTarget> buildSampleTargets(TableMeta tableMeta) {
        TreeMap<String, Set<String>> topology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        topology.putAll(tableMeta.getLatestTopology());
        List<SampleTarget> targets = new ArrayList<>(MAX_SAMPLE_PARTITIONS);
        for (Map.Entry<String, Set<String>> entry : topology.entrySet()) {
            List<String> tables = new ArrayList<>(entry.getValue());
            tables.sort(String.CASE_INSENSITIVE_ORDER);
            for (String physicalTable : tables) {
                targets.add(new SampleTarget(entry.getKey(), physicalTable));
                if (targets.size() == MAX_SAMPLE_PARTITIONS) {
                    return targets;
                }
            }
        }
        return targets;
    }

    private static ArrayResultCursor buildDryRunResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("REBUILD_CLEANUP_DRY_RUN");
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("GROUP_NAME", DataTypes.StringType);
        result.addColumn("PHYSICAL_TABLE", DataTypes.StringType);
        result.addColumn("CONTENT", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private static void appendSamples(ArrayResultCursor result, String schemaName, List<SampleTarget> targets,
                                      List<String> sampleColumns, String predicate, String sampleType,
                                      int maxSampleRows) {
        int sampleCount = 0;
        for (int i = 0; i < targets.size() && sampleCount < maxSampleRows; i++) {
            SampleTarget target = targets.get(i);
            int remainingTargets = targets.size() - i;
            int remainingRows = maxSampleRows - sampleCount;
            int limit = (remainingRows + remainingTargets - 1) / remainingTargets;
            try {
                List<String> samples = querySamples(schemaName, target, sampleColumns, predicate, limit);
                for (String sample : samples) {
                    result.addRow(new Object[] {
                        sampleType,
                        target.groupName,
                        target.physicalTable,
                        sample
                    });
                    sampleCount++;
                }
            } catch (SQLException e) {
                result.addRow(new Object[] {
                    "WARNING",
                    target.groupName,
                    target.physicalTable,
                    "Sample query failed or timed out: " + e.getMessage()
                });
            }
        }
    }

    private static List<String> querySamples(String schemaName, SampleTarget target, List<String> columns,
                                             String predicate, int limit) throws SQLException {
        String sql = buildSampleQuery(target.physicalTable, columns, predicate, limit);

        DataSource dataSource = ExecutorContext.getContext(schemaName)
            .getTopologyExecutor()
            .getGroupExecutor(target.groupName)
            .getDataSource();
        List<String> samples = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement()) {
            statement.setQueryTimeout(SAMPLE_QUERY_TIMEOUT_SECONDS);
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next() && samples.size() < limit) {
                    JSONObject row = new JSONObject(true);
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
                    }
                    samples.add(JSON.toJSONString(row, SerializerFeature.WriteMapNullValue));
                }
            }
        }
        return samples;
    }

    static String buildSampleQuery(String physicalTable, List<String> columns, String predicate, int limit) {
        String projection = columns.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.joining(", "));
        String orderBy = SqlIdentifier.surroundWithBacktick(columns.get(0));
        return String.format(
            "SELECT /*+ MAX_EXECUTION_TIME(%d) */ %s FROM %s WHERE ((%s) IS TRUE) ORDER BY %s LIMIT %d",
            SAMPLE_QUERY_TIMEOUT_MILLISECONDS,
            projection,
            SqlIdentifier.surroundWithBacktick(physicalTable),
            predicate,
            orderBy,
            limit);
    }

    private static class SampleTarget {
        private final String groupName;
        private final String physicalTable;

        private SampleTarget(String groupName, String physicalTable) {
            this.groupName = groupName;
            this.physicalTable = physicalTable;
        }
    }
}
