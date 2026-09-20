package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRebuildCleanupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.omc.OmcPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RebuildCleanupJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String tableName;
    private final String keepFilter;
    private final String originSql;
    private final ExecutionContext executionContext;

    public RebuildCleanupJobFactory(String schemaName, String tableName, String keepFilter, String originSql,
                                    ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.keepFilter = keepFilter;
        this.originSql = originSql;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, tableName, executionContext);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        boolean forceWithGsi =
            executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_REBUILD_CLEANUP_WITH_GSI);
        OmcUtils.validateRebuildCleanupTable(tableMeta, forceWithGsi);
        validateKeepFilter(tableName);
        for (String gsiName : getGsiTableNames(tableMeta, forceWithGsi)) {
            validateKeepFilter(gsiName, "GSI");
            if (!OmcUtils.supportOmc30(executionContext, schemaName, gsiName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "REBUILD CLEANUP requires OMC 3.0 support for GSI '" + gsiName + "'");
            }
        }
        for (String cciName : getCciTableNames(tableMeta, forceWithGsi)) {
            validateKeepFilter(cciName, "CCI");
        }
        if (!OmcUtils.supportOmc30(executionContext, schemaName, tableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "REBUILD CLEANUP requires OMC 3.0");
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        boolean forceWithGsi =
            executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_REBUILD_CLEANUP_WITH_GSI);
        List<String> omcTableNames = new ArrayList<>();
        omcTableNames.add(tableName);
        omcTableNames.addAll(getGsiTableNames(tableMeta, forceWithGsi));
        List<String> versionTableNames = new ArrayList<>(omcTableNames);
        versionTableNames.addAll(getCciTableNames(tableMeta, forceWithGsi));

        Map<String, Long> tableVersions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        List<DdlTask> tasks = new ArrayList<>();
        for (String cleanupTableName : versionTableNames) {
            TableMeta cleanupTableMeta =
                executionContext.getSchemaManager(schemaName).getTable(cleanupTableName);
            tableVersions.put(cleanupTableName, cleanupTableMeta.getVersion());
        }

        ValidateTableVersionTask validateTask = new ValidateTableVersionTask(schemaName, tableVersions);
        tasks.add(validateTask);
        for (String cleanupTableName : omcTableNames) {
            tasks.add(buildOmcTask(cleanupTableName));
        }
        boolean skipCdcTask =
            executionContext.getParamManager().getBoolean(ConnectionParams.REBUILD_CLEANUP_SKIP_CDC_TASK);
        if (!skipCdcTask) {
            tasks.add(new CdcRebuildCleanupMarkTask(schemaName, tableName));
        }

        ExecutableDdlJob job = new ExecutableDdlJob();
        job.addSequentialTasks(tasks);
        job.labelAsHead(validateTask);
        job.labelAsTail(tasks.get(tasks.size() - 1));
        return job;
    }

    private OmcPhyDdlTask buildOmcTask(String cleanupTableName) {
        TableMeta cleanupTableMeta =
            executionContext.getSchemaManager(schemaName).getTable(cleanupTableName);
        Map<String, Set<String>> topology = cleanupTableMeta.getLatestTopology();
        TreeMap<String, List<List<String>>> tableTopology = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        topology.forEach((group, tables) -> {
            List<String> sortedTables = new ArrayList<>(tables);
            sortedTables.sort(String.CASE_INSENSITIVE_ORDER);
            List<List<String>> physicalTables = new ArrayList<>();
            for (String physicalTable : sortedTables) {
                physicalTables.add(Collections.singletonList(physicalTable));
            }
            tableTopology.put(group, physicalTables);
        });

        Map<String, List<Pair<String, String>>> physicalTopology =
            OmcUtils.buildPhysicalTableTopology(schemaName, tableTopology);
        String quotedTable = SqlIdentifier.surroundWithBacktick(cleanupTableName);
        String physicalDdl = "ALTER TABLE " + quotedTable + " ENGINE=INNODB";
        return new OmcPhyDdlTask(
            schemaName, cleanupTableName, physicalDdl, originSql, physicalTopology, false, keepFilter);
    }

    private List<String> getGsiTableNames(TableMeta tableMeta, boolean forceWithGsi) {
        if (!forceWithGsi || !tableMeta.withGsiExcludingPureCci()) {
            return Collections.emptyList();
        }
        List<String> gsiTableNames = new ArrayList<>(tableMeta.getGsiPublished().keySet());
        gsiTableNames.sort(String.CASE_INSENSITIVE_ORDER);
        return gsiTableNames;
    }

    private List<String> getCciTableNames(TableMeta tableMeta, boolean forceWithGsi) {
        if (!forceWithGsi || !tableMeta.withCci()) {
            return Collections.emptyList();
        }
        List<String> cciTableNames = new ArrayList<>(tableMeta.getColumnarIndexPublished().keySet());
        cciTableNames.sort(String.CASE_INSENSITIVE_ORDER);
        return cciTableNames;
    }

    private void validateKeepFilter(String cleanupTableName) {
        validateKeepFilter(cleanupTableName, null);
    }

    private void validateKeepFilter(String cleanupTableName, String indexType) {
        TableMeta cleanupTableMeta =
            executionContext.getSchemaManager(schemaName).getTable(cleanupTableName);
        try {
            OmcUtils.validateKeepFilterForTable(keepFilter, cleanupTableMeta);
        } catch (TddlRuntimeException e) {
            if (indexType == null) {
                throw e;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "Invalid cleanup predicate for " + indexType + " '" + cleanupTableName + "': " + e.getMessage());
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, tableName));
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        boolean forceWithGsi =
            executionContext.getParamManager().getBoolean(ConnectionParams.FORCE_REBUILD_CLEANUP_WITH_GSI);
        for (String gsiName : getGsiTableNames(tableMeta, forceWithGsi)) {
            resources.add(concatWithDot(schemaName, gsiName));
        }
        for (String cciName : getCciTableNames(tableMeta, forceWithGsi)) {
            resources.add(concatWithDot(schemaName, cciName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }
}
