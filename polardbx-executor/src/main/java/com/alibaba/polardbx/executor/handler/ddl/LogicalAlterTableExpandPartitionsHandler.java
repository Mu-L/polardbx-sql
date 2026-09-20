package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.factory.ExpandTablePartitionsJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableExpandPartitions;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ExpandPartitionsPreparedData;

import java.util.Map;
import java.util.Set;

/**
 * Handler for: ALTER TABLE t EXPAND PARTITIONS TO N
 */
public class LogicalAlterTableExpandPartitionsHandler extends LogicalCommonDdlHandler {

    public LogicalAlterTableExpandPartitionsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public void prepareFixedResources(BaseDdlOperation logicalDdlPlan,
                                      ExecutionContext executionContext, Set<String> sharedResources,
                                      Set<String> exclusiveResources, Map<String, Long> tableVersions) {
        String tableName = logicalDdlPlan.getTableName();
        exclusiveResources.add(concatWithDot(logicalDdlPlan.getSchemaName(), tableName));
        TableMeta tableMeta =
            executionContext.getSchemaManager(logicalDdlPlan.getSchemaName()).getTableWithNull(tableName);
        if (tableMeta != null) {
            tableVersions.put(tableName, tableMeta.getVersion());
        }
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalAlterTableExpandPartitions expandPartitions =
            (LogicalAlterTableExpandPartitions) logicalDdlPlan;

        expandPartitions.preparedData(executionContext);
        ExpandPartitionsPreparedData preparedData = expandPartitions.getPreparedData();
        preparedData.setDdlVersionId(DdlUtils.generateVersionId(executionContext));

        return ExpandTablePartitionsJobFactory.create(expandPartitions.relDdl, preparedData, executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        String schemaName = logicalDdlPlan.getSchemaName();

        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "EXPAND PARTITIONS is only supported in auto-mode (new partition) database");
        }

        // Deep validation is delegated to LogicalAlterTableExpandPartitions.preparedData()
        return false;
    }
}
