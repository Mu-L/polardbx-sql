/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.util.AppNameUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * generate & insert columnar index table's metadata based on primaryTable's metadata
 * <p>
 * will insert into [indexes]
 */
@TaskName(name = "InsertColumnarIndexMetaTask")
@Getter
public class InsertColumnarIndexMetaTask extends BaseGmsTask {

    final String indexName;
    final List<String> columns;
    /**
     * columns排序键对应的collation，事实上值是A｜D｜NULL，NULL代表默认值，A代表升序asc，D代表降序desc
     */
    final List<String> collations;
    final List<String> coverings;
    final boolean unique;
    final String indexComment;
    final String indexType;
    final IndexStatus indexStatus;
    Integer originTableType;
    final boolean clusteredIndex;

    @JSONCreator
    public InsertColumnarIndexMetaTask(String schemaName,
                                       String logicalTableName,
                                       String indexName,
                                       List<String> columns,
                                       List<String> collations,
                                       List<String> coverings,
                                       boolean unique,
                                       String indexComment,
                                       String indexType,
                                       IndexStatus indexStatus,
                                       boolean clusteredIndex) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.columns = ImmutableList.copyOf(columns);
        this.collations = new ArrayList<>(collations);
        this.coverings = ImmutableList.copyOf(coverings);
        this.unique = unique;
        this.indexComment = indexComment == null ? "" : indexComment;
        this.indexType = indexType;
        this.indexStatus = indexStatus;
        this.clusteredIndex = clusteredIndex;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final List<GsiMetaManager.IndexRecord> indexRecords = new ArrayList<>();

        final String appName = AppNameUtil.buildAppNameByInstAndDbName(InstIdUtil.getInstId(), schemaName);
        final TableMeta primaryTableMeta =
            GmsTableMetaManager.fetchTableMeta(metaDbConnection,
                schemaName, appName, logicalTableName, null, null, true, true);

        FailPoint.assertNotNull(primaryTableMeta);
        primaryTableMeta.setSchemaName(schemaName);

        // For externalized columns, the tableMeta has logical names (e.g. "content")
        // but the columns/coverings lists have physical names (e.g. "content_addr_").
        // Build a columnMapping (physical addr name → logical name) so that
        // nullable() can look up the column in tableMeta, while the CCI index
        // records still keep the physical address column names.
        Map<String, String> columnMapping = null;
        if (primaryTableMeta.hasExternalizedColumn()) {
            columnMapping = new HashMap<>();
            for (ColumnMeta cm : primaryTableMeta.getPhysicalColumns()) {
                if (cm.isExternalizedColumn() && cm.getMappingName() != null) {
                    columnMapping.put(cm.getMappingName().toLowerCase(), cm.getName());
                }
            }
            if (columnMapping.isEmpty()) {
                columnMapping = null;
            }
        }

        GsiUtils.buildIndexMetaFromPrimary(
            indexRecords,
            primaryTableMeta,
            indexName,
            columns,
            collations,
            null,
            coverings,
            !unique,
            indexComment,
            indexType,
            indexStatus,
            LackLocalIndexStatus.NO_LACKIING,
            clusteredIndex,
            true,
            columnMapping,
            null
        );

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        TablesExtRecord indexTablesExtRecord =
            tableInfoManager.queryTableExt(schemaName, indexName, false);
        if (indexTablesExtRecord != null) {
            originTableType = indexTablesExtRecord.tableType;
        }
        tableInfoManager.setConnection(null);

        //1. insert metadata into indexes
        GsiMetaChanger.addIndexMeta(metaDbConnection, schemaName, indexRecords);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        //2. update tables_ext.table_type to GSI
        // TODO: Actually, unlike creating GSI, there is no record in tables_ext for columnar index,
        //       this update does nothing, maybe we should remove this step
        GsiMetaChanger.changeTableToColumnar(metaDbConnection, schemaName, indexName);

        //3. notify listeners
        // TableMetaChanger.notifyCreateColumnarIndex(metaDbConnection, schemaName, logicalTableName);
        LOGGER.info(String.format("Insert ColumnarIndex meta. schema:%s, table:%s, index:%s, state:%s",
            schemaName,
            logicalTableName,
            indexName,
            indexStatus.name()
        ));
    }

    /**
     * see undoCreateGsi()
     */
    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (originTableType != null) {
            ExecutorContext
                .getContext(schemaName)
                .getGsiManager()
                .getGsiMetaManager()
                .changeTablesExtType(metaDbConnection, schemaName, indexName, originTableType);
        }
        GsiMetaChanger.removeIndexMeta(metaDbConnection, schemaName, logicalTableName, indexName);

        LOGGER.info(String.format("Rollback Insert ColumnarIndex meta. schema:%s, table:%s, index:%s, state:%s",
            schemaName,
            logicalTableName,
            indexName,
            indexStatus.name()
        ));
    }

    @Override
    protected String remark() {
        return "|tableName: " + logicalTableName;
    }
}
