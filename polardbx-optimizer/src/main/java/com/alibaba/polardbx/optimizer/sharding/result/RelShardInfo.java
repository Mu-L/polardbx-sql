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

package com.alibaba.polardbx.optimizer.sharding.result;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import org.apache.calcite.sql.SqlNode;

import java.util.Map;
import java.util.TreeMap;

/**
 * AlL Shard info of plan
 *
 * @author chenghui.lch
 */
public class RelShardInfo {
    protected String schemaName;
    protected String tableName;
    protected boolean usePartTable = false;
    /**
     * <pre>
     *     key: shard column
     *     val(Comparative): the comp tree of shard columns of shard column
     * </pre>
     */
    protected Map<String, Comparative> allComps = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    /**
     * The definition of partition selection of current tableName
     */
    protected SqlNode partitions = null;
    /**
     * <pre>
     *     the prune step tree of table
     * </pre>
     */
    protected PartitionPruneStep partPruneStepInfo = null;

    public RelShardInfo() {
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isUsePartTable() {
        return usePartTable;
    }

    public Map<String, Comparative> getAllComps() {
        return allComps;
    }

    public PartitionPruneStep getPartPruneStepInfo() {
        return partPruneStepInfo;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setUsePartTable(boolean usePartTable) {
        this.usePartTable = usePartTable;
    }

    public void setAllComps(Map<String, Comparative> allComps) {
        this.allComps = allComps;
    }

    public void setPartPruneStepInfo(PartitionPruneStep partPruneStepInfo) {
        this.partPruneStepInfo = partPruneStepInfo;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public SqlNode getPartitions() {
        return partitions;
    }

    public void setPartitions(SqlNode partitions) {
        this.partitions = partitions;
    }
}
