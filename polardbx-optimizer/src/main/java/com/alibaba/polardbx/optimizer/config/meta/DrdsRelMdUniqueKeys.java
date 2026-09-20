package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUniqueKeys;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DrdsRelMdUniqueKeys extends RelMdUniqueKeys {

    private static final int UK_SIZE = 10;
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.UNIQUE_KEYS.method, new DrdsRelMdUniqueKeys());

    public Set<ImmutableBitSet> getUniqueKeys(Join rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        // to avoid combination explosion, limit set size
        Set<ImmutableBitSet> retSet = super.getUniqueKeys(rel, mq, ignoreNulls);
        if (retSet == null || retSet.size() < UK_SIZE) {
            return retSet;
        }
        return retSet.stream()
            .sorted()
            .limit(10)
            .collect(Collectors.toCollection(HashSet::new));
    }

    public Set<ImmutableBitSet> getUniqueKeys(SemiJoin rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return super.getUniqueKeys(rel, mq, ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(LogicalView rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return rel.getUniqueKeys(mq, ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(RelSubset rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(Util.first(rel.getBest(), rel.getOriginal()), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(HepRelVertex rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getCurrentRel(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(GroupTopN rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(CTEAnchor rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getRight(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(CTEProducer rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(LogicalCTEConsumer rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getInnerRel(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(PhysicalCTEConsumer rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        Set<ImmutableBitSet> producerKeys = mq.getUniqueKeys(CBOUtil.getCteProducer(rel), ignoreNulls);
        List<RexNode> projects = rel.getProjects();
        if (projects == null || projects.isEmpty() || producerKeys == null) {
            return producerKeys;
        }
        // Build reverse mapping: producer column -> output column
        Map<Integer, Integer> reverseMap = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            RexNode project = projects.get(i);
            if (project instanceof RexInputRef) {
                reverseMap.put(((RexInputRef) project).getIndex(), i);
            }
        }
        Set<ImmutableBitSet> result = new HashSet<>();
        for (ImmutableBitSet key : producerKeys) {
            ImmutableBitSet.Builder mapped = ImmutableBitSet.builder();
            boolean allMapped = true;
            for (int bit : key) {
                Integer outputCol = reverseMap.get(bit);
                if (outputCol != null) {
                    mapped.set(outputCol);
                } else {
                    allMapped = false;
                    break;
                }
            }
            if (allMapped) {
                result.add(mapped.build());
            }
        }
        return result.isEmpty() ? null : result;
    }

    public Set<ImmutableBitSet> getUniqueKeys(Exchange rel, RelMetadataQuery mq,
                                              boolean ignoreNulls) {
        return mq.getUniqueKeys(rel.getInput(), ignoreNulls);
    }

    public Set<ImmutableBitSet> getUniqueKeys(LogicalTableScan rel, RelMetadataQuery mq, boolean ignoreNulls) {
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return null;
        }
        String sourceTable = StatisticManager.getSourceTableName(tableMeta.getSchemaName(), tableMeta.getTableName());
        tableMeta = PlannerContext.getPlannerContext(rel).getExecutionContext()
            .getSchemaManager(tableMeta.getSchemaName())
            .getTableWithNull(sourceTable);
        if (tableMeta == null) {
            return null;
        }

        TddlRuleManager tddlRuleManager =
            PlannerContext.getPlannerContext(rel).getExecutionContext().getSchemaManager(tableMeta.getSchemaName())
                .getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        List<Integer> dbKeysAndTbKeys = Lists.newArrayList();
        if (partitionInfoManager.isNewPartDbTable(tableMeta.getTableName())) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableMeta.getTableName());
            dbKeysAndTbKeys = getColumnsIndex(tableMeta, partitionInfo.getPartitionColumns());
        } else {
            TableRule tableRule = tddlRuleManager.getTableRule(tableMeta.getTableName());
            if (tddlRuleManager.isShard(tableMeta.getTableName())) {
                dbKeysAndTbKeys = getColumnsIndex(tableMeta, tableRule.getDbPartitionKeys());
                dbKeysAndTbKeys.addAll(getColumnsIndex(tableMeta, tableRule.getTbPartitionKeys()));
            }
        }

        final Set<ImmutableBitSet> retSet = new HashSet<>();
        // check auto_increment primary key
        boolean includingPrimaryIndex = true;
        if (DrdsRelMdSelectivity.isPrimaryKeyAutoIncrement(tableMeta)) {
            int pkIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                tableMeta.getPrimaryIndex().getKeyColumnsExt().get(0).getColumnMeta());
            retSet.add(ImmutableBitSet.of(pkIndex));
            includingPrimaryIndex = false;
        }

        List<IndexMeta> ukList = tableMeta.getUniqueIndexes(includingPrimaryIndex);
        for (IndexMeta uk : ukList) {
            List<Integer> ukColumns = getColumnsIndex(tableMeta, uk);
            if (ukColumns == null) {
                continue;
            }
            ImmutableBitSet bitSet = ImmutableBitSet.builder()
                .addAll(dbKeysAndTbKeys)
                .addAll(ukColumns)
                .build();
            retSet.add(bitSet);
        }

        // gsi
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiIndexMetaBeanMap = tableMeta.getGsiPublished();
        if (gsiIndexMetaBeanMap != null) {
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : gsiIndexMetaBeanMap.entrySet()) {
                GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = entry.getValue();
                if (!gsiIndexMetaBean.nonUnique) {
                    List<Integer> ukColumns = getColumnsIndex(tableMeta,
                        gsiIndexMetaBean.indexColumns.stream().map(x -> x.columnName).collect(Collectors.toList()));
                    retSet.add(ImmutableBitSet.of(ukColumns));
                }
            }
        }

        if (retSet.isEmpty()) {
            return null;
        }
        return retSet;
    }

    private List<Integer> getColumnsIndex(TableMeta tableMeta, IndexMeta ukMeta) {
        List<Integer> indexes = Lists.newArrayList();
        for (IndexColumnMeta column : ukMeta.getKeyColumnsExt()) {
            if (!column.hasColumn()) {
                return null;
            }
            indexes.add(DrdsRelMdSelectivity.getColumnIndex(tableMeta, column.getColumnMeta()));
        }
        return indexes;
    }

    private List<Integer> getColumnsIndex(TableMeta tableMeta, List<String> columns) {
        List<Integer> indexes = Lists.newArrayList();
        for (String column : columns) {
            indexes.add(DrdsRelMdSelectivity.getColumnIndex(tableMeta, tableMeta.getColumnIgnoreCase(column)));
        }
        return indexes;
    }

    public Set<ImmutableBitSet> getUniqueKeys(
        ExternalTableScan rel,
        RelMetadataQuery mq, boolean ignoreNulls) {
        return rel.getUniqueKeys(mq, ignoreNulls);
    }
}
