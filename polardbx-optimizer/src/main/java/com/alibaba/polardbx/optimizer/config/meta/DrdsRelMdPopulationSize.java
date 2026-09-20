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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdPopulationSize;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

public class DrdsRelMdPopulationSize extends RelMdPopulationSize {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.POPULATION_SIZE.method, new DrdsRelMdPopulationSize());

    public Double getPopulationSize(RelSubset subset, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(Util.first(subset.getBest(), subset.getOriginal()), groupKey);
    }

    public Double getPopulationSize(LogicalView rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return rel.getPopulationSize(mq, groupKey);
    }

    public Double getPopulationSize(LogicalTableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        boolean unique = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey);
        if (unique) {
            return mq.getRowCount(rel);
        } else {
            return mq.getDistinctRowCount(rel, groupKey, null);
        }
    }

    public Double getPopulationSize(GroupTopN rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getInput(), groupKey);
    }

    public Double getPopulationSize(CTEAnchor rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getRight(), groupKey);
    }

    public Double getPopulationSize(CTEProducer rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getInput(), groupKey);
    }

    public Double getPopulationSize(LogicalCTEConsumer rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getInnerRel(), groupKey);
    }

    public Double getPopulationSize(PhysicalCTEConsumer rel, RelMetadataQuery mq,
                                    ImmutableBitSet groupKey) {
        java.util.List<RexNode> projects = rel.getProjects();
        if (projects != null && !projects.isEmpty()) {
            ImmutableBitSet.Builder mappedKey = ImmutableBitSet.builder();
            for (int bit : groupKey) {
                if (bit < projects.size()) {
                    RexNode project = projects.get(bit);
                    if (project instanceof RexInputRef) {
                        mappedKey.set(((RexInputRef) project).getIndex());
                    } else {
                        mappedKey.addAll(org.apache.calcite.plan.RelOptUtil.InputFinder.bits(project));
                    }
                }
            }
            groupKey = mappedKey.build();
        }
        return mq.getPopulationSize(CBOUtil.getCteProducer(rel), groupKey);
    }

    public Double getPopulationSize(TableLookup rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getProject(), groupKey);
    }

    public Double getPopulationSize(ViewPlan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getPlan(), groupKey);
    }

    public Double getPopulationSize(MysqlTableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(rel.getNodeForMetaQuery(), groupKey);
    }

    public Double getPopulationSize(
        ExternalTableScan rel,
        RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return rel.getPopulationSize(mq, groupKey);
    }
}