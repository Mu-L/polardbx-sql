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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdColumnOrigins;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class DrdsRelMdColumnOrigins extends RelMdColumnOrigins {
    /**
     * make sure you have overridden the SOURCE
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.COLUMN_ORIGIN.method, new DrdsRelMdColumnOrigins());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdColumnOrigins.class);

    public Set<RelColumnOrigin> getColumnOrigins(LogicalView rel, RelMetadataQuery mq, int iOutputColumn) {
        return rel.getColumnOrigins(mq, iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(RelSubset rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getOriginal(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(TableLookup tableLookup, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(tableLookup.getProject(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(ViewPlan viewPlan, RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(viewPlan.getPlan(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(MysqlTableScan mysqlTableScan, RelMetadataQuery mq,
                                                 int iOutputColumn) {
        return mq.getColumnOrigins(mysqlTableScan.getNodeForMetaQuery(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(
        ExternalTableScan rel,
        RelMetadataQuery mq, int iOutputColumn) {
        return rel.getColumnOrigins(mq, iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(GroupTopN rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(CTEAnchor rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getRight(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(CTEProducer rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(LogicalCTEConsumer rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(rel.getInnerRel(), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(PhysicalCTEConsumer rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        List<RexNode> projects = rel.getProjects();
        if (projects != null && !projects.isEmpty()) {
            if (iOutputColumn < projects.size()) {
                RexNode project = projects.get(iOutputColumn);
                if (project instanceof RexInputRef) {
                    return mq.getColumnOrigins(CBOUtil.getCteProducer(rel),
                        ((RexInputRef) project).getIndex());
                } else {
                    // Complex expression - collect origins from all input references
                    Set<RelColumnOrigin> result = new LinkedHashSet<>();
                    for (int inputRef : RelOptUtil.InputFinder.bits(project)) {
                        Set<RelColumnOrigin> origins =
                            mq.getColumnOrigins(CBOUtil.getCteProducer(rel), inputRef);
                        if (origins != null) {
                            result.addAll(origins);
                        }
                    }
                    return result;
                }
            }
            return null;
        }
        return mq.getColumnOrigins(CBOUtil.getCteProducer(rel), iOutputColumn);
    }

    public Set<RelColumnOrigin> getColumnOrigins(Window rel,
                                                 RelMetadataQuery mq, int iOutputColumn) {
        if (iOutputColumn < rel.getInput().getRowType().getFieldCount()) {
            return mq.getColumnOrigins(rel.getInput(), iOutputColumn);
        }
        return null;
    }
}