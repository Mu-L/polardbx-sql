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

package com.alibaba.polardbx.optimizer.index;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;

import java.util.Set;

/**
 * @author dylan
 */
public class IndexableColumnRexFinder extends RexVisitorImpl<Void> {

    private RelMetadataQuery mq;

    private RelNode rel;

    // {schema -> table -> columns}
    private IndexableColumnSet indexableColumnSet;

    private IndexAdvisor.AdviseType adviseType;

    public IndexableColumnRexFinder(RelMetadataQuery mq, RelNode rel,
                                    IndexableColumnSet indexableColumnSet, IndexAdvisor.AdviseType adviseType) {
        super(true);
        this.mq = mq;
        this.rel = rel;
        this.indexableColumnSet = indexableColumnSet;
        this.adviseType = adviseType;
    }

    public IndexableColumnSet getIndexableColumnSet() {
        return this.indexableColumnSet;
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
        RelColumnOrigin columnOrigin;
        if (rel instanceof Join) {
            int leftCount = ((Join) rel).getLeft().getRowType().getFieldCount();
            if (inputRef.getIndex() < leftCount) {
                columnOrigin = mq.getColumnOrigin(((Join) rel).getLeft(), inputRef.getIndex());
            } else {
                columnOrigin = mq.getColumnOrigin(((Join) rel).getRight(), inputRef.getIndex() - leftCount);
            }
        } else {
            columnOrigin = mq.getColumnOrigin(rel, inputRef.getIndex());
        }
        this.indexableColumnSet.addIndexableColumn(columnOrigin);
        return null;
    }

    @Override
    public Void visitCall(RexCall call) {
        SqlKind sqlKind = call.getOperator().getKind();
        boolean indexable = false;
        if (adviseType == IndexAdvisor.AdviseType.COLUMNAR_INDEX && sqlKind.belongsTo(SqlKind.INDEXABLE_FOR_CCI)) {
            indexable = true;
        }
        if (adviseType != IndexAdvisor.AdviseType.COLUMNAR_INDEX && sqlKind.belongsTo(SqlKind.INDEXABLE)) {
            indexable = true;
        }
        if (indexable) {
            if (adviseType == IndexAdvisor.AdviseType.COLUMNAR_INDEX
                && rel instanceof Join && call.getKind() == SqlKind.EQUALS && call.getOperands().size() == 2
                && call.operands.get(0) instanceof RexInputRef && call.operands.get(1) instanceof RexInputRef) {
                //不将join col列加入indexColumnSet
                Set<RelColumnOrigin> lastColumnOrigins = null;
                for (RexNode rex : call.operands) {
                    RexInputRef inputRef = (RexInputRef) rex;
                    int leftCount = ((Join) rel).getLeft().getRowType().getFieldCount();
                    Set<RelColumnOrigin> columnOrigins;
                    if (inputRef.getIndex() < leftCount) {
                        columnOrigins = mq.getColumnOrigins(((Join) rel).getLeft(), inputRef.getIndex());
                    } else {
                        columnOrigins = mq.getColumnOrigins(((Join) rel).getRight(), inputRef.getIndex() - leftCount);
                    }
                    for (RelColumnOrigin columnOrigin : columnOrigins) {
                        this.indexableColumnSet.addIndexableColumn(columnOrigin, true);
                    }
                    if (lastColumnOrigins != null) {
                        for (RelColumnOrigin lastColumnOrigin : lastColumnOrigins) {
                            for (RelColumnOrigin columnOrigin : columnOrigins) {
                                this.indexableColumnSet.addJoinColumns(lastColumnOrigin, columnOrigin);
                            }
                        }
                    }
                    lastColumnOrigins = columnOrigins;
                }
                return null;
            } else {
                return super.visitCall(call);
            }
        } else {
            return null;
        }
    }

    @Override
    public Void visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == -2 || dynamicParam.getIndex() == -3) {
            IndexableColumnRelFinder indexableColumnRelFinder =
                new IndexableColumnRelFinder(mq, indexableColumnSet, adviseType);
            indexableColumnRelFinder.go(dynamicParam.getRel());
        }
        return null;
    }
}
