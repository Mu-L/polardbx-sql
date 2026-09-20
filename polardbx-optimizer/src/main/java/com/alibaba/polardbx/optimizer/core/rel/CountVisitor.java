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

package com.alibaba.polardbx.optimizer.core.rel;

import com.clearspring.analytics.util.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.List;

public class CountVisitor extends RelShuttleImpl {
    List<CountVisitor> subVisitors = Lists.newArrayList();
    private int joinCount = 0;
    private int maxContinuousJoinCount = 0;
    private int outerJoinCount = 0;
    private int semiJoinCount = 0;
    private int sortCount = 0;
    private int limitCount = 0;
    boolean built = false;

    public CountVisitor() {
    }

    @Override
    public RelNode visit(RelNode other) {
        // other type
        if (other instanceof LogicalSemiJoin) {
            return this.visit((LogicalSemiJoin) other);
        } else if (other instanceof MergeSort) {
            if (((MergeSort) other).withLimit()) {
                limitCount++;
            }
            return visitChildren(other);
        } else {
            return visitChildren(other);
        }
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        for (RelNode input : union.getInputs()) {
            CountVisitor countVisitor = new CountVisitor();
            this.subVisitors.add(countVisitor);
            input.accept(countVisitor);
            countVisitor.build();
        }
        return union;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        joinCount++;
        maxContinuousJoinCount++;
        if (join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.RIGHT) {
            outerJoinCount++;
        }
        visitChildren(join);
        return join;
    }

    public RelNode visit(LogicalSemiJoin join) {
        joinCount++;
        maxContinuousJoinCount++;
        semiJoinCount++;
        visitChildren(join);
        return join;
    }

    @Override
    public RelNode visit(LogicalSort logicalSort) {
        if (logicalSort.withLimit()) {
            limitCount++;
        }
        sortCount++;
        visitChildren(logicalSort);
        return logicalSort;
    }

    @Override
    public RelNode visit(LogicalTableLookup tableLookup) {
        // we don't consider TableLookup as join here
        super.visit(tableLookup.getInput());
        return tableLookup;
    }

    @Override
    public RelNode visit(LogicalCTEConsumer cteConsumer) {
        return cteConsumer;
    }

    public void build() {
        for (CountVisitor subVisitor : subVisitors) {
            this.joinCount += subVisitor.getJoinCount();
            this.outerJoinCount += subVisitor.getOuterJoinCount();
            this.semiJoinCount += subVisitor.getSemiJoinCount();
            this.sortCount += subVisitor.getSortCount();
            this.limitCount += subVisitor.getLimitCount();
            this.maxContinuousJoinCount =
                Math.max(this.maxContinuousJoinCount, subVisitor.getMaxContinuousJoinCount());
        }
        this.subVisitors.clear();
        this.built = true;
    }

    private void checkBuild() {
        if (!built) {
            build();
        }
    }

    public int getJoinCount() {
        checkBuild();
        return joinCount;
    }

    public int getOuterJoinCount() {
        checkBuild();
        return outerJoinCount;
    }

    public int getSemiJoinCount() {
        checkBuild();
        return semiJoinCount;
    }

    public int getSortCount() {
        checkBuild();
        return sortCount;
    }

    public int getLimitCount() {
        checkBuild();
        return limitCount;
    }

    public int getMaxContinuousJoinCount() {
        checkBuild();
        return maxContinuousJoinCount;
    }
}
