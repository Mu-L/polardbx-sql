/*
 * Copyright [2013-2021] Alibaba Cloud All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Creates an execution-local copy of an externalized select-then-write input and evaluates its Project constants once.
 *
 * <p>For example, the cached UPDATE input may contain:</p>
 * <pre>{@code
 * PhysicalProject(
 *     body=FETCH_BLOB($body_addr, ...),
 *     new_body='hello',
 *     new_note=CURRENT_TIMESTAMP(6))
 *   -> LogicalView
 * }</pre>
 * {@code FETCH_BLOB} must remain a CN row expression because it consumes {@code $body_addr}. This visitor leaves that
 * expression in place, replaces {@code CURRENT_TIMESTAMP(6)} with one statement literal, and returns a copied
 * PhysicalProject. The cached plan and its LogicalView are not mutated. Projects without {@code FETCH_BLOB} retain the
 * ordinary execution path and are not rewritten by this visitor.
 */
public final class ExternalizedProjectLiteralVisitor extends ReplaceCallWithLiteralVisitor {

    public static RelNode evaluate(RelNode input, ExecutionContext executionContext) {
        final Map<Integer, ParameterContext> params = executionContext.getParams() == null
            ? Collections.emptyMap() : executionContext.getParams().getCurrentParameter();
        return input.accept(new ExternalizedProjectLiteralVisitor(params, RexUtils.getEvalFunc(executionContext)));
    }

    public ExternalizedProjectLiteralVisitor(Map<Integer, ParameterContext> params,
                                             Function<RexNode, Object> calcValueFunc) {
        super(Collections.emptyList(), params, calcValueFunc, true);
    }

    @Override
    public RelNode visit(RelNode other) {
        if (!(other instanceof PhysicalProject)) {
            return visitChildren(other);
        }

        final PhysicalProject project = (PhysicalProject) other;
        final RelNode visitedInput = project.getInput().accept(this);
        if (!containsFetchBlob(project.getProjects())) {
            return visitedInput == project.getInput() ? project
                : project.copy(project.getTraitSet(), visitedInput, project.getProjects(), project.getRowType(),
                project.getOriginalRowType()).setHints(project.getHints());
        }

        final List<RexNode> newProjects = replaceProjectExpressions(project.getProjects());
        if (visitedInput == project.getInput() && newProjects == project.getProjects()) {
            return project;
        }
        return project.copy(project.getTraitSet(), visitedInput, newProjects, project.getRowType(),
            project.getOriginalRowType()).setHints(project.getHints());
    }

    @Override
    public RelNode visit(LogicalProject project) {
        return visitChild(project, 0, project.getInput());
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        return visitChild(filter, 0, filter.getInput());
    }

    @Override
    public RelNode visit(TableScan scan) {
        return scan;
    }

    private static boolean containsFetchBlob(List<RexNode> projects) {
        for (RexNode expression : projects) {
            if (RexUtil.findOperatorCall(TddlOperatorTable.FETCH_BLOB, expression) != null) {
                return true;
            }
        }
        return false;
    }
}
