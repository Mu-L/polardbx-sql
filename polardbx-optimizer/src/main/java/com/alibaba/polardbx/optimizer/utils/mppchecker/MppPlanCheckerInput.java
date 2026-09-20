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

package com.alibaba.polardbx.optimizer.utils.mppchecker;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import org.apache.calcite.rel.RelNode;

public class MppPlanCheckerInput {
    private final RelNode originalPlan;
    private final PlannerContext plannerContext;
    private final ExecutionContext executionContext;
    private final RoutingType routingType;

    public MppPlanCheckerInput(RelNode originalPlan, PlannerContext plannerContext, ExecutionContext executionContext,
                               RoutingType routingType) {
        this.originalPlan = originalPlan;
        this.plannerContext = plannerContext;
        this.executionContext = executionContext;
        this.routingType = routingType;
    }

    public RelNode getOriginalPlan() {
        return originalPlan;
    }

    public PlannerContext getPlannerContext() {
        return plannerContext;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public boolean enableColumnar() {
        return RoutingType.containsColumnar(routingType);
    }

    public String getHintVariable(String param) {
        if (executionContext == null) {
            return null;
        }
        if (!executionContext.isUseHint()) {
            return null;
        }
        Object obj = executionContext.getHintCmds().get(param);
        if (obj == null) {
            return null;
        }
        return String.valueOf(obj);
    }

    public RoutingType getRoutingType() {
        return routingType;
    }
}
