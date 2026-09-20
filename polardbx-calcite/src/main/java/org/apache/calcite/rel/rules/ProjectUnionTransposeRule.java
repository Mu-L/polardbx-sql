/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalHybridUnion;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Union}.
 *
 * <p>This rule applies when Union.all is true, allowing the Project to be
 * pushed down to each input of the Union, which can improve execution
 * efficiency by reducing the amount of data flowing through the Union.
 */
public class ProjectUnionTransposeRule extends RelOptRule {

  /** Rule instance that matches LogicalProject over LogicalUnion. */
  public static final ProjectUnionTransposeRule HYBRID_UNION_INSTANCE =
      new ProjectUnionTransposeRule(LogicalProject.class, LogicalHybridUnion.class,
          RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectUnionTransposeRule.
   */
  public ProjectUnionTransposeRule(
      Class<? extends Project> projectClass,
      Class<? extends Union> unionClass,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(projectClass,
            operand(unionClass, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    final Union union = call.rel(1);
    // Only apply this rule when Union.all is true (UNION ALL)
    return union.all;
  }

  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Union union = call.rel(1);

    for (RexNode rexNode : project.getProjects()) {
      if (RexUtil.hasSubQuery(rexNode)) {
        return;
      }
    }

    // Create a new project for each input of the union
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : union.getInputs()) {
      // Create a copy of the project with the current union input as its input
      RelNode newProject = project.copy(
          project.getTraitSet(),
          input,
          project.getProjects(),
          project.getRowType());
      newInputs.add(newProject);
    }

    // Create a new union with the projected inputs
    RelNode newUnion = union.copy(union.getTraitSet(), newInputs, union.all);
    
    call.transformTo(newUnion);
  }
}

// End ProjectUnionTransposeRule.java