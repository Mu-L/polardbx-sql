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
package org.apache.calcite.rel.metadata;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.List;

/**
 * RelMdColumnOrigins supplies a default implementation of
 * {@link RelMetadataQuery#getColumnOrigins} for the standard logical algebra.
 */
public class RelMdColumnGroupSize
    implements MetadataHandler<BuiltInMetadata.ColumnGroupSize> {
  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(
          BuiltInMethod.COLUMN_GROUP_SIZE.method, new RelMdColumnGroupSize());

  //~ Constructors -----------------------------------------------------------

  protected RelMdColumnGroupSize() {
  }

  //~ Methods ----------------------------------------------------------------

  public MetadataDef<BuiltInMetadata.ColumnGroupSize> getDef() {
    return BuiltInMetadata.ColumnGroupSize.DEF;
  }

  // Catch-all rule when none of the others apply.
  public Integer getColumnsGroupSize(RelNode rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return null;
  }

  public Integer getColumnsGroupSize(Sort rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return mq.getColumnsGroupSize(rel.getInput(), columns);
  }

  public Integer getColumnsGroupSize(Exchange rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    return mq.getColumnsGroupSize(rel.getInput(), columns);
  }

  public Integer getColumnsGroupSize(Aggregate rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    ImmutableBitSet.Builder childColumns = ImmutableBitSet.builder();
    for (int i = columns.nextSetBit(0); i >= 0; i = columns.nextSetBit(i + 1)) {
      if (i >= rel.getGroupCount()) {
        return null;
      }
      childColumns.set(rel.getGroupSet().nth(i));
    }
    return mq.getColumnsGroupSize(rel.getInput(), childColumns.build());
  }

  public Integer getColumnsGroupSize(Join rel, RelMetadataQuery mq, ImmutableBitSet columns) {
    ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
    ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
    int leftCount = rel.getLeft().getRowType().getFieldCount();
    for (int bit : columns) {
      if (bit < leftCount) {
        leftBuilder.set(bit);
      } else {
        rightBuilder.set(bit - leftCount);
      }
    }
    final ImmutableBitSet leftColumns = leftBuilder.build();
    final ImmutableBitSet rightColumns = rightBuilder.build();

    Integer leftGroupSize;
    Integer rightGroupSize;
    if (leftColumns.cardinality() > 0 && rightColumns.cardinality() > 0) {
      leftGroupSize = mq.getColumnsGroupSize(rel.getLeft(), leftColumns);
      rightGroupSize = mq.getColumnsGroupSize(rel.getRight(), rightColumns);
      if (leftGroupSize == null ||rightGroupSize == null ) {
        return null;
      }
      if (rel.getJoinType().generatesNullsOnLeft()) {
        leftGroupSize++;
      }
      if (rel.getJoinType().generatesNullsOnRight()) {
        rightGroupSize++;
      }
      try {
        return Math.multiplyExact(leftGroupSize, rightGroupSize);
      } catch (ArithmeticException e) {
        return Integer.MAX_VALUE;
      }
    } else if (rightColumns.cardinality() > 0) {
      rightGroupSize = mq.getColumnsGroupSize(rel.getRight(), rightColumns);
      if (rightGroupSize != null && rel.getJoinType().generatesNullsOnRight()) {
        rightGroupSize++;
      }
      return rightGroupSize;
    } else if (leftColumns.cardinality() > 0) {
      leftGroupSize = mq.getColumnsGroupSize(rel.getLeft(), leftColumns);
      if (leftGroupSize != null && rel.getJoinType().generatesNullsOnLeft()) {
        leftGroupSize++;
      }
      return leftGroupSize;
    } else {
      return null;
    }
  }

  public Integer getColumnsGroupSize(Project rel, final RelMetadataQuery mq, ImmutableBitSet columns) {
    List<RexNode> projExprs = rel.getProjects();
    ImmutableBitSet.Builder childColumns = ImmutableBitSet.builder();
    for (int bit : columns) {
      RexNode projExpr = projExprs.get(bit);
      if (projExpr instanceof RexInputRef) {
        childColumns.set(((RexInputRef) projExpr).getIndex());
      } else if (projExpr instanceof RexCall) {
        RexCall call = (RexCall) projExpr;
        if (call.getOperator() != SqlStdOperatorTable.CAST) {
          return null;
        }
        RexNode castOperand = call.getOperands().get(0);
        if (!(castOperand instanceof RexInputRef)) {
          return null;
        }
        childColumns.set(((RexInputRef) castOperand).getIndex());
      } else {
        return null;
      }
    }
    return mq.getColumnsGroupSize(rel.getInput(), childColumns.build());
  }
}
