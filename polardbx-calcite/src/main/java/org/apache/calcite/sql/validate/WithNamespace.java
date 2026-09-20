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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.util.Util;

/**
 * Namespace for <code>WITH</code> clause.
 */
public class WithNamespace extends AbstractNamespace {
  //~ Instance fields --------------------------------------------------------

  private final SqlWith with;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a TableConstructorNamespace.
   *
   * @param validator     Validator
   * @param with          WITH clause
   * @param enclosingNode Enclosing node
   */
  WithNamespace(SqlValidatorImpl validator,
      SqlWith with,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.with = with;
  }

  //~ Methods ----------------------------------------------------------------

  protected RelDataType validateImpl(RelDataType targetRowType) {
    for (SqlNode withItem : with.withList) {
        // need to check recursive flag of the entire with clause,
        // but not the whole with clause flag
        boolean isRecursive = isWithItemRecursive((SqlWithItem) withItem);
        validator.validateWithItem((SqlWithItem) withItem, isRecursive);
    }
    final SqlValidatorScope scope2 =
        validator.getWithScope(Util.last(with.withList.getList()));
    validator.validateQuery(with.body, scope2, targetRowType);
    final RelDataType rowType = validator.getValidatedNodeType(with.body);
    validator.setValidatedNodeType(with, rowType);
    return rowType;
  }

    /**
     * check if the single withItem is recursive
     *
     * @param withItem CTE item
     * @return if the single withItem is recursive
     */
    private boolean isWithItemRecursive(SqlWithItem withItem) {
        // if the entire WITH clause is not recursive, all items are not recursive
        if (!with.isRecursive.booleanValue()) {
            return false;
        }

        // if the entire WITH clause is recursive,
        // need to further check if the single item is really recursive
        return hasSelfReference(withItem, withItem.query);
    }

    /**
     * Check whether the specified CTE name is referenced in the query (self-reference detection)
     *
     * @return whether self-reference exists
     */
    public static boolean hasSelfReference(SqlWithItem withItem, SqlNode node) {
        if (node == null) {
            return false;
        }

        if (node instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            // check whether the CTE name is referenced
            if (identifier.names.size() == 1 &&
                identifier.names.get(0).equals(withItem.name.getSimple())) {
                return true;
            }
        } else if (node instanceof SqlCall) {
            SqlCall call = (SqlCall) node;
            // check all operands
            for (SqlNode operand : call.getOperandList()) {
                if (hasSelfReference(withItem, operand)) {
                    return true;
                }
            }
        }

        return false;
    }
  public SqlNode getNode() {
        return with;
    }
}

// End WithNamespace.java