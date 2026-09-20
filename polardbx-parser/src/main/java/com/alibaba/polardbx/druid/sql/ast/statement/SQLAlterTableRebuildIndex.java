/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLAlterTableRebuildIndex extends SQLAlterTableAddIndex {

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (indexDefinition.getName() != null) {
                indexDefinition.getName().accept(visitor);
            }

            for (int i = 0; i < getColumns().size(); i++) {
                final SQLSelectOrderByItem item = getColumns().get(i);
                if (item != null) {
                    item.accept(visitor);
                }
            }
            for (int i = 0; i < getCovering().size(); i++) {
                final SQLName item = getCovering().get(i);
                if (item != null) {
                    item.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

}
