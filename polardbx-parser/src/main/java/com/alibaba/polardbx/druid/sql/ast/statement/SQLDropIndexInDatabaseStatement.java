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

public class SQLDropIndexInDatabaseStatement extends SQLDropIndexStatement {
    private SQLName dbName;
    private boolean isColumnar;
    private boolean in;

    public SQLDropIndexInDatabaseStatement() {
    }

    public SQLDropIndexInDatabaseStatement(SQLName dbName, boolean isColumnar) {
        super();
        this.dbName = dbName;
        this.isColumnar = isColumnar;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, dbName);
        }
        visitor.endVisit(this);
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setDbName(SQLName dbName) {
        this.dbName = dbName;
    }

    public boolean isColumnar() {
        return isColumnar;
    }

    public void setColumnar(boolean columnar) {
        isColumnar = columnar;
    }

    public boolean isIn() {
        return in;
    }

    public void setIn(boolean in) {
        this.in = in;
    }
}
