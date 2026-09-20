/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLAlterTableDropConstraint extends SQLObjectImpl implements SQLAlterTableItem {
    private SQLName schemaName;
    private SQLName constraintName;

    protected boolean cascade = false;
    protected boolean restrict = false;

    protected ConstraintType constraintType;

    public boolean isCascade() {
        return cascade;
    }

    public void setCascade(boolean cascade) {
        this.cascade = cascade;
    }

    public boolean isRestrict() {
        return restrict;
    }

    public void setRestrict(boolean restrict) {
        this.restrict = restrict;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, this.constraintName);
        }
        visitor.endVisit(this);
    }

    public String getSchemaName() {
        SQLName name = schemaName;
        if (name == null) {
            return null;
        }

        return name.getSimpleName();
    }

    public void setSchemaName(SQLName schemaName) {
        this.schemaName = schemaName;
    }

    public SQLName getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(SQLName constraintName) {
        this.constraintName = constraintName;
    }

    public ConstraintType getConstraintType() {
        return constraintType;
    }

    public void setConstraintType(ConstraintType constraintType) {
        this.constraintType = constraintType;
    }

    public enum ConstraintType {
        PRIMARY_KEY("PRIMARY KEY"), UNIQUE("UNIQUE"), FOREIGN_KEY("FOREIGN KEY"), CHECK("CHECK");

        public final String name;
        public final String name_lcase;

        ConstraintType(String name) {
            this.name = name;
            this.name_lcase = name.toLowerCase();
        }

        public String getText() {
            return name;
        }

        public static ConstraintType fromString(String text) {
            for (ConstraintType v : ConstraintType.values()) {
                if (v.name.equalsIgnoreCase(text)) {
                    return v;
                }
            }
            return null;
        }
    }

}
